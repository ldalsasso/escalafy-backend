import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime

import asyncpg
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Response dataclasses ──────────────────────────────────────────────────────

@dataclass
class DailyStatsRow:
    date:                 str
    unique_users:         int
    sessions:             int
    page_views:           int
    add_to_carts:         int
    checkouts_started:    int
    checkouts_completed:  int


@dataclass
class EventSummary:
    type:      str
    object_id: str


@dataclass
class SessionSummary:
    session_id: str
    first_seen: str
    last_seen:  str
    events:     list[EventSummary]


@dataclass
class JourneyResponse:
    checkout_id:      str
    store_id:         str
    conversion_date:  str
    sessions:         list[SessionSummary]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/stores/{store_id}/report")
async def store_report(
    request: Request,
    store_id: str,
    date_from: str = Query(..., alias="from"),
    date_to: str = Query(..., alias="to"),
) -> JSONResponse:
    try:
        from_date: date = date.fromisoformat(date_from)
        to_date: date = date.fromisoformat(date_to)
    except ValueError as exc:
        logger.warning("Invalid date params", extra={"error": str(exc)})
        return JSONResponse(status_code=400, content={"error": f"Invalid date format: {exc}"})

    db: asyncpg.Pool = request.app.state.db
    rows = await db.fetch(
        """
        SELECT date, unique_users, sessions, page_views,
               add_to_carts, checkouts_started, checkouts_completed
        FROM daily_store_stats
        WHERE store_id = $1 AND date >= $2 AND date <= $3
        ORDER BY date
        """,
        store_id,
        from_date,
        to_date,
    )

    if not rows:
        logger.info("No report data found", extra={"store_id": store_id, "from": date_from, "to": date_to})
        return JSONResponse(
            status_code=404,
            content={"error": f"No data for store '{store_id}' in range {date_from} – {date_to}"},
        )

    result = [
        asdict(DailyStatsRow(
            date=str(row["date"]),
            unique_users=row["unique_users"],
            sessions=row["sessions"],
            page_views=row["page_views"],
            add_to_carts=row["add_to_carts"],
            checkouts_started=row["checkouts_started"],
            checkouts_completed=row["checkouts_completed"],
        ))
        for row in rows
    ]

    logger.info(
        "Report served",
        extra={"store_id": store_id, "days": str(len(result)), "from": date_from, "to": date_to},
    )
    return JSONResponse(result)


@router.get("/conversions/{checkout_id}/journey")
async def conversion_journey(request: Request, checkout_id: str) -> JSONResponse:
    db: asyncpg.Pool = request.app.state.db

    # Resolve checkout → session → user in one query
    checkout_row = await db.fetchrow(
        """
        SELECT e.store_id, e.timestamp AS conversion_date, s.user_id
        FROM events e
        JOIN sessions s ON s.session_id = e.session_id AND s.store_id = e.store_id
        WHERE e.event_object_id = $1
          AND e.event_type IN ('checkout_start', 'checkout_success')
        ORDER BY e.timestamp DESC
        LIMIT 1
        """,
        checkout_id,
    )

    if checkout_row is None:
        return JSONResponse(status_code=404, content={"error": f"Checkout '{checkout_id}' not found"})

    user_id: int | None = checkout_row["user_id"]
    if user_id is None:
        return JSONResponse(
            status_code=404,
            content={"error": f"No resolved user for checkout '{checkout_id}' — recognition pending"},
        )

    store_id: str = checkout_row["store_id"]
    conversion_date: str = checkout_row["conversion_date"].isoformat()

    # All sessions for the user with their events aggregated — single round-trip
    session_rows = await db.fetch(
        """
        SELECT
            s.session_id,
            s.first_seen,
            s.last_seen,
            json_agg(
                json_build_object('type', e.event_type, 'object_id', e.event_object_id)
                ORDER BY e.timestamp
            ) AS events
        FROM sessions s
        JOIN events e ON e.session_id = s.session_id AND e.store_id = s.store_id
        WHERE s.store_id = $1 AND s.user_id = $2
        GROUP BY s.session_id, s.first_seen, s.last_seen
        ORDER BY s.first_seen
        """,
        store_id,
        user_id,
    )

    sessions: list[SessionSummary] = []
    for row in session_rows:
        raw: list[dict] = json.loads(row["events"]) if row["events"] else []
        sessions.append(SessionSummary(
            session_id=row["session_id"],
            first_seen=row["first_seen"].isoformat(),
            last_seen=row["last_seen"].isoformat(),
            events=[EventSummary(type=e["type"], object_id=e["object_id"]) for e in raw],
        ))

    journey = JourneyResponse(
        checkout_id=checkout_id,
        store_id=store_id,
        conversion_date=conversion_date,
        sessions=sessions,
    )

    logger.info(
        "Journey served",
        extra={"checkout_id": checkout_id, "store_id": store_id, "session_count": str(len(sessions))},
    )
    return JSONResponse(asdict(journey))

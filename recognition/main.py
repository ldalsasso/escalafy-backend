import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://escalafy:escalafy@postgres:5432/escalafy")
RECOGNITION_INTERVAL_SECONDS = int(os.getenv("RECOGNITION_INTERVAL_SECONDS", "60"))

logger = logging.getLogger(__name__)


class _JsonFormatter(logging.Formatter):
    _LOGRECORD_ATTRS = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__)

    def format(self, record: logging.LogRecord) -> str:
        base: dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "component": record.name,
        }
        for key, value in record.__dict__.items():
            if key not in self._LOGRECORD_ATTRS and not key.startswith("_"):
                base[key] = value
        return json.dumps(base)


def _configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


async def resolve_sessions_by_ip(conn: asyncpg.Connection, store_id: str) -> int:
    """
    Lock unresolved sessions (SKIP LOCKED to avoid contention with consumer),
    group them by IP, then assign a user_id to each group — reusing an existing
    user if any session with that IP already has one, creating a new one otherwise.
    Requires >1 session sharing an IP, or at least one resolved session on the IP.
    """
    rows = await conn.fetch(
        """
        WITH locked_unresolved AS (
            SELECT id, user_ip
            FROM sessions
            WHERE store_id = $1 AND user_id IS NULL
            FOR UPDATE SKIP LOCKED
        ),
        ip_existing_user AS (
            SELECT user_ip, MIN(user_id) AS user_id
            FROM sessions
            WHERE store_id = $1 AND user_id IS NOT NULL
            GROUP BY user_ip
        )
        SELECT
            lu.user_ip,
            ARRAY_AGG(lu.id ORDER BY lu.id) AS session_ids,
            MAX(ieu.user_id) AS existing_user_id
        FROM locked_unresolved lu
        LEFT JOIN ip_existing_user ieu ON ieu.user_ip = lu.user_ip
        GROUP BY lu.user_ip
        HAVING COUNT(lu.id) > 1 OR MAX(ieu.user_id) IS NOT NULL
        """,
        store_id,
    )

    assigned = 0
    for row in rows:
        session_ids: list[int] = row["session_ids"]
        existing_user_id: int | None = row["existing_user_id"]

        if existing_user_id is None:
            user_id: int = await conn.fetchval(
                "INSERT INTO users (store_id) VALUES ($1) RETURNING id",
                store_id,
            )
        else:
            user_id = existing_user_id

        result: str = await conn.execute(
            "UPDATE sessions SET user_id = $1 WHERE id = ANY($2::bigint[]) AND user_id IS NULL",
            user_id,
            session_ids,
        )
        assigned += int(result.split()[1])

    return assigned


async def resolve_sessions_by_checkout(conn: asyncpg.Connection, store_id: str) -> int:
    """
    For each checkout_id, collect resolved sessions (user_id IS NOT NULL) and
    unresolved ones (user_id IS NULL).
    - If multiple distinct user_ids share a checkout → merge to the oldest.
    - If resolved + unresolved sessions share a checkout → assign the resolved
      user_id to the unresolved ones.
    Both cases can occur in the same checkout_id.
    """
    rows = await conn.fetch(
        """
        SELECT
            e.event_object_id,
            ARRAY_AGG(DISTINCT s.user_id ORDER BY s.user_id)
                FILTER (WHERE s.user_id IS NOT NULL) AS resolved_user_ids,
            ARRAY_AGG(s.id ORDER BY s.id)
                FILTER (WHERE s.user_id IS NULL)     AS unresolved_session_ids
        FROM events e
        JOIN sessions s ON s.session_id = e.session_id AND s.store_id = e.store_id
        WHERE e.store_id = $1
          AND e.event_type IN ('checkout_start', 'checkout_success')
        GROUP BY e.event_object_id
        HAVING
            COUNT(DISTINCT s.user_id) FILTER (WHERE s.user_id IS NOT NULL) > 1
            OR (
                COUNT(DISTINCT s.user_id) FILTER (WHERE s.user_id IS NOT NULL) > 0
                AND COUNT(*)             FILTER (WHERE s.user_id IS NULL)     > 0
            )
        """,
        store_id,
    )

    merged = 0
    for row in rows:
        resolved_user_ids: list[int] = row["resolved_user_ids"] or []
        unresolved_session_ids: list[int] = row["unresolved_session_ids"] or []

        if not resolved_user_ids:
            continue

        canonical_id = resolved_user_ids[0]  # smallest id = oldest user

        if len(resolved_user_ids) > 1:
            duplicate_ids = resolved_user_ids[1:]
            result: str = await conn.execute(
                """
                UPDATE sessions SET user_id = $1
                WHERE store_id = $2 AND user_id = ANY($3::bigint[])
                """,
                canonical_id,
                store_id,
                duplicate_ids,
            )
            merged += int(result.split()[1])

        if unresolved_session_ids:
            result = await conn.execute(
                "UPDATE sessions SET user_id = $1 WHERE id = ANY($2::bigint[]) AND user_id IS NULL",
                canonical_id,
                unresolved_session_ids,
            )
            merged += int(result.split()[1])

    return merged


async def update_unique_users(conn: asyncpg.Connection, store_id: str) -> None:
    """
    Recompute unique_users for every (store_id, date) row by counting distinct
    user_ids across events joined to their sessions. Only updates rows that
    already exist in daily_store_stats (created by the consumer).
    """
    await conn.execute(
        """
        UPDATE daily_store_stats dss
        SET unique_users = subq.cnt,
            updated_at   = NOW()
        FROM (
            SELECT
                e.timestamp::date        AS date,
                COUNT(DISTINCT s.user_id) AS cnt
            FROM events e
            JOIN sessions s
              ON s.session_id = e.session_id
             AND s.store_id   = e.store_id
            WHERE e.store_id    = $1
              AND s.user_id IS NOT NULL
            GROUP BY e.timestamp::date
        ) subq
        WHERE dss.store_id = $1
          AND dss.date     = subq.date
        """,
        store_id,
    )


async def run_recognition(db_pool: asyncpg.Pool) -> None:
    async with db_pool.acquire() as conn:
        records = await conn.fetch("SELECT DISTINCT store_id FROM sessions")

    for record in records:
        store_id: str = record["store_id"]
        try:
            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    ip_count = await resolve_sessions_by_ip(conn, store_id)
                    checkout_count = await resolve_sessions_by_checkout(conn, store_id)
                    await update_unique_users(conn, store_id)

            logger.info(
                "Store recognition complete",
                extra={
                    "store_id": store_id,
                    "ip_resolved": str(ip_count),
                    "checkout_merged": str(checkout_count),
                },
            )
        except asyncpg.PostgresError as exc:
            logger.error(
                "Recognition failed for store",
                extra={"store_id": store_id, "error": str(exc)},
            )


async def main() -> None:
    _configure_logging()

    db_pool: asyncpg.Pool = await asyncpg.create_pool(DATABASE_URL)
    logger.info("Recognition worker started", extra={"interval_s": str(RECOGNITION_INTERVAL_SECONDS)})

    try:
        while True:
            start = datetime.now(timezone.utc)
            await run_recognition(db_pool)
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()

            sleep_time = max(0.0, RECOGNITION_INTERVAL_SECONDS - elapsed)
            logger.info(
                "Recognition cycle complete",
                extra={"elapsed_s": f"{elapsed:.2f}", "next_in_s": f"{sleep_time:.2f}"},
            )
            await asyncio.sleep(sleep_time)
    finally:
        await db_pool.close()
        logger.info("Recognition worker stopped")


if __name__ == "__main__":
    asyncio.run(main())

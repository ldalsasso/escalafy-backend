import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import asyncpg
import redis.asyncio as aioredis

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://escalafy:escalafy@postgres:5432/escalafy")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
BLOCK_MS = int(os.getenv("BLOCK_MS", "2000"))

STREAM_NAME = "events_stream"
LAST_ID_KEY = "consumer:last_id"

# col values come from this dict only — never from user input, safe to interpolate
EVENT_TYPE_TO_COLUMN: dict[str, str] = {
    "page_view":        "page_views",
    "add_to_cart":      "add_to_carts",
    "checkout_start":   "checkouts_started",
    "checkout_success": "checkouts_completed",
}

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


async def insert_event(
    conn: asyncpg.Connection, event: dict[str, str], timestamp: datetime, received_at: datetime
) -> None:
    await conn.execute(
        """
        INSERT INTO events
            (store_id, event_type, session_id, timestamp, user_ip, event_object_id, received_at)
        VALUES
            ($1, $2, $3, $4, $5::inet, $6, $7)
        ON CONFLICT DO NOTHING
        """,
        event["store_id"],
        event["event_type"],
        event["session_id"],
        timestamp,
        event["user_ip"],
        event["event_object_id"],
        received_at,
    )


async def upsert_session(
    conn: asyncpg.Connection, event: dict[str, str], timestamp: datetime
) -> bool:
    row = await conn.fetchrow(
        """
        INSERT INTO sessions (session_id, store_id, user_ip, first_seen, last_seen)
        VALUES ($1, $2, $3::inet, $4, $4)
        ON CONFLICT (session_id, store_id) DO UPDATE
            SET last_seen = GREATEST(sessions.last_seen, EXCLUDED.last_seen)
        RETURNING (xmax = 0) AS is_new
        """,
        event["session_id"],
        event["store_id"],
        event["user_ip"],
        timestamp,
    )
    return bool(row["is_new"])


async def update_daily_stats(
    conn: asyncpg.Connection, event: dict[str, str], timestamp: datetime, is_new_session: bool
) -> None:
    col = EVENT_TYPE_TO_COLUMN.get(event["event_type"])
    if col is None:
        return

    session_clause = ", sessions = daily_store_stats.sessions + 1" if is_new_session else ""

    await conn.execute(
        f"""
        INSERT INTO daily_store_stats (store_id, date, {col})
        VALUES ($1, $2, 1)
        ON CONFLICT (store_id, date) DO UPDATE SET
            {col} = daily_store_stats.{col} + 1{session_clause},
            updated_at = NOW()
        """,
        event["store_id"],
        timestamp.date(),
    )


async def process_batch(
    messages: list[tuple[str, dict[str, str]]],
    db_pool: asyncpg.Pool,
) -> tuple[int, int]:
    success = 0
    errors = 0

    for message_id, fields in messages:
        try:
            timestamp = datetime.fromisoformat(fields["timestamp"])
            received_at = datetime.fromisoformat(fields["received_at"])
            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    await insert_event(conn, fields, timestamp, received_at)
                    is_new_session = await upsert_session(conn, fields, timestamp)
                    await update_daily_stats(conn, fields, timestamp, is_new_session)
            success += 1
        except (asyncpg.PostgresError, KeyError, ValueError) as exc:
            errors += 1
            logger.error(
                "Failed to process event",
                extra={"message_id": message_id, "error": str(exc)},
            )

    return success, errors


async def main() -> None:
    _configure_logging()

    redis_client: aioredis.Redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    db_pool: asyncpg.Pool = await asyncpg.create_pool(DATABASE_URL)

    last_id: str = await redis_client.get(LAST_ID_KEY) or "0"
    logger.info("Consumer started", extra={"last_id": last_id})

    try:
        while True:
            response = await redis_client.xread(
                {STREAM_NAME: last_id},
                count=BATCH_SIZE,
                block=BLOCK_MS,
            )

            if not response:
                continue

            # response format: [[stream_name, [(id, fields), ...]], ...]
            messages: list[tuple[str, dict[str, str]]] = response[0][1]
            if not messages:
                continue

            success, errors = await process_batch(messages, db_pool)

            # Advance cursor after the full batch — poison pills are skipped, not retried.
            # ON CONFLICT DO NOTHING / DO UPDATE makes reprocessing idempotent on restart.
            last_id = messages[-1][0]
            await redis_client.set(LAST_ID_KEY, last_id)

            logger.info(
                "Batch processed",
                extra={"success": str(success), "errors": str(errors), "last_id": last_id},
            )
    finally:
        await redis_client.aclose()
        await db_pool.close()
        logger.info("Consumer stopped")


if __name__ == "__main__":
    asyncio.run(main())

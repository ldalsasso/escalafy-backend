import logging

import asyncpg
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from event_queue import EventQueue

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/health")
async def health(request: Request) -> JSONResponse:
    queue: EventQueue = request.app.state.queue
    db: asyncpg.Pool = request.app.state.db

    errors: dict[str, str] = {}

    try:
        await queue._client.ping()  # type: ignore[union-attr]
    except Exception as exc:
        errors["redis"] = str(exc)
        logger.error("Redis ping failed", extra={"error": str(exc)})

    try:
        async with db.acquire() as conn:
            await conn.fetchval("SELECT 1")
    except Exception as exc:
        errors["postgres"] = str(exc)
        logger.error("Postgres ping failed", extra={"error": str(exc)})

    if errors:
        return JSONResponse(status_code=503, content={"status": "error", **errors})

    return JSONResponse({"status": "ok", "redis": "ok", "postgres": "ok"})

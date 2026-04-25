import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncGenerator

import asyncpg
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from event_queue import EventQueue
from routes import events, health, reporting

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://escalafy:escalafy@postgres:5432/escalafy")


# Snapshot of standard LogRecord instance attributes to exclude from extras
_LOGRECORD_ATTRS = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__)


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base: dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "component": record.name,
        }
        for key, value in record.__dict__.items():
            if key not in _LOGRECORD_ATTRS and not key.startswith("_"):
                base[key] = value
        return json.dumps(base)


def _configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger = logging.getLogger(__name__)

    queue = EventQueue()
    await queue.connect(REDIS_URL)
    app.state.queue = queue

    db: asyncpg.Pool = await asyncpg.create_pool(DATABASE_URL)
    app.state.db = db
    logger.info("Startup complete")

    yield

    await queue.close()
    await db.close()
    logger.info("Shutdown complete")


_configure_logging()

app = FastAPI(title="Escalafy Analytics API", lifespan=lifespan)

app.include_router(health.router)
app.include_router(events.router)
app.include_router(reporting.router)

import asyncio
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from event_queue import EventQueue
from models import IngestedEvent
from validators import validate_batch, validate_event

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/events")
async def ingest_event(request: Request) -> JSONResponse:
    body: dict = await request.json()
    queue: EventQueue = request.app.state.queue

    try:
        event = validate_event(body)
    except ValueError as exc:
        logger.warning("Event validation failed", extra={"error": str(exc)})
        return JSONResponse(status_code=400, content={"error": str(exc)})

    ingested = IngestedEvent(**vars(event))
    await queue.publish(ingested)
    logger.info("Event accepted", extra={"store_id": event.store_id, "session_id": event.session_id})

    return JSONResponse({"accepted": True}, status_code=202)


@router.post("/events/batch")
async def ingest_batch(request: Request) -> JSONResponse:
    body: list = await request.json()
    queue: EventQueue = request.app.state.queue

    try:
        events = validate_batch(body)
    except ValueError as exc:
        logger.warning("Batch validation failed", extra={"error": str(exc)})
        return JSONResponse(status_code=400, content={"error": str(exc)})

    await asyncio.gather(
        *[queue.publish(IngestedEvent(**vars(e))) for e in events]
    )

    count = len(events)
    logger.info("Batch accepted", extra={"count": str(count)})

    return JSONResponse({"accepted": count}, status_code=202)

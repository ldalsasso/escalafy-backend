import logging

import redis.asyncio as aioredis

from models import EventType, IngestedEvent

STREAM_NAME = "events_stream"

logger = logging.getLogger(__name__)


class EventQueue:
    def __init__(self) -> None:
        self._client: aioredis.Redis | None = None
        self._last_id: str = "0"

    async def connect(self, redis_url: str) -> None:
        self._client = aioredis.from_url(redis_url, decode_responses=True)
        await self._client.ping()
        logger.info("Connected to Redis", extra={"url": redis_url})

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
            logger.info("Redis connection closed")

    async def publish(self, event: IngestedEvent) -> None:
        if self._client is None:
            raise RuntimeError("EventQueue.connect() must be called before publish()")
        fields: dict[str, str] = {
            "store_id":        event.store_id,
            "event_type":      event.event_type.value,
            "session_id":      event.session_id,
            "timestamp":       event.timestamp.isoformat(),
            "user_ip":         event.user_ip,
            "event_object_id": event.event_object_id,
            "received_at":     event.received_at.isoformat(),
        }
        message_id: str = await self._client.xadd(STREAM_NAME, fields)
        logger.debug("Event published", extra={"stream": STREAM_NAME, "message_id": message_id})

    async def read_batch(self, batch_size: int, block_ms: int) -> list[IngestedEvent]:
        if self._client is None:
            raise RuntimeError("EventQueue.connect() must be called before read_batch()")

        # XREAD returns: [[stream_name, [(id, {field: value}), ...]], ...] or None on timeout
        response: list | None = await self._client.xread(
            {STREAM_NAME: self._last_id},
            count=batch_size,
            block=block_ms,
        )

        if not response:
            return []

        messages: list[tuple[str, dict[str, str]]] = response[0][1]
        events: list[IngestedEvent] = []

        for message_id, fields in messages:
            try:
                event = IngestedEvent(
                    store_id=fields["store_id"],
                    event_type=EventType(fields["event_type"]),
                    session_id=fields["session_id"],
                    timestamp=fields["timestamp"],
                    user_ip=fields["user_ip"],
                    event_object_id=fields["event_object_id"],
                    received_at=fields["received_at"],
                )
                events.append(event)
            except (KeyError, ValueError) as exc:
                logger.error(
                    "Failed to deserialize message",
                    extra={"message_id": message_id, "error": str(exc)},
                )
                continue

            self._last_id = message_id

        logger.debug(
            "Read batch from stream",
            extra={"stream": STREAM_NAME, "count": str(len(events)), "last_id": self._last_id},
        )
        return events

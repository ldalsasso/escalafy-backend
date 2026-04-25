import json
import logging
from datetime import datetime, timezone

from models import Event, EventType

BATCH_MAX = 100

_REQUIRED_FIELDS = ("store_id", "event_type", "session_id", "timestamp", "user_ip", "event_object_id")
_NON_EMPTY_FIELDS = ("store_id", "session_id", "event_object_id")

logger = logging.getLogger(__name__)


def _log_validation_error(message: str) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": "WARNING",
        "message": message,
        "component": "validators",
    }
    logger.warning(json.dumps(record))


def validate_event(data: dict) -> Event:
    missing = [f for f in _REQUIRED_FIELDS if f not in data]
    if missing:
        msg = f"Missing required fields: {missing}"
        _log_validation_error(msg)
        raise ValueError(msg)

    empty = [f for f in _NON_EMPTY_FIELDS if not str(data[f]).strip()]
    if empty:
        msg = f"Fields must not be empty: {empty}"
        _log_validation_error(msg)
        raise ValueError(msg)

    try:
        event_type = EventType(data["event_type"])
    except ValueError:
        valid = [e.value for e in EventType]
        msg = f"Invalid event_type '{data['event_type']}'. Must be one of: {valid}"
        _log_validation_error(msg)
        raise ValueError(msg)

    try:
        timestamp = datetime.fromisoformat(data["timestamp"])
    except (ValueError, TypeError) as exc:
        msg = f"Invalid timestamp '{data['timestamp']}': not a valid ISO 8601 string"
        _log_validation_error(msg)
        raise ValueError(msg) from exc

    return Event(
        store_id=str(data["store_id"]),
        event_type=event_type,
        session_id=str(data["session_id"]),
        timestamp=timestamp,
        user_ip=str(data["user_ip"]),
        event_object_id=str(data["event_object_id"]),
    )


def validate_batch(data: list) -> list[Event]:
    if len(data) > BATCH_MAX:
        msg = f"Batch exceeds maximum size of {BATCH_MAX} items (got {len(data)})"
        _log_validation_error(msg)
        raise ValueError(msg)

    valid: list[Event] = []
    for index, item in enumerate(data):
        try:
            valid.append(validate_event(item))
        except ValueError as exc:
            _log_validation_error(f"Skipping item at index {index}: {exc}")

    return valid

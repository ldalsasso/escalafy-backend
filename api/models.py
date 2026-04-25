from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum


class EventType(str, Enum):
    page_view           = "page_view"
    add_to_cart         = "add_to_cart"
    checkout_start      = "checkout_start"
    checkout_success    = "checkout_success"


@dataclass
class Event:
    store_id:        str
    event_type:      EventType
    session_id:      str
    timestamp:       datetime
    user_ip:         str
    event_object_id: str


@dataclass
class IngestedEvent(Event):
    received_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class Session:
    session_id: str
    store_id:   str
    user_ip:    str
    first_seen: datetime
    last_seen:  datetime
    user_id:    int | None = None


@dataclass
class User:
    id:         int
    store_id:   str
    created_at: datetime


@dataclass
class DailyStoreStats:
    store_id:             str
    date:                 date
    unique_users:         int
    sessions:             int
    page_views:           int
    add_to_carts:         int
    checkouts_started:    int
    checkouts_completed:  int
    updated_at:           datetime

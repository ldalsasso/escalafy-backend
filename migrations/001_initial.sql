-- ============================================================
-- users
-- Inferred identities built by the recognition worker.
-- A user is created when multiple sessions from the same store
-- share the same IP and are judged to belong to one person.
-- Intentionally thin — enrichment happens in the application layer.
-- ============================================================
CREATE TABLE users (
    id         BIGSERIAL    PRIMARY KEY,
    store_id   VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);


-- ============================================================
-- events
-- Raw append-only log of every event received from the API.
-- Never updated or deleted — source of truth for all derived data.
-- The unique index enforces idempotency: the same event can be
-- sent more than once (network retries, client bugs) without
-- producing duplicate rows, since all five fields together
-- identify one discrete user action at a point in time.
-- ============================================================
CREATE TABLE events (
    id               BIGSERIAL    PRIMARY KEY,
    store_id         VARCHAR(255) NOT NULL,
    event_type       VARCHAR(50)  NOT NULL,
    session_id       VARCHAR(255) NOT NULL,
    timestamp        TIMESTAMPTZ  NOT NULL,
    user_ip          INET         NOT NULL,
    event_object_id  VARCHAR(255) NOT NULL,
    received_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uix_events_dedup
    ON events (store_id, session_id, event_type, timestamp, event_object_id);

CREATE INDEX idx_events_store_timestamp ON events (store_id, timestamp);
CREATE INDEX idx_events_session_id      ON events (session_id);


-- ============================================================
-- sessions
-- One row per (session_id, store_id) pair, created by the consumer
-- worker when it first sees a session and kept up-to-date as new
-- events arrive. user_id starts NULL and is filled in by the
-- recognition worker once it resolves the identity behind the IP.
-- user_ip is stored here so the recognition worker can group
-- sessions by IP without having to join back to events.
-- ============================================================
CREATE TABLE sessions (
    id          BIGSERIAL    PRIMARY KEY,
    session_id  VARCHAR(255) NOT NULL,
    store_id    VARCHAR(255) NOT NULL,
    user_id     BIGINT       NULL REFERENCES users (id),
    user_ip     INET         NOT NULL,
    first_seen  TIMESTAMPTZ  NOT NULL,
    last_seen   TIMESTAMPTZ  NOT NULL,
    CONSTRAINT uq_sessions_session_store UNIQUE (session_id, store_id)
);

CREATE INDEX idx_sessions_store_id ON sessions (store_id);
CREATE INDEX idx_sessions_user_id  ON sessions (user_id);
CREATE INDEX idx_sessions_user_ip  ON sessions (user_ip);


-- ============================================================
-- daily_store_stats
-- Pre-aggregated metrics per store per day, maintained by the
-- consumer worker via INSERT ... ON CONFLICT DO UPDATE.
-- Exists purely for read performance: reporting queries hit this
-- table instead of aggregating over the full events log.
-- ============================================================
CREATE TABLE daily_store_stats (
    store_id              VARCHAR(255) NOT NULL,
    date                  DATE         NOT NULL,
    unique_users          INTEGER      NOT NULL DEFAULT 0,
    sessions              INTEGER      NOT NULL DEFAULT 0,
    page_views            INTEGER      NOT NULL DEFAULT 0,
    add_to_carts          INTEGER      NOT NULL DEFAULT 0,
    checkouts_started     INTEGER      NOT NULL DEFAULT 0,
    checkouts_completed   INTEGER      NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_id, date)
);

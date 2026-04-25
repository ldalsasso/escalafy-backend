# escalafy-backend

Event analytics pipeline for e-commerce. Ingests raw behavioral events, resolves user identities across sessions, and serves pre-aggregated reporting.

---

## Architecture Overview

```
                        ┌─────────────────────────────────────────────────┐
                        │                  FastAPI (api)                  │
                        │  POST /events          POST /events/batch       │
                        │  GET /stores/{id}/report                        │
                        │  GET /conversions/{id}/journey                  │
                        └──────────────┬──────────────────────────────────┘
                                       │ XADD
                                       ▼
                              ┌─────────────────┐
                              │  Redis Streams  │
                              │  events_stream  │
                              └────────┬────────┘
                                       │ XREAD (consumer:last_id)
                                       ▼
                        ┌─────────────────────────────┐
                        │      Consumer Worker        │
                        │  INSERT events              │
                        │  UPSERT sessions            │
                        │  UPDATE daily_store_stats   │
                        └──────────────┬──────────────┘
                                       │
                                       ▼
                              ┌─────────────────┐
                              │   PostgreSQL    │
                              │  events         │◄──────────────────────┐
                              │  sessions       │                       │
                              │  users          │          ┌────────────┴───────────┐
                              │  daily_store_   │          │   Recognition Worker   │
                              │    stats        │◄─────────│  resolve by IP         │
                              └─────────────────┘          │  resolve by checkout   │
                                                           │  update unique_users   │
                                                           └────────────────────────┘
                                                           (runs every 60s)
```

**API** — validates and publishes events to Redis Streams, serves reporting endpoints directly from pre-aggregated tables.

**Consumer Worker** — reads batches from Redis Streams, writes raw events to Postgres, upserts sessions, and increments daily stats counters.

**Recognition Worker** — runs periodically, links sessions to user identities by grouping on shared IP and shared checkout IDs, then recomputes `unique_users` in `daily_store_stats`.

**PostgreSQL** — source of truth. Four tables: `events` (append-only raw log), `sessions` (one row per session, updated as events arrive), `users` (inferred identities), `daily_store_stats` (pre-aggregated for read performance).

---

## How to Run Locally

```bash
git clone <repo-url>
cd escalafy-backend
docker compose up --build
```

All services wait for Postgres and Redis healthchecks before starting. Migrations run automatically on first boot via `docker-entrypoint-initdb.d`.

Verify everything is up:

```bash
curl http://localhost:8000/health
# {"status":"ok","redis":"ok","postgres":"ok"}
```

Send a test event:

```bash
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{
    "store_id": "store_123",
    "event_type": "page_view",
    "session_id": "sess_abc",
    "timestamp": "2024-01-15T10:00:00Z",
    "user_ip": "10.0.0.1",
    "event_object_id": "/products"
  }'
# {"accepted":true}
```

Query the report after a few seconds (consumer needs to process the event):

```bash
curl "http://localhost:8000/stores/store_123/report?from=2024-01-15&to=2024-01-15"
```

---

## Component Interaction

| Component | Writes | Reads |
|---|---|---|
| **API** | `events_stream` (Redis) | `daily_store_stats`, `events`, `sessions` (for reporting) |
| **Consumer** | `events`, `sessions`, `daily_store_stats` (Postgres) · `consumer:last_id` (Redis) | `events_stream` (Redis) |
| **Recognition** | `sessions.user_id`, `users`, `daily_store_stats.unique_users` (Postgres) | `sessions`, `events` (Postgres) |

The API and Consumer never write to the same Postgres tables. The Consumer and Recognition Worker both write to `sessions`, but on different columns (`last_seen` vs `user_id`), avoiding conflict. Recognition uses `SELECT FOR UPDATE SKIP LOCKED` to avoid contention with the Consumer on session rows.

---

## Queue Choice — Redis Streams

Redis Streams was chosen over RabbitMQ and Kafka for this workload:

- **Already in the stack.** Redis is typically present in production Django/FastAPI deployments for caching and task queues. No additional infrastructure to operate.
- **Native consumer groups.** `XREADGROUP` supports horizontal scaling with multiple consumer instances processing disjoint message sets — no duplicate processing.
- **Configurable persistence.** Stream length can be capped with `MAXLEN` or retained indefinitely. Survives Redis restarts with AOF/RDB.
- **Right-sized for the load.** At ~2M events/day (~23 events/sec average), Redis Streams has ample headroom. Kafka's operational overhead is only justified at hundreds of thousands of events per second.

**Trade-off:** Kafka would be the right choice if throughput reached tens of millions of events per day, or if the team needed strong ordering guarantees across partitions, schema registry, or multi-datacenter replication. For this scale, that complexity would be waste.

**Current limitation:** The consumer uses a single `XREAD` cursor stored in `consumer:last_id`. Two consumer instances would both read from the same position and process duplicate events. Upgrading to `XREADGROUP` would fix this — see *One Thing I'd Change* below.

---

## At-Least-Once Delivery

The consumer guarantees every event is written to Postgres at least once:

1. **Cursor persistence.** After processing each batch, the consumer stores the last Redis Stream message ID in `consumer:last_id`. On restart, it resumes from that position — not from `0`.
2. **Idempotent writes.** If the process crashes mid-batch and reprocesses the same messages on restart, the database operations are safe:
   - `events` — `INSERT ... ON CONFLICT DO NOTHING` on the deduplication index `(store_id, session_id, event_type, timestamp, event_object_id)`.
   - `sessions` — `INSERT ... ON CONFLICT DO UPDATE SET last_seen = GREATEST(...)`.
   - `daily_store_stats` — `INSERT ... ON CONFLICT DO UPDATE SET counter = counter + 1`. Reprocessing will double-count. This is an accepted trade-off for the current design; a two-phase commit or idempotency key per event would fix it at the cost of complexity.
3. **Poison pills.** Events that fail processing (bad data, transient DB error) are logged and skipped. The cursor advances past them so one bad event cannot stall the pipeline indefinitely.

---

## Read Performance

Reporting queries read from `daily_store_stats`, never from the `events` table directly.

**Without pre-aggregation:** a report for a 90-day period over a store with 2M events would require a full scan and aggregation of millions of rows on every request — O(events).

**With pre-aggregation:** the same report reads at most 90 rows from `daily_store_stats` — O(days). The Consumer maintains this table in real time via upsert as events arrive.

The `/conversions/{checkout_id}/journey` endpoint avoids N+1 queries by using a single `json_agg` query that returns all sessions and their events in one round-trip to Postgres:

```sql
SELECT s.session_id, s.first_seen, s.last_seen,
       json_agg(json_build_object(...) ORDER BY e.timestamp) AS events
FROM sessions s
JOIN events e ON e.session_id = s.session_id AND e.store_id = s.store_id
WHERE s.store_id = $1 AND s.user_id = $2
GROUP BY s.session_id, s.first_seen, s.last_seen
ORDER BY s.first_seen
```

---

## Load Test Results

Tested against `POST /events` and `POST /events/batch` (30% batch, 70% single) for 60 seconds:

```
Duration:         60s
Total requests:   1289
Success rate:     100%
Actual RPS:       21.48 (peaks up to 42 RPS)

Response latencies (ms):
  p50:            2.91
  p95:            3.68
  p99:            4.18
```

All latency is dominated by the Redis `XADD` call (~1–2ms on localhost). The API itself adds no blocking I/O — validation is synchronous CPU work, and the response returns as soon as the message is enqueued.

---

## One Thing I'd Change With More Time

**Migrate the consumer from `XREAD` to `XREADGROUP`.**

The current design stores a single cursor in `consumer:last_id`. If you run two consumer instances, both read from the same position and process every event twice. The `ON CONFLICT DO NOTHING` on `events` prevents duplicate rows, but `daily_store_stats` counters would be double-incremented.

Redis Streams has native support for this via consumer groups: `XREADGROUP` assigns each message to exactly one consumer in a group, and `XACK` marks it as processed. Unacknowledged messages are redelivered to other consumers after a configurable timeout, providing both horizontal scaling and fault tolerance without application-level deduplication logic.

The change is isolated to `consumer/main.py` — the API, Recognition Worker, and schema are unaffected.

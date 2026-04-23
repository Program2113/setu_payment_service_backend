# Setu Payment Event Service

A production-minded payment lifecycle ingestion and reconciliation service built with **FastAPI**, **SQLAlchemy (async)**, and **PostgreSQL**.

---

## Architecture Overview

```
POST /events
     │
     ▼
┌─────────────────────────────────────────┐
│  Upsert Merchant  (ON CONFLICT DO NOTHING) │  ← FK parent created first
│  Upsert Transaction (ON CONFLICT DO NOTHING) │  ← FK parent before Event
│  Chronology Lock  (UPDATE WHERE ts > latest) │  ← out-of-order safety
│  Insert Event PK  (IntegrityError = dup)  │  ← idempotency gate
└─────────────────────────────────────────┘
     │
     ▼
PostgreSQL
  merchants        ← merchant master data
  transactions     ← materialised current state per transaction
  events           ← immutable append-only event log
```

**Key design decisions:**

- **Idempotency** is enforced at the DB level via the `event_id` primary key. Re-submitting any event returns `{"status": "ignored"}` without touching state.
- **Out-of-order events** are handled by the Chronology Lock: a transaction's `current_status` only advances if the incoming event's timestamp is strictly later than the stored one.
- **Race conditions** on concurrent first-event requests are eliminated by using `INSERT ... ON CONFLICT DO NOTHING` for both merchants and transactions instead of SELECT-then-INSERT.
- **FK ordering** — Merchant is created before Transaction, Transaction before Event. This is the correct dependency order to satisfy FK constraints.
- **Indexes** are placed on all filter/sort columns (`merchant_id`, `current_status`, `created_at`, `latest_event_timestamp`) plus two composite indexes for the most common combined filter patterns.
- **Reconciliation dates** use `latest_event_timestamp` (when the payment event occurred) rather than `created_at` (when the row was ingested), so late-arriving events land in the correct reporting date bucket.

---

## Local Setup

### Prerequisites
- Docker and Docker Compose

### Run

```bash
docker compose up --build
```

The service starts at `http://localhost:8000`. Postgres is ready before the app starts (healthcheck enforced).

### Seed ~10,000 test events

```bash
pip install httpx
python seed.py
```

This generates events across 5 merchants with a realistic mix of:
- Successful flows (initiated → processed → settled)
- Failed flows (initiated → failed)
- Stuck/pending (initiated only, older than 1 hour)
- Discrepancies (settled without processing, settled after failure)
- Out-of-order delivery
- Duplicate events (50 re-submissions — all ignored gracefully)

---

## API Documentation

Interactive docs are available at `http://localhost:8000/docs` (Swagger UI).

### POST /events

Ingest a payment lifecycle event.

**Request body:**
```json
{
  "event_id": "b768e3a7-9eb3-4603-b21c-a54cc95661bc",
  "event_type": "payment_initiated",
  "transaction_id": "2f86e94c-239c-4302-9874-75f28e3474ee",
  "merchant_id": "merchant_2",
  "merchant_name": "FreshBasket",
  "amount": 15248.29,
  "currency": "INR",
  "timestamp": "2026-01-08T12:11:58.085567+00:00"
}
```

Valid `event_type` values: `payment_initiated`, `payment_processed`, `payment_failed`, `settled`.

**Responses:**
- `201` — `{"status": "success", "detail": "event processed"}`
- `201` — `{"status": "ignored", "detail": "duplicate event"}` (idempotent re-submission)
- `422` — validation error (invalid event_type, missing fields, etc.)

---

### GET /transactions

List transactions with optional filters, sorting, and pagination.

**Query params:**

| Param | Type | Description |
|---|---|---|
| `merchant_id` | string | Filter by merchant |
| `status` | string | Filter by current status |
| `start_date` | ISO 8601 datetime | Filter `created_at >= start_date` |
| `end_date` | ISO 8601 datetime | Filter `created_at <= end_date` |
| `sort_by` | `created_at` \| `latest_event_timestamp` \| `amount` | Default: `created_at` |
| `sort_dir` | `asc` \| `desc` | Default: `desc` |
| `limit` | int (1–100) | Default: 50 |
| `offset` | int | Default: 0 |

**Example:**
```
GET /transactions?merchant_id=merchant_2&status=settled&sort_by=amount&sort_dir=desc&limit=20
```

---

### GET /transactions/{transaction_id}

Fetch full details for a single transaction.

**Response includes:**
- Transaction fields (id, amount, currency, status, timestamps)
- Merchant info (`merchant_id`, `merchant_name`)
- Full event history ordered by timestamp

**Responses:**
- `200` — transaction object
- `404` — `{"detail": "Transaction not found"}`

---

### GET /reconciliation/summary

Returns transaction counts and amounts grouped by merchant, date (from event timestamp), and status.

**Example response:**
```json
[
  {
    "merchant_id": "merchant_1",
    "date": "2026-01-08",
    "status": "settled",
    "count": 142,
    "total_amount": 1823456.50
  }
]
```

---

### GET /reconciliation/discrepancies

Returns transactions with logical inconsistencies, labelled by type.

| `discrepancy_type` | Meaning |
|---|---|
| `settled_without_processing` | `current_status=settled` but no `payment_processed` event exists |
| `settled_after_failure` | `current_status=settled` but a `payment_failed` event exists |
| `stuck_initiated` | `current_status=payment_initiated` with no follow-up event, older than 1 hour |

**Example response:**
```json
[
  {
    "transaction_id": "2f86e94c-...",
    "merchant_id": "merchant_3",
    "current_status": "settled",
    "amount": 9500.00,
    "currency": "INR",
    "latest_event_timestamp": "2026-01-08T14:00:00+00:00",
    "discrepancy_type": "settled_after_failure"
  }
]
```

---

## Deployment

Deployed on **Render** at: `https://<your-app>.onrender.com`

To seed the deployed instance:
```bash
BASE_URL=https://<your-app>.onrender.com python seed.py
```

---

## Assumptions and Tradeoffs

**Amount immutability** — `amount` and `currency` are accepted on every event payload (to match the spec's sample format) but only stored on first creation. If a later event carries a different amount, it is silently ignored. A stricter system would reject mismatches; this was simplified given the assignment scope.

**Enum enforcement** — `event_type` is validated against the four known lifecycle values at both the Pydantic and SQLAlchemy layers. Unknown event types are rejected with a `422`.

**Reconciliation date** — Grouped by `DATE(latest_event_timestamp)` not `DATE(created_at)`. This correctly places a late-arriving event in the date the payment actually occurred, not the day it was ingested.

**Discrepancy threshold for stuck transactions** — Set to 1 hour. In a production system this would be configurable per merchant SLA.

**No authentication** — Out of scope for this assignment. A production deployment would require API key or JWT auth on all endpoints.

**Table creation on startup** — `Base.metadata.create_all` runs on every startup. For a production system, Alembic migrations would replace this.

**What I'd do with more time:**
- Pytest integration test suite with a test DB fixture
- Rate limiting on POST /events
- Async background worker for settlement reconciliation jobs and decouple the API service from heavy computation


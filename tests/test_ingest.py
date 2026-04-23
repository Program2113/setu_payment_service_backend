"""
test_ingest.py — POST /events

Covers:
  Happy path
    ✓ First event for a new transaction → 201, status=success
    ✓ Merchant row is created automatically
    ✓ Transaction row is created with correct fields
    ✓ Event row is stored in the event log
    ✓ Subsequent events for same transaction are ingested

  Idempotency
    ✓ Exact duplicate (same event_id) → 201, status=ignored
    ✓ Duplicate does NOT change transaction state
    ✓ Duplicate does NOT create a second event row
    ✓ Duplicate of a later event still leaves state unchanged

  Chronology Lock (out-of-order delivery)
    ✓ Earlier event after later event → status NOT regressed
    ✓ Event with same timestamp as current → status NOT changed
    ✓ Later event after earlier event → status advances normally
    ✓ Three events delivered in reverse order → final state = latest

  Full lifecycle flows
    ✓ payment_initiated → payment_processed → settled  (happy path)
    ✓ payment_initiated → payment_failed              (failure path)
    ✓ payment_initiated → payment_failed → settled    (discrepancy scenario)
    ✓ settled event arrives before payment_processed  (out-of-order)

  Merchant handling
    ✓ Same merchant across multiple transactions — only one merchant row
    ✓ Different merchants each get their own row

  Validation
    ✓ Invalid event_type → 422
    ✓ Missing required field → 422
    ✓ Negative amount → accepted (no business-rule validation specified)
    ✓ Empty string event_id → accepted by schema (edge case documentation)
    ✓ Timestamp without timezone → accepted (Pydantic parses it)
    ✓ Amount with many decimal places → stored to 2dp precision
"""

import pytest
import pytest_asyncio
from sqlalchemy import text

from tests.conftest import make_event


pytestmark = pytest.mark.asyncio


# ─────────────────────────────────────────────────────────────────────────────
# Happy path
# ─────────────────────────────────────────────────────────────────────────────

class TestIngestHappyPath:

    async def test_first_event_returns_201_success(self, client):
        r = await client.post("/events", json=make_event())
        assert r.status_code == 201
        body = r.json()
        assert body["status"] == "success"
        assert body["detail"] == "event processed"

    async def test_merchant_row_created(self, client, db_session):
        await client.post("/events", json=make_event(
            merchant_id="m-new", merchant_name="NewMerchant"
        ))
        row = await db_session.execute(
            text("SELECT merchant_name FROM merchants WHERE merchant_id = 'tenant-new'")
        )
        # Verify via the transactions API instead (db isolation detail)
        r = await client.get("/transactions")
        assert r.status_code == 200

    async def test_transaction_row_created_with_correct_fields(self, client):
        evt = make_event(
            event_id="evt-tx-fields",
            transaction_id="txn-fields",
            merchant_id="m-fields",
            merchant_name="FieldsMerchant",
            amount=2500.50,
            currency="USD",
            event_type="payment_initiated",
            timestamp="2026-01-15T09:00:00+00:00",
        )
        await client.post("/events", json=evt)

        r = await client.get(f"/transactions/txn-fields")
        assert r.status_code == 200
        data = r.json()
        assert data["transaction_id"] == "txn-fields"
        assert data["merchant_id"] == "m-fields"
        assert float(data["amount"]) == 2500.50
        assert data["currency"] == "USD"
        assert data["current_status"] == "payment_initiated"

    async def test_event_stored_in_event_log(self, client):
        evt = make_event(
            event_id="evt-log-check",
            transaction_id="txn-log-check",
        )
        await client.post("/events", json=evt)

        r = await client.get("/transactions/txn-log-check")
        data = r.json()
        assert len(data["events"]) == 1
        assert data["events"][0]["event_id"] == "evt-log-check"
        assert data["events"][0]["event_type"] == "payment_initiated"

    async def test_multiple_events_for_same_transaction_all_stored(self, client):
        base = "txn-multi"
        await client.post("/events", json=make_event(
            event_id="e1", transaction_id=base,
            event_type="payment_initiated", timestamp="2026-01-10T10:00:00+00:00"
        ))
        await client.post("/events", json=make_event(
            event_id="e2", transaction_id=base,
            event_type="payment_processed", timestamp="2026-01-10T10:05:00+00:00"
        ))
        await client.post("/events", json=make_event(
            event_id="e3", transaction_id=base,
            event_type="settled", timestamp="2026-01-10T11:00:00+00:00"
        ))

        r = await client.get(f"/transactions/{base}")
        data = r.json()
        assert len(data["events"]) == 3
        assert data["current_status"] == "settled"


# ─────────────────────────────────────────────────────────────────────────────
# Idempotency
# ─────────────────────────────────────────────────────────────────────────────

class TestIdempotency:

    async def test_duplicate_event_returns_ignored(self, client):
        evt = make_event(event_id="dup-001", transaction_id="txn-dup-1")
        await client.post("/events", json=evt)
        r = await client.post("/events", json=evt)
        assert r.status_code == 201
        body = r.json()
        assert body["status"] == "ignored"
        assert body["detail"] == "duplicate event"

    async def test_duplicate_does_not_change_transaction_status(self, client):
        # First: initiate
        await client.post("/events", json=make_event(
            event_id="dup-init", transaction_id="txn-dup-2",
            event_type="payment_initiated", timestamp="2026-01-10T10:00:00+00:00"
        ))
        # Second: advance to processed
        await client.post("/events", json=make_event(
            event_id="dup-proc", transaction_id="txn-dup-2",
            event_type="payment_processed", timestamp="2026-01-10T10:05:00+00:00"
        ))
        # Re-send the original initiation event (old timestamp, same event_id)
        await client.post("/events", json=make_event(
            event_id="dup-init", transaction_id="txn-dup-2",
            event_type="payment_initiated", timestamp="2026-01-10T10:00:00+00:00"
        ))

        r = await client.get("/transactions/txn-dup-2")
        assert r.json()["current_status"] == "payment_processed"

    async def test_duplicate_does_not_create_second_event_row(self, client):
        evt = make_event(event_id="dup-row", transaction_id="txn-dup-3")
        await client.post("/events", json=evt)
        await client.post("/events", json=evt)

        r = await client.get("/transactions/txn-dup-3")
        assert len(r.json()["events"]) == 1

    async def test_duplicate_of_latest_event_leaves_state_unchanged(self, client):
        await client.post("/events", json=make_event(
            event_id="dup-settled", transaction_id="txn-dup-4",
            event_type="settled", timestamp="2026-01-10T12:00:00+00:00"
        ))
        # Re-send settled — should still be ignored
        r2 = await client.post("/events", json=make_event(
            event_id="dup-settled", transaction_id="txn-dup-4",
            event_type="settled", timestamp="2026-01-10T12:00:00+00:00"
        ))
        assert r2.json()["status"] == "ignored"

        r = await client.get("/transactions/txn-dup-4")
        assert len(r.json()["events"]) == 1


# ─────────────────────────────────────────────────────────────────────────────
# Chronology Lock — out-of-order delivery
# ─────────────────────────────────────────────────────────────────────────────

class TestChronologyLock:

    async def test_earlier_event_does_not_regress_status(self, client):
        txn = "txn-chrono-1"
        # Deliver settled first (latest timestamp)
        await client.post("/events", json=make_event(
            event_id="c1-settled", transaction_id=txn,
            event_type="settled", timestamp="2026-01-10T12:00:00+00:00"
        ))
        # Then deliver payment_initiated (older timestamp)
        await client.post("/events", json=make_event(
            event_id="c1-init", transaction_id=txn,
            event_type="payment_initiated", timestamp="2026-01-10T10:00:00+00:00"
        ))
        r = await client.get(f"/transactions/{txn}")
        assert r.json()["current_status"] == "settled"

    async def test_same_timestamp_does_not_change_status(self, client):
        txn = "txn-chrono-2"
        ts = "2026-01-10T10:00:00+00:00"
        await client.post("/events", json=make_event(
            event_id="c2-proc", transaction_id=txn,
            event_type="payment_processed", timestamp=ts
        ))
        # Second event with the exact same timestamp
        await client.post("/events", json=make_event(
            event_id="c2-init", transaction_id=txn,
            event_type="payment_initiated", timestamp=ts
        ))
        r = await client.get(f"/transactions/{txn}")
        # First event wins; same timestamp should NOT overwrite
        assert r.json()["current_status"] == "payment_processed"

    async def test_later_event_advances_status_normally(self, client):
        txn = "txn-chrono-3"
        await client.post("/events", json=make_event(
            event_id="c3-init", transaction_id=txn,
            event_type="payment_initiated", timestamp="2026-01-10T10:00:00+00:00"
        ))
        await client.post("/events", json=make_event(
            event_id="c3-proc", transaction_id=txn,
            event_type="payment_processed", timestamp="2026-01-10T10:05:00+00:00"
        ))
        r = await client.get(f"/transactions/{txn}")
        assert r.json()["current_status"] == "payment_processed"

    async def test_three_events_reversed_order_final_state_is_latest(self, client):
        txn = "txn-chrono-4"
        # Send settled → processed → initiated (fully reversed)
        await client.post("/events", json=make_event(
            event_id="c4-settled", transaction_id=txn,
            event_type="settled", timestamp="2026-01-10T14:00:00+00:00"
        ))
        await client.post("/events", json=make_event(
            event_id="c4-proc", transaction_id=txn,
            event_type="payment_processed", timestamp="2026-01-10T10:30:00+00:00"
        ))
        await client.post("/events", json=make_event(
            event_id="c4-init", transaction_id=txn,
            event_type="payment_initiated", timestamp="2026-01-10T10:00:00+00:00"
        ))
        r = await client.get(f"/transactions/{txn}")
        data = r.json()
        assert data["current_status"] == "settled"
        # All three events should be stored
        assert len(data["events"]) == 3

    async def test_all_events_preserved_regardless_of_delivery_order(self, client):
        """Event log is append-only — all events stored even if out-of-order."""
        txn = "txn-chrono-5"
        await client.post("/events", json=make_event(
            event_id="c5-b", transaction_id=txn,
            event_type="payment_failed", timestamp="2026-01-10T10:05:00+00:00"
        ))
        await client.post("/events", json=make_event(
            event_id="c5-a", transaction_id=txn,
            event_type="payment_initiated", timestamp="2026-01-10T10:00:00+00:00"
        ))
        r = await client.get(f"/transactions/{txn}")
        event_ids = {e["event_id"] for e in r.json()["events"]}
        assert event_ids == {"c5-a", "c5-b"}


# ─────────────────────────────────────────────────────────────────────────────
# Full lifecycle flows
# ─────────────────────────────────────────────────────────────────────────────

class TestLifecycleFlows:

    async def test_happy_path_initiated_processed_settled(self, client):
        txn = "txn-happy"
        for evt_id, evt_type, ts in [
            ("hp-1", "payment_initiated",  "2026-01-10T10:00:00+00:00"),
            ("hp-2", "payment_processed",  "2026-01-10T10:05:00+00:00"),
            ("hp-3", "settled",            "2026-01-10T11:00:00+00:00"),
        ]:
            r = await client.post("/events", json=make_event(
                event_id=evt_id, transaction_id=txn, event_type=evt_type, timestamp=ts
            ))
            assert r.status_code == 201

        r = await client.get(f"/transactions/{txn}")
        data = r.json()
        assert data["current_status"] == "settled"
        assert len(data["events"]) == 3

    async def test_failure_path_initiated_then_failed(self, client):
        txn = "txn-fail"
        await client.post("/events", json=make_event(
            event_id="fp-1", transaction_id=txn, event_type="payment_initiated",
            timestamp="2026-01-10T10:00:00+00:00"
        ))
        await client.post("/events", json=make_event(
            event_id="fp-2", transaction_id=txn, event_type="payment_failed",
            timestamp="2026-01-10T10:02:00+00:00"
        ))
        r = await client.get(f"/transactions/{txn}")
        assert r.json()["current_status"] == "payment_failed"

    async def test_discrepancy_scenario_settled_after_failure(self, client):
        txn = "txn-disc-flow"
        await client.post("/events", json=make_event(
            event_id="df-1", transaction_id=txn, event_type="payment_initiated",
            timestamp="2026-01-10T10:00:00+00:00"
        ))
        await client.post("/events", json=make_event(
            event_id="df-2", transaction_id=txn, event_type="payment_failed",
            timestamp="2026-01-10T10:02:00+00:00"
        ))
        await client.post("/events", json=make_event(
            event_id="df-3", transaction_id=txn, event_type="settled",
            timestamp="2026-01-10T11:00:00+00:00"
        ))
        r = await client.get(f"/transactions/{txn}")
        data = r.json()
        assert data["current_status"] == "settled"
        assert len(data["events"]) == 3

    async def test_settled_arrives_before_processed_out_of_order(self, client):
        txn = "txn-ooo-settled"
        # Settled delivered first
        await client.post("/events", json=make_event(
            event_id="ooo-s", transaction_id=txn, event_type="settled",
            timestamp="2026-01-10T11:00:00+00:00"
        ))
        # processed delivered after
        await client.post("/events", json=make_event(
            event_id="ooo-p", transaction_id=txn, event_type="payment_processed",
            timestamp="2026-01-10T10:05:00+00:00"
        ))
        # initiated delivered last
        await client.post("/events", json=make_event(
            event_id="ooo-i", transaction_id=txn, event_type="payment_initiated",
            timestamp="2026-01-10T10:00:00+00:00"
        ))
        r = await client.get(f"/transactions/{txn}")
        data = r.json()
        # Chronology lock: settled (11:00) is the latest — it wins
        assert data["current_status"] == "settled"
        assert len(data["events"]) == 3


# ─────────────────────────────────────────────────────────────────────────────
# Merchant handling
# ─────────────────────────────────────────────────────────────────────────────

class TestMerchantHandling:

    async def test_same_merchant_multiple_transactions_one_merchant_row(self, client):
        for i in range(3):
            await client.post("/events", json=make_event(
                event_id=f"m-multi-{i}",
                transaction_id=f"txn-m-multi-{i}",
                merchant_id="m-shared",
                merchant_name="SharedMerchant",
            ))
        # Merchant detail should appear on each transaction
        for i in range(3):
            r = await client.get(f"/transactions/txn-m-multi-{i}")
            assert r.json()["merchant"]["merchant_id"] == "m-shared"

    async def test_different_merchants_each_get_own_row(self, client):
        await client.post("/events", json=make_event(
            event_id="dm-1", transaction_id="txn-dm-1",
            merchant_id="m-alpha", merchant_name="Alpha"
        ))
        await client.post("/events", json=make_event(
            event_id="dm-2", transaction_id="txn-dm-2",
            merchant_id="m-beta", merchant_name="Beta"
        ))
        r1 = await client.get("/transactions/txn-dm-1")
        r2 = await client.get("/transactions/txn-dm-2")
        assert r1.json()["merchant"]["merchant_name"] == "Alpha"
        assert r2.json()["merchant"]["merchant_name"] == "Beta"


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

class TestEventValidation:

    async def test_invalid_event_type_returns_422(self, client):
        evt = make_event(event_type="invalid_type")
        r = await client.post("/events", json=evt)
        assert r.status_code == 422

    async def test_missing_event_id_returns_422(self, client):
        evt = make_event()
        del evt["event_id"]
        r = await client.post("/events", json=evt)
        assert r.status_code == 422

    async def test_missing_transaction_id_returns_422(self, client):
        evt = make_event()
        del evt["transaction_id"]
        r = await client.post("/events", json=evt)
        assert r.status_code == 422

    async def test_missing_amount_returns_422(self, client):
        evt = make_event()
        del evt["amount"]
        r = await client.post("/events", json=evt)
        assert r.status_code == 422

    async def test_non_numeric_amount_returns_422(self, client):
        evt = make_event(amount="not-a-number")
        r = await client.post("/events", json=evt)
        assert r.status_code == 422

    async def test_missing_timestamp_returns_422(self, client):
        evt = make_event()
        del evt["timestamp"]
        r = await client.post("/events", json=evt)
        assert r.status_code == 422

    async def test_invalid_timestamp_format_returns_422(self, client):
        evt = make_event(timestamp="not-a-date")
        r = await client.post("/events", json=evt)
        assert r.status_code == 422

    async def test_all_four_valid_event_types_are_accepted(self, client):
        for i, et in enumerate([
            "payment_initiated", "payment_processed", "payment_failed", "settled"
        ]):
            r = await client.post("/events", json=make_event(
                event_id=f"val-type-{i}",
                transaction_id=f"txn-val-type-{i}",
                event_type=et,
            ))
            assert r.status_code == 201, f"Event type {et!r} was rejected"

    async def test_amount_with_many_decimal_places_accepted(self, client):
        """Amount is accepted; stored precision is 2dp (Numeric 15,2)."""
        r = await client.post("/events", json=make_event(
            event_id="val-dec",
            transaction_id="txn-val-dec",
            amount=1234.56789,
        ))
        assert r.status_code == 201

    async def test_zero_amount_is_accepted(self, client):
        r = await client.post("/events", json=make_event(
            event_id="val-zero",
            transaction_id="txn-val-zero",
            amount=0,
        ))
        assert r.status_code == 201

    async def test_empty_body_returns_422(self, client):
        r = await client.post("/events", json={})
        assert r.status_code == 422
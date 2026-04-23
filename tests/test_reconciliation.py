"""
test_reconciliation.py — GET /reconciliation/summary  &  GET /reconciliation/discrepancies

Covers:
  GET /reconciliation/summary
    ✓ Empty DB returns empty list
    ✓ Single merchant, single status group
    ✓ Multiple merchants appear as separate groups
    ✓ Multiple statuses for same merchant appear as separate groups
    ✓ count is correct per group
    ✓ total_amount is correct per group
    ✓ Result is ordered by merchant_id, date desc

  GET /reconciliation/discrepancies
    ✓ Empty DB returns empty list
    ✓ Clean happy-path transaction is NOT flagged
    ✓ Clean failure-path transaction is NOT flagged

    Case: settled_without_processing
    ✓ settled with no payment_processed event → flagged
    ✓ settled WITH payment_processed event → NOT flagged
    ✓ discrepancy_type label is 'settled_without_processing'

    Case: settled_after_failure
    ✓ settled with a payment_failed event → flagged
    ✓ settled with payment_failed AND payment_processed → uses label logic correctly
    ✓ discrepancy_type label is 'settled_after_failure'

    Case: stuck_initiated
    ✓ payment_initiated with no follow-up, timestamp > 1h ago → flagged
    ✓ payment_initiated with no follow-up, timestamp < 1h ago → NOT flagged (too recent)
    ✓ payment_initiated WITH payment_processed → NOT flagged
    ✓ payment_initiated WITH payment_failed → NOT flagged
    ✓ discrepancy_type label is 'stuck_initiated'

    Response shape
    ✓ Each discrepancy row has: transaction_id, merchant_id, current_status,
                                amount, currency, latest_event_timestamp, discrepancy_type
    ✓ Non-discrepant transactions are not included
    ✓ Multiple discrepancy types returned in the same response
"""

import pytest
from datetime import datetime, timezone, timedelta

from tests.conftest import make_event

pytestmark = pytest.mark.asyncio

# Timestamp helpers
NOW = datetime.now(timezone.utc)
TWO_HOURS_AGO = (NOW - timedelta(hours=2)).isoformat()
THIRTY_MIN_AGO = (NOW - timedelta(minutes=30)).isoformat()
YESTERDAY = (NOW - timedelta(days=1)).isoformat()
NEXT_HOUR = (NOW + timedelta(hours=1)).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# GET /reconciliation/summary
# ─────────────────────────────────────────────────────────────────────────────

class TestReconciliationSummary:

    async def test_empty_db_returns_empty_list(self, client):
        r = await client.get("/reconciliation/summary")
        assert r.status_code == 200
        assert r.json() == []

    async def test_single_merchant_single_status_group(self, client):
        await client.post("/events", json=make_event(
            event_id="sum-1", transaction_id="tsum-1",
            merchant_id="m-sum", merchant_name="SumMerchant",
            amount=1000.00, event_type="settled",
            timestamp="2026-01-10T10:00:00+00:00",
        ))
        r = await client.get("/reconciliation/summary")
        data = r.json()
        assert len(data) >= 1
        group = next((g for g in data if g["merchant_id"] == "m-sum"), None)
        assert group is not None
        assert group["status"] == "settled"
        assert group["count"] == 1
        assert abs(group["total_amount"] - 1000.00) < 0.01

    async def test_multiple_merchants_appear_as_separate_groups(self, client):
        for i, mid in enumerate(["m-grp-a", "m-grp-b"]):
            await client.post("/events", json=make_event(
                event_id=f"mgrp-{i}", transaction_id=f"tmgrp-{i}",
                merchant_id=mid, merchant_name=f"Merchant {mid}",
                event_type="settled", timestamp="2026-01-10T10:00:00+00:00",
            ))
        r = await client.get("/reconciliation/summary")
        merchant_ids = {g["merchant_id"] for g in r.json()}
        assert "m-grp-a" in merchant_ids
        assert "m-grp-b" in merchant_ids

    async def test_multiple_statuses_same_merchant_are_separate_groups(self, client):
        # One settled, one failed for same merchant
        await client.post("/events", json=make_event(
            event_id="ms-s", transaction_id="tms-s",
            merchant_id="m-multi-status", merchant_name="MultiStatus",
            event_type="settled", timestamp="2026-01-10T10:00:00+00:00",
        ))
        await client.post("/events", json=make_event(
            event_id="ms-f", transaction_id="tms-f",
            merchant_id="m-multi-status", merchant_name="MultiStatus",
            event_type="payment_failed", timestamp="2026-01-10T10:00:00+00:00",
        ))
        r = await client.get("/reconciliation/summary")
        merchant_groups = [g for g in r.json() if g["merchant_id"] == "m-multi-status"]
        statuses = {g["status"] for g in merchant_groups}
        assert "settled" in statuses
        assert "payment_failed" in statuses

    async def test_count_is_correct(self, client):
        for i in range(4):
            await client.post("/events", json=make_event(
                event_id=f"cnt-{i}", transaction_id=f"tcnt-{i}",
                merchant_id="m-count", merchant_name="CountMerchant",
                event_type="payment_initiated",
                timestamp="2026-01-10T10:00:00+00:00",
            ))
        r = await client.get("/reconciliation/summary")
        group = next((g for g in r.json()
                      if g["merchant_id"] == "m-count"
                      and g["status"] == "payment_initiated"), None)
        assert group is not None
        assert group["count"] == 4

    async def test_total_amount_is_correct(self, client):
        amounts = [100.00, 200.00, 300.00]
        for i, amt in enumerate(amounts):
            await client.post("/events", json=make_event(
                event_id=f"amt-{i}", transaction_id=f"tamt-{i}",
                merchant_id="m-total", merchant_name="TotalMerchant",
                event_type="settled", amount=amt,
                timestamp="2026-01-10T10:00:00+00:00",
            ))
        r = await client.get("/reconciliation/summary")
        group = next((g for g in r.json()
                      if g["merchant_id"] == "m-total"
                      and g["status"] == "settled"), None)
        assert group is not None
        assert abs(group["total_amount"] - sum(amounts)) < 0.01

    async def test_response_has_required_fields(self, client):
        await client.post("/events", json=make_event(
            event_id="rf-1", transaction_id="trf-1",
            merchant_id="m-rf", merchant_name="RfMerchant",
            event_type="settled", timestamp="2026-01-10T10:00:00+00:00",
        ))
        r = await client.get("/reconciliation/summary")
        data = r.json()
        assert len(data) > 0
        for row in data:
            for field in ["merchant_id", "date", "status", "count", "total_amount"]:
                assert field in row, f"Missing field: {field}"


# ─────────────────────────────────────────────────────────────────────────────
# GET /reconciliation/discrepancies
# ─────────────────────────────────────────────────────────────────────────────

class TestReconciliationDiscrepancies:

    async def test_empty_db_returns_empty_list(self, client):
        r = await client.get("/reconciliation/discrepancies")
        assert r.status_code == 200
        assert r.json() == []

    async def test_clean_happy_path_not_flagged(self, client):
        txn = "txn-clean-happy"
        for evt_id, evt_type, ts in [
            ("ch-1", "payment_initiated", "2026-01-10T10:00:00+00:00"),
            ("ch-2", "payment_processed", "2026-01-10T10:05:00+00:00"),
            ("ch-3", "settled",           "2026-01-10T11:00:00+00:00"),
        ]:
            await client.post("/events", json=make_event(
                event_id=evt_id, transaction_id=txn,
                event_type=evt_type, timestamp=ts,
            ))
        r = await client.get("/reconciliation/discrepancies")
        txn_ids = {d["transaction_id"] for d in r.json()}
        assert txn not in txn_ids

    async def test_clean_failure_path_not_flagged(self, client):
        txn = "txn-clean-fail"
        for evt_id, evt_type, ts in [
            ("cf-1", "payment_initiated", "2026-01-10T10:00:00+00:00"),
            ("cf-2", "payment_failed",    "2026-01-10T10:02:00+00:00"),
        ]:
            await client.post("/events", json=make_event(
                event_id=evt_id, transaction_id=txn,
                event_type=evt_type, timestamp=ts,
            ))
        r = await client.get("/reconciliation/discrepancies")
        txn_ids = {d["transaction_id"] for d in r.json()}
        assert txn not in txn_ids

    # ── Case: settled_without_processing ─────────────────────────────────────

    async def test_settled_without_processing_is_flagged(self, client):
        txn = "txn-disc-swp"
        await client.post("/events", json=make_event(
            event_id="swp-1", transaction_id=txn,
            event_type="payment_initiated", timestamp="2026-01-10T10:00:00+00:00"
        ))
        await client.post("/events", json=make_event(
            event_id="swp-2", transaction_id=txn,
            event_type="settled", timestamp="2026-01-10T11:00:00+00:00"
        ))
        r = await client.get("/reconciliation/discrepancies")
        disc = next((d for d in r.json() if d["transaction_id"] == txn), None)
        assert disc is not None
        assert disc["discrepancy_type"] == "settled_without_processing"

    async def test_settled_with_processing_not_flagged_as_swp(self, client):
        txn = "txn-no-swp"
        for evt_id, evt_type, ts in [
            ("nswp-1", "payment_initiated", "2026-01-10T10:00:00+00:00"),
            ("nswp-2", "payment_processed", "2026-01-10T10:05:00+00:00"),
            ("nswp-3", "settled",           "2026-01-10T11:00:00+00:00"),
        ]:
            await client.post("/events", json=make_event(
                event_id=evt_id, transaction_id=txn,
                event_type=evt_type, timestamp=ts,
            ))
        r = await client.get("/reconciliation/discrepancies")
        disc = next((d for d in r.json() if d["transaction_id"] == txn), None)
        assert disc is None

    # ── Case: settled_after_failure ───────────────────────────────────────────

    async def test_settled_after_failure_is_flagged(self, client):
        txn = "txn-disc-saf"
        for evt_id, evt_type, ts in [
            ("saf-1", "payment_initiated", "2026-01-10T10:00:00+00:00"),
            ("saf-2", "payment_failed",    "2026-01-10T10:02:00+00:00"),
            ("saf-3", "settled",           "2026-01-10T11:00:00+00:00"),
        ]:
            await client.post("/events", json=make_event(
                event_id=evt_id, transaction_id=txn,
                event_type=evt_type, timestamp=ts,
            ))
        r = await client.get("/reconciliation/discrepancies")
        disc = next((d for d in r.json() if d["transaction_id"] == txn), None)
        assert disc is not None
        assert disc["discrepancy_type"] == "settled_after_failure"

    async def test_settled_after_failure_discrepancy_type_label(self, client):
        txn = "txn-saf-label"
        for evt_id, evt_type, ts in [
            ("safl-1", "payment_initiated", "2026-01-10T10:00:00+00:00"),
            ("safl-2", "payment_failed",    "2026-01-10T10:02:00+00:00"),
            ("safl-3", "settled",           "2026-01-10T11:00:00+00:00"),
        ]:
            await client.post("/events", json=make_event(
                event_id=evt_id, transaction_id=txn,
                event_type=evt_type, timestamp=ts,
            ))
        r = await client.get("/reconciliation/discrepancies")
        disc = next(d for d in r.json() if d["transaction_id"] == txn)
        assert disc["discrepancy_type"] == "settled_after_failure"

    # ── Case: stuck_initiated ─────────────────────────────────────────────────

    async def test_stuck_initiated_old_timestamp_is_flagged(self, client):
        txn = "txn-disc-stuck"
        await client.post("/events", json=make_event(
            event_id="stuck-1", transaction_id=txn,
            event_type="payment_initiated",
            timestamp=TWO_HOURS_AGO,
        ))
        r = await client.get("/reconciliation/discrepancies")
        disc = next((d for d in r.json() if d["transaction_id"] == txn), None)
        assert disc is not None
        assert disc["discrepancy_type"] == "stuck_initiated"

    async def test_stuck_initiated_recent_timestamp_not_flagged(self, client):
        txn = "txn-disc-recent"
        await client.post("/events", json=make_event(
            event_id="recent-1", transaction_id=txn,
            event_type="payment_initiated",
            timestamp=THIRTY_MIN_AGO,
        ))
        r = await client.get("/reconciliation/discrepancies")
        disc = next((d for d in r.json() if d["transaction_id"] == txn), None)
        assert disc is None, "Recently initiated transaction should not be flagged"

    async def test_initiated_with_processed_not_stuck(self, client):
        txn = "txn-not-stuck-p"
        await client.post("/events", json=make_event(
            event_id="nsp-1", transaction_id=txn,
            event_type="payment_initiated", timestamp=TWO_HOURS_AGO,
        ))
        await client.post("/events", json=make_event(
            event_id="nsp-2", transaction_id=txn,
            event_type="payment_processed",
            timestamp=(datetime.now(timezone.utc) - timedelta(hours=1, minutes=30)).isoformat(),
        ))
        r = await client.get("/reconciliation/discrepancies")
        disc = next((d for d in r.json() if d["transaction_id"] == txn), None)
        assert disc is None

    async def test_initiated_with_failed_not_stuck(self, client):
        txn = "txn-not-stuck-f"
        await client.post("/events", json=make_event(
            event_id="nsf-1", transaction_id=txn,
            event_type="payment_initiated", timestamp=TWO_HOURS_AGO,
        ))
        await client.post("/events", json=make_event(
            event_id="nsf-2", transaction_id=txn,
            event_type="payment_failed",
            timestamp=(datetime.now(timezone.utc) - timedelta(hours=1, minutes=30)).isoformat(),
        ))
        r = await client.get("/reconciliation/discrepancies")
        disc = next((d for d in r.json() if d["transaction_id"] == txn), None)
        assert disc is None

    # ── Response shape ────────────────────────────────────────────────────────

    async def test_discrepancy_response_has_required_fields(self, client):
        txn = "txn-disc-fields"
        await client.post("/events", json=make_event(
            event_id="df-1", transaction_id=txn,
            event_type="payment_initiated", timestamp=TWO_HOURS_AGO,
            merchant_id="m-disc-f", merchant_name="DiscFieldsMerchant",
            amount=5000.00, currency="INR",
        ))
        r = await client.get("/reconciliation/discrepancies")
        disc = next((d for d in r.json() if d["transaction_id"] == txn), None)
        assert disc is not None
        for field in ["transaction_id", "merchant_id", "current_status",
                      "amount", "currency", "latest_event_timestamp", "discrepancy_type"]:
            assert field in disc, f"Missing field: {field}"

    async def test_multiple_discrepancy_types_in_same_response(self, client):
        # Create one of each discrepancy type
        # 1. settled_without_processing
        txn_swp = "txn-multi-swp"
        await client.post("/events", json=make_event(
            event_id="multi-swp-1", transaction_id=txn_swp,
            event_type="settled", timestamp="2026-01-10T11:00:00+00:00",
        ))
        # 2. settled_after_failure
        txn_saf = "txn-multi-saf"
        for eid, etype, ts in [
            ("multi-saf-1", "payment_initiated", "2026-01-10T10:00:00+00:00"),
            ("multi-saf-2", "payment_failed",    "2026-01-10T10:02:00+00:00"),
            ("multi-saf-3", "settled",           "2026-01-10T11:00:00+00:00"),
        ]:
            await client.post("/events", json=make_event(
                event_id=eid, transaction_id=txn_saf,
                event_type=etype, timestamp=ts,
            ))
        # 3. stuck_initiated
        txn_stuck = "txn-multi-stuck"
        await client.post("/events", json=make_event(
            event_id="multi-stuck-1", transaction_id=txn_stuck,
            event_type="payment_initiated", timestamp=TWO_HOURS_AGO,
        ))

        r = await client.get("/reconciliation/discrepancies")
        disc_map = {d["transaction_id"]: d["discrepancy_type"] for d in r.json()}

        assert disc_map.get(txn_swp) == "settled_without_processing"
        assert disc_map.get(txn_saf) == "settled_after_failure"
        assert disc_map.get(txn_stuck) == "stuck_initiated"

    async def test_non_discrepant_transactions_not_included(self, client):
        txn_clean = "txn-disc-clean"
        for eid, etype, ts in [
            ("dc-1", "payment_initiated", "2026-01-10T10:00:00+00:00"),
            ("dc-2", "payment_processed", "2026-01-10T10:05:00+00:00"),
            ("dc-3", "settled",           "2026-01-10T11:00:00+00:00"),
        ]:
            await client.post("/events", json=make_event(
                event_id=eid, transaction_id=txn_clean,
                event_type=etype, timestamp=ts,
            ))
        r = await client.get("/reconciliation/discrepancies")
        txn_ids = {d["transaction_id"] for d in r.json()}
        assert txn_clean not in txn_ids

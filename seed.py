"""
seed.py — generates and ingests ~10,000 realistic payment events.

Usage:
    # Against local docker-compose:
    python seed.py

    # Against a remote deployment:
    BASE_URL=https://your-app.onrender.com python seed.py

The script deliberately includes:
  - Successful flows  (initiated → processed → settled)
  - Failed flows      (initiated → failed)
  - Partial flows     (initiated only — stuck_initiated discrepancies)
  - Out-of-order      (events sent in reverse timestamp order)
  - Duplicate events  (same event_id re-sent — should be ignored)
  - Discrepancies     (settled with no processed event, settled after failure)
"""

import asyncio
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import httpx

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")
TOTAL_TRANSACTIONS = 2500   # × avg ~4 events = ~10,000 events
CONCURRENCY = 20            # parallel POST /events workers

MERCHANTS = [
    {"merchant_id": "merchant_1", "merchant_name": "QuickMart"},
    {"merchant_id": "merchant_2", "merchant_name": "FreshBasket"},
    {"merchant_id": "merchant_3", "merchant_name": "TechZone"},
    {"merchant_id": "merchant_4", "merchant_name": "UrbanEats"},
    {"merchant_id": "merchant_5", "merchant_name": "SwiftPay"},
]

CURRENCIES = ["INR", "INR", "INR", "USD", "EUR"]  # weighted towards INR


def random_ts(base: datetime, jitter_minutes: int = 120) -> datetime:
    return base + timedelta(minutes=random.randint(0, jitter_minutes))


def make_event(event_id, event_type, txn_id, merchant, amount, currency, ts) -> dict:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "transaction_id": txn_id,
        "merchant_id": merchant["merchant_id"],
        "merchant_name": merchant["merchant_name"],
        "amount": float(amount),
        "currency": currency,
        "timestamp": ts.isoformat(),
    }


def generate_events() -> list[dict]:
    events = []
    base_time = datetime.now(timezone.utc) - timedelta(days=90)

    for i in range(TOTAL_TRANSACTIONS):
        txn_id = str(uuid.uuid4())
        merchant = random.choice(MERCHANTS)
        amount = round(random.uniform(100, 50000), 2)
        currency = random.choice(CURRENCIES)
        t0 = random_ts(base_time + timedelta(days=random.randint(0, 89)))

        roll = random.random()

        if roll < 0.60:
            # Happy path: initiated → processed → settled
            events.append(make_event(str(uuid.uuid4()), "payment_initiated",  txn_id, merchant, amount, currency, t0))
            events.append(make_event(str(uuid.uuid4()), "payment_processed",  txn_id, merchant, amount, currency, t0 + timedelta(minutes=random.randint(1, 10))))
            events.append(make_event(str(uuid.uuid4()), "settled",            txn_id, merchant, amount, currency, t0 + timedelta(hours=random.randint(1, 24))))

        elif roll < 0.75:
            # Failed path: initiated → failed
            events.append(make_event(str(uuid.uuid4()), "payment_initiated", txn_id, merchant, amount, currency, t0))
            events.append(make_event(str(uuid.uuid4()), "payment_failed",    txn_id, merchant, amount, currency, t0 + timedelta(minutes=random.randint(1, 5))))

        elif roll < 0.85:
            # Stuck/pending: only initiated (older than 1 hour → stuck_initiated discrepancy)
            stuck_ts = datetime.now(timezone.utc) - timedelta(hours=random.randint(2, 48))
            events.append(make_event(str(uuid.uuid4()), "payment_initiated", txn_id, merchant, amount, currency, stuck_ts))

        elif roll < 0.90:
            # Discrepancy — settled without processing
            events.append(make_event(str(uuid.uuid4()), "payment_initiated", txn_id, merchant, amount, currency, t0))
            events.append(make_event(str(uuid.uuid4()), "settled",           txn_id, merchant, amount, currency, t0 + timedelta(hours=1)))

        elif roll < 0.95:
            # Discrepancy — settled after failure
            events.append(make_event(str(uuid.uuid4()), "payment_initiated", txn_id, merchant, amount, currency, t0))
            events.append(make_event(str(uuid.uuid4()), "payment_failed",    txn_id, merchant, amount, currency, t0 + timedelta(minutes=2)))
            events.append(make_event(str(uuid.uuid4()), "settled",           txn_id, merchant, amount, currency, t0 + timedelta(hours=2)))

        else:
            # Out-of-order delivery: send settled first, then processed, then initiated
            e1 = make_event(str(uuid.uuid4()), "payment_initiated", txn_id, merchant, amount, currency, t0)
            e2 = make_event(str(uuid.uuid4()), "payment_processed", txn_id, merchant, amount, currency, t0 + timedelta(minutes=5))
            e3 = make_event(str(uuid.uuid4()), "settled",           txn_id, merchant, amount, currency, t0 + timedelta(hours=3))
            events.extend([e3, e1, e2])  # reversed — Chronology Lock should handle this

    # Load additional events from sample_events folder
    extra_events = load_sample_events()
    events.extend(extra_events)

    # Append 50 intentional duplicates (same event_id re-sent)
    duplicates = random.sample(events, min(50, len(events)))
    events.extend(duplicates)

    random.shuffle(events)
    print(f"Generated {len(events)} events across {TOTAL_TRANSACTIONS} transactions (+ {len(extra_events)} from files).")
    return events


async def post_event(client: httpx.AsyncClient, semaphore: asyncio.Semaphore, event: dict, stats: dict):
    async with semaphore:
        try:
            r = await client.post(f"{BASE_URL}/events", json=event, timeout=10)
            if r.status_code in (200, 201):
                body = r.json()
                stats["success"] += 1
                if body.get("detail") == "duplicate event":
                    stats["duplicates"] += 1
            else:
                stats["errors"] += 1
                print(f"  ERROR {r.status_code}: {r.text[:120]}")
        except Exception as exc:
            stats["errors"] += 1
            print(f"  EXCEPTION: {exc}")


async def main():
    events = generate_events()

    # Optionally save to file for reference
    with open("sample_events.json", "w") as f:
        json.dump(events, f, indent=2)
    print("Saved to sample_events.json")

    stats = {"success": 0, "duplicates": 0, "errors": 0}
    semaphore = asyncio.Semaphore(CONCURRENCY)

    print(f"Ingesting {len(events)} events → {BASE_URL} (concurrency={CONCURRENCY}) ...")
    async with httpx.AsyncClient() as client:
        tasks = [post_event(client, semaphore, e, stats) for e in events]
        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            await coro
            if i % 500 == 0:
                print(f"  {i}/{len(events)} — {stats}")

    print(f"\nDone. {stats}")

def load_sample_events(folder: str = "sample_events") -> list[dict]:
    extra_events = []

    if not os.path.exists(folder):
        print(f"No '{folder}' folder found. Skipping sample events.")
        return extra_events

    for filename in os.listdir(folder):
        if not filename.endswith(".json"):
            continue

        path = os.path.join(folder, filename)

        try:
            with open(path, "r") as f:
                data = json.load(f)

                if isinstance(data, list):
                    extra_events.extend(data)
                elif isinstance(data, dict):
                    extra_events.append(data)
                else:
                    print(f"Skipping {filename}: unsupported JSON format")

        except Exception as e:
            print(f"Error reading {filename}: {e}")

    print(f"Loaded {len(extra_events)} events from '{folder}'")
    return extra_events

if __name__ == "__main__":
    asyncio.run(main())

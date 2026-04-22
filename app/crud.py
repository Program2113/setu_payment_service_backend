from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload
from sqlalchemy import text, asc, desc
from datetime import datetime
from typing import Optional

from .models import Merchant, Transaction, Event, EventType
from .schemas import EventCreate, SortField, SortDir


# ── POST /events ──────────────────────────────────────────────────────────────

async def process_incoming_event(db: AsyncSession, event_data: EventCreate) -> dict:
    """
    Ingest a payment lifecycle event safely and idempotently.

    Order of operations matters here:
      1. Upsert Merchant    — must exist before Transaction (FK)
      2. Upsert Transaction — must exist before Event (FK)
      3. Chronology Lock    — advance status only if this event is newer
      4. Insert Event       — PK uniqueness enforces idempotency

    Steps 1 & 2 use INSERT ... ON CONFLICT DO NOTHING so concurrent requests
    for the same merchant / transaction never race into an unhandled IntegrityError.
    """

    # ── 1. Upsert Merchant ────────────────────────────────────────────────────
    await db.execute(
        text("""
            INSERT INTO merchants (merchant_id, merchant_name)
            VALUES (:merchant_id, :merchant_name)
            ON CONFLICT (merchant_id) DO NOTHING
        """),
        {"merchant_id": event_data.merchant_id, "merchant_name": event_data.merchant_name},
    )

    # ── 2. Upsert Transaction ─────────────────────────────────────────────────
    # Insert only if it does not exist yet; concurrent requests are safe.
    await db.execute(
        text("""
            INSERT INTO transactions
                (transaction_id, merchant_id, amount, currency, current_status, latest_event_timestamp, created_at)
            VALUES
                (:transaction_id, :merchant_id, :amount, :currency, :current_status, :latest_event_timestamp, NOW())
            ON CONFLICT (transaction_id) DO NOTHING
        """),
        {
            "transaction_id": event_data.transaction_id,
            "merchant_id": event_data.merchant_id,
            "amount": str(event_data.amount),
            "currency": event_data.currency,
            "current_status": event_data.event_type.value,
            "latest_event_timestamp": event_data.timestamp,
        },
    )

    # ── 3. Chronology Lock — advance status only if this event is newer ───────
    await db.execute(
        text("""
            UPDATE transactions
            SET current_status          = :current_status,
                latest_event_timestamp  = :latest_event_timestamp
            WHERE transaction_id = :transaction_id
              AND :latest_event_timestamp > latest_event_timestamp
        """),
        {
            "transaction_id": event_data.transaction_id,
            "current_status": event_data.event_type.value,
            "latest_event_timestamp": event_data.timestamp,
        },
    )

    # ── 4. Insert Event (idempotency gate) ────────────────────────────────────
    # The Event PK is the definitive idempotency lock.
    # If this event_id was already ingested, skip gracefully without touching state.
    try:
        db.add(
            Event(
                event_id=event_data.event_id,
                transaction_id=event_data.transaction_id,
                event_type=event_data.event_type,
                timestamp=event_data.timestamp,
            )
        )
        await db.flush()
    except IntegrityError:
        await db.rollback()
        return {"status": "ignored", "detail": "duplicate event"}

    await db.commit()
    return {"status": "success", "detail": "event processed"}


# ── GET /transactions ─────────────────────────────────────────────────────────

async def get_transactions(
    db: AsyncSession,
    merchant_id: Optional[str] = None,
    status: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    sort_by: SortField = "created_at",
    sort_dir: SortDir = "desc",
    limit: int = 50,
    offset: int = 0,
):
    query = select(Transaction)

    if merchant_id:
        query = query.where(Transaction.merchant_id == merchant_id)
    if status:
        query = query.where(Transaction.current_status == status)
    if start_date:
        query = query.where(Transaction.created_at >= start_date)
    if end_date:
        query = query.where(Transaction.created_at <= end_date)

    sort_col = getattr(Transaction, sort_by)
    query = query.order_by(desc(sort_col) if sort_dir == "desc" else asc(sort_col))
    query = query.limit(limit).offset(offset)

    result = await db.execute(query)
    return result.scalars().all()


# ── GET /transactions/{id} ────────────────────────────────────────────────────

async def get_transaction_by_id(db: AsyncSession, transaction_id: str):
    query = (
        select(Transaction)
        .options(
            joinedload(Transaction.events),    # full event history, ordered by timestamp
            joinedload(Transaction.merchant),  # merchant name surfaced in response
        )
        .where(Transaction.transaction_id == transaction_id)
    )
    result = await db.execute(query)
    return result.scalar_one_or_none()


# ── GET /reconciliation/summary ───────────────────────────────────────────────

async def get_reconciliation_summary(db: AsyncSession):
    """
    Groups by merchant, settlement date (from the event timestamp, not ingestion
    time) and status. Using latest_event_timestamp ensures late-arriving events
    land in the correct date bucket.
    """
    query = text("""
        SELECT
            merchant_id,
            DATE(latest_event_timestamp AT TIME ZONE 'UTC') AS date,
            current_status,
            COUNT(*)                                        AS count,
            SUM(amount)                                     AS total_amount
        FROM transactions
        GROUP BY merchant_id, DATE(latest_event_timestamp AT TIME ZONE 'UTC'), current_status
        ORDER BY merchant_id, date DESC, current_status;
    """)
    result = await db.execute(query)
    return [
        {
            "merchant_id": row[0],
            "date": str(row[1]),
            "status": row[2],
            "count": row[3],
            "total_amount": float(row[4]) if row[4] is not None else 0.0,
        }
        for row in result.all()
    ]


# ── GET /reconciliation/discrepancies ─────────────────────────────────────────

async def get_discrepancies(db: AsyncSession):
    """
    Detects three classes of logical inconsistency:

      settled_without_processing — current_status='settled' but no
        'payment_processed' event exists. Settlement recorded without
        a prior successful payment.

      settled_after_failure — current_status='settled' but a
        'payment_failed' event exists. A failed payment reached settled.

      stuck_initiated — current_status='payment_initiated' with no
        subsequent 'payment_processed' or 'payment_failed' event and
        the transaction is older than 1 hour. Flags abandoned/stalled flows.
    """
    query = text("""
        SELECT
            t.transaction_id,
            t.merchant_id,
            t.current_status,
            t.amount,
            t.currency,
            t.latest_event_timestamp,
            CASE
                WHEN t.current_status = 'settled'
                     AND NOT EXISTS (
                         SELECT 1 FROM events e
                         WHERE e.transaction_id = t.transaction_id
                           AND e.event_type = 'payment_processed'
                     )
                THEN 'settled_without_processing'

                WHEN t.current_status = 'settled'
                     AND EXISTS (
                         SELECT 1 FROM events e
                         WHERE e.transaction_id = t.transaction_id
                           AND e.event_type = 'payment_failed'
                     )
                THEN 'settled_after_failure'

                WHEN t.current_status = 'payment_initiated'
                     AND NOT EXISTS (
                         SELECT 1 FROM events e
                         WHERE e.transaction_id = t.transaction_id
                           AND e.event_type IN ('payment_processed', 'payment_failed')
                     )
                     AND t.latest_event_timestamp < NOW() - INTERVAL '1 hour'
                THEN 'stuck_initiated'
            END AS discrepancy_type
        FROM transactions t
        WHERE
            (
                t.current_status = 'settled'
                AND NOT EXISTS (
                    SELECT 1 FROM events e
                    WHERE e.transaction_id = t.transaction_id
                      AND e.event_type = 'payment_processed'
                )
            )
            OR (
                t.current_status = 'settled'
                AND EXISTS (
                    SELECT 1 FROM events e
                    WHERE e.transaction_id = t.transaction_id
                      AND e.event_type = 'payment_failed'
                )
            )
            OR (
                t.current_status = 'payment_initiated'
                AND NOT EXISTS (
                    SELECT 1 FROM events e
                    WHERE e.transaction_id = t.transaction_id
                      AND e.event_type IN ('payment_processed', 'payment_failed')
                )
                AND t.latest_event_timestamp < NOW() - INTERVAL '1 hour'
            )
        ORDER BY t.latest_event_timestamp DESC;
    """)
    result = await db.execute(query)
    return [
        {
            "transaction_id": row[0],
            "merchant_id": row[1],
            "current_status": row[2],
            "amount": float(row[3]) if row[3] is not None else 0.0,
            "currency": row[4],
            "latest_event_timestamp": row[5].isoformat() if row[5] else None,
            "discrepancy_type": row[6],
        }
        for row in result.all()
    ]

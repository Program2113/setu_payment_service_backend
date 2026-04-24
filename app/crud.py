from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload, selectinload, noload
from sqlalchemy import text, asc, desc
from datetime import datetime
from typing import Optional

from .models import Merchant, Transaction, Event, EventType
from .schemas import EventCreate, SortField, SortDir


# ── POST /events ──────────────────────────────────────────────────────────────
# This is initial version. The new version modified the order of step 3 and step 4
# async def process_incoming_event(db: AsyncSession, event_data: EventCreate) -> dict:
    # """
    # Ingest a payment lifecycle event safely and idempotently.

    # Order of operations matters here:
    #   1. Upsert Merchant    — must exist before Transaction (FK)
    #   2. Upsert Transaction — must exist before Event (FK)
    #   3. Chronology Lock    — advance status only if this event is newer
    #   4. Insert Event       — PK uniqueness enforces idempotency

    # Steps 1 & 2 use INSERT ... ON CONFLICT DO NOTHING so concurrent requests
    # for the same merchant / transaction never race into an unhandled IntegrityError.
    # """

    # # ── 1. Upsert Merchant ────────────────────────────────────────────────────
    # await db.execute(
    #     text("""
    #         INSERT INTO merchants (merchant_id, merchant_name)
    #         VALUES (:merchant_id, :merchant_name)
    #         ON CONFLICT (merchant_id) DO NOTHING
    #     """),
    #     {"merchant_id": event_data.merchant_id, "merchant_name": event_data.merchant_name},
    # )

    # # ── 2. Upsert Transaction ─────────────────────────────────────────────────
    # # Insert only if it does not exist yet; concurrent requests are safe.
    # await db.execute(
    #     text("""
    #         INSERT INTO transactions
    #             (transaction_id, merchant_id, amount, currency, current_status, latest_event_timestamp, created_at)
    #         VALUES
    #             (:transaction_id, :merchant_id, :amount, :currency, :current_status, :latest_event_timestamp, NOW())
    #         ON CONFLICT (transaction_id) DO NOTHING
    #     """),
    #     {
    #         "transaction_id": event_data.transaction_id,
    #         "merchant_id": event_data.merchant_id,
    #         "amount": str(event_data.amount),
    #         "currency": event_data.currency,
    #         "current_status": event_data.event_type.value,
    #         "latest_event_timestamp": event_data.timestamp,
    #     },
    # )

    # # ── 3. Chronology Lock — advance status only if this event is newer ───────
    # await db.execute(
    #     text("""
    #         UPDATE transactions
    #         SET current_status          = :current_status,
    #             latest_event_timestamp  = :latest_event_timestamp
    #         WHERE transaction_id = :transaction_id
    #           AND :latest_event_timestamp > latest_event_timestamp
    #     """),
    #     {
    #         "transaction_id": event_data.transaction_id,
    #         "current_status": event_data.event_type.value,
    #         "latest_event_timestamp": event_data.timestamp,
    #     },
    # )

    # # ── 4. Insert Event (idempotency gate) ────────────────────────────────────
    # # The Event PK is the definitive idempotency lock.
    # # If this event_id was already ingested, skip gracefully without touching state.
    # try:
    #     db.add(
    #         Event(
    #             event_id=event_data.event_id,
    #             transaction_id=event_data.transaction_id,
    #             event_type=event_data.event_type,
    #             timestamp=event_data.timestamp,
    #         )
    #     )
    #     await db.flush()
    # except IntegrityError:
    #     await db.rollback()
    #     return {"status": "ignored", "detail": "duplicate event"}

    # await db.commit()
    # return {"status": "success", "detail": "event processed"}

async def process_incoming_event(db: AsyncSession, event_data: EventCreate) -> dict:
    """
    Ingest a payment lifecycle event safely and idempotently.

    Order of operations:
      1. Upsert Merchant
      2. Upsert Transaction
      3. Insert Event (idempotency gate)
      4. Advance Transaction status only if this event is newer
    """

    # ── 1. Upsert Merchant ────────────────────────────────────────────────────
    await db.execute(
        text("""
            INSERT INTO merchants (merchant_id, merchant_name)
            VALUES (:merchant_id, :merchant_name)
            ON CONFLICT (merchant_id) DO NOTHING
        """),
        {
            "merchant_id": event_data.merchant_id,
            "merchant_name": event_data.merchant_name,
        },
    )

    # ── 2. Upsert Transaction ─────────────────────────────────────────────────
    await db.execute(
        text("""
            INSERT INTO transactions
                (transaction_id, merchant_id, amount, currency, current_status, latest_event_timestamp, created_at)
            VALUES
                (:transaction_id, :merchant_id, :amount, :currency, :current_status, :latest_event_timestamp, :latest_event_timestamp)
            ON CONFLICT (transaction_id) DO NOTHING
        """),
        {
            "transaction_id": event_data.transaction_id,
            "merchant_id": event_data.merchant_id,
            "amount": str(event_data.amount),
            "currency": event_data.currency,
            "current_status": event_data.event_type.value,
            "latest_event_timestamp": event_data.timestamp,
            "created_at": event_data.timestamp,
        },
    )

    # ── 3. Insert Event (idempotency gate) ───────────────────────────────────
    result = await db.execute(
        text("""
            INSERT INTO events (event_id, transaction_id, event_type, timestamp)
            VALUES (:event_id, :transaction_id, :event_type, :timestamp)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
        """),
        {
            "event_id": event_data.event_id,
            "transaction_id": event_data.transaction_id,
            "event_type": event_data.event_type.value,
            "timestamp": event_data.timestamp,
        },
    )

    # If no row was returned, this event was already processed
    if result.fetchone() is None:
        await db.commit()
        return {"status": "ignored", "detail": "duplicate event"}

    # ── 4. Chronology Lock — advance status only if this event is newer ──────
    await db.execute(
        text("""
            UPDATE transactions
            SET current_status         = :current_status,
                latest_event_timestamp = :latest_event_timestamp
            WHERE transaction_id = :transaction_id
              AND :latest_event_timestamp > latest_event_timestamp
        """),
        {
            "transaction_id": event_data.transaction_id,
            "current_status": event_data.event_type.value,
            "latest_event_timestamp": event_data.timestamp,
        },
    )

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
    query = (
        select(Transaction)
        .options(
            joinedload(Transaction.merchant),
            noload(Transaction.events)  # Prevents the crash while keeping events out of the list
        )
    )


    if merchant_id:
        query = query.where(Transaction.merchant_id == merchant_id)
        
    if status:
        # Validate against Enum to prevent Postgres casting crashes
        valid_statuses = [e.value for e in EventType]
        if status not in valid_statuses:
            return []  
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
            selectinload(Transaction.events),  # Safe async load for collections
            joinedload(Transaction.merchant),  # Required for the X-to-1 relationship
        )
        .where(Transaction.transaction_id == transaction_id)
    )
    result = await db.execute(query)
    return result.scalar_one_or_none()

# ── GET /reconciliation/summary ───────────────────────────────────────────────

async def get_reconciliation_summary(db: AsyncSession):
    # Detect if we are running the test suite (SQLite) or production (Postgres)
    is_sqlite = db.bind.dialect.name == "sqlite"
    date_expr = "DATE(latest_event_timestamp)" if is_sqlite else "DATE(latest_event_timestamp AT TIME ZONE 'UTC')"

    query = text(f"""
        SELECT
            merchant_id,
            {date_expr} AS date,
            current_status,
            COUNT(*) AS count,
            SUM(amount) AS total_amount
        FROM transactions
        GROUP BY merchant_id, {date_expr}, current_status
        ORDER BY merchant_id, date DESC, current_status;
    """)
    result = await db.execute(query)
    return [
        {
            "merchant_id": row[0],
            "date": row[1],
            "status": row[2],
            "count": row[3],
            "total_amount": float(row[4]) if row[4] is not None else 0.0,
        }
        for row in result.fetchall()
    ]

# ── GET /reconciliation/discrepancies ─────────────────────────────────────────

async def get_discrepancies(db: AsyncSession):
    # Detect if we are running the test suite (SQLite) or production (Postgres)
    is_sqlite = db.bind.dialect.name == "sqlite"
    time_expr = "datetime('now', '-1 hour')" if is_sqlite else "NOW() - INTERVAL '1 hour'"

    query = text(f"""
        SELECT
            t.transaction_id,
            t.merchant_id,
            t.current_status,
            t.amount,
            t.currency,
            t.latest_event_timestamp,
            CASE
                WHEN t.current_status = 'settled'
                     AND EXISTS (
                         SELECT 1 FROM events e
                         WHERE e.transaction_id = t.transaction_id
                           AND e.event_type = 'payment_failed'
                     )
                THEN 'settled_after_failure'

                WHEN t.current_status = 'settled'
                     AND NOT EXISTS (
                         SELECT 1 FROM events e
                         WHERE e.transaction_id = t.transaction_id
                           AND e.event_type = 'payment_processed'
                     )
                THEN 'settled_without_processing'

                WHEN t.current_status = 'payment_initiated'
                     AND NOT EXISTS (
                         SELECT 1 FROM events e
                         WHERE e.transaction_id = t.transaction_id
                           AND e.event_type IN ('payment_processed', 'payment_failed')
                     )
                     AND t.latest_event_timestamp < {time_expr}
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
                AND t.latest_event_timestamp < {time_expr}
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
            # If it has isoformat (Postgres datetime), use it. Otherwise, use the string (SQLite)
            "latest_event_timestamp": row[5].isoformat() if hasattr(row[5], 'isoformat') else row[5],
            "discrepancy_type": row[6],
        }
        for row in result.fetchall()
    ]

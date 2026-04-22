from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from datetime import datetime

from .database import engine, get_db
from .models import Base
from . import schemas, crud
from .schemas import SortField, SortDir


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Creates all tables on startup (idempotent — skips existing tables)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(title="SetuHQ Solutions Engineer Assignment", lifespan=lifespan)


# ── POST /events ──────────────────────────────────────────────────────────────

@app.post("/events", status_code=201)
async def ingest_event(event: schemas.EventCreate, db: AsyncSession = Depends(get_db)):
    result = await crud.process_incoming_event(db, event)
    return result


# ── GET /transactions ─────────────────────────────────────────────────────────

@app.get("/transactions", response_model=List[schemas.TransactionResponse])
async def list_transactions(
    merchant_id: Optional[str] = None,
    status: Optional[str] = None,
    start_date: Optional[datetime] = Query(None, description="Filter by created_at >= start_date (ISO 8601)"),
    end_date: Optional[datetime] = Query(None, description="Filter by created_at <= end_date (ISO 8601)"),
    sort_by: SortField = Query("created_at", description="Field to sort by"),
    sort_dir: SortDir = Query("desc", description="Sort direction: asc or desc"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    return await crud.get_transactions(
        db,
        merchant_id=merchant_id,
        status=status,
        start_date=start_date,
        end_date=end_date,
        sort_by=sort_by,
        sort_dir=sort_dir,
        limit=limit,
        offset=offset,
    )


# ── GET /transactions/{transaction_id} ────────────────────────────────────────

@app.get("/transactions/{transaction_id}", response_model=schemas.TransactionResponse)
async def get_transaction(transaction_id: str, db: AsyncSession = Depends(get_db)):
    txn = await crud.get_transaction_by_id(db, transaction_id)
    if not txn:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return txn


# ── GET /reconciliation/summary ───────────────────────────────────────────────

@app.get("/reconciliation/summary")
async def reconciliation_summary(db: AsyncSession = Depends(get_db)):
    return await crud.get_reconciliation_summary(db)


# ── GET /reconciliation/discrepancies ─────────────────────────────────────────

@app.get("/reconciliation/discrepancies")
async def reconciliation_discrepancies(db: AsyncSession = Depends(get_db)):
    return await crud.get_discrepancies(db)

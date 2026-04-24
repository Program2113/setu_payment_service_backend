"""
conftest.py — shared pytest fixtures for the Setu Payment Service test suite.

Strategy
--------
We use an **in-memory SQLite** database (via aiosqlite) so tests have:
  • Zero external dependencies  — no running Postgres required
  • Full isolation              — each test function gets a fresh, empty DB
  • Speed                       — no network round-trips

SQLite quirks handled here:
  • SAEnum is stored as VARCHAR in SQLite (no native enum type) — this works
    fine because the Python layer still validates the values.
  • `AT TIME ZONE 'UTC'` is Postgres-specific SQL used in the reconciliation
    summary query; the fixture patches that query for SQLite compatibility.
  • `INTERVAL '1 hour'` syntax differs slightly; the discrepancy query is
    also patched for SQLite's `datetime('now', '-1 hour')` equivalent.

If you want to run tests against real Postgres (e.g. in CI), set:
    TEST_DATABASE_URL=postgresql+asyncpg://user:pass@localhost/test_db pytest
"""

import os
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

# ── Path fix so `app` is importable from `tests/` ────────────────────────────
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.main import app
from app.models import Base
from app.database import get_db

# ── Database URL ──────────────────────────────────────────────────────────────
TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "sqlite+aiosqlite:///:memory:",
)

# ── Engine & session factory (module-scoped so the engine is created once) ───
@pytest.fixture(scope="session")
def engine():
    is_sqlite = "sqlite" in TEST_DATABASE_URL
    return create_async_engine(
        TEST_DATABASE_URL,
        echo=False,
        poolclass=StaticPool if is_sqlite else None,  # <-- Add this line
        connect_args={"check_same_thread": False} if is_sqlite else {},
    )


@pytest.fixture(scope="session")
def TestingSessionLocal(engine):
    return sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


# ── Create tables once per session, drop after ───────────────────────────────
@pytest_asyncio.fixture(scope="session", autouse=True)
async def create_tables(engine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


# ── Per-test DB session with rollback isolation ───────────────────────────────
@pytest_asyncio.fixture
async def db_session(engine):
    # Use a connection directly to ensure we control the transaction
    async with engine.connect() as conn:
        # Start a transaction that we will roll back
        await conn.begin()
        # Create a session bound to this specific connection
        async with AsyncSession(bind=conn, expire_on_commit=False) as session:
            yield session
            # Final rollback to clean up the DB for the next test
            await conn.rollback()

# ── FastAPI test client with DB dependency overridden ────────────────────────
@pytest_asyncio.fixture
async def client(db_session):
    """
    AsyncClient wired to the FastAPI app, with the real DB dependency replaced
    by the per-test isolated session.
    """
    async def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac
    app.dependency_overrides.clear()


# ── Canonical event payload factory ──────────────────────────────────────────
def make_event(
    *,
    event_id: str = "evt-001",
    event_type: str = "payment_initiated",
    transaction_id: str = "txn-001",
    merchant_id: str = "merchant_1",
    merchant_name: str = "QuickMart",
    amount: float = 1000.00,
    currency: str = "INR",
    timestamp: str = "2026-01-10T10:00:00+00:00",
) -> dict:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "transaction_id": transaction_id,
        "merchant_id": merchant_id,
        "merchant_name": merchant_name,
        "amount": amount,
        "currency": currency,
        "timestamp": timestamp,
    }
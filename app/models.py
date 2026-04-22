from sqlalchemy import Column, String, Numeric, DateTime, ForeignKey, Index, Enum as SAEnum
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime, timezone
import enum

Base = declarative_base()


class EventType(str, enum.Enum):
    payment_initiated = "payment_initiated"
    payment_processed = "payment_processed"
    payment_failed = "payment_failed"
    settled = "settled"


class Merchant(Base):
    __tablename__ = "merchants"

    merchant_id = Column(String, primary_key=True, index=True)
    merchant_name = Column(String, nullable=False)

    transactions = relationship("Transaction", back_populates="merchant")


class Transaction(Base):
    __tablename__ = "transactions"

    transaction_id = Column(String, primary_key=True, index=True)
    merchant_id = Column(String, ForeignKey("merchants.merchant_id"), nullable=False, index=True)
    amount = Column(Numeric(15, 2), nullable=False)
    currency = Column(String, nullable=False)

    # Materialised view of the true current state — updated via the Chronology Lock
    current_status = Column(SAEnum(EventType), nullable=False, index=True)
    latest_event_timestamp = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )

    merchant = relationship("Merchant", back_populates="transactions")
    events = relationship("Event", back_populates="transaction", order_by="Event.timestamp")

    # Composite indexes for the two most common combined filter patterns
    __table_args__ = (
        Index("ix_transactions_merchant_status", "merchant_id", "current_status"),
        Index("ix_transactions_merchant_created", "merchant_id", "created_at"),
    )


class Event(Base):
    __tablename__ = "events"

    # Primary key acts as the idempotency lock — duplicate event_id inserts are rejected at DB level
    event_id = Column(String, primary_key=True, index=True)
    transaction_id = Column(
        String, ForeignKey("transactions.transaction_id"), nullable=False, index=True
    )
    event_type = Column(SAEnum(EventType), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    ingested_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    transaction = relationship("Transaction", back_populates="events")
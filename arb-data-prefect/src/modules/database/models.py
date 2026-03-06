"""
SQLAlchemy models for market event and market data.

This module defines the database schema for Kalshi and Polymarket events and markets.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Numeric,
    Text,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all database models."""

    pass


class KalshiEvent(Base):
    """Model for Kalshi events."""

    __tablename__ = "kalshi_events"

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sub_category: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    close_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Relationship to markets
    markets: Mapped[list["KalshiMarket"]] = relationship("KalshiMarket", back_populates="event", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<KalshiEvent(event_id='{self.event_id}', title='{self.title[:50]}...')>"


class KalshiMarket(Base):
    """Model for Kalshi markets."""

    __tablename__ = "kalshi_markets"

    market_id: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[str] = mapped_column(Text, ForeignKey("kalshi_events.event_id", ondelete="CASCADE"), nullable=False, index=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    subtitle: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    yes_subtitle: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    no_subtitle: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    rules_primary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    yes_price: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), nullable=True)
    no_price: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), nullable=True)
    volume: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), nullable=True)
    open_interest: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), nullable=True)
    status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Relationship to event
    event: Mapped["KalshiEvent"] = relationship("KalshiEvent", back_populates="markets")

    def __repr__(self) -> str:
        return f"<KalshiMarket(market_id='{self.market_id}', title='{self.title[:50]}...')>"


class PolymarketEvent(Base):
    """Model for Polymarket events."""

    __tablename__ = "polymarket_events"

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    close_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Relationship to markets
    markets: Mapped[list["PolymarketMarket"]] = relationship("PolymarketMarket", back_populates="event", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<PolymarketEvent(event_id='{self.event_id}', title='{self.title[:50]}...')>"


class PolymarketMarket(Base):
    """Model for Polymarket markets."""

    __tablename__ = "polymarket_markets"

    market_id: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[str] = mapped_column(Text, ForeignKey("polymarket_events.event_id", ondelete="CASCADE"), nullable=False, index=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    slug: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    yes_price: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), nullable=True)
    no_price: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), nullable=True)
    liquidity: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), nullable=True)
    volume: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), nullable=True)
    status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Relationship to event
    event: Mapped["PolymarketEvent"] = relationship("PolymarketEvent", back_populates="markets")

    def __repr__(self) -> str:
        return f"<PolymarketMarket(market_id='{self.market_id}', title='{self.title[:50]}...')>"

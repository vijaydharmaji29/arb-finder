"""
Database module for storing market event and market data.

This module provides database-agnostic upsert operations for Kalshi and Polymarket data.
"""

from src.modules.database.connection import get_db_session, init_db
from src.modules.database.upserts import (
    upsert_kalshi_event,
    upsert_kalshi_events_batch,
    upsert_kalshi_market,
    upsert_kalshi_markets_batch,
    upsert_polymarket_event,
    upsert_polymarket_events_batch,
    upsert_polymarket_market,
    upsert_polymarket_markets_batch,
)

__all__ = [
    "get_db_session",
    "init_db",
    "upsert_kalshi_event",
    "upsert_kalshi_events_batch",
    "upsert_kalshi_market",
    "upsert_kalshi_markets_batch",
    "upsert_polymarket_event",
    "upsert_polymarket_events_batch",
    "upsert_polymarket_market",
    "upsert_polymarket_markets_batch",
]


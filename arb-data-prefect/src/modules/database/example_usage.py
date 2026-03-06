"""
Example usage of database upsert functions.

This file demonstrates how to use the upsert functions for Kalshi and Polymarket data.
"""

import asyncio
from datetime import datetime

from src.modules.database import (
    init_db,
    upsert_kalshi_event,
    upsert_kalshi_market,
    upsert_polymarket_event,
    upsert_polymarket_market,
)


async def example_kalshi_upserts():
    """Example of upserting Kalshi events and markets."""
    
    # Example: Upsert a Kalshi event
    event_data = {
        "event_ticker": "PRES-2024",
        "title": "2024 US Presidential Election",
        "subtitle": "Who will win the 2024 US Presidential Election?",
        "status": "open",
        "category": "politics",
        "series_ticker": "PRES",
        "created_time": datetime.now(),
        "expiration_time": datetime(2024, 11, 5, 23, 59, 59),
    }
    await upsert_kalshi_event(event_data)
    
    # Example: Upsert a Kalshi market
    market_data = {
        "market_ticker": "PRES-2024-BIDEN",
        "event_ticker": "PRES-2024",
        "title": "Will Biden win the 2024 US Presidential Election?",
        "subtitle": "YES/NO market on Biden winning",
        "status": "open",
        "yes_bid": 45,  # 45 cents
        "yes_ask": 46,  # 46 cents
        "no_bid": 54,   # 54 cents
        "no_ask": 55,   # 55 cents
        "last_price": 45,  # 45 cents
        "volume": 1000000,
        "open_interest": 5000000,
        "liquidity": 200000,
        "created_time": datetime.now(),
        "expiration_time": datetime(2024, 11, 5, 23, 59, 59),
    }
    await upsert_kalshi_market(market_data)


async def example_polymarket_upserts():
    """Example of upserting Polymarket events and markets."""
    
    # Example: Upsert a Polymarket event
    event_data = {
        "event_id": "0x1234567890abcdef",
        "slug": "2024-us-presidential-election",
        "title": "2024 US Presidential Election",
        "description": "Who will win the 2024 US Presidential Election?",
        "image": "https://example.com/image.jpg",
        "icon": "https://example.com/icon.jpg",
        "active": True,
        "closed": False,
        "archived": False,
        "new": False,
        "featured": True,
        "liquidity": 1000000.0,
        "volume": 5000000.0,
        "end_date_iso": datetime(2024, 11, 5, 23, 59, 59),
    }
    await upsert_polymarket_event(event_data)
    
    # Example: Upsert a Polymarket market
    market_data = {
        "market_id": "0xabcdef1234567890",
        "event_id": "0x1234567890abcdef",
        "question": "Will Biden win the 2024 US Presidential Election?",
        "description": "YES/NO market on Biden winning",
        "image": "https://example.com/market-image.jpg",
        "icon": "https://example.com/market-icon.jpg",
        "slug": "biden-2024-presidential-election",
        "active": True,
        "closed": False,
        "archived": False,
        "new": False,
        "featured": True,
        "liquidity": 500000.0,
        "volume": 2000000.0,
        "end_date_iso": datetime(2024, 11, 5, 23, 59, 59),
    }
    await upsert_polymarket_market(market_data)


async def main():
    """Main function to run examples."""
    # Initialize database (create tables if they don't exist)
    print("Initializing database...")
    await init_db()
    print("Database initialized!")
    
    # Run examples
    print("\nRunning Kalshi upsert examples...")
    await example_kalshi_upserts()
    print("Kalshi upserts completed!")
    
    print("\nRunning Polymarket upsert examples...")
    await example_polymarket_upserts()
    print("Polymarket upserts completed!")
    
    print("\nAll examples completed!")


if __name__ == "__main__":
    # Set DATABASE_URL environment variable before running
    # export DATABASE_URL="postgresql+asyncpg://user:password@host:port/database"
    asyncio.run(main())


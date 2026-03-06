import asyncio
import json
import logging

from prefect import task

from src.modules.database.parsers import parse_kalshi_event, parse_kalshi_market
from src.modules.database.upserts import (
    upsert_kalshi_events_batch,
    upsert_kalshi_markets_batch,
)
from src.modules.kalshi import fetch_kalshi_events

logger = logging.getLogger(__name__)


async def _save_kalshi_events(events):
    """Save Kalshi events and markets to database using batch operations."""
    # Collect all events and markets first
    all_events_data = []
    all_markets_data = []
    
    for event in events:
        try:
            # Parse event
            event_data = parse_kalshi_event(event)
            if event_data.get("event_id") and event_data.get("title"):
                all_events_data.append(event_data)
                
                # Parse markets for this event
                markets = event.get("markets", [])
                for market in markets:
                    try:
                        market_data = parse_kalshi_market(market, event_data["event_id"])
                        if market_data.get("market_id"):
                            all_markets_data.append(market_data)
                    except Exception as e:
                        logger.error(f"Error parsing Kalshi market {market.get('ticker', 'unknown')}: {e}")
            else:
                logger.warning(f"Skipping Kalshi event with missing required fields: {event.get('event_ticker', 'unknown')}")
        except Exception as e:
            logger.error(f"Error parsing Kalshi event {event.get('event_ticker', 'unknown')}: {e}")
    
    # Batch upsert events
    events_inserted, events_updated = await upsert_kalshi_events_batch(all_events_data)
    
    # Batch upsert markets
    markets_inserted, markets_updated = await upsert_kalshi_markets_batch(all_markets_data)
    
    return {
        "events": {
            "inserted": events_inserted,
            "updated": events_updated,
        },
        "markets": {
            "inserted": markets_inserted,
            "updated": markets_updated,
        },
    }


@task(retries=3, retry_delay_seconds=5)
def fetch_kalshi_data():
    """Fetch Kalshi events and save them to the database."""
    events = fetch_kalshi_events()
    print(f"\nFound {len(events)} events\n")
    
    stats = {"events": {"inserted": 0, "updated": 0}, "markets": {"inserted": 0, "updated": 0}}
    
    if events:
        print("Saving events to database...")
        stats = asyncio.run(_save_kalshi_events(events))
        
        # Print detailed statistics for each table
        print("\n" + "=" * 60)
        print("KALSHI DATABASE UPSERT STATISTICS")
        print("=" * 60)
        
        # Kalshi Events table
        events_total = stats["events"]["inserted"] + stats["events"]["updated"]
        print(f"\nkalshi_events table:")
        print(f"  • New rows inserted: {stats['events']['inserted']}")
        print(f"  • Existing rows updated: {stats['events']['updated']}")
        print(f"  • Total upserted: {events_total}")
        
        # Kalshi Markets table
        markets_total = stats["markets"]["inserted"] + stats["markets"]["updated"]
        print(f"\nkalshi_markets table:")
        print(f"  • New rows inserted: {stats['markets']['inserted']}")
        print(f"  • Existing rows updated: {stats['markets']['updated']}")
        print(f"  • Total upserted: {markets_total}")
        
        print("\n" + "=" * 60)
        
        # Print sample events
        print("\nSample events:")
        print("-" * 60)
        for event in events[:5]:
            print(f"Event Ticker: {event.get('event_ticker', 'N/A')}")
            print(f"Title: {event.get('title', 'N/A')}")
            print(f"Markets: {len(event.get('markets', []))}")
            print(f"Status: {event.get('status', 'N/A')}")
            print("-" * 60)
        
        print("\nFull JSON for first event:")
        print(json.dumps(events[0], indent=2))
    
    return {
        "events_fetched": len(events),
        "kalshi_events": stats["events"],
        "kalshi_markets": stats["markets"],
    }
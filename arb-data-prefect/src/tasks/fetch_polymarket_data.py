import asyncio
import json
import logging

from prefect import task

from src.modules.database.parsers import parse_polymarket_event, parse_polymarket_market
from src.modules.database.upserts import (
    upsert_polymarket_events_batch,
    upsert_polymarket_markets_batch,
)
from src.modules.polymarket import fetch_polymarket_events

logger = logging.getLogger(__name__)


async def _save_polymarket_events(events):
    """Save Polymarket events and markets to database using batch operations."""
    # Collect all events and markets first
    all_events_data = []
    all_markets_data = []
    
    for event in events:
        try:
            # Parse event
            event_data = parse_polymarket_event(event)
            if event_data.get("event_id") and event_data.get("title"):
                all_events_data.append(event_data)
                
                # Parse markets for this event
                markets = event.get("markets", [])
                for market in markets:
                    try:
                        market_data = parse_polymarket_market(market, event_data["event_id"])
                        if market_data.get("market_id"):
                            all_markets_data.append(market_data)
                    except Exception as e:
                        logger.error(f"Error parsing Polymarket market {market.get('id', 'unknown')}: {e}")
            else:
                logger.warning(f"Skipping Polymarket event with missing required fields: {event.get('id', 'unknown')}")
        except Exception as e:
            logger.error(f"Error parsing Polymarket event {event.get('id', 'unknown')}: {e}")
    
    # Batch upsert events
    events_inserted, events_updated = await upsert_polymarket_events_batch(all_events_data)
    
    # Batch upsert markets
    markets_inserted, markets_updated = await upsert_polymarket_markets_batch(all_markets_data)
    
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
def fetch_polymarket_data():
    """Fetch Polymarket events and save them to the database."""
    events = fetch_polymarket_events()
    print(f"\nFound {len(events)} events\n")
    
    stats = {"events": {"inserted": 0, "updated": 0}, "markets": {"inserted": 0, "updated": 0}}
    
    if events:
        print("Saving events to database...")
        stats = asyncio.run(_save_polymarket_events(events))
        
        # Print detailed statistics for each table
        print("\n" + "=" * 60)
        print("POLYMARKET DATABASE UPSERT STATISTICS")
        print("=" * 60)
        
        # Polymarket Events table
        events_total = stats["events"]["inserted"] + stats["events"]["updated"]
        print(f"\npolymarket_events table:")
        print(f"  • New rows inserted: {stats['events']['inserted']}")
        print(f"  • Existing rows updated: {stats['events']['updated']}")
        print(f"  • Total upserted: {events_total}")
        
        # Polymarket Markets table
        markets_total = stats["markets"]["inserted"] + stats["markets"]["updated"]
        print(f"\npolymarket_markets table:")
        print(f"  • New rows inserted: {stats['markets']['inserted']}")
        print(f"  • Existing rows updated: {stats['markets']['updated']}")
        print(f"  • Total upserted: {markets_total}")
        
        print("\n" + "=" * 60)
        
        print("\nFull JSON for first event:")
        print(json.dumps(events[0], indent=2))
    
    return {
        "events_fetched": len(events),
        "polymarket_events": stats["events"],
        "polymarket_markets": stats["markets"],
    }

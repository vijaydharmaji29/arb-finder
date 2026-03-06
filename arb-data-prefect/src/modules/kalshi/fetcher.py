"""
Kalshi API fetcher for all events.

This module fetches all Kalshi events and returns raw JSON data.

Running standalone:
    python -m modules.kalshi.fetcher
"""

from typing import Any, Dict, List, Optional, Tuple

from src.modules.utils.api import api_get


# Kalshi API configuration
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
MAX_PAGES = 100  # Safety limit for pagination


def _fetch_all_kalshi_events() -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Fetch ALL Kalshi events with nested markets using cursor pagination.
    
    Returns:
        Tuple of (events_list, error_message)
    """
    all_events: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    
    for page in range(MAX_PAGES):
        params: Dict[str, Any] = {
            "with_nested_markets": "true",
            "status": "open",
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor
        
        data, error = api_get(f"{KALSHI_BASE_URL}/events", params=params)
        
        if error:
            if all_events:
                # Return partial results with warning
                return all_events, f"Partial fetch: {error}"
            return [], error
        
        if not isinstance(data, dict):
            break
        
        events = data.get("events", [])
        if not events:
            break
        
        all_events.extend(events)
        
        cursor = data.get("cursor")
        if not cursor:
            break
    
    return all_events, None


def fetch_kalshi_events() -> List[Dict[str, Any]]:
    """
    Fetch all Kalshi events and return raw JSON data.
    
    This function:
    1. Fetches all open Kalshi events via pagination
    2. Returns raw JSON event data with nested markets
    
    Returns:
        List of raw event dictionaries from the Kalshi API
        
    Example:
        >>> events = fetch_kalshi_events()
        >>> print(f"Found {len(events)} Kalshi events")
        >>> for event in events[:5]:
        ...     print(f"  {event.get('event_ticker')}: {event.get('title')}")
    """
    raw_events, error = _fetch_all_kalshi_events()
    
    if error and not raw_events:
        print(f"Error fetching Kalshi events: {error}")
        return []
    
    if error:
        print(f"Warning: {error}")
    
    return raw_events


# Allow running as standalone script for testing
if __name__ == "__main__":
    import json
    
    print("Fetching all Kalshi events...")
    events = fetch_kalshi_events()
    
    print(f"\nFound {len(events)} events\n")
    print("Sample events:")
    print("-" * 60)
    
    for event in events[:5]:
        print(f"Event Ticker: {event.get('event_ticker', 'N/A')}")
        print(f"Title: {event.get('title', 'N/A')}")
        print(f"Markets: {len(event.get('markets', []))}")
        print(f"Status: {event.get('status', 'N/A')}")
        print("-" * 60)
    
    # Optionally print full JSON for first event
    if events:
        print("\nFull JSON for first event:")
        print(json.dumps(events[0], indent=2))



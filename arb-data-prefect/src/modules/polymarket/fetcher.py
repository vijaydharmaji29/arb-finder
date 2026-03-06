"""
Polymarket API fetcher for all events.

This module fetches all Polymarket events and returns raw JSON data.

Running standalone:
    python -m modules.polymarket.fetcher
"""

from typing import Any, Dict, List, Optional, Tuple

from src.modules.utils.api import api_get


# Polymarket Gamma API configuration
POLYMARKET_BASE_URL = "https://gamma-api.polymarket.com"
MAX_PAGES = 100  # Safety limit for pagination
PAGE_SIZE = 100


def _fetch_all_polymarket_events() -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Fetch ALL Polymarket events using offset pagination.
    
    Returns:
        Tuple of (events_list, error_message)
    """
    all_events: List[Dict[str, Any]] = []
    offset = 0
    
    for page in range(MAX_PAGES):
        params: Dict[str, Any] = {
            "order": "id",
            "ascending": "false",
            "closed": "false",  # Only active events
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        
        data, error = api_get(f"{POLYMARKET_BASE_URL}/events", params=params)
        
        if error:
            if all_events:
                return all_events, f"Partial fetch: {error}"
            return [], error
        
        if not isinstance(data, list):
            break
        
        if not data:
            break
        
        all_events.extend(data)
        
        # If we got fewer than limit, we've reached the end
        if len(data) < PAGE_SIZE:
            break
        
        offset += PAGE_SIZE
    
    return all_events, None


def fetch_polymarket_events() -> List[Dict[str, Any]]:
    """
    Fetch all Polymarket events and return raw JSON data.
    
    This function:
    1. Fetches all open Polymarket events via pagination
    2. Returns raw JSON event data with nested markets
    
    Returns:
        List of raw event dictionaries from the Polymarket API
        
    Example:
        >>> events = fetch_polymarket_events()
        >>> print(f"Found {len(events)} Polymarket events")
        >>> for event in events[:5]:
        ...     print(f"  {event.get('id')}: {event.get('title')}")
    """
    raw_events, error = _fetch_all_polymarket_events()
    
    if error and not raw_events:
        print(f"Error fetching Polymarket events: {error}")
        return []
    
    if error:
        print(f"Warning: {error}")
    
    return raw_events


# Allow running as standalone script for testing
if __name__ == "__main__":
    import json
    
    print("Fetching all Polymarket events...")
    events = fetch_polymarket_events()
    
    print(f"\nFound {len(events)} events\n")
    print("Sample events:")
    print("-" * 60)
    
    for event in events[:5]:
        print(f"Event ID: {event.get('id', 'N/A')}")
        print(f"Title: {event.get('title', 'N/A')}")
        print(f"Slug: {event.get('slug', 'N/A')}")
        print(f"Markets: {len(event.get('markets', []))}")
        print(f"Closed: {event.get('closed', 'N/A')}")
        print("-" * 60)
    
    # Optionally print full JSON for first event
    if events:
        print("\nFull JSON for first event:")
        print(json.dumps(events[0], indent=2))


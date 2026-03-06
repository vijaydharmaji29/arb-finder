"""
Polymarket Data Fetch and Processing Module
===========================================

Fetches events from Polymarket Gamma API and returns raw JSON data.

Usage:
    from modules.polymarket import fetch_polymarket_events
    
    # Get all Polymarket events (raw JSON)
    events = fetch_polymarket_events()
    
    # Events is a list of raw event dictionaries
    for event in events:
        print(f"{event.get('id')}: {event.get('title')}")
"""

from src.modules.polymarket.fetcher import fetch_polymarket_events

__all__ = ["fetch_polymarket_events"]


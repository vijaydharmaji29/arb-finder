"""
Kalshi Data Fetch and Processing Module
=======================================

Fetches events from Kalshi API and returns raw JSON data.

Usage:
    from modules.kalshi import fetch_kalshi_events
    
    # Get all Kalshi events (raw JSON)
    events = fetch_kalshi_events()
    
    # Events is a list of raw event dictionaries
    for event in events:
        print(f"{event.get('event_ticker')}: {event.get('title')}")
"""

from src.modules.kalshi.fetcher import fetch_kalshi_events

__all__ = ["fetch_kalshi_events"]


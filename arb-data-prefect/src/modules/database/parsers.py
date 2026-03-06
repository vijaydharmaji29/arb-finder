"""
Parser functions to convert raw API JSON data into parsed fields for database upserts.

These functions extract and normalize data from the raw API responses.
"""

from typing import Any, Dict, Optional


def _round_to_2_decimals(value: Optional[float]) -> Optional[float]:
    """Round a numeric value to 2 decimal places."""
    if value is None:
        return None
    try:
        return round(float(value), 2)
    except (ValueError, TypeError):
        return None


def parse_kalshi_event(raw_event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse raw Kalshi event JSON into fields for upsert_kalshi_event.
    
    Args:
        raw_event: Raw event dictionary from Kalshi API
        
    Returns:
        Dictionary with parsed event fields
    """
    return {
        "event_id": raw_event.get("event_ticker", ""),  # Map event_ticker to event_id
        "title": raw_event.get("title", ""),
        "category": raw_event.get("category"),
        "sub_category": raw_event.get("sub_category") or raw_event.get("subcategory"),
        "close_time": raw_event.get("expiration_time") or raw_event.get("close_time"),
        "status": raw_event.get("status"),
    }


def parse_kalshi_market(raw_market: Dict[str, Any], event_id: str) -> Dict[str, Any]:
    """
    Parse raw Kalshi market JSON into fields for upsert_kalshi_market.
    
    Args:
        raw_market: Raw market dictionary from Kalshi API
        event_id: Associated event ID (from event_ticker)
        
    Returns:
        Dictionary with parsed market fields
    """
    # Calculate prices from bid_dollars fields
    # no_price = 1 - yes_bid_dollars
    # yes_price = 1 - no_bid_dollars
    yes_bid_dollars = raw_market.get("yes_bid_dollars")
    no_bid_dollars = raw_market.get("no_bid_dollars")
    
    # Calculate prices
    yes_price = None
    no_price = None
    
    if no_bid_dollars is not None:
        # yes_price = 1 - no_bid_dollars
        try:
            yes_price = 1.0 - float(no_bid_dollars)
        except (ValueError, TypeError):
            pass
    
    if yes_bid_dollars is not None:
        # no_price = 1 - yes_bid_dollars
        try:
            no_price = 1.0 - float(yes_bid_dollars)
        except (ValueError, TypeError):
            pass
    
    return {
        "market_id": raw_market.get("ticker", ""),  # Map ticker to market_id
        "event_id": event_id,
        "title": raw_market.get("title", ""),
        "subtitle": raw_market.get("subtitle"),
        "yes_subtitle": raw_market.get("yes_sub_title"),
        "no_subtitle": raw_market.get("no_sub_title"),  # Map no_sub_title to no_subtitle
        "rules_primary": raw_market.get("rules_primary"),
        "yes_price": _round_to_2_decimals(yes_price),
        "no_price": _round_to_2_decimals(no_price),
        "volume": _round_to_2_decimals(raw_market.get("volume")),
        "open_interest": _round_to_2_decimals(raw_market.get("open_interest")),
        "status": raw_market.get("status"),
    }


def parse_polymarket_event(raw_event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse raw Polymarket event JSON into fields for upsert_polymarket_event.
    
    Args:
        raw_event: Raw event dictionary from Polymarket API
        
    Returns:
        Dictionary with parsed event fields
    """
    return {
        "event_id": str(raw_event.get("id", "")),
        "title": raw_event.get("title", ""),
        "category": raw_event.get("category"),
        "close_time": raw_event.get("endDateISO") or raw_event.get("close_time"),
        "status": "closed" if raw_event.get("closed") else "open" if raw_event.get("active") else None,
    }


def parse_polymarket_market(raw_market: Dict[str, Any], event_id: str) -> Dict[str, Any]:
    """
    Parse raw Polymarket market JSON into fields for upsert_polymarket_market.
    
    Args:
        raw_market: Raw market dictionary from Polymarket API
        event_id: Associated event ID
        
    Returns:
        Dictionary with parsed market fields
    """
    # Extract prices - Polymarket typically provides prices as decimals (0.0-1.0)
    # Look for various price fields
    yes_price = raw_market.get("bestAsk")
    no_price = 1 - float(raw_market.get("bestBid")) if raw_market.get("bestBid") is not None else None
        
    # If we only have yes price, calculate no price
    if yes_price and not no_price:
        try:
            yes_val = float(yes_price)
            if 0 <= yes_val <= 1:
                no_price = 1.0 - yes_val
        except (ValueError, TypeError):
            pass
    
    return {
        "market_id": str(raw_market.get("id", "")),
        "event_id": event_id,
        "title": raw_market.get("question"),  # Use question as title
        "description": raw_market.get("description"),
        "slug": raw_market.get("slug"),
        "yes_price": _round_to_2_decimals(float(yes_price) if yes_price is not None else None),
        "no_price": _round_to_2_decimals(float(no_price) if no_price is not None else None),
        "liquidity": _round_to_2_decimals(raw_market.get("liquidity")),
        "volume": _round_to_2_decimals(raw_market.get("volume")),
        "status": "closed" if raw_market.get("closed") else "open" if raw_market.get("active") else None,
    }

"""
Shared type definitions for market events.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any


@dataclass
class MarketEvent:
    """
    Normalized market event structure used across all modules.
    
    Attributes:
        event_id: Unique identifier for the event
        event_title: Cleaned/normalized event title
        market_id: Associated market identifier
        source: Data source ("kalshi" or "polymarket")
        price_yes: Display-friendly YES price (e.g. "45¢" or "45.0%")
        price_no: Display-friendly NO price
        raw_data: Original API response data (optional)
    """
    event_id: str
    event_title: str
    market_id: str
    source: str
    price_yes: Optional[str] = None
    price_no: Optional[str] = None
    raw_data: Optional[Dict[str, Any]] = field(default=None, repr=False)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "event_id": self.event_id,
            "event_title": self.event_title,
            "market_id": self.market_id,
            "source": self.source,
            "price_yes": self.price_yes,
            "price_no": self.price_no,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MarketEvent":
        """Create MarketEvent from dictionary."""
        return cls(
            event_id=data["event_id"],
            event_title=data["event_title"],
            market_id=data["market_id"],
            source=data["source"],
            price_yes=data.get("price_yes"),
            price_no=data.get("price_no"),
            raw_data=data.get("raw_data"),
        )


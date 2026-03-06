"""Data fetching module for kalshi_markets and polymarket_markets tables with event details."""
from typing import List, Dict, Optional
from db_connection import DatabaseConnection


class DataFetcher:
    """Fetches data from markets tables and joins with events tables to get title and category."""
    
    def __init__(self, db_connection: DatabaseConnection):
        """
        Initialize data fetcher.
        
        Args:
            db_connection: DatabaseConnection instance
        """
        self.db = db_connection
    
    def fetch_kalshi_markets(self) -> List[Dict]:
        """
        Fetch rows from kalshi_markets where embedding_created = False
        AND extracted_text is NOT NULL (must have been processed by extract_text_flow first),
        joined with kalshi_events to get title, description, and category.
        
        Returns:
            List of dictionaries with all market data plus title, description, and category from events
        """
        query = """
            SELECT 
                km.*,
                ke.title,
                km.rules_primary as description,
                ke.category
            FROM kalshi_markets km
            INNER JOIN kalshi_events ke ON km.event_id = ke.event_id
            WHERE km.embedding_created = FALSE
            AND km.extracted_text IS NOT NULL
            AND km.status = 'active'
        """
        return self.db.execute_query(query)
    
    def fetch_polymarket_markets(self) -> List[Dict]:
        """
        Fetch rows from polymarket_markets where embedding_created = False
        AND extracted_text is NOT NULL (must have been processed by extract_text_flow first),
        joined with polymarket_events to get title, description, and category.
        
        Returns:
            List of dictionaries with all market data plus title, description, and category from events
        """
        query = """
            SELECT 
                pm.*,
                pe.title,
                pe.category
            FROM polymarket_markets pm
            INNER JOIN polymarket_events pe ON pm.event_id = pe.event_id
            WHERE pm.embedding_created = FALSE 
            AND pm.extracted_text IS NOT NULL
            AND pm.status = 'open'
        """
        return self.db.execute_query(query)
    
    def fetch_all(self) -> Dict[str, List[Dict]]:
        """
        Fetch data from both markets tables with event details.
        
        Returns:
            Dictionary with 'kalshi_markets' and 'polymarket_markets' keys
        """
        return {
            'kalshi_markets': self.fetch_kalshi_markets(),
            'polymarket_markets': self.fetch_polymarket_markets()
        }


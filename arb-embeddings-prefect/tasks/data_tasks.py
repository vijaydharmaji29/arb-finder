"""Data fetching tasks for Prefect."""
from prefect import task
from prefect.cache_policies import NO_CACHE
from typing import Dict, List
from db_connection import DatabaseConnection
from data_fetcher import DataFetcher


@task(name="fetch_markets_data", log_prints=True, cache_policy=NO_CACHE)
def fetch_markets_data(db_connection: DatabaseConnection) -> Dict[str, List[Dict]]:
    """
    Fetch market data from database.
    
    Args:
        db_connection: DatabaseConnection instance
        
    Returns:
        Dictionary with 'kalshi_markets' and 'polymarket_markets' keys
    """
    print("Fetching market data from database...")
    fetcher = DataFetcher(db_connection)
    results = fetcher.fetch_all()
    
    print(f"Found {len(results['kalshi_markets'])} kalshi markets")
    print(f"Found {len(results['polymarket_markets'])} polymarket markets")
    
    return results


import asyncio

from prefect import flow

from src.modules.database import init_db
from src.tasks.fetch_kalshi_data import fetch_kalshi_data
from src.tasks.fetch_polymarket_data import fetch_polymarket_data


@flow(name="fetch-all-market-data")
def fetch_all_data():
    """Flow to fetch data from both Kalshi and Polymarket."""
    print("Starting data fetch from all markets...")
    
    # Initialize database tables if they don't exist
    print("Initializing database tables...")
    try:
        asyncio.run(init_db())
        print("Database tables initialized successfully!")
    except Exception as e:
        print(f"Warning: Database initialization failed: {e}")
        print("Continuing anyway - tables may already exist...")
    
    # Fetch data from both markets
    kalshi_result = fetch_kalshi_data()
    polymarket_result = fetch_polymarket_data()
    
    print("\nCompleted fetching data from all markets!")
    return {
        "kalshi": kalshi_result,
        "polymarket": polymarket_result
    }


if __name__ == "__main__":
    fetch_all_data()


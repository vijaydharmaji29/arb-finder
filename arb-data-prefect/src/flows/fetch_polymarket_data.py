import asyncio

from prefect import flow

from src.modules.database import init_db
from src.tasks.fetch_polymarket_data import fetch_polymarket_data


@flow(name="fetch-polymarket-data")
def fetch_polymarket_data_flow():
    """Flow to fetch data from Polymarket."""
    print("Starting data fetch from Polymarket...")
    
    # Initialize database tables if they don't exist
    # print("Initializing database tables...")
    # try:
    #     asyncio.run(init_db())
    #     print("Database tables initialized successfully!")
    # except Exception as e:
    #     print(f"Warning: Database initialization failed: {e}")
    #     print("Continuing anyway - tables may already exist...")
    
    # Fetch data from Polymarket
    polymarket_result = fetch_polymarket_data()
    
    print("\nCompleted fetching data from Polymarket!")
    return polymarket_result


if __name__ == "__main__":
    fetch_polymarket_data_flow()


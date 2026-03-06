"""Database connection tasks for Prefect."""
from prefect import task
from prefect.cache_policies import NO_CACHE
from typing import Optional
import os
from pathlib import Path
from dotenv import load_dotenv
from db_connection import DatabaseConnection

# Load .env file from project root
project_root = Path(__file__).parent.parent
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)


@task(name="get_db_connection", log_prints=True, cache_policy=NO_CACHE)
def get_db_connection_task(db_url: Optional[str] = None) -> DatabaseConnection:
    """
    Create and return a database connection.
    
    Args:
        db_url: Optional database URL. If None, reads from DATABASE_URL env var.
        
    Returns:
        DatabaseConnection instance
    """
    if not db_url:
        db_url = os.getenv('DATABASE_URL') or os.getenv('DB_URL')
    
    if not db_url:
        raise ValueError(
            "Database URL must be provided either as argument or "
            "DATABASE_URL/DB_URL environment variable"
        )
    
    print(f"Connecting to database...")
    db = DatabaseConnection(db_url)
    db.connect()
    return db


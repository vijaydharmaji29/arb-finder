"""
Database connection setup for PostgreSQL (Neon).

This module provides database-agnostic connection management using SQLAlchemy.
"""

import os
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional, Dict, Any, Tuple

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src.modules.database.models import Base

# Try to load .env file from src/ directory if python-dotenv is available
try:
    from dotenv import load_dotenv
    # Get the src/ directory (parent of modules/)
    src_dir = Path(__file__).parent.parent.parent
    env_path = src_dir / ".env"
    load_dotenv(dotenv_path=env_path)
except ImportError:
    # python-dotenv not installed, skip .env loading
    pass


def get_database_url() -> Tuple[str, Dict[str, Any]]:
    """
    Get database connection URL from environment variable and extract SSL parameters.
    
    Handles Neon-specific connection parameters by:
    - Removing unsupported parameters (sslmode, channel_binding, etc.) from URL
    - Automatically enabling SSL for Neon databases (required by Neon)
    - Converting SSL settings to asyncpg-compatible format
    
    Expected format: postgresql+asyncpg://user:password@host:port/database
    For Neon: postgresql+asyncpg://user:password@ep-xxx-xxx.region.aws.neon.tech/database?sslmode=require&channel_binding=require
    
    Note: Neon connection strings may include sslmode=require and channel_binding=require,
    which will be automatically removed and converted to asyncpg-compatible SSL settings.
    
    Returns:
        Tuple of (database_url, connect_args) where:
        - database_url: Cleaned URL without unsupported parameters
        - connect_args: Dictionary with SSL settings for asyncpg
        
    Raises:
        ValueError: If DATABASE_URL is not set
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError(
            "DATABASE_URL environment variable is not set. "
            "Please set it to your PostgreSQL connection string, e.g., "
            "postgresql+asyncpg://user:password@host:port/database"
        )
    
    # Ensure we're using asyncpg driver for PostgreSQL
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    elif not database_url.startswith("postgresql+asyncpg://"):
        raise ValueError(
            "DATABASE_URL must be a PostgreSQL connection string. "
            "Expected format: postgresql+asyncpg://user:password@host:port/database"
        )
    
    # Parse URL to extract query parameters
    parsed = urlparse(database_url)
    query_params = parse_qs(parsed.query)
    
    # Handle SSL and Neon-specific parameters
    # asyncpg doesn't accept sslmode, channel_binding, or other psycopg2-style parameters
    connect_args: Dict[str, Any] = {}
    
    # Check if this is a Neon database (by hostname)
    is_neon = "neon.tech" in parsed.hostname if parsed.hostname else False
    
    # Extract and remove Neon/PostgreSQL parameters that asyncpg doesn't support
    ssl_mode = query_params.get("sslmode", [None])[0]
    channel_binding = query_params.get("channel_binding", [None])[0]
    
    # Remove unsupported parameters from query string
    unsupported_params = ["sslmode", "channel_binding", "connect_timeout", "application_name"]
    for param in unsupported_params:
        query_params.pop(param, None)
    
    # Configure SSL for asyncpg
    # Neon requires SSL/TLS encryption, so always enable it for Neon databases
    if is_neon:
        # Neon requires SSL - use True for basic SSL connection
        connect_args["ssl"] = True
    elif ssl_mode:
        # For non-Neon databases, respect sslmode setting
        if ssl_mode in ["require", "prefer", "allow", "verify-ca", "verify-full"]:
            connect_args["ssl"] = True
        elif ssl_mode == "disable":
            connect_args["ssl"] = False
        else:
            # Default to True for any other SSL mode
            connect_args["ssl"] = True
    
    # Rebuild URL without unsupported parameters
    if query_params:
        new_query = urlencode(query_params, doseq=True)
        new_parsed = parsed._replace(query=new_query)
        database_url = urlunparse(new_parsed)
    else:
        # Remove query string entirely if no params left
        new_parsed = parsed._replace(query="")
        database_url = urlunparse(new_parsed)
    
    return database_url, connect_args


# Global engine and session factory
_engine = None
_async_session_maker: Optional[async_sessionmaker[AsyncSession]] = None


def get_engine():
    """Get or create the database engine."""
    global _engine
    if _engine is None:
        database_url, connect_args = get_database_url()
        _engine = create_async_engine(
            database_url,
            poolclass=NullPool,  # Neon works well with NullPool
            echo=False,  # Set to True for SQL query logging
            connect_args=connect_args,  # Always pass dict (empty or with SSL settings)
        )
    return _engine


def get_async_session_maker() -> async_sessionmaker[AsyncSession]:
    """Get or create the async session maker."""
    global _async_session_maker
    if _async_session_maker is None:
        engine = get_engine()
        _async_session_maker = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _async_session_maker


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Get a database session context manager.
    
    Usage:
        async with get_db_session() as session:
            # Use session here
            await session.commit()
    """
    async_session_maker = get_async_session_maker()
    async with async_session_maker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db() -> None:
    """
    Initialize the database by creating all tables and adding missing columns.
    
    This function:
    1. Creates all tables if they don't exist
    2. Adds missing columns to existing tables
    
    This should be called once at application startup.
    """
    from sqlalchemy import text
    
    engine = get_engine()
    async with engine.begin() as conn:
        # Create all tables if they don't exist
        await conn.run_sync(Base.metadata.create_all)
        
        # Add missing columns to existing tables (for schema migrations)
        # Kalshi markets - add new columns if they don't exist
        await conn.execute(text("""
            DO $$ 
            BEGIN
                -- Add subtitle column if it doesn't exist
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'kalshi_markets' AND column_name = 'subtitle'
                ) THEN
                    ALTER TABLE kalshi_markets ADD COLUMN subtitle TEXT;
                END IF;
                
                -- Add yes_subtitle column if it doesn't exist
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'kalshi_markets' AND column_name = 'yes_subtitle'
                ) THEN
                    ALTER TABLE kalshi_markets ADD COLUMN yes_subtitle TEXT;
                END IF;
                
                -- Add no_subtitle column if it doesn't exist
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'kalshi_markets' AND column_name = 'no_subtitle'
                ) THEN
                    ALTER TABLE kalshi_markets ADD COLUMN no_subtitle TEXT;
                END IF;
                
                -- Add rules_primary column if it doesn't exist
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'kalshi_markets' AND column_name = 'rules_primary'
                ) THEN
                    ALTER TABLE kalshi_markets ADD COLUMN rules_primary TEXT;
                END IF;
                
                -- Add description column to polymarket_markets if it doesn't exist
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'polymarket_markets' AND column_name = 'description'
                ) THEN
                    ALTER TABLE polymarket_markets ADD COLUMN description TEXT;
                END IF;
                
                -- Drop raw_json column from kalshi_markets if it exists
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'kalshi_markets' AND column_name = 'raw_json'
                ) THEN
                    ALTER TABLE kalshi_markets DROP COLUMN raw_json;
                END IF;
                
                -- Drop raw_json column from polymarket_markets if it exists
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'polymarket_markets' AND column_name = 'raw_json'
                ) THEN
                    ALTER TABLE polymarket_markets DROP COLUMN raw_json;
                END IF;
                
                -- Update numeric columns to 2 decimal precision
                -- Kalshi markets numeric columns
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'kalshi_markets' AND column_name = 'yes_price'
                ) THEN
                    ALTER TABLE kalshi_markets 
                    ALTER COLUMN yes_price TYPE NUMERIC(10, 2) USING CASE WHEN yes_price IS NULL THEN NULL ELSE ROUND(yes_price::NUMERIC, 2) END,
                    ALTER COLUMN no_price TYPE NUMERIC(10, 2) USING CASE WHEN no_price IS NULL THEN NULL ELSE ROUND(no_price::NUMERIC, 2) END,
                    ALTER COLUMN volume TYPE NUMERIC(10, 2) USING CASE WHEN volume IS NULL THEN NULL ELSE ROUND(volume::NUMERIC, 2) END,
                    ALTER COLUMN open_interest TYPE NUMERIC(10, 2) USING CASE WHEN open_interest IS NULL THEN NULL ELSE ROUND(open_interest::NUMERIC, 2) END;
                END IF;
                
                -- Polymarket markets numeric columns
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'polymarket_markets' AND column_name = 'yes_price'
                ) THEN
                    ALTER TABLE polymarket_markets 
                    ALTER COLUMN yes_price TYPE NUMERIC(10, 2) USING CASE WHEN yes_price IS NULL THEN NULL ELSE ROUND(yes_price::NUMERIC, 2) END,
                    ALTER COLUMN no_price TYPE NUMERIC(10, 2) USING CASE WHEN no_price IS NULL THEN NULL ELSE ROUND(no_price::NUMERIC, 2) END,
                    ALTER COLUMN liquidity TYPE NUMERIC(10, 2) USING CASE WHEN liquidity IS NULL THEN NULL ELSE ROUND(liquidity::NUMERIC, 2) END,
                    ALTER COLUMN volume TYPE NUMERIC(10, 2) USING CASE WHEN volume IS NULL THEN NULL ELSE ROUND(volume::NUMERIC, 2) END;
                END IF;
            END $$;
        """))
    
    await engine.dispose()


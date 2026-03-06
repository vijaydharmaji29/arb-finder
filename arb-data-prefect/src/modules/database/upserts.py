"""
Database upsert functions for market events and markets.

These functions are idempotent and safe to run multiple times per flow.
Each function accepts parsed fields (not raw JSON) and logs whether a row was inserted or updated.

Batch functions are available for better performance when inserting many records.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert

from src.modules.database.connection import get_db_session
from src.modules.database.models import KalshiEvent, KalshiMarket, PolymarketEvent, PolymarketMarket

logger = logging.getLogger(__name__)

# Batch size for temp table inserts (stays under PostgreSQL's 32,767 query arg limit)
BATCH_SIZE = 1500



def _parse_datetime(value: Any) -> Optional[datetime]:
    """Parse datetime from various formats."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            # Try ISO format first
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            try:
                # Try common formats
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
            except Exception:
                pass
    return None


async def upsert_kalshi_event(event_data: Dict[str, Any]) -> None:
    """
    Upsert a Kalshi event.
    
    Inserts when missing, updates when present. Accepts parsed fields (not raw JSON).
    Logs whether a row was inserted or updated.
    
    Args:
        event_data: Dictionary with parsed event fields:
            - event_id (str, required): Unique event identifier
            - title (str, required): Event title
            - category (str, optional): Event category
            - sub_category (str, optional): Event sub-category
            - close_time (datetime or str, optional): Close time
            - status (str, optional): Event status
    """
    if "event_id" not in event_data or "title" not in event_data:
        raise ValueError("event_data must contain 'event_id' and 'title' fields")
    
    async with get_db_session() as session:
        # Prepare data for upsert
        upsert_data = {
            "event_id": event_data["event_id"],
            "title": event_data["title"],
            "category": event_data.get("category"),
            "sub_category": event_data.get("sub_category"),
            "close_time": _parse_datetime(event_data.get("close_time")),
            "status": event_data.get("status"),
        }
        
        # Check if record exists before upsert to determine insert vs update
        result = await session.execute(
            select(KalshiEvent.event_id).where(KalshiEvent.event_id == upsert_data["event_id"])
        )
        existing_id = result.scalar_one_or_none()
        is_update = existing_id is not None
        
        # Use PostgreSQL ON CONFLICT for atomic upsert
        # Only update if any field has changed
        stmt = insert(KalshiEvent).values(**upsert_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["event_id"],
            set_={
                "title": upsert_data["title"],
                "category": upsert_data["category"],
                "sub_category": upsert_data["sub_category"],
                "close_time": upsert_data["close_time"],
                "status": upsert_data["status"],
                "updated_at": func.now(),
            },
            where=(
                (stmt.excluded.title.is_distinct_from(KalshiEvent.title)) |
                (stmt.excluded.category.is_distinct_from(KalshiEvent.category)) |
                (stmt.excluded.sub_category.is_distinct_from(KalshiEvent.sub_category)) |
                (stmt.excluded.close_time.is_distinct_from(KalshiEvent.close_time)) |
                (stmt.excluded.status.is_distinct_from(KalshiEvent.status))
            )
        )
        await session.execute(stmt)
        await session.commit()
        
        if is_update:
            logger.info(f"Updated Kalshi event: {upsert_data['event_id']}")
        else:
            logger.info(f"Inserted Kalshi event: {upsert_data['event_id']}")


async def upsert_kalshi_market(market_data: Dict[str, Any]) -> None:
    """
    Upsert a Kalshi market.
    
    Inserts when missing, updates when present. Accepts parsed fields (not raw JSON).
    Logs whether a row was inserted or updated.
    
    Args:
        market_data: Dictionary with parsed market fields:
            - market_id (str, required): Unique market identifier
            - event_id (str, required): Associated event ID
            - title (str, required): Market title
            - subtitle (str, optional): Market subtitle
            - yes_subtitle (str, optional): YES subtitle
            - no_subtitle (str, optional): NO subtitle
            - rules_primary (str, optional): Primary rules text
            - yes_price (float, optional): YES price (0.0-1.0)
            - no_price (float, optional): NO price (0.0-1.0)
            - volume (float, optional): Trading volume
            - open_interest (float, optional): Open interest
            - status (str, optional): Market status
    """
    if "market_id" not in market_data or "event_id" not in market_data or "title" not in market_data:
        raise ValueError("market_data must contain 'market_id', 'event_id', and 'title' fields")
    
    async with get_db_session() as session:
        # Prepare data for upsert
        upsert_data = {
            "market_id": market_data["market_id"],
            "event_id": market_data["event_id"],
            "title": market_data["title"],
            "subtitle": market_data.get("subtitle"),
            "yes_subtitle": market_data.get("yes_subtitle"),
            "no_subtitle": market_data.get("no_subtitle"),
            "rules_primary": market_data.get("rules_primary"),
            "yes_price": market_data.get("yes_price"),
            "no_price": market_data.get("no_price"),
            "volume": market_data.get("volume"),
            "open_interest": market_data.get("open_interest"),
            "status": market_data.get("status"),
        }
        
        # Check if record exists before upsert to determine insert vs update
        result = await session.execute(
            select(KalshiMarket.market_id).where(KalshiMarket.market_id == upsert_data["market_id"])
        )
        existing_id = result.scalar_one_or_none()
        is_update = existing_id is not None
        
        # Use PostgreSQL ON CONFLICT for atomic upsert
        # Only update if yes_price, no_price, or status have changed
        stmt = insert(KalshiMarket).values(**upsert_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["market_id"],
            set_={
                "event_id": upsert_data["event_id"],
                "title": upsert_data["title"],
                "subtitle": upsert_data["subtitle"],
                "yes_subtitle": upsert_data["yes_subtitle"],
                "no_subtitle": upsert_data["no_subtitle"],
                "rules_primary": upsert_data["rules_primary"],
                "yes_price": upsert_data["yes_price"],
                "no_price": upsert_data["no_price"],
                "volume": upsert_data["volume"],
                "open_interest": upsert_data["open_interest"],
                "status": upsert_data["status"],
                "updated_at": func.now(),
            },
            where=(
                (stmt.excluded.yes_price.is_distinct_from(KalshiMarket.yes_price)) |
                (stmt.excluded.no_price.is_distinct_from(KalshiMarket.no_price)) |
                (stmt.excluded.status.is_distinct_from(KalshiMarket.status))
            )
        )
        await session.execute(stmt)
        await session.commit()
        
        if is_update:
            logger.info(f"Updated Kalshi market: {upsert_data['market_id']}")
        else:
            logger.info(f"Inserted Kalshi market: {upsert_data['market_id']}")


async def upsert_polymarket_event(event_data: Dict[str, Any]) -> None:
    """
    Upsert a Polymarket event.
    
    Inserts when missing, updates when present. Accepts parsed fields (not raw JSON).
    Logs whether a row was inserted or updated.
    
    Args:
        event_data: Dictionary with parsed event fields:
            - event_id (str, required): Unique event identifier
            - title (str, required): Event title
            - category (str, optional): Event category
            - close_time (datetime or str, optional): Close time
            - status (str, optional): Event status
    """
    if "event_id" not in event_data or "title" not in event_data:
        raise ValueError("event_data must contain 'event_id' and 'title' fields")
    
    async with get_db_session() as session:
        # Prepare data for upsert
        upsert_data = {
            "event_id": event_data["event_id"],
            "title": event_data["title"],
            "category": event_data.get("category"),
            "close_time": _parse_datetime(event_data.get("close_time")),
            "status": event_data.get("status"),
        }
        
        # Check if record exists before upsert to determine insert vs update
        result = await session.execute(
            select(PolymarketEvent.event_id).where(PolymarketEvent.event_id == upsert_data["event_id"])
        )
        existing_id = result.scalar_one_or_none()
        is_update = existing_id is not None
        
        # Use PostgreSQL ON CONFLICT for atomic upsert
        # Only update if any field has changed
        stmt = insert(PolymarketEvent).values(**upsert_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["event_id"],
            set_={
                "title": upsert_data["title"],
                "category": upsert_data["category"],
                "close_time": upsert_data["close_time"],
                "status": upsert_data["status"],
                "updated_at": func.now(),
            },
            where=(
                (stmt.excluded.title.is_distinct_from(PolymarketEvent.title)) |
                (stmt.excluded.category.is_distinct_from(PolymarketEvent.category)) |
                (stmt.excluded.close_time.is_distinct_from(PolymarketEvent.close_time)) |
                (stmt.excluded.status.is_distinct_from(PolymarketEvent.status))
            )
        )
        await session.execute(stmt)
        await session.commit()
        
        if is_update:
            logger.info(f"Updated Polymarket event: {upsert_data['event_id']}")
        else:
            logger.info(f"Inserted Polymarket event: {upsert_data['event_id']}")


async def upsert_polymarket_market(market_data: Dict[str, Any]) -> None:
    """
    Upsert a Polymarket market.
    
    Inserts when missing, updates when present. Accepts parsed fields (not raw JSON).
    Logs whether a row was inserted or updated.
    
    Args:
        market_data: Dictionary with parsed market fields:
            - market_id (str, required): Unique market identifier
            - event_id (str, required): Associated event ID
            - title (str, required): Market title
            - description (str, optional): Market description
            - slug (str, optional): Market slug
            - yes_price (float, optional): YES price (0.0-1.0)
            - no_price (float, optional): NO price (0.0-1.0)
            - liquidity (float, optional): Market liquidity
            - volume (float, optional): Market volume
            - status (str, optional): Market status
    """
    if "market_id" not in market_data or "event_id" not in market_data or "title" not in market_data:
        raise ValueError("market_data must contain 'market_id', 'event_id', and 'title' fields")
    
    async with get_db_session() as session:
        # Prepare data for upsert
        upsert_data = {
            "market_id": market_data["market_id"],
            "event_id": market_data["event_id"],
            "title": market_data["title"],
            "description": market_data.get("description"),
            "slug": market_data.get("slug"),
            "yes_price": market_data.get("yes_price"),
            "no_price": market_data.get("no_price"),
            "liquidity": market_data.get("liquidity"),
            "volume": market_data.get("volume"),
            "status": market_data.get("status"),
        }
        
        # Check if record exists before upsert to determine insert vs update
        result = await session.execute(
            select(PolymarketMarket.market_id).where(PolymarketMarket.market_id == upsert_data["market_id"])
        )
        existing_id = result.scalar_one_or_none()
        is_update = existing_id is not None
        
        # Use PostgreSQL ON CONFLICT for atomic upsert
        # Only update if yes_price, no_price, status, or slug have changed
        stmt = insert(PolymarketMarket).values(**upsert_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["market_id"],
            set_={
                "event_id": upsert_data["event_id"],
                "title": upsert_data["title"],
                "description": upsert_data["description"],
                "slug": upsert_data["slug"],
                "yes_price": upsert_data["yes_price"],
                "no_price": upsert_data["no_price"],
                "liquidity": upsert_data["liquidity"],
                "volume": upsert_data["volume"],
                "status": upsert_data["status"],
                "updated_at": func.now(),
            },
            where=(
                (stmt.excluded.yes_price.is_distinct_from(PolymarketMarket.yes_price)) |
                (stmt.excluded.no_price.is_distinct_from(PolymarketMarket.no_price)) |
                (stmt.excluded.status.is_distinct_from(PolymarketMarket.status)) |
                (stmt.excluded.slug.is_distinct_from(PolymarketMarket.slug))
            )
        )
        await session.execute(stmt)
        await session.commit()
        
        if is_update:
            logger.info(f"Updated Polymarket market: {upsert_data['market_id']}")
        else:
            logger.info(f"Inserted Polymarket market: {upsert_data['market_id']}")


# ============================================================================
# BATCH UPSERT FUNCTIONS (for better performance)
# ============================================================================

async def upsert_kalshi_events_batch(events_data: List[Dict[str, Any]]) -> tuple[int, int]:
    """
    Batch upsert Kalshi events using temporary tables for better performance.
    
    Inserts/updates multiple events using a temp table approach:
    1. Create temp table with same structure
    2. Bulk insert all data into temp table
    3. Merge from temp table to target table
    4. Drop temp table
    
    Args:
        events_data: List of dictionaries with parsed event fields
        
    Returns:
        Tuple of (inserted_count, updated_count)
    """
    if not events_data:
        return 0, 0
    
    # Prepare all data first
    prepared_data = []
    for event_data in events_data:
        if "event_id" not in event_data or "title" not in event_data:
            continue
        
        close_time = _parse_datetime(event_data.get("close_time"))
        prepared_data.append({
            "event_id": event_data["event_id"],
            "title": event_data["title"],
            "category": event_data.get("category"),
            "sub_category": event_data.get("sub_category"),
            "close_time": close_time,
            "status": event_data.get("status"),
        })
    
    if not prepared_data:
        return 0, 0
    
    # Deduplicate by event_id (keep last occurrence)
    seen = {}
    for data in prepared_data:
        seen[data["event_id"]] = data
    prepared_data = list(seen.values())
    
    async with get_db_session() as session:
        temp_table_name = "temp_kalshi_events_upsert"
        
        try:
            # Get count of existing records before merge (batch the IN clause to stay under arg limit)
            event_ids = [data["event_id"] for data in prepared_data]
            existing_before = 0
            for i in range(0, len(event_ids), BATCH_SIZE):
                batch_ids = event_ids[i:i + BATCH_SIZE]
                result = await session.execute(
                    select(func.count()).where(KalshiEvent.event_id.in_(batch_ids))
                )
                existing_before += result.scalar() or 0
            
            # Create temporary table with same structure (without constraints)
            await session.execute(text(f"""
                CREATE TEMP TABLE {temp_table_name} (
                    event_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    category TEXT,
                    sub_category TEXT,
                    close_time TIMESTAMP WITH TIME ZONE,
                    status TEXT
                )
            """))
            
            # Bulk insert all data into temp table in batches (to stay under 32767 arg limit)
            if prepared_data:
                insert_stmt = text(f"""
                    INSERT INTO {temp_table_name} (event_id, title, category, sub_category, close_time, status)
                    VALUES (:event_id, :title, :category, :sub_category, :close_time, :status)
                """)
                for i in range(0, len(prepared_data), BATCH_SIZE):
                    batch = prepared_data[i:i + BATCH_SIZE]
                    await session.execute(insert_stmt, batch)
            
            # Merge from temp table to target table using ON CONFLICT (single merge for all data)
            await session.execute(text(f"""
                INSERT INTO kalshi_events (event_id, title, category, sub_category, close_time, status, updated_at)
                SELECT 
                    event_id,
                    title,
                    category,
                    sub_category,
                    close_time,
                    status,
                    NOW()
                FROM {temp_table_name}
                ON CONFLICT (event_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    category = EXCLUDED.category,
                    sub_category = EXCLUDED.sub_category,
                    close_time = EXCLUDED.close_time,
                    status = EXCLUDED.status,
                    updated_at = CASE 
                        WHEN (kalshi_events.title IS DISTINCT FROM EXCLUDED.title 
                              OR kalshi_events.category IS DISTINCT FROM EXCLUDED.category
                              OR kalshi_events.sub_category IS DISTINCT FROM EXCLUDED.sub_category
                              OR kalshi_events.close_time IS DISTINCT FROM EXCLUDED.close_time
                              OR kalshi_events.status IS DISTINCT FROM EXCLUDED.status)
                        THEN NOW()
                        ELSE kalshi_events.updated_at
                    END
            """))
            
            await session.commit()
            
            inserted_count = max(0, len(prepared_data) - existing_before)
            updated_count = existing_before
            
            logger.info(f"Upserted {len(prepared_data)} Kalshi events using temp table (inserted: {inserted_count}, updated: {updated_count})")
            
        finally:
            # Drop temp table
            try:
                await session.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
            except Exception as e:
                logger.warning(f"Error dropping temp table {temp_table_name}: {e}")
    
    return inserted_count, updated_count


async def upsert_kalshi_markets_batch(markets_data: List[Dict[str, Any]]) -> tuple[int, int]:
    """
    Batch upsert Kalshi markets using temporary tables for better performance.
    
    Inserts/updates multiple markets using a temp table approach:
    1. Create temp table with same structure
    2. Bulk insert all data into temp table
    3. Merge from temp table to target table
    4. Drop temp table
    
    Args:
        markets_data: List of dictionaries with parsed market fields
        
    Returns:
        Tuple of (inserted_count, updated_count)
    """
    if not markets_data:
        return 0, 0
    
    # Prepare all data first
    prepared_data = []
    for market_data in markets_data:
        if "market_id" not in market_data or "event_id" not in market_data or "title" not in market_data:
            continue
        
        prepared_data.append({
            "market_id": market_data["market_id"],
            "event_id": market_data["event_id"],
            "title": market_data["title"],
            "subtitle": market_data.get("subtitle"),
            "yes_subtitle": market_data.get("yes_subtitle"),
            "no_subtitle": market_data.get("no_subtitle"),
            "rules_primary": market_data.get("rules_primary"),
            "yes_price": market_data.get("yes_price"),
            "no_price": market_data.get("no_price"),
            "volume": market_data.get("volume"),
            "open_interest": market_data.get("open_interest"),
            "status": market_data.get("status"),
        })
    
    if not prepared_data:
        return 0, 0
    
    # Deduplicate by market_id (keep last occurrence)
    seen = {}
    for data in prepared_data:
        seen[data["market_id"]] = data
    prepared_data = list(seen.values())
    
    async with get_db_session() as session:
        temp_table_name = "temp_kalshi_markets_upsert"
        
        try:
            # Get count of existing records before merge (batch the IN clause to stay under arg limit)
            market_ids = [data["market_id"] for data in prepared_data]
            existing_before = 0
            for i in range(0, len(market_ids), BATCH_SIZE):
                batch_ids = market_ids[i:i + BATCH_SIZE]
                result = await session.execute(
                    select(func.count()).where(KalshiMarket.market_id.in_(batch_ids))
                )
                existing_before += result.scalar() or 0
            
            # Create temporary table with same structure (without constraints)
            await session.execute(text(f"""
                CREATE TEMP TABLE {temp_table_name} (
                    market_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    subtitle TEXT,
                    yes_subtitle TEXT,
                    no_subtitle TEXT,
                    rules_primary TEXT,
                    yes_price NUMERIC(10, 2),
                    no_price NUMERIC(10, 2),
                    volume NUMERIC(10, 2),
                    open_interest NUMERIC(10, 2),
                    status TEXT
                )
            """))
            
            # Bulk insert all data into temp table in batches (to stay under 32767 arg limit)
            if prepared_data:
                insert_stmt = text(f"""
                    INSERT INTO {temp_table_name} (market_id, event_id, title, subtitle, yes_subtitle, no_subtitle, rules_primary, yes_price, no_price, volume, open_interest, status)
                    VALUES (:market_id, :event_id, :title, :subtitle, :yes_subtitle, :no_subtitle, :rules_primary, :yes_price, :no_price, :volume, :open_interest, :status)
                """)
                for i in range(0, len(prepared_data), BATCH_SIZE):
                    batch = prepared_data[i:i + BATCH_SIZE]
                    await session.execute(insert_stmt, batch)
            
            # Merge from temp table to target table using ON CONFLICT (single merge for all data)
            await session.execute(text(f"""
                INSERT INTO kalshi_markets (market_id, event_id, title, subtitle, yes_subtitle, no_subtitle, rules_primary, yes_price, no_price, volume, open_interest, status, updated_at)
                SELECT 
                    market_id,
                    event_id,
                    title,
                    subtitle,
                    yes_subtitle,
                    no_subtitle,
                    rules_primary,
                    yes_price,
                    no_price,
                    volume,
                    open_interest,
                    status,
                    NOW()
                FROM {temp_table_name}
                ON CONFLICT (market_id) DO UPDATE SET
                    event_id = EXCLUDED.event_id,
                    title = EXCLUDED.title,
                    subtitle = EXCLUDED.subtitle,
                    yes_subtitle = EXCLUDED.yes_subtitle,
                    no_subtitle = EXCLUDED.no_subtitle,
                    rules_primary = EXCLUDED.rules_primary,
                    yes_price = EXCLUDED.yes_price,
                    no_price = EXCLUDED.no_price,
                    volume = EXCLUDED.volume,
                    open_interest = EXCLUDED.open_interest,
                    status = EXCLUDED.status,
                    updated_at = CASE 
                        WHEN (kalshi_markets.yes_price IS DISTINCT FROM EXCLUDED.yes_price 
                              OR kalshi_markets.no_price IS DISTINCT FROM EXCLUDED.no_price
                              OR kalshi_markets.status IS DISTINCT FROM EXCLUDED.status)
                        THEN NOW()
                        ELSE kalshi_markets.updated_at
                    END
            """))
            
            await session.commit()
            
            inserted_count = max(0, len(prepared_data) - existing_before)
            updated_count = existing_before
            
            logger.info(f"Upserted {len(prepared_data)} Kalshi markets using temp table (inserted: {inserted_count}, updated: {updated_count})")
            
        finally:
            # Drop temp table
            try:
                await session.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
            except Exception as e:
                logger.warning(f"Error dropping temp table {temp_table_name}: {e}")
    
    return inserted_count, updated_count


async def upsert_polymarket_events_batch(events_data: List[Dict[str, Any]]) -> tuple[int, int]:
    """
    Batch upsert Polymarket events using temporary tables for better performance.
    
    Inserts/updates multiple events using a temp table approach:
    1. Create temp table with same structure
    2. Bulk insert all data into temp table
    3. Merge from temp table to target table
    4. Drop temp table
    
    Args:
        events_data: List of dictionaries with parsed event fields
        
    Returns:
        Tuple of (inserted_count, updated_count)
    """
    if not events_data:
        return 0, 0
    
    # Prepare all data first
    prepared_data = []
    for event_data in events_data:
        if "event_id" not in event_data or "title" not in event_data:
            continue
        
        close_time = _parse_datetime(event_data.get("close_time"))
        prepared_data.append({
            "event_id": event_data["event_id"],
            "title": event_data["title"],
            "category": event_data.get("category"),
            "close_time": close_time,
            "status": event_data.get("status"),
        })
    
    if not prepared_data:
        return 0, 0
    
    # Deduplicate by event_id (keep last occurrence)
    seen = {}
    for data in prepared_data:
        seen[data["event_id"]] = data
    prepared_data = list(seen.values())
    
    async with get_db_session() as session:
        temp_table_name = "temp_polymarket_events_upsert"
        
        try:
            # Get count of existing records before merge (batch the IN clause to stay under arg limit)
            event_ids = [data["event_id"] for data in prepared_data]
            existing_before = 0
            for i in range(0, len(event_ids), BATCH_SIZE):
                batch_ids = event_ids[i:i + BATCH_SIZE]
                result = await session.execute(
                    select(func.count()).where(PolymarketEvent.event_id.in_(batch_ids))
                )
                existing_before += result.scalar() or 0
            
            # Create temporary table with same structure (without constraints)
            await session.execute(text(f"""
                CREATE TEMP TABLE {temp_table_name} (
                    event_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    category TEXT,
                    close_time TIMESTAMP WITH TIME ZONE,
                    status TEXT
                )
            """))
            
            # Bulk insert all data into temp table in batches (to stay under 32767 arg limit)
            if prepared_data:
                insert_stmt = text(f"""
                    INSERT INTO {temp_table_name} (event_id, title, category, close_time, status)
                    VALUES (:event_id, :title, :category, :close_time, :status)
                """)
                for i in range(0, len(prepared_data), BATCH_SIZE):
                    batch = prepared_data[i:i + BATCH_SIZE]
                    await session.execute(insert_stmt, batch)
            
            # Merge from temp table to target table using ON CONFLICT (single merge for all data)
            await session.execute(text(f"""
                INSERT INTO polymarket_events (event_id, title, category, close_time, status, updated_at)
                SELECT 
                    event_id,
                    title,
                    category,
                    close_time,
                    status,
                    NOW()
                FROM {temp_table_name}
                ON CONFLICT (event_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    category = EXCLUDED.category,
                    close_time = EXCLUDED.close_time,
                    status = EXCLUDED.status,
                    updated_at = CASE 
                        WHEN (polymarket_events.title IS DISTINCT FROM EXCLUDED.title 
                              OR polymarket_events.category IS DISTINCT FROM EXCLUDED.category
                              OR polymarket_events.close_time IS DISTINCT FROM EXCLUDED.close_time
                              OR polymarket_events.status IS DISTINCT FROM EXCLUDED.status)
                        THEN NOW()
                        ELSE polymarket_events.updated_at
                    END
            """))
            
            await session.commit()
            
            inserted_count = max(0, len(prepared_data) - existing_before)
            updated_count = existing_before
            
            logger.info(f"Upserted {len(prepared_data)} Polymarket events using temp table (inserted: {inserted_count}, updated: {updated_count})")
            
        finally:
            # Drop temp table
            try:
                await session.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
            except Exception as e:
                logger.warning(f"Error dropping temp table {temp_table_name}: {e}")
    
    return inserted_count, updated_count


async def upsert_polymarket_markets_batch(markets_data: List[Dict[str, Any]]) -> tuple[int, int]:
    """
    Batch upsert Polymarket markets using temporary tables for better performance.
    
    Inserts/updates multiple markets using a temp table approach:
    1. Create temp table with same structure
    2. Bulk insert all data into temp table
    3. Merge from temp table to target table
    4. Drop temp table
    
    Args:
        markets_data: List of dictionaries with parsed market fields
        
    Returns:
        Tuple of (inserted_count, updated_count)
    """
    if not markets_data:
        return 0, 0
    
    # Prepare all data first
    prepared_data = []
    for market_data in markets_data:
        if "market_id" not in market_data or "event_id" not in market_data or "title" not in market_data:
            continue
        
        prepared_data.append({
            "market_id": market_data["market_id"],
            "event_id": market_data["event_id"],
            "title": market_data["title"],
            "description": market_data.get("description"),
            "slug": market_data.get("slug"),
            "yes_price": market_data.get("yes_price"),
            "no_price": market_data.get("no_price"),
            "liquidity": market_data.get("liquidity"),
            "volume": market_data.get("volume"),
            "status": market_data.get("status"),
        })
    
    if not prepared_data:
        return 0, 0
    
    # Deduplicate by market_id (keep last occurrence)
    seen = {}
    for data in prepared_data:
        seen[data["market_id"]] = data
    prepared_data = list(seen.values())
    
    async with get_db_session() as session:
        temp_table_name = "temp_polymarket_markets_upsert"
        
        try:
            # Get count of existing records before merge (batch the IN clause to stay under arg limit)
            market_ids = [data["market_id"] for data in prepared_data]
            existing_before = 0
            for i in range(0, len(market_ids), BATCH_SIZE):
                batch_ids = market_ids[i:i + BATCH_SIZE]
                result = await session.execute(
                    select(func.count()).where(PolymarketMarket.market_id.in_(batch_ids))
                )
                existing_before += result.scalar() or 0
            
            # Create temporary table with same structure (without constraints)
            await session.execute(text(f"""
                CREATE TEMP TABLE {temp_table_name} (
                    market_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    slug TEXT,
                    yes_price NUMERIC(10, 2),
                    no_price NUMERIC(10, 2),
                    liquidity NUMERIC(10, 2),
                    volume NUMERIC(10, 2),
                    status TEXT
                )
            """))
            
            # Bulk insert all data into temp table in batches (to stay under 32767 arg limit)
            if prepared_data:
                insert_stmt = text(f"""
                    INSERT INTO {temp_table_name} (market_id, event_id, title, description, slug, yes_price, no_price, liquidity, volume, status)
                    VALUES (:market_id, :event_id, :title, :description, :slug, :yes_price, :no_price, :liquidity, :volume, :status)
                """)
                for i in range(0, len(prepared_data), BATCH_SIZE):
                    batch = prepared_data[i:i + BATCH_SIZE]
                    await session.execute(insert_stmt, batch)
            
            # Merge from temp table to target table using ON CONFLICT (single merge for all data)
            await session.execute(text(f"""
                INSERT INTO polymarket_markets (market_id, event_id, title, description, slug, yes_price, no_price, liquidity, volume, status, updated_at)
                SELECT 
                    market_id,
                    event_id,
                    title,
                    description,
                    slug,
                    yes_price,
                    no_price,
                    liquidity,
                    volume,
                    status,
                    NOW()
                FROM {temp_table_name}
                ON CONFLICT (market_id) DO UPDATE SET
                    event_id = EXCLUDED.event_id,
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    slug = EXCLUDED.slug,
                    yes_price = EXCLUDED.yes_price,
                    no_price = EXCLUDED.no_price,
                    liquidity = EXCLUDED.liquidity,
                    volume = EXCLUDED.volume,
                    status = EXCLUDED.status,
                    updated_at = CASE 
                        WHEN (polymarket_markets.yes_price IS DISTINCT FROM EXCLUDED.yes_price 
                              OR polymarket_markets.no_price IS DISTINCT FROM EXCLUDED.no_price
                              OR polymarket_markets.status IS DISTINCT FROM EXCLUDED.status
                              OR polymarket_markets.slug IS DISTINCT FROM EXCLUDED.slug)
                        THEN NOW()
                        ELSE polymarket_markets.updated_at
                    END
            """))
            
            await session.commit()
            
            inserted_count = max(0, len(prepared_data) - existing_before)
            updated_count = existing_before
            
            logger.info(f"Upserted {len(prepared_data)} Polymarket markets using temp table (inserted: {inserted_count}, updated: {updated_count})")
            
        finally:
            # Drop temp table
            try:
                await session.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
            except Exception as e:
                logger.warning(f"Error dropping temp table {temp_table_name}: {e}")
    
    return inserted_count, updated_count

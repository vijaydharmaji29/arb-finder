"""Market processing module for handling kalshi and polymarket data."""
from typing import List, Dict, Optional, Tuple
from embedding_service import EmbeddingService


# ID column candidates for each market type
KALSHI_ID_COLUMNS = ['id', 'market_id', 'kalshi_market_id', 'kalshi_id']
POLYMARKET_ID_COLUMNS = ['id', 'market_id', 'polymarket_market_id', 'polymarket_id']


def find_id_column(markets: List[Dict], candidates: List[str]) -> Optional[str]:
    """
    Find the ID column name from a list of candidates.
    
    Args:
        markets: List of market dictionaries
        candidates: List of possible ID column names to try
        
    Returns:
        ID column name if found, None otherwise
    """
    if not markets:
        return None
    
    sample_market = markets[0]
    for possible_id in candidates:
        if possible_id in sample_market:
            return possible_id
    
    return None


def process_kalshi_markets(
    markets: List[Dict],
    embedding_service: EmbeddingService,
    db_connection=None
) -> Tuple[List[str], Optional[str]]:
    """
    Process kalshi markets and create embeddings.
    
    Args:
        markets: List of kalshi market dictionaries
        embedding_service: EmbeddingService instance
        db_connection: Optional database connection
        
    Returns:
        Tuple of (processed_ids, id_column)
    """
    if not markets:
        return [], None
    
    print(f"\nProcessing {len(markets)} kalshi markets...")
    
    # Find ID column
    id_column = find_id_column(markets, KALSHI_ID_COLUMNS)
    if id_column:
        print(f"Found ID column: {id_column}")
    else:
        sample_market = markets[0] if markets else {}
        print("Warning: Could not determine ID column for kalshi_markets. Database updates skipped.")
        print(f"Sample market keys: {list(sample_market.keys())}")
    
    # Process markets
    processed_ids = embedding_service.process_markets(
        markets,
        market_type='kalshi',
        db_connection=db_connection if id_column else None,
        table_name='kalshi_markets' if id_column else None,
        id_column=id_column
    )
    
    return processed_ids, id_column


def process_polymarket_markets(
    markets: List[Dict],
    embedding_service: EmbeddingService,
    db_connection=None
) -> Tuple[List[str], Optional[str]]:
    """
    Process polymarket markets and create embeddings.
    
    Args:
        markets: List of polymarket market dictionaries
        embedding_service: EmbeddingService instance
        db_connection: Optional database connection
        
    Returns:
        Tuple of (processed_ids, id_column)
    """
    if not markets:
        return [], None
    
    print(f"\nProcessing {len(markets)} polymarket markets...")
    
    # Find ID column
    id_column = find_id_column(markets, POLYMARKET_ID_COLUMNS)
    if id_column:
        print(f"Found ID column: {id_column}")
    else:
        sample_market = markets[0] if markets else {}
        print("Warning: Could not determine ID column for polymarket_markets. Database updates skipped.")
        print(f"Sample market keys: {list(sample_market.keys())}")
    
    # Process markets
    processed_ids = embedding_service.process_markets(
        markets,
        market_type='polymarket',
        db_connection=db_connection if id_column else None,
        table_name='polymarket_markets' if id_column else None,
        id_column=id_column
    )
    
    return processed_ids, id_column


"""Embedding creation tasks for Prefect."""
from prefect import task
from prefect.cache_policies import NO_CACHE
from typing import Dict, List, Optional, Tuple
from db_connection import DatabaseConnection
from embedding_service import EmbeddingService
from market_processor import process_kalshi_markets, process_polymarket_markets


@task(name="initialize_embedding_service", log_prints=True)
def initialize_embedding_service_task(
    rerank_model_name: Optional[str] = None,
    pinecone_api_key: Optional[str] = None,
    pinecone_index_name: Optional[str] = None,
    pinecone_embedding_model: Optional[str] = None,
    pinecone_cloud: Optional[str] = None,
    pinecone_region: Optional[str] = None,
    pinecone_rerank_model: Optional[str] = None
) -> EmbeddingService:
    """
    Initialize embedding service with Pinecone.
    
    Args:
        rerank_model_name: Cross-encoder model name for fallback reranking (optional)
        pinecone_api_key: Pinecone API key (default: from PINECONE_API_KEY env var)
        pinecone_index_name: Pinecone index name (default: from PINECONE_INDEX_NAME env var)
        pinecone_embedding_model: Embedding model for Pinecone (default: from PINECONE_EMBEDDING_MODEL env var)
        pinecone_cloud: Cloud provider ('aws' or 'gcp', default: from PINECONE_CLOUD env var)
        pinecone_region: Region for the index (default: from PINECONE_REGION env var)
        pinecone_rerank_model: Pinecone reranker model name (default: from PINECONE_RERANK_MODEL env var or 'bge-reranker-v2-m3')
        
    Returns:
        Initialized EmbeddingService instance
    """
    print(f"Initializing Pinecone embedding service...")
    service = EmbeddingService(
        rerank_model_name=rerank_model_name,
        pinecone_api_key=pinecone_api_key,
        pinecone_index_name=pinecone_index_name,
        pinecone_embedding_model=pinecone_embedding_model,
        pinecone_cloud=pinecone_cloud,
        pinecone_region=pinecone_region,
        pinecone_rerank_model=pinecone_rerank_model
    )
    print(f"✓ Embedding service initialized")
    return service


@task(name="process_kalshi_markets", log_prints=True, cache_policy=NO_CACHE)
def process_kalshi_markets_task(
    markets: List[Dict],
    embedding_service: EmbeddingService,
    db_connection: DatabaseConnection
) -> Tuple[List[str], Optional[str]]:
    """
    Process kalshi markets and create embeddings.
    
    Args:
        markets: List of kalshi market dictionaries
        embedding_service: EmbeddingService instance
        db_connection: Database connection
        
    Returns:
        Tuple of (processed_ids, id_column)
    """
    if not markets:
        print("No kalshi markets to process")
        return [], None
    
    print(f"Processing {len(markets)} kalshi markets...")
    processed_ids, id_column = process_kalshi_markets(
        markets,
        embedding_service,
        db_connection=db_connection
    )
    print(f"Processed {len(processed_ids)} kalshi markets")
    return processed_ids, id_column


@task(name="process_polymarket_markets", log_prints=True, cache_policy=NO_CACHE)
def process_polymarket_markets_task(
    markets: List[Dict],
    embedding_service: EmbeddingService,
    db_connection: DatabaseConnection
) -> Tuple[List[str], Optional[str]]:
    """
    Process polymarket markets and create embeddings.
    
    Args:
        markets: List of polymarket market dictionaries
        embedding_service: EmbeddingService instance
        db_connection: Database connection
        
    Returns:
        Tuple of (processed_ids, id_column)
    """
    if not markets:
        print("No polymarket markets to process")
        return [], None
    
    print(f"Processing {len(markets)} polymarket markets...")
    processed_ids, id_column = process_polymarket_markets(
        markets,
        embedding_service,
        db_connection=db_connection
    )
    print(f"✓ Processed {len(processed_ids)} polymarket markets")
    return processed_ids, id_column


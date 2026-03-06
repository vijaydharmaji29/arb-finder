"""Prefect flow for creating embeddings from market data."""
from prefect import flow
from typing import Dict, List, Optional
from db_connection import DatabaseConnection
from embedding_service import EmbeddingService
from tasks.data_tasks import fetch_markets_data
from tasks.embedding_tasks import (
    initialize_embedding_service_task,
    process_kalshi_markets_task,
    process_polymarket_markets_task
)


@flow(name="create_embeddings", log_prints=True)
def create_embeddings_flow(
    db_connection: DatabaseConnection,
    rerank_model_name: Optional[str] = None,
    pinecone_api_key: Optional[str] = None,
    pinecone_index_name: Optional[str] = None,
    pinecone_embedding_model: Optional[str] = None,
    pinecone_cloud: Optional[str] = None,
    pinecone_region: Optional[str] = None,
    pinecone_rerank_model: Optional[str] = None
) -> Dict:
    """
    Flow to create embeddings for market data using Pinecone.
    
    Args:
        db_connection: Database connection
        rerank_model_name: Cross-encoder model name for fallback reranking (optional)
        pinecone_api_key: Pinecone API key (default: from PINECONE_API_KEY env var)
        pinecone_index_name: Pinecone index name (default: from PINECONE_INDEX_NAME env var)
        pinecone_embedding_model: Embedding model for Pinecone (default: from PINECONE_EMBEDDING_MODEL env var)
        pinecone_cloud: Cloud provider ('aws' or 'gcp', default: from PINECONE_CLOUD env var)
        pinecone_region: Region for the index (default: from PINECONE_REGION env var)
        pinecone_rerank_model: Pinecone reranker model name (default: from PINECONE_RERANK_MODEL env var or 'bge-reranker-v2-m3')
        
    Returns:
        Dictionary with embedding service and processing results
    """
    print("="*80)
    print("CREATE EMBEDDINGS FLOW")
    print("="*80)
    
    # Initialize embedding service
    embedding_service = initialize_embedding_service_task(
        rerank_model_name=rerank_model_name,
        pinecone_api_key=pinecone_api_key,
        pinecone_index_name=pinecone_index_name,
        pinecone_embedding_model=pinecone_embedding_model,
        pinecone_cloud=pinecone_cloud,
        pinecone_region=pinecone_region,
        pinecone_rerank_model=pinecone_rerank_model
    )
    
    # Fetch market data
    market_data = fetch_markets_data(db_connection)
    
    # Process kalshi markets
    kalshi_result = None
    if market_data['kalshi_markets']:
        kalshi_result = process_kalshi_markets_task(
            markets=market_data['kalshi_markets'],
            embedding_service=embedding_service,
            db_connection=db_connection
        )
    
    # Process polymarket markets
    polymarket_result = None
    if market_data['polymarket_markets']:
        polymarket_result = process_polymarket_markets_task(
            markets=market_data['polymarket_markets'],
            embedding_service=embedding_service,
            db_connection=db_connection
        )
    
    print("\n✓ Embedding creation flow completed successfully!")
    
    return {
        'embedding_service': embedding_service,
        'kalshi_processed': kalshi_result[0] if kalshi_result else [],
        'polymarket_processed': polymarket_result[0] if polymarket_result else [],
        'market_data': market_data
    }


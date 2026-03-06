"""Prefect flow for finding and saving market matches."""
from prefect import flow
from typing import Dict, List, Optional
from db_connection import DatabaseConnection
from embedding_service import EmbeddingService
from tasks.matching_tasks import (
    run_similarity_matching_task,
    save_matches_to_db_task,
    generate_report_task,
    load_all_markets_from_pinecone_task
)
from tasks.embedding_tasks import initialize_embedding_service_task
from tasks.database_tasks import get_db_connection_task


@flow(name="find_matches", log_prints=True)
def find_matches_flow(
    embedding_service: Optional[EmbeddingService] = None,
    db_connection: Optional[DatabaseConnection] = None,
    db_url: Optional[str] = None,
    rerank_model_name: Optional[str] = None,
    pinecone_api_key: Optional[str] = None,
    pinecone_index_name: Optional[str] = None,
    pinecone_embedding_model: Optional[str] = None,
    pinecone_cloud: Optional[str] = None,
    pinecone_region: Optional[str] = None,
    pinecone_rerank_model: Optional[str] = None,
    similarity_threshold: float = 0.65,
    max_matches_per_market: int = 5,
    namespace: str = "markets",
    generate_report: bool = True,
    output_file: str = 'similar_markets_matches.csv',
    load_all_markets: bool = False,
    source_market_type: str = 'kalshi',
    skip_matched: bool = True,
    mark_matched: bool = False,
    bucket_index: Optional[int] = None,
    total_buckets: Optional[int] = None
) -> Dict:
    """
    Flow to find similar markets and save matches to database.
    
    Args:
        embedding_service: EmbeddingService instance (optional, will be created if not provided)
        db_connection: Database connection (optional, will be created if not provided)
        db_url: Database URL (required if db_connection not provided)
        rerank_model_name: Cross-encoder model name for fallback reranking (optional)
        pinecone_rerank_model: Pinecone reranker model name (default: from PINECONE_RERANK_MODEL env var or 'bge-reranker-v2-m3')
        pinecone_api_key: Pinecone API key (default: from PINECONE_API_KEY env var)
        pinecone_index_name: Pinecone index name (default: from PINECONE_INDEX_NAME env var)
        pinecone_embedding_model: Embedding model for Pinecone (default: from PINECONE_EMBEDDING_MODEL env var)
        pinecone_cloud: Cloud provider ('aws' or 'gcp', default: from PINECONE_CLOUD env var)
        pinecone_region: Region for the index (default: from PINECONE_REGION env var)
        similarity_threshold: Similarity threshold for matching (applied AFTER reranking)
        max_matches_per_market: Maximum matches to keep per market after reranking (default: 5)
        namespace: Namespace for vector storage
        generate_report: Whether to generate CSV report
        output_file: Output CSV file path
        load_all_markets: If True, load all markets from Pinecone regardless of embedding_created status
        source_market_type: Market type to use as source for matching ('kalshi' or 'polymarket'). 
                            Only markets of this type will be processed. Default: 'kalshi'
        skip_matched: If True, skip markets that already have match_found=True in Pinecone (default: True)
        mark_matched: If True, mark matched markets in Pinecone with match_found=True after matching (default: True)
        bucket_index: If provided along with total_buckets, only process this bucket (0-indexed).
                      IDs are sorted and split into total_buckets partitions.
        total_buckets: Total number of buckets to split the IDs into for partitioned processing.
        
    Returns:
        Dictionary with matching results
    """
    print("="*80)
    print("FIND MATCHES FLOW")
    print("="*80)
    
    # Initialize embedding service if not provided
    if embedding_service is None:
        if rerank_model_name is None:
            import os
            rerank_model_name = os.getenv('RERANK_MODEL')
        if pinecone_rerank_model is None:
            import os
            pinecone_rerank_model = os.getenv('PINECONE_RERANK_MODEL')
        embedding_service = initialize_embedding_service_task(
            rerank_model_name=rerank_model_name,
            pinecone_api_key=pinecone_api_key,
            pinecone_index_name=pinecone_index_name,
            pinecone_embedding_model=pinecone_embedding_model,
            pinecone_cloud=pinecone_cloud,
            pinecone_region=pinecone_region,
            pinecone_rerank_model=pinecone_rerank_model
        )
    
    # Load all markets from Pinecone if requested (for standalone deployment)
    if load_all_markets:
        embedding_service = load_all_markets_from_pinecone_task(
            embedding_service=embedding_service,
            namespace=namespace,
            skip_matched=skip_matched
        )
    
    # Apply bucket partitioning if both bucket_index and total_buckets are specified
    if bucket_index is not None and total_buckets is not None:
        if total_buckets <= 0:
            raise ValueError(f"total_buckets must be > 0, got {total_buckets}")
        if bucket_index < 0 or bucket_index >= total_buckets:
            raise ValueError(f"bucket_index must be in range [0, {total_buckets}), got {bucket_index}")
        
        all_markets = embedding_service.uploaded_markets
        total_markets = len(all_markets)
        
        if total_markets > 0:
            # Sort markets by record_id
            sorted_markets = sorted(all_markets, key=lambda x: x.get('record_id', ''))
            
            # Split into total_buckets buckets and take bucket_index
            bucket_size = total_markets // total_buckets
            remainder = total_markets % total_buckets
            
            # Calculate start and end indices for this bucket
            # Distribute remainder across first 'remainder' buckets
            start_idx = bucket_index * bucket_size + min(bucket_index, remainder)
            end_idx = start_idx + bucket_size + (1 if bucket_index < remainder else 0)
            
            bucket_markets = sorted_markets[start_idx:end_idx]
            
            print(f"Bucket partitioning: bucket {bucket_index + 1}/{total_buckets}")
            print(f"  Total markets: {total_markets}")
            print(f"  This bucket: {len(bucket_markets)} markets (indices {start_idx}-{end_idx - 1})")
            
            # Update the embedding service with only this bucket's markets
            embedding_service.uploaded_markets = bucket_markets
            embedding_service._backend.uploaded_markets = bucket_markets
        else:
            print(f"Bucket partitioning: No markets to partition")
    
    # Get database connection if not provided
    connection_created = False
    if db_connection is None:
        connection_created = True
        if db_url is None:
            import os
            db_url = os.getenv('DATABASE_URL') or os.getenv('DB_URL')
        db_connection = get_db_connection_task(db_url=db_url)
    
    try:
        # Run similarity matching
        matches = run_similarity_matching_task(
            embedding_service=embedding_service,
            threshold=similarity_threshold,
            namespace=namespace,
            label="All Markets",
            max_matches_per_market=max_matches_per_market,
            source_market_type=source_market_type,
            mark_matched=mark_matched
        )
        
        # Save matches to database
        inserted_count = save_matches_to_db_task(
            matches=matches,
            db_connection=db_connection,
            label="Final"
        )
        
        # Generate report if requested
        report_path = None
        if generate_report:
            report_path = generate_report_task(
                embedding_service=embedding_service,
                threshold=similarity_threshold,
                output_file=output_file
            )
        
        print("Find matches flow completed successfully!")
        
        return {
            'matches_found': len(matches),
            'matches_inserted': inserted_count,
            'report_path': report_path
        }
    except Exception as e:
        print(f"Error in find_matches flow: {e}")
        raise e
    finally:
        # Clean up database connection if we created it
        if connection_created and db_connection:
            db_connection.disconnect()


# Deployment configuration
# To deploy this flow, run:
#   python -m flows.find_matches_flow deploy
if __name__ == "__main__":
    import sys
    import os
    
    # Check if deployment is requested
    if len(sys.argv) > 1 and sys.argv[1] == "deploy":
        # Get deployment parameters from environment or defaults
        deploy_name = os.getenv("DEPLOYMENT_NAME", "find-matches-deployment")
        work_pool = os.getenv("WORK_POOL_NAME")
        work_queue = os.getenv("WORK_QUEUE_NAME")
        cron_schedule = os.getenv("CRON_SCHEDULE")
        interval_seconds = int(os.getenv("INTERVAL_SECONDS", "0")) or None
        
        deploy_kwargs = {
            "name": deploy_name,
            "push": os.getenv("PUSH_IMAGE", "false").lower() == "true"
        }
        
        if work_pool:
            deploy_kwargs["work_pool_name"] = work_pool
        if work_queue:
            deploy_kwargs["work_queue_name"] = work_queue
        if cron_schedule:
            deploy_kwargs["cron"] = cron_schedule
        elif interval_seconds:
            deploy_kwargs["interval"] = interval_seconds
        
        # Add default parameters - set load_all_markets=True for standalone deployment
        params = {
            "load_all_markets": True  # Load all markets regardless of embedding_created status
        }
        if os.getenv("PINECONE_INDEX_NAME"):
            params["pinecone_index_name"] = os.getenv("PINECONE_INDEX_NAME")
        if os.getenv("PINECONE_EMBEDDING_MODEL"):
            params["pinecone_embedding_model"] = os.getenv("PINECONE_EMBEDDING_MODEL")
        if os.getenv("PINECONE_CLOUD"):
            params["pinecone_cloud"] = os.getenv("PINECONE_CLOUD")
        if os.getenv("PINECONE_REGION"):
            params["pinecone_region"] = os.getenv("PINECONE_REGION")
        if os.getenv("RERANK_MODEL"):
            params["rerank_model_name"] = os.getenv("RERANK_MODEL")
        if os.getenv("PINECONE_RERANK_MODEL"):
            params["pinecone_rerank_model"] = os.getenv("PINECONE_RERANK_MODEL")
        if os.getenv("SIMILARITY_THRESHOLD"):
            params["similarity_threshold"] = float(os.getenv("SIMILARITY_THRESHOLD"))
        if os.getenv("SOURCE_MARKET_TYPE"):
            params["source_market_type"] = os.getenv("SOURCE_MARKET_TYPE")
        if os.getenv("GENERATE_REPORT", "true").lower() == "false":
            params["generate_report"] = False
        if os.getenv("OUTPUT_FILE"):
            params["output_file"] = os.getenv("OUTPUT_FILE")
        if os.getenv("SKIP_MATCHED", "true").lower() == "false":
            params["skip_matched"] = False
        if os.getenv("MARK_MATCHED", "true").lower() == "false":
            params["mark_matched"] = False
        if os.getenv("BUCKET_INDEX"):
            params["bucket_index"] = int(os.getenv("BUCKET_INDEX"))
        if os.getenv("TOTAL_BUCKETS"):
            params["total_buckets"] = int(os.getenv("TOTAL_BUCKETS"))
        
        deploy_kwargs["parameters"] = params
        
        print(f"Deploying find_matches flow with configuration:")
        for key, value in deploy_kwargs.items():
            print(f"  {key}: {value}")
        
        find_matches_flow.deploy(**deploy_kwargs)
        print("✓ Deployment complete!")
    else:
        # Run the flow directly (for testing)
        # Parse optional bucket arguments: python -m flows.find_matches_flow [bucket_index] [total_buckets]
        bucket_index = None
        total_buckets = None
        
        if len(sys.argv) >= 3:
            try:
                bucket_index = int(sys.argv[1])
                total_buckets = int(sys.argv[2])
                print(f"Running find_matches flow with bucket {bucket_index + 1}/{total_buckets}...")
            except ValueError:
                print(f"Invalid bucket arguments: {sys.argv[1:]}")
                print("Usage: python -m flows.find_matches_flow [bucket_index] [total_buckets]")
                print("  bucket_index: 0-indexed bucket number (X)")
                print("  total_buckets: total number of buckets to split into (Y)")
                sys.exit(1)
        else:
            print("Running find_matches flow directly...")
        
        result = find_matches_flow(
            load_all_markets=True,
            bucket_index=bucket_index,
            total_buckets=total_buckets
        )
        print(f"Flow completed successfully!")
        print(f"  Matches found: {result.get('matches_found', 0)}")
        print(f"  Matches inserted: {result.get('matches_inserted', 0)}")
        if result.get('report_path'):
            print(f"  Report saved to: {result.get('report_path')}")


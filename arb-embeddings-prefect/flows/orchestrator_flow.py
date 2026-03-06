"""Orchestrator flow that runs embedding creation and matching flows."""
from prefect import flow
from typing import Optional
from db_connection import DatabaseConnection
from flows.create_embeddings_flow import create_embeddings_flow
from flows.find_matches_flow import find_matches_flow
from tasks.database_tasks import get_db_connection_task


@flow(name="orchestrator", log_prints=True)
def orchestrator_flow(
    db_url: Optional[str] = None,
    rerank_model_name: Optional[str] = None,
    pinecone_api_key: Optional[str] = None,
    pinecone_index_name: Optional[str] = None,
    pinecone_embedding_model: Optional[str] = None,
    pinecone_cloud: Optional[str] = None,
    pinecone_region: Optional[str] = None,
    pinecone_rerank_model: Optional[str] = None,
    similarity_threshold: float = 0.65,
    generate_report: bool = True,
    output_file: str = 'similar_markets_matches.csv',
    source_market_type: str = 'kalshi'
) -> dict:
    """
    Main orchestrator flow that runs embedding creation and matching flows using Pinecone.
    
    Args:
        db_url: Database URL (if None, reads from DATABASE_URL env var)
        rerank_model_name: Cross-encoder model name for fallback reranking (optional)
        pinecone_rerank_model: Pinecone reranker model name (default: from PINECONE_RERANK_MODEL env var or 'bge-reranker-v2-m3')
        pinecone_api_key: Pinecone API key (default: from PINECONE_API_KEY env var)
        pinecone_index_name: Pinecone index name (default: from PINECONE_INDEX_NAME env var)
        pinecone_embedding_model: Embedding model for Pinecone (default: from PINECONE_EMBEDDING_MODEL env var)
        pinecone_cloud: Cloud provider ('aws' or 'gcp', default: from PINECONE_CLOUD env var)
        pinecone_region: Region for the index (default: from PINECONE_REGION env var)
        similarity_threshold: Similarity threshold for matching
        generate_report: Whether to generate CSV report
        output_file: Output CSV file path
        source_market_type: Market type to use as source for matching ('kalshi' or 'polymarket'). 
                            Only markets of this type will be processed. Default: 'kalshi'
        
    Returns:
        Dictionary with results from both flows
    """
    print("="*80)
    print("MARKET EMBEDDING AND SIMILARITY MATCHING ORCHESTRATOR (Pinecone)")
    print("="*80)
    import os
    print(f"Pinecone Index: {pinecone_index_name or os.getenv('PINECONE_INDEX_NAME', 'arb-prediction-market-data')}")
    print(f"Embedding Model: {pinecone_embedding_model or os.getenv('PINECONE_EMBEDDING_MODEL', 'llama-text-embed-v2')}")
    print(f"Pinecone Rerank Model: {pinecone_rerank_model or os.getenv('PINECONE_RERANK_MODEL', 'bge-reranker-v2-m3')}")
    if rerank_model_name or os.getenv('RERANK_MODEL'):
        print(f"Fallback Rerank Model: {rerank_model_name or os.getenv('RERANK_MODEL')}")
    print(f"Similarity Threshold: {similarity_threshold:.0%}")
    print("="*80)
    
    # Get database connection
    db_connection = get_db_connection_task(db_url=db_url)
    
    try:
        # Run embedding creation flow
        embeddings_result = create_embeddings_flow(
            db_connection=db_connection,
            rerank_model_name=rerank_model_name,
            pinecone_api_key=pinecone_api_key,
            pinecone_index_name=pinecone_index_name,
            pinecone_embedding_model=pinecone_embedding_model,
            pinecone_cloud=pinecone_cloud,
            pinecone_region=pinecone_region,
            pinecone_rerank_model=pinecone_rerank_model
        )
        
        # Run matching flow
        matching_result = find_matches_flow(
            embedding_service=embeddings_result['embedding_service'],
            db_connection=db_connection,
            rerank_model_name=rerank_model_name,
            pinecone_api_key=pinecone_api_key,
            pinecone_index_name=pinecone_index_name,
            pinecone_embedding_model=pinecone_embedding_model,
            pinecone_cloud=pinecone_cloud,
            pinecone_region=pinecone_region,
            pinecone_rerank_model=pinecone_rerank_model,
            similarity_threshold=similarity_threshold,
            generate_report=generate_report,
            output_file=output_file,
            source_market_type=source_market_type
        )
        
        print("\n" + "="*80)
        print("ORCHESTRATOR FLOW COMPLETED SUCCESSFULLY")
        print("="*80)
        print(f"Kalshi markets processed: {len(embeddings_result['kalshi_processed'])}")
        print(f"Polymarket markets processed: {len(embeddings_result['polymarket_processed'])}")
        print(f"Matches found: {matching_result['matches_found']}")
        print(f"Matches inserted: {matching_result['matches_inserted']}")
        if matching_result['report_path']:
            print(f"Report saved to: {matching_result['report_path']}")
        print("="*80)
        
        return {
            'embeddings': embeddings_result,
            'matching': matching_result
        }
    
    finally:
        # Clean up database connection
        if db_connection:
            db_connection.disconnect()


# Deployment configuration
# To deploy this flow, run:
#   python -m flows.orchestrator_flow
# Or use the deploy.py script:
#   python deploy.py deploy --name orchestrator-deployment --cron "0 2 * * *"
if __name__ == "__main__":
    import sys
    
    # Check if deployment is requested
    if len(sys.argv) > 1 and sys.argv[1] == "deploy":
        import os
        
        # Get deployment parameters from environment or defaults
        deploy_name = os.getenv("DEPLOYMENT_NAME", "orchestrator-deployment")
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
        
        # Add default parameters
        params = {}
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
        if os.getenv("SIMILARITY_THRESHOLD"):
            params["similarity_threshold"] = float(os.getenv("SIMILARITY_THRESHOLD"))
        if params:
            deploy_kwargs["parameters"] = params
        
        print(f"Deploying orchestrator flow with configuration:")
        for key, value in deploy_kwargs.items():
            print(f"  {key}: {value}")
        
        orchestrator_flow.deploy(**deploy_kwargs)
        print("✓ Deployment complete!")
    else:
        # Run the flow directly (for testing)
        print("Running orchestrator flow directly...")
        result = orchestrator_flow()
        print(f"Flow completed successfully!")
        print(f"  Kalshi markets processed: {len(result.get('embeddings', {}).get('kalshi_processed', []))}")
        print(f"  Polymarket markets processed: {len(result.get('embeddings', {}).get('polymarket_processed', []))}")
        print(f"  Matches found: {result.get('matching', {}).get('matches_found', 0)}")
        print(f"  Matches inserted: {result.get('matching', {}).get('matches_inserted', 0)}")


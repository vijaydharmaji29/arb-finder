"""Similarity matching tasks for Prefect."""
from prefect import task
from prefect.cache_policies import NO_CACHE
from typing import List, Tuple, Optional
from db_connection import DatabaseConnection
from embedding_service import EmbeddingService
from report_generator import generate_similarity_report


@task(name="load_all_markets_from_pinecone", log_prints=True, cache_policy=NO_CACHE)
def load_all_markets_from_pinecone_task(
    embedding_service: EmbeddingService,
    namespace: str = "markets",
    skip_matched: bool = True
) -> EmbeddingService:
    """
    Load all markets from Pinecone for matching (regardless of embedding_created status).
    
    Args:
        embedding_service: EmbeddingService instance
        namespace: Namespace to load from
        skip_matched: If True, skip markets that already have match_found=True (default: True)
        
    Returns:
        EmbeddingService instance with all markets loaded
    """
    print(f"Loading all markets from Pinecone namespace '{namespace}'...")
    if skip_matched:
        print("  (Skipping markets with existing matches)")
    embedding_service.load_all_markets_from_pinecone(namespace=namespace, skip_matched=skip_matched)
    return embedding_service


@task(name="run_similarity_matching", log_prints=True, cache_policy=NO_CACHE)
def run_similarity_matching_task(
    embedding_service: EmbeddingService,
    threshold: float,
    namespace: str = "markets",
    label: str = "",
    max_matches_per_market: int = 5,
    source_market_type: str = 'kalshi',
    mark_matched: bool = False
) -> List[Tuple[str, str, float]]:
    """
    Run similarity matching and return matches.
    
    Args:
        embedding_service: EmbeddingService instance
        threshold: Similarity threshold (applied AFTER reranking, default: 0.65)
        namespace: Namespace for vector storage
        label: Label for logging
        max_matches_per_market: Maximum matches to keep per market after reranking (default: 5)
        source_market_type: Market type to use as source for matching ('kalshi' or 'polymarket'). Default: 'kalshi'
        mark_matched: If True, mark matched markets in Pinecone with match_found=True (default: True)
        
    Returns:
        List of tuples (kalshi_market_id, polymarket_market_id, confidence)
    """
    if label:
        print(f"\nRunning similarity matching ({label})...")
    else:
        print("\nRunning similarity matching...")
    
    embedding_service.run_similarity_matching(
        namespace=namespace,
        threshold=threshold,
        max_matches_per_market=max_matches_per_market,
        source_market_type=source_market_type,
        mark_matched=mark_matched
    )
    
    matches = embedding_service.get_market_matches_for_db()
    print(f"Found {len(matches)} matches with similarity >= {threshold:.0%}")
    
    return matches


@task(name="save_matches_to_db", log_prints=True, cache_policy=NO_CACHE)
def save_matches_to_db_task(
    matches: List[Tuple[str, str, float]],
    db_connection: DatabaseConnection,
    label: str = ""
) -> int:
    """
    Save matches to database.
    
    Args:
        matches: List of tuples (kalshi_market_id, polymarket_market_id, confidence)
        db_connection: Database connection
        label: Label for logging
        
    Returns:
        Number of matches inserted
    """
    if not matches:
        if label:
            print(f"No matches to save {label.lower()}")
        else:
            print("No matches to save")
        return 0
    
    try:
        inserted_count = db_connection.batch_insert_market_matches(matches)
        if label:
            print(f"✓ Inserted {inserted_count} matches {label.lower()}")
        else:
            print(f"✓ Successfully inserted {inserted_count} matches into database")
        return inserted_count
    except Exception as e:
        error_msg = f"Failed to save matches {label.lower()}" if label else "Error saving matches to database"
        print(f"Warning: {error_msg}: {e}")
        return 0


@task(name="generate_report", log_prints=True, cache_policy=NO_CACHE)
def generate_report_task(
    embedding_service: EmbeddingService,
    threshold: float,
    output_file: str = 'similar_markets_matches.csv'
) -> str:
    """
    Generate similarity report.
    
    Args:
        embedding_service: EmbeddingService instance
        threshold: Similarity threshold used
        output_file: Output CSV file path
        
    Returns:
        Path to generated report file
    """
    print("\nGenerating similarity report...")
    generate_similarity_report(
        embedding_service,
        threshold,
        output_file=output_file
    )
    return output_file


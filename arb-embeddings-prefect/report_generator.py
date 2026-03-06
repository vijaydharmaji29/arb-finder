"""Report generation module for similarity matching results."""
import pandas as pd
from embedding_service import EmbeddingService


def generate_similarity_report(
    embedding_service: EmbeddingService,
    threshold: float,
    output_file: str = 'similar_markets_matches.csv'
) -> pd.DataFrame:
    """
    Generate and display similarity matching report.
    
    Args:
        embedding_service: EmbeddingService instance
        threshold: Similarity threshold used
        output_file: Output CSV file path
        
    Returns:
        DataFrame with similarity matches
    """
    print("\n" + "="*80)
    print("GENERATING SIMILARITY MATCHES TABLE")
    print("="*80)
    
    similarity_table = embedding_service.get_similarity_table()
    
    if similarity_table.empty:
        print(f"\nNo similar markets found with similarity >= {threshold:.0%}")
        return similarity_table
    
    print(f"\nFound {len(similarity_table)} similar market matches (similarity >= {threshold:.0%})")
    print("\nSimilar Markets Table (Kalshi ↔ Polymarket):")
    print("-"*80)
    
    # Display table
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 50)
    print(similarity_table.to_string(index=False))
    
    # Save to CSV
    similarity_table.to_csv(output_file, index=False)
    print(f"\n✓ Similarity table saved to {output_file}")
    
    return similarity_table


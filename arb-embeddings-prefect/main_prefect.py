"""Main entry point using Prefect flows."""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from flows.orchestrator_flow import orchestrator_flow

# Load .env file from project root
project_root = Path(__file__).parent
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)


def print_config():
    """Print configuration summary from environment variables."""
    print("="*80)
    print("MARKET EMBEDDING AND SIMILARITY MATCHING (Pinecone)")
    print("="*80)
    print(f"Pinecone Index: {os.getenv('PINECONE_INDEX_NAME', 'arb-prediction-market-data')}")
    print(f"Embedding Model: {os.getenv('PINECONE_EMBEDDING_MODEL', 'llama-text-embed-v2')}")
    print(f"Similarity Threshold: {float(os.getenv('SIMILARITY_THRESHOLD', '0.75')):.0%}")
    print("="*80)


def main():
    """Main execution function using Prefect orchestrator flow."""
    print_config()
    
    db_url = os.getenv('DATABASE_URL') or os.getenv('DB_URL')
    similarity_threshold = float(os.getenv('SIMILARITY_THRESHOLD', '0.75'))
    
    try:
        # Run the orchestrator flow with Pinecone
        result = orchestrator_flow(
            db_url=db_url,
            similarity_threshold=similarity_threshold,
            generate_report=True,
            output_file='similar_markets_matches.csv'
        )
        
        print("All flows completed successfully!")
        return result
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()


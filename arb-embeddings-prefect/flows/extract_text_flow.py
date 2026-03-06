"""Prefect flow for extracting text from markets using OpenAI."""
import os
from prefect import flow
from typing import Dict, List, Optional
from dotenv import load_dotenv
from openai import OpenAI

from tasks.database_tasks import get_db_connection_task

load_dotenv()
openai_api_key = os.getenv('OPENAI_API_KEY')
openai_client = OpenAI(api_key=openai_api_key)

# Batch size for database updates
BATCH_UPDATE_SIZE = 100


def build_prompt_text(market: Dict, market_type: str) -> str:
    """Build prompt text from market data based on market type."""
    prompt_text = ""
    title = market.get('title') or ''
    
    if market_type == 'kalshi':
        if title:
            prompt_text += "Title: " + str(title) + "\n"
        subtitle = market.get('subtitle') or ''
        if subtitle:
            prompt_text += "Subtitle: " + str(subtitle) + "\n"
        yes_subtitle = market.get('yes_subtitle') or ''
        if yes_subtitle:
            prompt_text += "Yes Subtitle: " + str(yes_subtitle) + "\n"
        no_subtitle = market.get('no_subtitle') or ''
        if no_subtitle:
            prompt_text += "No Subtitle: " + str(no_subtitle) + "\n"
        rules_primary = market.get('rules_primary') or ''
        if rules_primary:
            prompt_text += "Rules Primary: " + str(rules_primary) + "\n"
    elif market_type == 'polymarket':
        if title:
            prompt_text += "Title: " + str(title) + "\n"
        description = market.get('description') or ''
        if description:
            prompt_text += "Description: " + str(description) + "\n"
    else:
        # Fallback
        if title:
            prompt_text += "Title: " + str(title) + "\n"
        description = market.get('description') or ''
        if description:
            prompt_text += "Description: " + str(description) + "\n"
    
    return prompt_text


def extract_text_from_openai(prompt_text: str, base_prompt: str) -> str:
    """Send prompt to OpenAI and get extracted text response."""
    full_prompt = base_prompt + "\n\n" + prompt_text
    
    completion = openai_client.responses.create(
        model="gpt-4.1-nano-2025-04-14",
        input=full_prompt
    )
    return completion.output_text


# ID column for markets tables
KALSHI_ID_COLUMN = 'market_id'
POLYMARKET_ID_COLUMN = 'market_id'


def get_market_id(market: Dict, id_column: str) -> Optional[str]:
    """Extract market ID from market dictionary using the specified column."""
    if id_column in market:
        return str(market[id_column])
    return None


@flow(name="extract_text", log_prints=True)
def extract_text_flow(
    db_url: Optional[str] = None
) -> Dict:
    """
    Flow to extract text from markets using OpenAI and update the extracted_text column.
    
    This flow:
    1. Fetches markets where extracted_text is NULL
    2. Sends each market to OpenAI with the extraction prompt
    3. Updates the extracted_text column with the response
    4. Batches updates every 100 OpenAI calls
    
    Args:
        db_url: Database URL (if None, reads from DATABASE_URL/DB_URL env var)
        
    Returns:
        Dictionary with extraction results
    """
    print("="*80)
    print("EXTRACT TEXT FLOW")
    print("="*80)
    
    # Get database connection using the task
    db_connection = get_db_connection_task(db_url=db_url)
    
    try:
        # Load the prompt from file
        prompt_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'embedding_text_generation_prompt.txt')
        with open(prompt_path, 'r') as f:
            base_prompt = f.read()
        
        # Fetch markets with NULL extracted_text
        kalshi_markets = db_connection.fetch_markets_with_null_extracted_text('kalshi_markets', 'kalshi_events')
        polymarket_markets = db_connection.fetch_markets_with_null_extracted_text('polymarket_markets', 'polymarket_events')
        
        print(f"Found {len(kalshi_markets)} Kalshi markets with NULL extracted_text")
        print(f"Found {len(polymarket_markets)} Polymarket markets with NULL extracted_text")
        
        # Process Kalshi markets
        kalshi_updates = []
        kalshi_processed = 0
        kalshi_total_updated = 0
        
        print(f"Using ID column for Kalshi: {KALSHI_ID_COLUMN}")
        
        print("\n--- Processing Kalshi Markets ---")
        for market in kalshi_markets:
            market_id = get_market_id(market, KALSHI_ID_COLUMN)
            if not market_id:
                print(f"Warning: No ID found for market with title: {market.get('title', '')[:50]}...")
                continue
            
            prompt_text = build_prompt_text(market, 'kalshi')
            if not prompt_text:
                continue
            
            try:
                extracted_text = extract_text_from_openai(prompt_text, base_prompt)
                kalshi_updates.append((market_id, extracted_text))
                kalshi_processed += 1
                
                if kalshi_processed % 50 == 0:
                    print(f"  Processed {kalshi_processed}/{len(kalshi_markets)} Kalshi markets...")
                
                # Batch update every BATCH_UPDATE_SIZE calls
                if len(kalshi_updates) >= BATCH_UPDATE_SIZE:
                    updated = db_connection.batch_update_extracted_text('kalshi_markets', kalshi_updates, id_column=KALSHI_ID_COLUMN)
                    kalshi_total_updated += updated
                    print(f"  Batch updated {updated} Kalshi markets (total: {kalshi_total_updated})")
                    kalshi_updates = []
                    
            except Exception as e:
                print(f"  Error processing Kalshi market {market_id}: {e}")
                continue
        
        # Final batch for remaining Kalshi markets
        if kalshi_updates:
            updated = db_connection.batch_update_extracted_text('kalshi_markets', kalshi_updates, id_column=KALSHI_ID_COLUMN)
            kalshi_total_updated += updated
            print(f"  Final batch updated {updated} Kalshi markets (total: {kalshi_total_updated})")
        
        # Process Polymarket markets
        polymarket_updates = []
        polymarket_processed = 0
        polymarket_total_updated = 0
        
        print(f"Using ID column for Polymarket: {POLYMARKET_ID_COLUMN}")
        
        print("\n--- Processing Polymarket Markets ---")
        for market in polymarket_markets:
            market_id = get_market_id(market, POLYMARKET_ID_COLUMN)
            if not market_id:
                print(f"Warning: No ID found for market with title: {market.get('title', '')[:50]}...")
                continue
            
            prompt_text = build_prompt_text(market, 'polymarket')
            if not prompt_text:
                continue
            
            try:
                extracted_text = extract_text_from_openai(prompt_text, base_prompt)
                polymarket_updates.append((market_id, extracted_text))
                polymarket_processed += 1
                
                if polymarket_processed % 100 == 0:
                    print(f"  Processed {polymarket_processed}/{len(polymarket_markets)} Polymarket markets...")
                
                # Batch update every BATCH_UPDATE_SIZE calls
                if len(polymarket_updates) >= BATCH_UPDATE_SIZE:
                    updated = db_connection.batch_update_extracted_text('polymarket_markets', polymarket_updates, id_column=POLYMARKET_ID_COLUMN)
                    polymarket_total_updated += updated
                    print(f"  Batch updated {updated} Polymarket markets (total: {polymarket_total_updated})")
                    polymarket_updates = []
                    
            except Exception as e:
                print(f"  Error processing Polymarket market {market_id}: {e}")
                continue
        
        # Final batch for remaining Polymarket markets
        if polymarket_updates:
            updated = db_connection.batch_update_extracted_text('polymarket_markets', polymarket_updates, id_column=POLYMARKET_ID_COLUMN)
            polymarket_total_updated += updated
            print(f"  Final batch updated {updated} Polymarket markets (total: {polymarket_total_updated})")
        
        print("\n" + "="*80)
        print("EXTRACT TEXT FLOW COMPLETED")
        print(f"  Kalshi: {kalshi_processed} processed, {kalshi_total_updated} updated")
        print(f"  Polymarket: {polymarket_processed} processed, {polymarket_total_updated} updated")
        print("="*80)
        
        return {
            'kalshi_processed': kalshi_processed,
            'kalshi_updated': kalshi_total_updated,
            'polymarket_processed': polymarket_processed,
            'polymarket_updated': polymarket_total_updated
        }
    
    finally:
        # Clean up database connection
        if db_connection:
            db_connection.disconnect()


if __name__ == "__main__":
    result = extract_text_flow()
    print(f"\nFinal Result: {result}")

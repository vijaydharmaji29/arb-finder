"""
Prefect flow for detecting arbitrage opportunities between matched Kalshi and Polymarket markets.
"""
import os
import time
import traceback
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values, execute_batch
from prefect import flow, task
from typing import List, Dict
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()


@task(name="get_matched_markets_with_prices")
def get_matched_markets_with_prices(database_url: str) -> List[Dict]:
    """
    Fetch all matched markets with their prices using a 3-way join.
    Only returns markets where yes prices or no prices differ by at least 0.01 between the two markets.
    
    Args:
        database_url: Database connection URL string
        
    Returns:
        List of matched market records with prices from both Kalshi and Polymarket
        where prices differ by at least 0.01 (potential arbitrage opportunities)
    """
    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    mm.id,
                    mm.kalshi_market_id,
                    mm.polymarket_market_id,
                    mm.arb_price,
                    mm.confidence,
                    km.yes_price AS kalshi_yes_price,
                    km.no_price AS kalshi_no_price,
                    km.status AS kalshi_status,
                    pm.yes_price AS polymarket_yes_price,
                    pm.no_price AS polymarket_no_price,
                    pm.status AS polymarket_status
                FROM market_matching mm
                INNER JOIN kalshi_markets km 
                    ON mm.kalshi_market_id = km.market_id
                INNER JOIN polymarket_markets pm 
                    ON mm.polymarket_market_id = pm.market_id
                WHERE mm.kalshi_market_id IS NOT NULL 
                  AND mm.polymarket_market_id IS NOT NULL
                  AND km.yes_price IS NOT NULL
                  AND km.no_price IS NOT NULL
                  AND pm.yes_price IS NOT NULL
                  AND pm.no_price IS NOT NULL
                  AND km.yes_price BETWEEN 0.1 AND 0.99
                  AND km.no_price BETWEEN 0.1 AND 0.99
                  AND pm.yes_price BETWEEN 0.1 AND 0.99
                  AND pm.no_price BETWEEN 0.1 AND 0.99
                  AND (ABS(km.yes_price - pm.yes_price) >= 0.01 OR ABS(km.no_price - pm.no_price) >= 0.01)
                  AND km.status = 'active'
                  AND pm.status = 'open'
            """)
            fetched_markets = cur.fetchall()
            print(f"Found {len(fetched_markets)} matched markets with prices")
            print(fetched_markets[:3])
            return fetched_markets
    finally:
        conn.close()


@task(name="calculate_arbitrage")
def calculate_arbitrage(matched_markets: List[Dict]) -> List[Dict]:
    """
    Calculate arbitrage opportunities for matched markets.
    
    Arbitrage exists when:
    - Yes prices differ: buy lower yes, sell higher yes
    - No prices differ: buy lower no, sell higher no
    
    Args:
        matched_markets: List of matched market records with prices from both markets
        
    Returns:
        List of arbitrage opportunities with calculated arb_price
    """
    arbitrage_opportunities = []
    
    for match in matched_markets:
        # Skip if either market is inactive
        if match.get('kalshi_status') != 'active' or match.get('polymarket_status') != 'open':
            continue
        
        kalshi_yes = match.get('kalshi_yes_price')
        kalshi_no = match.get('kalshi_no_price')
        polymarket_yes = match.get('polymarket_yes_price')
        polymarket_no = match.get('polymarket_no_price')
        
        # Skip if prices are None
        if None in [kalshi_yes, kalshi_no, polymarket_yes, polymarket_no]:
            continue
        
        # Convert to Decimal for precise calculations
        k_yes = Decimal(str(kalshi_yes))
        k_no = Decimal(str(kalshi_no))
        p_yes = Decimal(str(polymarket_yes))
        p_no = Decimal(str(polymarket_no))
        
        # Calculate arbitrage opportunities
        arb_type = None
        max_arb_price = 0
        arb_price = 0
        
        # Check YES arbitrage: buy lower yes, sell higher yes
        sell_price = 1
        if k_yes < p_yes:
            # Buy on Kalshi (lower), sell on Polymarket (higher)
            buy_price = k_yes + p_no
            arb_price = sell_price - buy_price
            max_arb_price = max(max_arb_price, arb_price)
        elif k_yes > p_yes:
            # Buy on Polymarket (lower), sell on Kalshi (higher)
            buy_price = p_yes + k_no
            arb_price = sell_price - buy_price
            max_arb_price = max(max_arb_price, arb_price)

        # Check NO arbitrage: buy lower no, sell higher no
        if k_no < p_no:
            # Buy on Kalshi (lower), sell on Polymarket (higher)
            buy_price = k_no + p_yes
            arb_price = sell_price - buy_price
            max_arb_price = max(max_arb_price, arb_price)
        elif k_no > p_no:
            # Buy on Polymarket (lower), sell on Kalshi (higher)
            buy_price = p_no + k_yes
            arb_price = sell_price - buy_price
            max_arb_price = max(max_arb_price, arb_price)
        
        # Store the arbitrage opportunity
        arbitrage_opportunities.append({
            'id': match['id'],
            'kalshi_market_id': match['kalshi_market_id'],
            'polymarket_market_id': match['polymarket_market_id'],
            'arb_price': float(arb_price),
            'arb_type': None,
            'kalshi_yes': float(k_yes),
            'kalshi_no': float(k_no),
            'polymarket_yes': float(p_yes),
            'polymarket_no': float(p_no),
        })
    
    return arbitrage_opportunities


@task(name="update_arbitrage_prices")
def update_arbitrage_prices(database_url: str, arbitrage_opportunities: List[Dict], method: str = "temp_table") -> int:
    """
    Update the arb_price field in market_matching table for detected arbitrage opportunities.
    
    Multiple optimization methods available:
    - "temp_table" (default): Fastest - Uses temporary table + UPDATE FROM JOIN
    - "execute_values": Fast - Uses psycopg2.extras.execute_values
    - "execute_batch": Fast - Uses psycopg2.extras.execute_batch
    - "unnest": Very fast - Uses PostgreSQL unnest with arrays (single UPDATE)
    
    Args:
        database_url: Database connection URL string
        arbitrage_opportunities: List of arbitrage opportunities with calculated prices
        method: Optimization method to use ("temp_table", "execute_values", "execute_batch", "unnest")
        
    Returns:
        Number of records updated
    """
    if not arbitrage_opportunities:
        print("No arbitrage opportunities to update")
        return 0

    total_opportunities = len(arbitrage_opportunities)
    start_time = time.time()
    
    print(f"Starting update process for {total_opportunities} arbitrage opportunities using method: {method}")
    
    conn = None
    try:
        conn = psycopg2.connect(database_url)
        print("Database connection established")
        
        with conn.cursor() as cur:
            if method == "temp_table":
                # METHOD 1: Temporary table + UPDATE FROM JOIN (FASTEST - typically 10-100x faster)
                # This is the fastest method for bulk updates in PostgreSQL
                updated_count = _update_via_temp_table(cur, arbitrage_opportunities)
                
            elif method == "execute_values":
                # METHOD 2: execute_values (FAST - typically 5-10x faster than executemany)
                updated_count = _update_via_execute_values(cur, arbitrage_opportunities)
                
            elif method == "execute_batch":
                # METHOD 3: execute_batch (FAST - typically 3-5x faster than executemany)
                updated_count = _update_via_execute_batch(cur, arbitrage_opportunities)
                
            elif method == "unnest":
                # METHOD 4: PostgreSQL unnest with arrays (VERY FAST - single UPDATE statement)
                updated_count = _update_via_unnest(cur, arbitrage_opportunities)
                
            else:
                raise ValueError(f"Unknown method: {method}. Use 'temp_table', 'execute_values', 'execute_batch', or 'unnest'")
            
            commit_start_time = time.time()
            conn.commit()
            commit_duration = time.time() - commit_start_time
            total_duration = time.time() - start_time
            
            print(f"Transaction committed in {commit_duration:.4f}s")
            print(
                f"Update process completed successfully: {updated_count} records updated "
                f"in {total_duration:.4f}s (avg: {total_duration/updated_count*1000:.4f}ms per record, "
                f"{updated_count/total_duration:.0f} records/sec)"
            )
            
            return updated_count
            
    except psycopg2.Error as e:
        print(f"ERROR: Database error during update process: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback()
            print("Transaction rolled back due to error")
        raise
    except Exception as e:
        print(f"ERROR: Unexpected error during update process: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback()
            print("Transaction rolled back due to error")
        raise
    finally:
        if conn:
            conn.close()
            print("Database connection closed")


def _update_via_temp_table(cur, arbitrage_opportunities: List[Dict]) -> int:
    """
    Fastest method: Create temporary table, bulk insert, then UPDATE FROM JOIN.
    This is typically 10-100x faster than individual UPDATE statements.
    """
    print("Using temporary table method (fastest)")
    
    # Create temporary table
    cur.execute("""
        CREATE TEMPORARY TABLE temp_arb_updates (
            id INTEGER PRIMARY KEY,
            arb_price NUMERIC
        ) ON COMMIT DROP
    """)
    
    # Prepare data for bulk insert
    update_data = [(opp['id'], opp['arb_price']) for opp in arbitrage_opportunities]
    
    # Bulk insert using execute_values (very fast)
    execute_values(
        cur,
        "INSERT INTO temp_arb_updates (id, arb_price) VALUES %s",
        update_data,
        template=None,
        page_size=10000
    )
    
    # Single UPDATE statement using JOIN (extremely fast)
    cur.execute("""
        UPDATE market_matching mm
        SET arb_price = t.arb_price
        FROM temp_arb_updates t
        WHERE mm.id = t.id
    """)
    
    updated_count = cur.rowcount
    print(f"Updated {updated_count} records via temporary table method")
    return updated_count


def _update_via_execute_values(cur, arbitrage_opportunities: List[Dict]) -> int:
    """
    Fast method: Use execute_values for efficient batch inserts/updates.
    Typically 5-10x faster than executemany.
    """
    print("Using execute_values method (fast)")
    
    update_data = [(opp['arb_price'], opp['id']) for opp in arbitrage_opportunities]
    
    # Use execute_values with a single UPDATE statement via VALUES
    execute_values(
        cur,
        """
        UPDATE market_matching AS mm
        SET arb_price = v.arb_price::NUMERIC
        FROM (VALUES %s) AS v(arb_price, id)
        WHERE mm.id = v.id::INTEGER
        """,
        update_data,
        template=None,
        page_size=10000
    )
    
    updated_count = cur.rowcount
    print(f"Updated {updated_count} records via execute_values method")
    return updated_count


def _update_via_execute_batch(cur, arbitrage_opportunities: List[Dict]) -> int:
    """
    Fast method: Use execute_batch for efficient batch updates.
    Typically 3-5x faster than executemany.
    """
    print("Using execute_batch method (fast)")
    
    update_data = [(opp['arb_price'], opp['id']) for opp in arbitrage_opportunities]
    
    # Use execute_batch with page_size for optimal performance
    execute_batch(
        cur,
        """
        UPDATE market_matching
        SET arb_price = %s
        WHERE id = %s
        """,
        update_data,
        page_size=10000
    )
    
    updated_count = len(update_data)
    print(f"Updated {updated_count} records via execute_batch method")
    return updated_count


def _update_via_unnest(cur, arbitrage_opportunities: List[Dict]) -> int:
    """
    Very fast method: Use PostgreSQL unnest with arrays in a single UPDATE statement.
    Typically 20-50x faster than individual UPDATE statements.
    """
    print("Using unnest method (very fast)")
    
    # Extract IDs and prices into separate arrays
    ids = [opp['id'] for opp in arbitrage_opportunities]
    prices = [opp['arb_price'] for opp in arbitrage_opportunities]
    
    # Single UPDATE using unnest with arrays
    cur.execute("""
        UPDATE market_matching mm
        SET arb_price = u.arb_price
        FROM unnest(%s::INTEGER[], %s::NUMERIC[]) AS u(id, arb_price)
        WHERE mm.id = u.id
    """, (ids, prices))
    
    updated_count = cur.rowcount
    print(f"Updated {updated_count} records via unnest method")
    return updated_count


@flow(name="detect_arbitrage_opportunities", log_prints=True)
def detect_arbitrage_opportunities():
    """
    Main Prefect flow to detect arbitrage opportunities between matched markets.
    
    Flow steps:
    1. Get all matched markets with prices using a 3-way join
    2. Calculate arbitrage opportunities
    3. Update arb_price in market_matching table
    """
    # Get database URL from environment variable
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    
    # Step 1: Get matched markets with prices using 3-way join
    print("Fetching matched markets with prices...")
    matched_markets = get_matched_markets_with_prices(database_url)
    print(f"Found {len(matched_markets)} matched markets with prices")
    
    if not matched_markets:
        print("No matched markets found. Exiting.")
        return
    
    # Step 2: Calculate arbitrage opportunities
    print("Calculating arbitrage opportunities...")
    arbitrage_opportunities = calculate_arbitrage(matched_markets)
    print(f"Found {len(arbitrage_opportunities)} arbitrage opportunities")
    
    # Log some examples
    if arbitrage_opportunities:
        sorted_opps = sorted(arbitrage_opportunities, key=lambda x: x['arb_price'], reverse=True)
        
        # Print top 5 highest arb prices
        print("\nTop 5 highest arbitrage prices:")
        for i, opp in enumerate(sorted_opps[:5], 1):
            print(f"  {i}. arb_price={opp['arb_price']:.4f}")
        
        # Print detailed top arbitrage opportunities
        print("\nTop arbitrage opportunities (detailed):")
        for opp in sorted_opps[:5]:
            print(f"  Match ID {opp['id']}: arb_price={opp['arb_price']:.4f}, "
                  f"type={opp['arb_type']}, "
                  f"Kalshi: yes={opp['kalshi_yes']:.4f}, no={opp['kalshi_no']:.4f}, "
                  f"Polymarket: yes={opp['polymarket_yes']:.4f}, no={opp['polymarket_no']:.4f}")
    
    # Step 3: Update database
    # Choose update method: "temp_table" (fastest), "unnest", "execute_values", or "execute_batch"
    update_method = os.getenv('UPDATE_METHOD', 'temp_table')
    print(f"\nUpdating arbitrage prices in database using method: {update_method}...")
    updated_count = update_arbitrage_prices(database_url, arbitrage_opportunities, method=update_method)
    print(f"Updated {updated_count} records")


if __name__ == "__main__":
    detect_arbitrage_opportunities()


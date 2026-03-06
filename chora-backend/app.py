import os
from flask import Flask, request, jsonify
from psycopg2 import pool, OperationalError
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import logging
from contextlib import contextmanager

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection pool
db_pool = None


def init_db_pool():
    """Initialize database connection pool"""
    global db_pool
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    
    # Add keepalive parameters to detect dead connections faster
    # Append to existing URL or add new query params
    if '?' in database_url:
        database_url += '&'
    else:
        database_url += '?'
    database_url += 'keepalives=1&keepalives_idle=30&keepalives_interval=10&keepalives_count=5'
    
    try:
        db_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=database_url
        )
        logger.info("Database connection pool created successfully")
    except Exception as e:
        logger.error(f"Error creating database connection pool: {e}")
        raise


def is_connection_alive(conn):
    """Check if a database connection is still alive"""
    try:
        # Try a simple query to test the connection
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        return True
    except Exception:
        return False


@contextmanager
def get_db_connection(max_retries=3):
    """
    Context manager that provides a healthy database connection.
    Automatically handles stale connections and retries.
    """
    global db_pool
    conn = None
    last_error = None
    
    for attempt in range(max_retries):
        try:
            conn = db_pool.getconn()
            
            # Test if connection is alive
            if not is_connection_alive(conn):
                logger.warning(f"Stale connection detected (attempt {attempt + 1}), getting fresh connection")
                # Close the stale connection and mark it as bad
                try:
                    conn.close()
                except Exception:
                    pass
                db_pool.putconn(conn, close=True)
                conn = None
                continue
            
            # Connection is good, yield it
            yield conn
            return
            
        except OperationalError as e:
            last_error = e
            logger.warning(f"Connection error (attempt {attempt + 1}): {e}")
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
                try:
                    db_pool.putconn(conn, close=True)
                except Exception:
                    pass
                conn = None
        except Exception as e:
            # For non-connection errors, still clean up but re-raise
            if conn:
                try:
                    db_pool.putconn(conn)
                except Exception:
                    pass
            raise
        finally:
            # Return connection to pool if we still have it
            if conn:
                try:
                    db_pool.putconn(conn)
                except Exception:
                    pass
    
    # All retries exhausted
    raise OperationalError(f"Failed to get database connection after {max_retries} attempts: {last_error}")


@app.before_request
def before_request():
    """Initialize database pool before request if not already initialized"""
    global db_pool
    if db_pool is None:
        init_db_pool()


@app.route('/arbitrage-opportunities', methods=['POST'])
def get_arbitrage_opportunities():
    """
    POST endpoint to get top arbitrage opportunities from market_matching table.
    
    Request body (JSON, completely optional - can be empty or omitted):
    - num_markets: number of markets to return (default: 10)
    - min_arb_price: minimum arbitrage price (default: 0.5)
    - max_arb_price: maximum arbitrage price (default: 0.05)
    
    Content-Type header should be set to application/json, but JSON body is optional.
    If Content-Type is set but body is empty/invalid, defaults will be used.
    
    Returns:
    - JSON object with success status, count, and opportunities array
    - Each opportunity includes flattened fields from:
      - market_matching: id, arb_price, confidence
      - kalshi_markets: market_id, title, yes_price, no_price, volume, open_interest, status, subtitle, no_subtitle, rules_primary
      - kalshi_events: event_id, title, category, sub_category, close_time, status
      - polymarket_markets: market_id, title, yes_price, no_price, liquidity, volume, status, description
      - polymarket_events: event_id, title, category, close_time, status
    """
    try:
        # Get JSON body - silent=True handles empty/invalid JSON gracefully
        # This works even when Content-Type is application/json but body is empty
        data = request.get_json(silent=True) or {}
        
        # Extract parameters with defaults
        if data.get('num_markets') is None:
            num_markets = 10
        else:
            num_markets = int(data.get('num_markets', 10))
        if data.get('min_arb_price') is None:
            min_arb_price = 0.0
        else:
            min_arb_price = float(data.get('min_arb_price', 0.0))
        min_arb_price = float(data.get('min_arb_price', 0.0))
        if data.get('max_arb_price') is None:
            max_arb_price = 0.05
        else:
            max_arb_price = float(data.get('max_arb_price', 0.05))  # Default is 0.05
        
        # Validate parameters
        if num_markets < 1:
            return jsonify({'error': 'num_markets must be at least 1'}), 400
        
        if num_markets > 100:
            return jsonify({'error': 'num_markets must be at most 100'}), 400
        
        if min_arb_price < 0:
            return jsonify({'error': 'min_arb_price must be non-negative'}), 400
        
        max_arb_price = float(max_arb_price)
        if max_arb_price < min_arb_price:
            return jsonify({'error': 'max_arb_price must be >= min_arb_price'}), 400
        
        # Get connection from pool with automatic retry on stale connections
        with get_db_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Build query with JOINs to fetch market and event details
            # Use ROW_NUMBER to get only the highest confidence match per kalshi_market_id
            query = """
                SELECT * FROM (
                    SELECT 
                        mm.id,
                        mm.arb_price,
                        mm.confidence,
                        -- Kalshi market fields
                        km.market_id as kalshi_market_id,
                        km.title as kalshi_market_title,
                        km.yes_price as kalshi_yes_price,
                        km.no_price as kalshi_no_price,
                        km.volume as kalshi_volume,
                        km.open_interest as kalshi_open_interest,
                        km.status as kalshi_market_status,
                        km.subtitle as kalshi_subtitle,
                        km.no_subtitle as kalshi_no_subtitle,
                        km.rules_primary as kalshi_rules_primary,
                        -- Kalshi event fields
                        ke.event_id as kalshi_event_id,
                        ke.title as kalshi_event_title,
                        ke.category as kalshi_event_category,
                        ke.sub_category as kalshi_event_sub_category,
                        ke.close_time as kalshi_event_close_time,
                        ke.status as kalshi_event_status,
                        -- Polymarket market fields
                        pm.market_id as polymarket_market_id,
                        pm.title as polymarket_market_title,
                        pm.yes_price as polymarket_yes_price,
                        pm.no_price as polymarket_no_price,
                        pm.liquidity as polymarket_liquidity,
                        pm.volume as polymarket_volume,
                        pm.status as polymarket_market_status,
                        pm.description as polymarket_description,
                        pm.slug as polymarket_slug,
                        -- Polymarket event fields
                        pe.event_id as polymarket_event_id,
                        pe.title as polymarket_event_title,
                        pe.category as polymarket_event_category,
                        pe.close_time as polymarket_event_close_time,
                        pe.status as polymarket_event_status,
                        -- Row number to pick highest confidence per kalshi_market_id
                        ROW_NUMBER() OVER (PARTITION BY km.market_id ORDER BY mm.confidence DESC NULLS LAST) as rn
                    FROM market_matching mm
                    LEFT JOIN kalshi_markets km ON mm.kalshi_market_id = km.market_id
                    LEFT JOIN kalshi_events ke ON km.event_id = ke.event_id
                    LEFT JOIN polymarket_markets pm ON mm.polymarket_market_id = pm.market_id
                    LEFT JOIN polymarket_events pe ON pm.event_id = pe.event_id
                    WHERE mm.arb_price >= %s
                    AND mm.confidence >= 0.65
                    AND km.yes_price BETWEEN 0.05 AND 0.95
                    AND km.no_price BETWEEN 0.05 AND 0.95
                    AND pm.yes_price BETWEEN 0.05 AND 0.95
                    AND pm.no_price BETWEEN 0.05 AND 0.95
                    AND pm.status = 'open'
                    AND km.status = 'active'
                    AND mm.show = true
                ) AS ranked
                WHERE rn = 1
                AND arb_price <= %s
                ORDER BY arb_price DESC NULLS LAST, confidence DESC NULLS LAST, id DESC
                LIMIT %s
            """
            params = [min_arb_price, max_arb_price, num_markets]
            
            
            # Execute query
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Structure the response with flattened fields
            opportunities = []
            for row in results:
                opportunity = {
                    # Market matching fields
                    'id': row['id'],
                    'arb_price': float(row['arb_price']) if row['arb_price'] is not None else None,
                    'confidence': float(row['confidence']) if row['confidence'] is not None else None,
                    # Kalshi market fields
                    'kalshi_market_id': row['kalshi_market_id'],
                    'kalshi_market_title': row['kalshi_market_title'],
                    'kalshi_market_yes_price': float(row['kalshi_yes_price']) if row['kalshi_yes_price'] is not None else None,
                    'kalshi_market_no_price': float(row['kalshi_no_price']) if row['kalshi_no_price'] is not None else None,
                    'kalshi_market_volume': float(row['kalshi_volume']) if row['kalshi_volume'] is not None else None,
                    'kalshi_market_open_interest': float(row['kalshi_open_interest']) if row['kalshi_open_interest'] is not None else None,
                    'kalshi_market_status': row['kalshi_market_status'],
                    'kalshi_market_subtitle': row['kalshi_subtitle'],
                    'kalshi_market_no_subtitle': row['kalshi_no_subtitle'],
                    'kalshi_market_rules_primary': row['kalshi_rules_primary'],
                    # Kalshi event fields
                    'kalshi_event_id': row['kalshi_event_id'],
                    'kalshi_event_title': row['kalshi_event_title'],
                    'kalshi_event_category': row['kalshi_event_category'],
                    'kalshi_event_sub_category': row['kalshi_event_sub_category'],
                    'kalshi_event_close_time': row['kalshi_event_close_time'].isoformat() if row['kalshi_event_close_time'] else None,
                    'kalshi_event_status': row['kalshi_event_status'],
                    # Polymarket market fields
                    'polymarket_market_id': row['polymarket_market_id'],
                    'polymarket_market_title': row['polymarket_market_title'],
                    'polymarket_market_yes_price': float(row['polymarket_yes_price']) if row['polymarket_yes_price'] is not None else None,
                    'polymarket_market_no_price': float(row['polymarket_no_price']) if row['polymarket_no_price'] is not None else None,
                    'polymarket_market_liquidity': float(row['polymarket_liquidity']) if row['polymarket_liquidity'] is not None else None,
                    'polymarket_market_volume': float(row['polymarket_volume']) if row['polymarket_volume'] is not None else None,
                    'polymarket_market_status': row['polymarket_market_status'],
                    'polymarket_market_description': row['polymarket_description'],
                    'polymarket_market_slug': row['polymarket_slug'],
                    # Polymarket event fields
                    'polymarket_event_id': row['polymarket_event_id'],
                    'polymarket_event_title': row['polymarket_event_title'],
                    'polymarket_event_category': row['polymarket_event_category'],
                    'polymarket_event_close_time': row['polymarket_event_close_time'].isoformat() if row['polymarket_event_close_time'] else None,
                    'polymarket_event_status': row['polymarket_event_status']
                }
                opportunities.append(opportunity)
            
            cursor.close()
            
            return jsonify({
                'success': True,
                'count': len(opportunities),
                'opportunities': opportunities
            }), 200
            
    except ValueError as e:
        return jsonify({'error': f'Invalid parameter: {str(e)}'}), 400
    except OperationalError as e:
        logger.error(f"Database connection error: {e}")
        return jsonify({'error': 'Database connection error. Please try again.'}), 503
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


def structure_opportunity(row):
    """Helper function to structure opportunity data from a database row"""
    return {
        # Market matching fields
        'id': row['id'],
        'arb_price': float(row['arb_price']) if row['arb_price'] is not None else None,
        'confidence': float(row['confidence']) if row['confidence'] is not None else None,
        # Kalshi market fields
        'kalshi_market_id': row['kalshi_market_id'],
        'kalshi_market_title': row['kalshi_market_title'],
        'kalshi_market_yes_price': float(row['kalshi_yes_price']) if row['kalshi_yes_price'] is not None else None,
        'kalshi_market_no_price': float(row['kalshi_no_price']) if row['kalshi_no_price'] is not None else None,
        'kalshi_market_volume': float(row['kalshi_volume']) if row['kalshi_volume'] is not None else None,
        'kalshi_market_open_interest': float(row['kalshi_open_interest']) if row['kalshi_open_interest'] is not None else None,
        'kalshi_market_status': row['kalshi_market_status'],
        'kalshi_market_subtitle': row['kalshi_subtitle'],
        'kalshi_market_no_subtitle': row['kalshi_no_subtitle'],
        'kalshi_market_rules_primary': row['kalshi_rules_primary'],
        'kalshi_market_extracted_text': row.get('kalshi_extracted_text'),
        'kalshi_market_resolution_date': row.get('kalshi_resolution_date').isoformat() if row.get('kalshi_resolution_date') is not None else None,
        # Kalshi event fields
        'kalshi_event_id': row['kalshi_event_id'],
        'kalshi_event_title': row['kalshi_event_title'],
        'kalshi_event_category': row['kalshi_event_category'],
        'kalshi_event_sub_category': row['kalshi_event_sub_category'],
        'kalshi_event_close_time': row['kalshi_event_close_time'].isoformat() if row['kalshi_event_close_time'] else None,
        'kalshi_event_status': row['kalshi_event_status'],
        # Polymarket market fields
        'polymarket_market_id': row['polymarket_market_id'],
        'polymarket_market_title': row['polymarket_market_title'],
        'polymarket_market_yes_price': float(row['polymarket_yes_price']) if row['polymarket_yes_price'] is not None else None,
        'polymarket_market_no_price': float(row['polymarket_no_price']) if row['polymarket_no_price'] is not None else None,
        'polymarket_market_liquidity': float(row['polymarket_liquidity']) if row['polymarket_liquidity'] is not None else None,
        'polymarket_market_volume': float(row['polymarket_volume']) if row['polymarket_volume'] is not None else None,
        'polymarket_market_status': row['polymarket_market_status'],
        'polymarket_market_description': row['polymarket_description'],
        'polymarket_market_slug': row['polymarket_slug'],
        'polymarket_market_extracted_text': row.get('polymarket_extracted_text'),
        'polymarket_market_resolution_date': row.get('polymarket_resolution_date').isoformat() if row.get('polymarket_resolution_date') is not None else None,
        # Polymarket event fields
        'polymarket_event_id': row['polymarket_event_id'],
        'polymarket_event_title': row['polymarket_event_title'],
        'polymarket_event_category': row['polymarket_event_category'],
        'polymarket_event_close_time': row['polymarket_event_close_time'].isoformat() if row['polymarket_event_close_time'] else None,
        'polymarket_event_status': row['polymarket_event_status']
    }


@app.route('/arbitrage-opportunities-with-matches', methods=['POST'])
def get_arbitrage_opportunities_with_matches():
    """
    POST endpoint to get top arbitrage opportunities and all additional matches for those Kalshi market IDs.
    Similar to /arbitrage-opportunities, but also returns all other matches (show=true) for the same Kalshi market IDs.
    
    Request body (JSON, completely optional - can be empty or omitted):
    - num_markets: number of markets to return (default: 10)
    - min_arb_price: minimum arbitrage price (default: 0.5)
    - max_arb_price: maximum arbitrage price (default: 0.05)
    
    Content-Type header should be set to application/json, but JSON body is optional.
    If Content-Type is set but body is empty/invalid, defaults will be used.
    
    Returns:
    - JSON object with success status, count, total_opportunities, and opportunities dictionary
    - opportunities: dictionary where keys are kalshi_market_id and values are lists of all opportunities
      for that market (including the best one from top opportunities)
    - count: number of unique Kalshi market IDs
    - total_opportunities: total number of opportunities across all markets
    """
    try:
        # Get JSON body - silent=True handles empty/invalid JSON gracefully
        data = request.get_json(silent=True) or {}
        
        # Extract parameters with defaults
        if data.get('num_markets') is None:
            num_markets = 10
        else:
            num_markets = int(data.get('num_markets', 10))
        if data.get('min_arb_price') is None:
            min_arb_price = 0.0
        else:
            min_arb_price = float(data.get('min_arb_price', 0.0))
        min_arb_price = float(data.get('min_arb_price', 0.0))
        if data.get('max_arb_price') is None:
            max_arb_price = 0.05
        else:
            max_arb_price = float(data.get('max_arb_price', 0.05))
        
        # Validate parameters
        if num_markets < 1:
            return jsonify({'error': 'num_markets must be at least 1'}), 400
        
        if num_markets > 100:
            return jsonify({'error': 'num_markets must be at most 100'}), 400
        
        if min_arb_price < 0:
            return jsonify({'error': 'min_arb_price must be non-negative'}), 400
        
        max_arb_price = float(max_arb_price)
        if max_arb_price < min_arb_price:
            return jsonify({'error': 'max_arb_price must be >= min_arb_price'}), 400
        
        # Get connection from pool with automatic retry on stale connections
        with get_db_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # First, get the top opportunities (same query as /arbitrage-opportunities)
            query = """
                SELECT * FROM (
                    SELECT 
                        mm.id,
                        mm.arb_price,
                        mm.confidence,
                        -- Kalshi market fields
                        km.market_id as kalshi_market_id,
                        km.title as kalshi_market_title,
                        km.yes_price as kalshi_yes_price,
                        km.no_price as kalshi_no_price,
                        km.volume as kalshi_volume,
                        km.open_interest as kalshi_open_interest,
                        km.status as kalshi_market_status,
                        km.subtitle as kalshi_subtitle,
                        km.no_subtitle as kalshi_no_subtitle,
                        km.rules_primary as kalshi_rules_primary,
                        km.extracted_text as kalshi_extracted_text,
                        -- Kalshi event fields
                        ke.event_id as kalshi_event_id,
                        ke.title as kalshi_event_title,
                        ke.category as kalshi_event_category,
                        ke.sub_category as kalshi_event_sub_category,
                        ke.close_time as kalshi_event_close_time,
                        ke.status as kalshi_event_status,
                        -- Polymarket market fields
                        pm.market_id as polymarket_market_id,
                        pm.title as polymarket_market_title,
                        pm.yes_price as polymarket_yes_price,
                        pm.no_price as polymarket_no_price,
                        pm.liquidity as polymarket_liquidity,
                        pm.volume as polymarket_volume,
                        pm.status as polymarket_market_status,
                        pm.description as polymarket_description,
                        pm.slug as polymarket_slug,
                        pm.extracted_text as polymarket_extracted_text,
                        -- Polymarket event fields
                        pe.event_id as polymarket_event_id,
                        pe.title as polymarket_event_title,
                        pe.category as polymarket_event_category,
                        pe.close_time as polymarket_event_close_time,
                        pe.status as polymarket_event_status,
                        -- Row number to pick highest confidence per kalshi_market_id
                        ROW_NUMBER() OVER (PARTITION BY km.market_id ORDER BY mm.confidence DESC NULLS LAST) as rn
                    FROM market_matching mm
                    LEFT JOIN kalshi_markets km ON mm.kalshi_market_id = km.market_id
                    LEFT JOIN kalshi_events ke ON km.event_id = ke.event_id
                    LEFT JOIN polymarket_markets pm ON mm.polymarket_market_id = pm.market_id
                    LEFT JOIN polymarket_events pe ON pm.event_id = pe.event_id
                    WHERE mm.arb_price >= %s
                    AND mm.confidence >= 0.65
                    AND km.yes_price BETWEEN 0.05 AND 0.95
                    AND km.no_price BETWEEN 0.05 AND 0.95
                    AND pm.yes_price BETWEEN 0.05 AND 0.95
                    AND pm.no_price BETWEEN 0.05 AND 0.95
                    AND pm.status = 'open'
                    AND km.status = 'active'
                    AND mm.show = true
                    AND mm.verified = false
                ) AS ranked
                WHERE rn = 1
                AND arb_price <= %s
                ORDER BY arb_price DESC NULLS LAST, confidence DESC NULLS LAST, id DESC
                LIMIT %s
            """
            params = [min_arb_price, max_arb_price, num_markets]
            
            # Execute query to get top opportunities
            cursor.execute(query, params)
            top_results = cursor.fetchall()
            
            # Structure the top opportunities and collect kalshi_market_ids
            top_opportunities = []
            kalshi_market_ids = set()
            top_opportunity_ids = set()
            main_match_by_kalshi = {}  # Track main match (polymarket_market_id) for each kalshi_market_id
            # The main match is the one that initially passed the filters (from top_results), not necessarily the highest confidence
            
            for row in top_results:
                opportunity = structure_opportunity(row)
                top_opportunities.append(opportunity)
                if row['kalshi_market_id']:
                    kalshi_market_ids.add(row['kalshi_market_id'])
                    # Store the main match (polymarket_market_id) - this is the one that initially passed the filters
                    if row['kalshi_market_id'] not in main_match_by_kalshi:
                        main_match_by_kalshi[row['kalshi_market_id']] = row['polymarket_market_id']
                top_opportunity_ids.add(row['id'])
            
            # Now get all additional matches for these Kalshi market IDs (excluding the ones already returned)
            additional_matches = []
            if kalshi_market_ids:
                # Build query for additional matches
                # Use IN clause for kalshi_market_id and NOT IN for excluding top opportunity IDs
                kalshi_ids_list = list(kalshi_market_ids)
                top_ids_list = list(top_opportunity_ids)
                
                additional_query = """
                    SELECT 
                        mm.id,
                        mm.arb_price,
                        mm.confidence,
                        -- Kalshi market fields
                        km.market_id as kalshi_market_id,
                        km.title as kalshi_market_title,
                        km.yes_price as kalshi_yes_price,
                        km.no_price as kalshi_no_price,
                        km.volume as kalshi_volume,
                        km.open_interest as kalshi_open_interest,
                        km.status as kalshi_market_status,
                        km.subtitle as kalshi_subtitle,
                        km.no_subtitle as kalshi_no_subtitle,
                        km.rules_primary as kalshi_rules_primary,
                        km.extracted_text as kalshi_extracted_text,
                        -- Kalshi event fields
                        ke.event_id as kalshi_event_id,
                        ke.title as kalshi_event_title,
                        ke.category as kalshi_event_category,
                        ke.sub_category as kalshi_event_sub_category,
                        ke.close_time as kalshi_event_close_time,
                        ke.status as kalshi_event_status,
                        -- Polymarket market fields
                        pm.market_id as polymarket_market_id,
                        pm.title as polymarket_market_title,
                        pm.yes_price as polymarket_yes_price,
                        pm.no_price as polymarket_no_price,
                        pm.liquidity as polymarket_liquidity,
                        pm.volume as polymarket_volume,
                        pm.status as polymarket_market_status,
                        pm.description as polymarket_description,
                        pm.slug as polymarket_slug,
                        pm.extracted_text as polymarket_extracted_text,
                        -- Polymarket event fields
                        pe.event_id as polymarket_event_id,
                        pe.title as polymarket_event_title,
                        pe.category as polymarket_event_category,
                        pe.close_time as polymarket_event_close_time,
                        pe.status as polymarket_event_status
                    FROM market_matching mm
                    LEFT JOIN kalshi_markets km ON mm.kalshi_market_id = km.market_id
                    LEFT JOIN kalshi_events ke ON km.event_id = ke.event_id
                    LEFT JOIN polymarket_markets pm ON mm.polymarket_market_id = pm.market_id
                    LEFT JOIN polymarket_events pe ON pm.event_id = pe.event_id
                    WHERE mm.kalshi_market_id = ANY(%s)
                    AND mm.show = true
                    AND mm.id NOT IN %s
                    AND mm.verified = false
                    ORDER BY mm.arb_price DESC NULLS LAST, mm.confidence DESC NULLS LAST, mm.id DESC
                """
                additional_params = [
                    kalshi_ids_list,
                    tuple(top_ids_list) if top_ids_list else (0,)  # Use tuple for NOT IN, fallback if empty
                ]
                
                cursor.execute(additional_query, additional_params)
                additional_results = cursor.fetchall()
                
                for row in additional_results:
                    match = structure_opportunity(row)
                    additional_matches.append(match)
            
            # Group all opportunities by kalshi_market_id
            # Dictionary where key is kalshi_market_id and value is dict with 'main_match' and 'opportunities'
            opportunities_by_market = {}
            
            # First, add all top opportunities (these are the ones that initially passed the filters)
            for opportunity in top_opportunities:
                kalshi_id = opportunity['kalshi_market_id']
                if kalshi_id:
                    if kalshi_id not in opportunities_by_market:
                        opportunities_by_market[kalshi_id] = {
                            'main_match': main_match_by_kalshi.get(kalshi_id),
                            'opportunities': []
                        }
                    opportunities_by_market[kalshi_id]['opportunities'].append(opportunity)
            
            # Then, add all additional matches
            for match in additional_matches:
                kalshi_id = match['kalshi_market_id']
                if kalshi_id:
                    if kalshi_id not in opportunities_by_market:
                        opportunities_by_market[kalshi_id] = {
                            'main_match': main_match_by_kalshi.get(kalshi_id),
                            'opportunities': []
                        }
                    opportunities_by_market[kalshi_id]['opportunities'].append(match)
            
            # Sort each list by arb_price DESC, confidence DESC, id DESC
            # This ensures the best opportunity is always first in each list
            for kalshi_id in opportunities_by_market:
                opportunities_by_market[kalshi_id]['opportunities'].sort(
                    key=lambda x: (
                        x['arb_price'] if x['arb_price'] is not None else float('-inf'),
                        x['confidence'] if x['confidence'] is not None else float('-inf'),
                        x['id'] if x['id'] is not None else 0
                    ),
                    reverse=True
                )
            
            cursor.close()
            
            # Calculate total count of all opportunities
            total_opportunities = sum(len(market_data['opportunities']) for market_data in opportunities_by_market.values())
            
            return jsonify({
                'success': True,
                'count': len(opportunities_by_market),
                'total_opportunities': total_opportunities,
                'opportunities': opportunities_by_market
            }), 200
            
    except ValueError as e:
        return jsonify({'error': f'Invalid parameter: {str(e)}'}), 400
    except OperationalError as e:
        logger.error(f"Database connection error: {e}")
        return jsonify({'error': 'Database connection error. Please try again.'}), 503
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@app.route('/arbitrage-opportunities-with-matches-polymarket', methods=['POST'])
def get_arbitrage_opportunities_with_matches_polymarket():
    """
    POST endpoint to get top arbitrage opportunities and all additional matches for those Polymarket market IDs.
    Similar to /arbitrage-opportunities, but also returns all other matches (show=true) for the same Polymarket market IDs.
    
    Request body (JSON, completely optional - can be empty or omitted):
    - num_markets: number of markets to return (default: 10)
    - min_arb_price: minimum arbitrage price (default: 0.5)
    - max_arb_price: maximum arbitrage price (default: 0.05)
    - min_confidence: minimum confidence threshold (default: 0.65)
    - resolution_date_sort: if true, sort opportunities by resolution_date (closest first, nulls last) (default: false)
    
    Content-Type header should be set to application/json, but JSON body is optional.
    If Content-Type is set but body is empty/invalid, defaults will be used.
    
    Returns:
    - JSON object with success status, count, total_opportunities, and opportunities dictionary
    - opportunities: dictionary where keys are polymarket_market_id and values are lists of all opportunities
      for that market (including the best one from top opportunities)
    - count: number of unique Polymarket market IDs
    - total_opportunities: total number of opportunities across all markets
    - Only includes markets where resolution_date is null or post today (valid dates)
    """
    try:
        # Get JSON body - silent=True handles empty/invalid JSON gracefully
        data = request.get_json(silent=True) or {}
        
        # Extract parameters with defaults
        if data.get('num_markets') is None:
            num_markets = 10
        else:
            num_markets = int(data.get('num_markets', 10))
        if data.get('min_arb_price') is None:
            min_arb_price = 0.0
        else:
            min_arb_price = float(data.get('min_arb_price', 0.0))
        min_arb_price = float(data.get('min_arb_price', 0.0))
        if data.get('max_arb_price') is None:
            max_arb_price = 0.05
        else:
            max_arb_price = float(data.get('max_arb_price', 0.05))
        if data.get('min_confidence') is None:
            min_confidence = 0.65
        else:
            min_confidence = float(data.get('min_confidence', 0.65))
        
        # Validate parameters
        if num_markets < 1:
            return jsonify({'error': 'num_markets must be at least 1'}), 400
        
        if num_markets > 100:
            return jsonify({'error': 'num_markets must be at most 100'}), 400
        
        if min_arb_price < 0:
            return jsonify({'error': 'min_arb_price must be non-negative'}), 400
        
        max_arb_price = float(max_arb_price)
        if max_arb_price < min_arb_price:
            return jsonify({'error': 'max_arb_price must be >= min_arb_price'}), 400
        
        if min_confidence < 0 or min_confidence > 1:
            return jsonify({'error': 'min_confidence must be between 0 and 1'}), 400
        
        # Extract resolution_date_sort parameter (default: False)
        resolution_date_sort = data.get('resolution_date_sort', False)
        if isinstance(resolution_date_sort, str):
            resolution_date_sort = resolution_date_sort.lower() in ('true', '1', 'yes')
        else:
            resolution_date_sort = bool(resolution_date_sort)
        
        # Get connection from pool with automatic retry on stale connections
        with get_db_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # First, get the top opportunities (same query as /arbitrage-opportunities)
            # Build ORDER BY clause based on resolution_date_sort parameter
            if resolution_date_sort:
                # Sort by closest resolution_date first (earliest date), nulls last, then by arb_price, confidence, id
                # Use CASE to handle NULLs: if both are NULL, return NULL (sorted last), otherwise return the earliest date
                order_by_clause = """
                    ORDER BY CASE
                        WHEN kalshi_resolution_date IS NULL AND polymarket_resolution_date IS NULL THEN NULL
                        WHEN kalshi_resolution_date IS NULL THEN polymarket_resolution_date
                        WHEN polymarket_resolution_date IS NULL THEN kalshi_resolution_date
                        ELSE LEAST(kalshi_resolution_date, polymarket_resolution_date)
                    END ASC NULLS LAST,
                    arb_price DESC NULLS LAST,
                    confidence DESC NULLS LAST,
                    id DESC
                """
            else:
                # Default sorting: arb_price DESC, confidence DESC, id DESC
                order_by_clause = """
                    ORDER BY arb_price DESC NULLS LAST, confidence DESC NULLS LAST, id DESC
                """
            
            query = """
                SELECT * FROM (
                    SELECT 
                        mm.id,
                        mm.arb_price,
                        mm.confidence,
                        -- Kalshi market fields
                        km.market_id as kalshi_market_id,
                        km.title as kalshi_market_title,
                        km.yes_price as kalshi_yes_price,
                        km.no_price as kalshi_no_price,
                        km.volume as kalshi_volume,
                        km.open_interest as kalshi_open_interest,
                        km.status as kalshi_market_status,
                        km.subtitle as kalshi_subtitle,
                        km.no_subtitle as kalshi_no_subtitle,
                        km.rules_primary as kalshi_rules_primary,
                        km.extracted_text as kalshi_extracted_text,
                        km.resolution_date as kalshi_resolution_date,
                        -- Kalshi event fields
                        ke.event_id as kalshi_event_id,
                        ke.title as kalshi_event_title,
                        ke.category as kalshi_event_category,
                        ke.sub_category as kalshi_event_sub_category,
                        ke.close_time as kalshi_event_close_time,
                        ke.status as kalshi_event_status,
                        -- Polymarket market fields
                        pm.market_id as polymarket_market_id,
                        pm.title as polymarket_market_title,
                        pm.yes_price as polymarket_yes_price,
                        pm.no_price as polymarket_no_price,
                        pm.liquidity as polymarket_liquidity,
                        pm.volume as polymarket_volume,
                        pm.status as polymarket_market_status,
                        pm.description as polymarket_description,
                        pm.slug as polymarket_slug,
                        pm.extracted_text as polymarket_extracted_text,
                        pm.resolution_date as polymarket_resolution_date,
                        -- Polymarket event fields
                        pe.event_id as polymarket_event_id,
                        pe.title as polymarket_event_title,
                        pe.category as polymarket_event_category,
                        pe.close_time as polymarket_event_close_time,
                        pe.status as polymarket_event_status,
                        -- Row number to pick highest confidence per polymarket_market_id
                        ROW_NUMBER() OVER (PARTITION BY pm.market_id ORDER BY mm.confidence DESC NULLS LAST) as rn
                    FROM market_matching mm
                    LEFT JOIN kalshi_markets km ON mm.kalshi_market_id = km.market_id
                    LEFT JOIN kalshi_events ke ON km.event_id = ke.event_id
                    LEFT JOIN polymarket_markets pm ON mm.polymarket_market_id = pm.market_id
                    LEFT JOIN polymarket_events pe ON pm.event_id = pe.event_id
                    WHERE mm.arb_price >= %s
                    AND mm.confidence >= %s
                    AND km.yes_price BETWEEN 0.05 AND 0.95
                    AND km.no_price BETWEEN 0.05 AND 0.95
                    AND pm.yes_price BETWEEN 0.05 AND 0.95
                    AND pm.no_price BETWEEN 0.05 AND 0.95
                    AND pm.status = 'open'
                    AND km.status = 'active'
                    AND mm.show = true
                    AND mm.verified = false
                    AND (km.resolution_date IS NULL OR km.resolution_date > CURRENT_DATE)
                    AND (pm.resolution_date IS NULL OR pm.resolution_date > CURRENT_DATE)
                ) AS ranked
                WHERE rn = 1
                AND arb_price <= %s
            """ + order_by_clause + """
                LIMIT %s
            """
            params = [min_arb_price, min_confidence, max_arb_price, num_markets]
            
            # Execute query to get top opportunities
            cursor.execute(query, params)
            top_results = cursor.fetchall()
            
            # Structure the top opportunities and collect polymarket_market_ids
            top_opportunities = []
            polymarket_market_ids = set()
            top_opportunity_ids = set()
            main_match_by_polymarket = {}  # Track main match (kalshi_market_id) for each polymarket_market_id
            # The main match is the one that initially passed the filters (from top_results), not necessarily the highest confidence
            
            for row in top_results:
                opportunity = structure_opportunity(row)
                top_opportunities.append(opportunity)
                if row['polymarket_market_id']:
                    polymarket_market_ids.add(row['polymarket_market_id'])
                    # Store the main match (kalshi_market_id) - this is the one that initially passed the filters
                    if row['polymarket_market_id'] not in main_match_by_polymarket:
                        main_match_by_polymarket[row['polymarket_market_id']] = row['kalshi_market_id']
                top_opportunity_ids.add(row['id'])
            
            # Now get all additional matches for these Polymarket market IDs (excluding the ones already returned)
            additional_matches = []
            if polymarket_market_ids:
                # Build query for additional matches
                # Use IN clause for polymarket_market_id and NOT IN for excluding top opportunity IDs
                polymarket_ids_list = list(polymarket_market_ids)
                top_ids_list = list(top_opportunity_ids)
                
                additional_query = """
                    SELECT 
                        mm.id,
                        mm.arb_price,
                        mm.confidence,
                        -- Kalshi market fields
                        km.market_id as kalshi_market_id,
                        km.title as kalshi_market_title,
                        km.yes_price as kalshi_yes_price,
                        km.no_price as kalshi_no_price,
                        km.volume as kalshi_volume,
                        km.open_interest as kalshi_open_interest,
                        km.status as kalshi_market_status,
                        km.subtitle as kalshi_subtitle,
                        km.no_subtitle as kalshi_no_subtitle,
                        km.rules_primary as kalshi_rules_primary,
                        km.extracted_text as kalshi_extracted_text,
                        km.resolution_date as kalshi_resolution_date,
                        -- Kalshi event fields
                        ke.event_id as kalshi_event_id,
                        ke.title as kalshi_event_title,
                        ke.category as kalshi_event_category,
                        ke.sub_category as kalshi_event_sub_category,
                        ke.close_time as kalshi_event_close_time,
                        ke.status as kalshi_event_status,
                        -- Polymarket market fields
                        pm.market_id as polymarket_market_id,
                        pm.title as polymarket_market_title,
                        pm.yes_price as polymarket_yes_price,
                        pm.no_price as polymarket_no_price,
                        pm.liquidity as polymarket_liquidity,
                        pm.volume as polymarket_volume,
                        pm.status as polymarket_market_status,
                        pm.description as polymarket_description,
                        pm.slug as polymarket_slug,
                        pm.extracted_text as polymarket_extracted_text,
                        pm.resolution_date as polymarket_resolution_date,
                        -- Polymarket event fields
                        pe.event_id as polymarket_event_id,
                        pe.title as polymarket_event_title,
                        pe.category as polymarket_event_category,
                        pe.close_time as polymarket_event_close_time,
                        pe.status as polymarket_event_status
                    FROM market_matching mm
                    LEFT JOIN kalshi_markets km ON mm.kalshi_market_id = km.market_id
                    LEFT JOIN kalshi_events ke ON km.event_id = ke.event_id
                    LEFT JOIN polymarket_markets pm ON mm.polymarket_market_id = pm.market_id
                    LEFT JOIN polymarket_events pe ON pm.event_id = pe.event_id
                    WHERE mm.polymarket_market_id = ANY(%s)
                    AND mm.show = true
                    AND mm.verified = false
                    AND mm.id NOT IN %s
                    AND (km.resolution_date IS NULL OR km.resolution_date > CURRENT_DATE)
                    AND (pm.resolution_date IS NULL OR pm.resolution_date > CURRENT_DATE)
                    ORDER BY mm.arb_price DESC NULLS LAST, mm.confidence DESC NULLS LAST, mm.id DESC
                """
                additional_params = [
                    polymarket_ids_list,
                    tuple(top_ids_list) if top_ids_list else (0,)  # Use tuple for NOT IN, fallback if empty
                ]
                
                cursor.execute(additional_query, additional_params)
                additional_results = cursor.fetchall()
                
                for row in additional_results:
                    match = structure_opportunity(row)
                    additional_matches.append(match)
            
            # Group all opportunities by polymarket_market_id
            # Dictionary where key is polymarket_market_id and value is dict with 'main_match' and 'opportunities'
            opportunities_by_market = {}
            
            # First, add all top opportunities (these are the ones that initially passed the filters)
            for opportunity in top_opportunities:
                polymarket_id = opportunity['polymarket_market_id']
                if polymarket_id:
                    if polymarket_id not in opportunities_by_market:
                        opportunities_by_market[polymarket_id] = {
                            'main_match': main_match_by_polymarket.get(polymarket_id),
                            'opportunities': []
                        }
                    opportunities_by_market[polymarket_id]['opportunities'].append(opportunity)
            
            # Then, add all additional matches
            for match in additional_matches:
                polymarket_id = match['polymarket_market_id']
                if polymarket_id:
                    if polymarket_id not in opportunities_by_market:
                        opportunities_by_market[polymarket_id] = {
                            'main_match': main_match_by_polymarket.get(polymarket_id),
                            'opportunities': []
                        }
                    opportunities_by_market[polymarket_id]['opportunities'].append(match)
            
            cursor.close()
            
            # Calculate total count of all opportunities
            total_opportunities = sum(len(market_data['opportunities']) for market_data in opportunities_by_market.values())
            
            return jsonify({
                'success': True,
                'count': len(opportunities_by_market),
                'total_opportunities': total_opportunities,
                'opportunities': opportunities_by_market
            }), 200
            
    except ValueError as e:
        return jsonify({'error': f'Invalid parameter: {str(e)}'}), 400
    except OperationalError as e:
        logger.error(f"Database connection error: {e}")
        return jsonify({'error': 'Database connection error. Please try again.'}), 503
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@app.route('/arbitrage-opportunities-with-matches-unverified', methods=['POST'])
def get_arbitrage_opportunities_with_matches_unverified():
    """
    POST endpoint to get top arbitrage opportunities and all additional matches for those Kalshi market IDs.
    Similar to /arbitrage-opportunities-with-matches, but only returns market matches which are not verified (verified = false).
    
    Request body (JSON, completely optional - can be empty or omitted):
    - num_markets: number of markets to return (default: 10)
    - min_arb_price: minimum arbitrage price (default: 0.5)
    - max_arb_price: maximum arbitrage price (default: 0.05)
    - min_confidence: minimum confidence threshold (default: 0.65)
    - resolution_date_sort: if true, sort opportunities by resolution_date (closest first, nulls last) (default: false)
    
    Content-Type header should be set to application/json, but JSON body is optional.
    If Content-Type is set but body is empty/invalid, defaults will be used.
    
    Returns:
    - JSON object with success status, count, total_opportunities, and opportunities dictionary
    - opportunities: dictionary where keys are kalshi_market_id and values are lists of all opportunities
      for that market (including the best one from top opportunities)
    - count: number of unique Kalshi market IDs
    - total_opportunities: total number of opportunities across all markets
    - Only includes market matches where verified = false
    - Only includes markets where resolution_date is null or post today (valid dates)
    """
    try:
        # Get JSON body - silent=True handles empty/invalid JSON gracefully
        data = request.get_json(silent=True) or {}
        
        # Extract parameters with defaults
        if data.get('num_markets') is None:
            num_markets = 10
        else:
            num_markets = int(data.get('num_markets', 10))
        if data.get('min_arb_price') is None:
            min_arb_price = 0.0
        else:
            min_arb_price = float(data.get('min_arb_price', 0.0))
        min_arb_price = float(data.get('min_arb_price', 0.0))
        if data.get('max_arb_price') is None:
            max_arb_price = 0.05
        else:
            max_arb_price = float(data.get('max_arb_price', 0.05))
        if data.get('min_confidence') is None:
            min_confidence = 0.65
        else:
            min_confidence = float(data.get('min_confidence', 0.65))
        
        # Validate parameters
        if num_markets < 1:
            return jsonify({'error': 'num_markets must be at least 1'}), 400
        
        if num_markets > 100:
            return jsonify({'error': 'num_markets must be at most 100'}), 400
        
        if min_arb_price < 0:
            return jsonify({'error': 'min_arb_price must be non-negative'}), 400
        
        max_arb_price = float(max_arb_price)
        if max_arb_price < min_arb_price:
            return jsonify({'error': 'max_arb_price must be >= min_arb_price'}), 400
        
        if min_confidence < 0 or min_confidence > 1:
            return jsonify({'error': 'min_confidence must be between 0 and 1'}), 400
        
        # Extract resolution_date_sort parameter (default: False)
        resolution_date_sort = data.get('resolution_date_sort', False)
        if isinstance(resolution_date_sort, str):
            resolution_date_sort = resolution_date_sort.lower() in ('true', '1', 'yes')
        else:
            resolution_date_sort = bool(resolution_date_sort)
        
        # Get connection from pool with automatic retry on stale connections
        with get_db_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # First, get the top opportunities (same query as /arbitrage-opportunities-with-matches but with verified = false)
            # Build ORDER BY clause based on resolution_date_sort parameter
            if resolution_date_sort:
                # Sort by closest resolution_date first (earliest date), nulls last, then by arb_price, confidence, id
                # Use CASE to handle NULLs: if both are NULL, return NULL (sorted last), otherwise return the earliest date
                order_by_clause = """
                    ORDER BY CASE
                        WHEN kalshi_resolution_date IS NULL AND polymarket_resolution_date IS NULL THEN NULL
                        WHEN kalshi_resolution_date IS NULL THEN polymarket_resolution_date
                        WHEN polymarket_resolution_date IS NULL THEN kalshi_resolution_date
                        ELSE LEAST(kalshi_resolution_date, polymarket_resolution_date)
                    END ASC NULLS LAST,
                    arb_price DESC NULLS LAST,
                    confidence DESC NULLS LAST,
                    id DESC
                """
            else:
                # Default sorting: arb_price DESC, confidence DESC, id DESC
                order_by_clause = """
                    ORDER BY arb_price DESC NULLS LAST, confidence DESC NULLS LAST, id DESC
                """
            
            query = """
                SELECT * FROM (
                    SELECT 
                        mm.id,
                        mm.arb_price,
                        mm.confidence,
                        -- Kalshi market fields
                        km.market_id as kalshi_market_id,
                        km.title as kalshi_market_title,
                        km.yes_price as kalshi_yes_price,
                        km.no_price as kalshi_no_price,
                        km.volume as kalshi_volume,
                        km.open_interest as kalshi_open_interest,
                        km.status as kalshi_market_status,
                        km.subtitle as kalshi_subtitle,
                        km.no_subtitle as kalshi_no_subtitle,
                        km.rules_primary as kalshi_rules_primary,
                        km.extracted_text as kalshi_extracted_text,
                        km.resolution_date as kalshi_resolution_date,
                        -- Kalshi event fields
                        ke.event_id as kalshi_event_id,
                        ke.title as kalshi_event_title,
                        ke.category as kalshi_event_category,
                        ke.sub_category as kalshi_event_sub_category,
                        ke.close_time as kalshi_event_close_time,
                        ke.status as kalshi_event_status,
                        -- Polymarket market fields
                        pm.market_id as polymarket_market_id,
                        pm.title as polymarket_market_title,
                        pm.yes_price as polymarket_yes_price,
                        pm.no_price as polymarket_no_price,
                        pm.liquidity as polymarket_liquidity,
                        pm.volume as polymarket_volume,
                        pm.status as polymarket_market_status,
                        pm.description as polymarket_description,
                        pm.slug as polymarket_slug,
                        pm.extracted_text as polymarket_extracted_text,
                        pm.resolution_date as polymarket_resolution_date,
                        -- Polymarket event fields
                        pe.event_id as polymarket_event_id,
                        pe.title as polymarket_event_title,
                        pe.category as polymarket_event_category,
                        pe.close_time as polymarket_event_close_time,
                        pe.status as polymarket_event_status,
                        -- Row number to pick highest confidence per kalshi_market_id
                        ROW_NUMBER() OVER (PARTITION BY km.market_id ORDER BY mm.confidence DESC NULLS LAST) as rn
                    FROM market_matching mm
                    LEFT JOIN kalshi_markets km ON mm.kalshi_market_id = km.market_id
                    LEFT JOIN kalshi_events ke ON km.event_id = ke.event_id
                    LEFT JOIN polymarket_markets pm ON mm.polymarket_market_id = pm.market_id
                    LEFT JOIN polymarket_events pe ON pm.event_id = pe.event_id
                    WHERE mm.arb_price >= %s
                    AND mm.confidence >= %s
                    AND km.yes_price BETWEEN 0.05 AND 0.95
                    AND km.no_price BETWEEN 0.05 AND 0.95
                    AND pm.yes_price BETWEEN 0.05 AND 0.95
                    AND pm.no_price BETWEEN 0.05 AND 0.95
                    AND pm.status = 'open'
                    AND km.status = 'active'
                    AND mm.show = true
                    AND mm.verified = false
                    AND (km.resolution_date IS NULL OR km.resolution_date > CURRENT_DATE)
                    AND (pm.resolution_date IS NULL OR pm.resolution_date > CURRENT_DATE)
                ) AS ranked
                WHERE rn = 1
                AND arb_price <= %s
            """ + order_by_clause + """
                LIMIT %s
            """
            params = [min_arb_price, min_confidence, max_arb_price, num_markets]
            
            # Execute query to get top opportunities
            cursor.execute(query, params)
            top_results = cursor.fetchall()
            
            # Structure the top opportunities and collect kalshi_market_ids
            top_opportunities = []
            kalshi_market_ids = set()
            top_opportunity_ids = set()
            main_match_by_kalshi = {}  # Track main match (polymarket_market_id) for each kalshi_market_id
            # The main match is the one that initially passed the filters (from top_results), not necessarily the highest confidence
            
            for row in top_results:
                opportunity = structure_opportunity(row)
                top_opportunities.append(opportunity)
                if row['kalshi_market_id']:
                    kalshi_market_ids.add(row['kalshi_market_id'])
                    # Store the main match (polymarket_market_id) - this is the one that initially passed the filters
                    if row['kalshi_market_id'] not in main_match_by_kalshi:
                        main_match_by_kalshi[row['kalshi_market_id']] = row['polymarket_market_id']
                top_opportunity_ids.add(row['id'])
            
            # Now get all additional matches for these Kalshi market IDs (excluding the ones already returned)
            additional_matches = []
            if kalshi_market_ids:
                # Build query for additional matches
                # Use IN clause for kalshi_market_id and NOT IN for excluding top opportunity IDs
                kalshi_ids_list = list(kalshi_market_ids)
                top_ids_list = list(top_opportunity_ids)
                
                additional_query = """
                    SELECT 
                        mm.id,
                        mm.arb_price,
                        mm.confidence,
                        -- Kalshi market fields
                        km.market_id as kalshi_market_id,
                        km.title as kalshi_market_title,
                        km.yes_price as kalshi_yes_price,
                        km.no_price as kalshi_no_price,
                        km.volume as kalshi_volume,
                        km.open_interest as kalshi_open_interest,
                        km.status as kalshi_market_status,
                        km.subtitle as kalshi_subtitle,
                        km.no_subtitle as kalshi_no_subtitle,
                        km.rules_primary as kalshi_rules_primary,
                        km.extracted_text as kalshi_extracted_text,
                        km.resolution_date as kalshi_resolution_date,
                        -- Kalshi event fields
                        ke.event_id as kalshi_event_id,
                        ke.title as kalshi_event_title,
                        ke.category as kalshi_event_category,
                        ke.sub_category as kalshi_event_sub_category,
                        ke.close_time as kalshi_event_close_time,
                        ke.status as kalshi_event_status,
                        -- Polymarket market fields
                        pm.market_id as polymarket_market_id,
                        pm.title as polymarket_market_title,
                        pm.yes_price as polymarket_yes_price,
                        pm.no_price as polymarket_no_price,
                        pm.liquidity as polymarket_liquidity,
                        pm.volume as polymarket_volume,
                        pm.status as polymarket_market_status,
                        pm.description as polymarket_description,
                        pm.slug as polymarket_slug,
                        pm.extracted_text as polymarket_extracted_text,
                        pm.resolution_date as polymarket_resolution_date,
                        -- Polymarket event fields
                        pe.event_id as polymarket_event_id,
                        pe.title as polymarket_event_title,
                        pe.category as polymarket_event_category,
                        pe.close_time as polymarket_event_close_time,
                        pe.status as polymarket_event_status
                    FROM market_matching mm
                    LEFT JOIN kalshi_markets km ON mm.kalshi_market_id = km.market_id
                    LEFT JOIN kalshi_events ke ON km.event_id = ke.event_id
                    LEFT JOIN polymarket_markets pm ON mm.polymarket_market_id = pm.market_id
                    LEFT JOIN polymarket_events pe ON pm.event_id = pe.event_id
                    WHERE mm.kalshi_market_id = ANY(%s)
                    AND mm.show = true
                    AND mm.verified = false
                    AND mm.id NOT IN %s
                    AND (km.resolution_date IS NULL OR km.resolution_date > CURRENT_DATE)
                    AND (pm.resolution_date IS NULL OR pm.resolution_date > CURRENT_DATE)
                    ORDER BY mm.arb_price DESC NULLS LAST, mm.confidence DESC NULLS LAST, mm.id DESC
                """
                additional_params = [
                    kalshi_ids_list,
                    tuple(top_ids_list) if top_ids_list else (0,)  # Use tuple for NOT IN, fallback if empty
                ]
                
                cursor.execute(additional_query, additional_params)
                additional_results = cursor.fetchall()
                
                for row in additional_results:
                    match = structure_opportunity(row)
                    additional_matches.append(match)
            
            # Group all opportunities by kalshi_market_id
            # Dictionary where key is kalshi_market_id and value is dict with 'main_match' and 'opportunities'
            opportunities_by_market = {}
            
            # First, add all top opportunities (these are the ones that initially passed the filters)
            for opportunity in top_opportunities:
                kalshi_id = opportunity['kalshi_market_id']
                if kalshi_id:
                    if kalshi_id not in opportunities_by_market:
                        opportunities_by_market[kalshi_id] = {
                            'main_match': main_match_by_kalshi.get(kalshi_id),
                            'opportunities': []
                        }
                    opportunities_by_market[kalshi_id]['opportunities'].append(opportunity)
            
            # Then, add all additional matches
            for match in additional_matches:
                kalshi_id = match['kalshi_market_id']
                if kalshi_id:
                    if kalshi_id not in opportunities_by_market:
                        opportunities_by_market[kalshi_id] = {
                            'main_match': main_match_by_kalshi.get(kalshi_id),
                            'opportunities': []
                        }
                    opportunities_by_market[kalshi_id]['opportunities'].append(match)
            
            cursor.close()
            
            # Calculate total count of all opportunities
            total_opportunities = sum(len(market_data['opportunities']) for market_data in opportunities_by_market.values())
            
            return jsonify({
                'success': True,
                'count': len(opportunities_by_market),
                'total_opportunities': total_opportunities,
                'opportunities': opportunities_by_market
            }), 200
            
    except ValueError as e:
        return jsonify({'error': f'Invalid parameter: {str(e)}'}), 400
    except OperationalError as e:
        logger.error(f"Database connection error: {e}")
        return jsonify({'error': 'Database connection error. Please try again.'}), 503
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@app.route('/matched-markets', methods=['POST'])
def get_matched_markets():
    """
    POST endpoint to get all matched markets based on the matching criteria.
    
    Request body (JSON, optional):
    - show_only: if true, only return markets where show = true (default: true)
    - min_arb_price: minimum arbitrage price (default: 0.5)
    - max_arb_price: maximum arbitrage price (default: 0.05)
    
    Returns:
    - JSON object with success status, count, and markets array
    - Each market includes all fields from the matching query
    """
    try:
        # Get JSON body - silent=True handles empty/invalid JSON gracefully
        data = request.get_json(silent=True) or {}
        
        # Extract parameters with defaults
        show_only = data.get('show_only', True)
        if data.get('min_arb_price') is None:
            min_arb_price = 0.0
        else:
            min_arb_price = float(data.get('min_arb_price', 0.0))   
        if data.get('max_arb_price') is None:
            max_arb_price = 0.05
        else:
            max_arb_price = float(data.get('max_arb_price', 0.05))
        
        # Validate parameters
        if min_arb_price < 0:
            return jsonify({'error': 'min_arb_price must be non-negative'}), 400
        
        max_arb_price = float(max_arb_price)
        if max_arb_price < min_arb_price:
            return jsonify({'error': 'max_arb_price must be >= min_arb_price'}), 400
        
        # Get connection from pool with automatic retry on stale connections
        with get_db_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Build query with JOINs to fetch market and event details (same as arbitrage-opportunities)
            query = """
                SELECT 
                    mm.id,
                    mm.arb_price,
                    mm.confidence,
                    mm.show,
                    -- Kalshi market fields
                    km.market_id as kalshi_market_id,
                    km.title as kalshi_market_title,
                    km.yes_price as kalshi_yes_price,
                    km.no_price as kalshi_no_price,
                    km.volume as kalshi_volume,
                    km.open_interest as kalshi_open_interest,
                    km.status as kalshi_market_status,
                    km.subtitle as kalshi_subtitle,
                    km.no_subtitle as kalshi_no_subtitle,
                    km.rules_primary as kalshi_rules_primary,
                    km.extracted_text as kalshi_extracted_text,
                    -- Kalshi event fields
                    ke.event_id as kalshi_event_id,
                    ke.title as kalshi_event_title,
                    ke.category as kalshi_event_category,
                    ke.sub_category as kalshi_event_sub_category,
                    ke.close_time as kalshi_event_close_time,
                    ke.status as kalshi_event_status,
                    -- Polymarket market fields
                    pm.market_id as polymarket_market_id,
                    pm.title as polymarket_market_title,
                    pm.yes_price as polymarket_yes_price,
                    pm.no_price as polymarket_no_price,
                    pm.liquidity as polymarket_liquidity,
                    pm.volume as polymarket_volume,
                    pm.status as polymarket_market_status,
                    pm.description as polymarket_description,
                    pm.slug as polymarket_slug,
                    pm.extracted_text as polymarket_extracted_text,
                    -- Polymarket event fields
                    pe.event_id as polymarket_event_id,
                    pe.title as polymarket_event_title,
                    pe.category as polymarket_event_category,
                    pe.close_time as polymarket_event_close_time,
                    pe.status as polymarket_event_status
                FROM market_matching mm
                LEFT JOIN kalshi_markets km ON mm.kalshi_market_id = km.market_id
                LEFT JOIN kalshi_events ke ON km.event_id = ke.event_id
                LEFT JOIN polymarket_markets pm ON mm.polymarket_market_id = pm.market_id
                LEFT JOIN polymarket_events pe ON pm.event_id = pe.event_id
                WHERE mm.arb_price >= %s
                AND mm.confidence >= 0.65
                AND km.yes_price BETWEEN 0.05 AND 0.95
                AND km.no_price BETWEEN 0.05 AND 0.95
                AND pm.yes_price BETWEEN 0.05 AND 0.95
                AND pm.no_price BETWEEN 0.05 AND 0.95
                AND pm.status = 'open'
                AND km.status = 'active'
                AND mm.verified = false
            """
            params = [min_arb_price]
            
            # Add max_arb_price filter
            query += " AND mm.arb_price <= %s"
            params.append(max_arb_price)
            
            # Add show filter if requested
            if show_only:
                query += " AND mm.show = true"
            
            # Order by arb_price descending (NULLs last), then by confidence descending, then by id descending
            query += " ORDER BY mm.arb_price DESC NULLS LAST, mm.confidence DESC NULLS LAST, mm.id DESC"
            
            # Execute query
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Structure the response with flattened fields
            markets = []
            for row in results:
                market = {
                    # Market matching fields
                    'id': row['id'],
                    'arb_price': float(row['arb_price']) if row['arb_price'] is not None else None,
                    'confidence': float(row['confidence']) if row['confidence'] is not None else None,
                    'show': row.get('show', None),
                    # Kalshi market fields
                    'kalshi_market_id': row['kalshi_market_id'],
                    'kalshi_market_title': row['kalshi_market_title'],
                    'kalshi_market_yes_price': float(row['kalshi_yes_price']) if row['kalshi_yes_price'] is not None else None,
                    'kalshi_market_no_price': float(row['kalshi_no_price']) if row['kalshi_no_price'] is not None else None,
                    'kalshi_market_volume': float(row['kalshi_volume']) if row['kalshi_volume'] is not None else None,
                    'kalshi_market_open_interest': float(row['kalshi_open_interest']) if row['kalshi_open_interest'] is not None else None,
                    'kalshi_market_status': row['kalshi_market_status'],
                    'kalshi_market_subtitle': row['kalshi_subtitle'],
                    'kalshi_market_no_subtitle': row['kalshi_no_subtitle'],
                    'kalshi_market_rules_primary': row['kalshi_rules_primary'],
                    'kalshi_market_extracted_text': row['kalshi_extracted_text'],
                    # Kalshi event fields
                    'kalshi_event_id': row['kalshi_event_id'],
                    'kalshi_event_title': row['kalshi_event_title'],
                    'kalshi_event_category': row['kalshi_event_category'],
                    'kalshi_event_sub_category': row['kalshi_event_sub_category'],
                    'kalshi_event_close_time': row['kalshi_event_close_time'].isoformat() if row['kalshi_event_close_time'] else None,
                    'kalshi_event_status': row['kalshi_event_status'],
                    # Polymarket market fields
                    'polymarket_market_id': row['polymarket_market_id'],
                    'polymarket_market_title': row['polymarket_market_title'],
                    'polymarket_market_yes_price': float(row['polymarket_yes_price']) if row['polymarket_yes_price'] is not None else None,
                    'polymarket_market_no_price': float(row['polymarket_no_price']) if row['polymarket_no_price'] is not None else None,
                    'polymarket_market_liquidity': float(row['polymarket_liquidity']) if row['polymarket_liquidity'] is not None else None,
                    'polymarket_market_volume': float(row['polymarket_volume']) if row['polymarket_volume'] is not None else None,
                    'polymarket_market_status': row['polymarket_market_status'],
                    'polymarket_market_description': row['polymarket_description'],
                    'polymarket_market_slug': row['polymarket_slug'],
                    'polymarket_market_extracted_text': row['polymarket_extracted_text'],
                    # Polymarket event fields
                    'polymarket_event_id': row['polymarket_event_id'],
                    'polymarket_event_title': row['polymarket_event_title'],
                    'polymarket_event_category': row['polymarket_event_category'],
                    'polymarket_event_close_time': row['polymarket_event_close_time'].isoformat() if row['polymarket_event_close_time'] else None,
                    'polymarket_event_status': row['polymarket_event_status']
                }
                markets.append(market)
            
            cursor.close()
            
            return jsonify({
                'success': True,
                'count': len(markets),
                'markets': markets
            }), 200
            
    except ValueError as e:
        return jsonify({'error': f'Invalid parameter: {str(e)}'}), 400
    except OperationalError as e:
        logger.error(f"Database connection error: {e}")
        return jsonify({'error': 'Database connection error. Please try again.'}), 503
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@app.route('/hide-market', methods=['POST'])
def hide_market():
    """
    POST endpoint to update a market match by setting its 'show' and 'verified' columns.
    
    Request body (JSON, required):
    - market_match_id: integer, the id of the market match to update
    - show: boolean, the value to set for the 'show' column
    - verified: boolean, the value to set for the 'verified' column
    
    Returns:
    - JSON object with success status and message
    """
    try:
        # Get JSON body
        data = request.get_json(silent=True)
        
        if not data:
            return jsonify({'error': 'Request body is required'}), 400
        
        market_match_id = data.get('market_match_id')
        
        if market_match_id is None:
            return jsonify({'error': 'market_match_id is required'}), 400
        
        try:
            market_match_id = int(market_match_id)
        except (ValueError, TypeError):
            return jsonify({'error': 'market_match_id must be an integer'}), 400
        
        # Get show and verified values from request
        show = data.get('show')
        verified = data.get('verified')
        
        if show is None:
            return jsonify({'error': 'show is required'}), 400
        
        if verified is None:
            return jsonify({'error': 'verified is required'}), 400
        
        # Validate that show and verified are boolean values
        if not isinstance(show, bool):
            return jsonify({'error': 'show must be a boolean'}), 400
        
        if not isinstance(verified, bool):
            return jsonify({'error': 'verified must be a boolean'}), 400
        
        # Get connection from pool with automatic retry on stale connections
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Update both show and verified columns
            update_query = """
                UPDATE market_matching
                SET show = %s, verified = %s
                WHERE id = %s
            """
            
            cursor.execute(update_query, (show, verified, market_match_id))
            
            # Check if any row was updated
            if cursor.rowcount == 0:
                cursor.close()
                return jsonify({
                    'success': False,
                    'error': f'Market match with id {market_match_id} not found'
                }), 404
            
            conn.commit()
            cursor.close()
            
            return jsonify({
                'success': True,
                'message': f'Market match {market_match_id} has been updated (show={show}, verified={verified})'
            }), 200
            
    except ValueError as e:
        return jsonify({'error': f'Invalid parameter: {str(e)}'}), 400
    except OperationalError as e:
        logger.error(f"Database connection error: {e}")
        return jsonify({'error': 'Database connection error. Please try again.'}), 503
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@app.route('/featured-markets', methods=['POST'])
def get_featured_markets():
    """
    POST endpoint to get all featured markets from the market_matching table.
    
    Request body (JSON, completely optional - can be empty or omitted):
    - No parameters required
    
    Content-Type header should be set to application/json, but JSON body is optional.
    
    Returns:
    - JSON object with success status, count, and featured_markets array
    - Each market includes all fields from the matching query with featured = true
    """
    try:
        # Get JSON body - silent=True handles empty/invalid JSON gracefully
        data = request.get_json(silent=True) or {}
        
        # Get connection from pool with automatic retry on stale connections
        with get_db_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Build query with JOINs to fetch market and event details for featured markets
            query = """
                SELECT 
                    mm.id,
                    mm.arb_price,
                    mm.confidence,
                    mm.featured,
                    -- Kalshi market fields
                    km.market_id as kalshi_market_id,
                    km.title as kalshi_market_title,
                    km.yes_price as kalshi_yes_price,
                    km.no_price as kalshi_no_price,
                    km.volume as kalshi_volume,
                    km.open_interest as kalshi_open_interest,
                    km.status as kalshi_market_status,
                    km.subtitle as kalshi_subtitle,
                    km.no_subtitle as kalshi_no_subtitle,
                    km.rules_primary as kalshi_rules_primary,
                    -- Kalshi event fields
                    ke.event_id as kalshi_event_id,
                    ke.title as kalshi_event_title,
                    ke.category as kalshi_event_category,
                    ke.sub_category as kalshi_event_sub_category,
                    ke.close_time as kalshi_event_close_time,
                    ke.status as kalshi_event_status,
                    -- Polymarket market fields
                    pm.market_id as polymarket_market_id,
                    pm.title as polymarket_market_title,
                    pm.yes_price as polymarket_yes_price,
                    pm.no_price as polymarket_no_price,
                    pm.liquidity as polymarket_liquidity,
                    pm.volume as polymarket_volume,
                    pm.status as polymarket_market_status,
                    pm.description as polymarket_description,
                    pm.slug as polymarket_slug,
                    -- Polymarket event fields
                    pe.event_id as polymarket_event_id,
                    pe.title as polymarket_event_title,
                    pe.category as polymarket_event_category,
                    pe.close_time as polymarket_event_close_time,
                    pe.status as polymarket_event_status
                FROM market_matching mm
                LEFT JOIN kalshi_markets km ON mm.kalshi_market_id = km.market_id
                LEFT JOIN kalshi_events ke ON km.event_id = ke.event_id
                LEFT JOIN polymarket_markets pm ON mm.polymarket_market_id = pm.market_id
                LEFT JOIN polymarket_events pe ON pm.event_id = pe.event_id
                WHERE mm.featured = true
                ORDER BY mm.arb_price DESC NULLS LAST, mm.confidence DESC NULLS LAST, mm.id DESC
            """
            
            # Execute query
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Structure the response with flattened fields
            featured_markets = []
            for row in results:
                market = {
                    # Market matching fields
                    'id': row['id'],
                    'arb_price': float(row['arb_price']) if row['arb_price'] is not None else None,
                    'confidence': float(row['confidence']) if row['confidence'] is not None else None,
                    'featured': row.get('featured', False),
                    # Kalshi market fields
                    'kalshi_market_id': row['kalshi_market_id'],
                    'kalshi_market_title': row['kalshi_market_title'],
                    'kalshi_market_yes_price': float(row['kalshi_yes_price']) if row['kalshi_yes_price'] is not None else None,
                    'kalshi_market_no_price': float(row['kalshi_no_price']) if row['kalshi_no_price'] is not None else None,
                    'kalshi_market_volume': float(row['kalshi_volume']) if row['kalshi_volume'] is not None else None,
                    'kalshi_market_open_interest': float(row['kalshi_open_interest']) if row['kalshi_open_interest'] is not None else None,
                    'kalshi_market_status': row['kalshi_market_status'],
                    'kalshi_market_subtitle': row['kalshi_subtitle'],
                    'kalshi_market_no_subtitle': row['kalshi_no_subtitle'],
                    'kalshi_market_rules_primary': row['kalshi_rules_primary'],
                    # Kalshi event fields
                    'kalshi_event_id': row['kalshi_event_id'],
                    'kalshi_event_title': row['kalshi_event_title'],
                    'kalshi_event_category': row['kalshi_event_category'],
                    'kalshi_event_sub_category': row['kalshi_event_sub_category'],
                    'kalshi_event_close_time': row['kalshi_event_close_time'].isoformat() if row['kalshi_event_close_time'] else None,
                    'kalshi_event_status': row['kalshi_event_status'],
                    # Polymarket market fields
                    'polymarket_market_id': row['polymarket_market_id'],
                    'polymarket_market_title': row['polymarket_market_title'],
                    'polymarket_market_yes_price': float(row['polymarket_yes_price']) if row['polymarket_yes_price'] is not None else None,
                    'polymarket_market_no_price': float(row['polymarket_no_price']) if row['polymarket_no_price'] is not None else None,
                    'polymarket_market_liquidity': float(row['polymarket_liquidity']) if row['polymarket_liquidity'] is not None else None,
                    'polymarket_market_volume': float(row['polymarket_volume']) if row['polymarket_volume'] is not None else None,
                    'polymarket_market_status': row['polymarket_market_status'],
                    'polymarket_market_description': row['polymarket_description'],
                    'polymarket_market_slug': row['polymarket_slug'],
                    # Polymarket event fields
                    'polymarket_event_id': row['polymarket_event_id'],
                    'polymarket_event_title': row['polymarket_event_title'],
                    'polymarket_event_category': row['polymarket_event_category'],
                    'polymarket_event_close_time': row['polymarket_event_close_time'].isoformat() if row['polymarket_event_close_time'] else None,
                    'polymarket_event_status': row['polymarket_event_status']
                }
                featured_markets.append(market)
            
            cursor.close()
            
            return jsonify({
                'success': True,
                'count': len(featured_markets),
                'featured_markets': featured_markets
            }), 200
            
    except OperationalError as e:
        logger.error(f"Database connection error: {e}")
        return jsonify({'error': 'Database connection error. Please try again.'}), 503
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@app.route('/mark-featured', methods=['POST'])
def mark_featured():
    """
    POST endpoint to update a market match by setting its 'featured' column.
    
    Request body (JSON, required):
    - market_match_id: integer, the id of the market match to update
    - featured: boolean, the value to set for the 'featured' column
    
    Returns:
    - JSON object with success status and message
    """
    try:
        # Get JSON body
        data = request.get_json(silent=True)
        
        if not data:
            return jsonify({'error': 'Request body is required'}), 400
        
        market_match_id = data.get('market_match_id')
        
        if market_match_id is None:
            return jsonify({'error': 'market_match_id is required'}), 400
        
        try:
            market_match_id = int(market_match_id)
        except (ValueError, TypeError):
            return jsonify({'error': 'market_match_id must be an integer'}), 400
        
        # Get featured value from request
        featured = data.get('featured')
        
        if featured is None:
            return jsonify({'error': 'featured is required'}), 400
        
        # Validate that featured is a boolean value
        if not isinstance(featured, bool):
            return jsonify({'error': 'featured must be a boolean'}), 400
        
        # Get connection from pool with automatic retry on stale connections
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Update the featured column
            update_query = """
                UPDATE market_matching
                SET featured = %s
                WHERE id = %s
            """
            
            cursor.execute(update_query, (featured, market_match_id))
            
            # Check if any row was updated
            if cursor.rowcount == 0:
                cursor.close()
                return jsonify({
                    'success': False,
                    'error': f'Market match with id {market_match_id} not found'
                }), 404
            
            conn.commit()
            cursor.close()
            
            return jsonify({
                'success': True,
                'message': f'Market match {market_match_id} has been {"marked as featured" if featured else "unmarked as featured"}'
            }), 200
            
    except ValueError as e:
        return jsonify({'error': f'Invalid parameter: {str(e)}'}), 400
    except OperationalError as e:
        logger.error(f"Database connection error: {e}")
        return jsonify({'error': 'Database connection error. Please try again.'}), 503
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@app.route('/market-match', methods=['POST'])
def get_market_match():
    """
    POST endpoint to retrieve the details of a market match by match ID.
    
    Request body (JSON, required):
    - match_id: integer, the id of the market match to retrieve
    
    Returns:
    - JSON object with success status and market match details
    - Includes all fields from market_matching, kalshi_markets, kalshi_events, 
      polymarket_markets, and polymarket_events tables
    """
    try:
        # Get JSON body
        data = request.get_json(silent=True)
        
        if not data:
            return jsonify({'error': 'Request body is required'}), 400
        
        match_id = data.get('match_id')
        
        if match_id is None:
            return jsonify({'error': 'match_id is required'}), 400
        
        try:
            match_id = int(match_id)
        except (ValueError, TypeError):
            return jsonify({'error': 'match_id must be an integer'}), 400
        
        # Get connection from pool with automatic retry on stale connections
        with get_db_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Build query with JOINs to fetch market and event details
            query = """
                SELECT 
                    mm.id,
                    mm.arb_price,
                    mm.confidence,
                    mm.show,
                    mm.verified,
                    mm.featured,
                    -- Kalshi market fields
                    km.market_id as kalshi_market_id,
                    km.title as kalshi_market_title,
                    km.yes_price as kalshi_yes_price,
                    km.no_price as kalshi_no_price,
                    km.volume as kalshi_volume,
                    km.open_interest as kalshi_open_interest,
                    km.status as kalshi_market_status,
                    km.subtitle as kalshi_subtitle,
                    km.no_subtitle as kalshi_no_subtitle,
                    km.rules_primary as kalshi_rules_primary,
                    km.extracted_text as kalshi_extracted_text,
                    -- Kalshi event fields
                    ke.event_id as kalshi_event_id,
                    ke.title as kalshi_event_title,
                    ke.category as kalshi_event_category,
                    ke.sub_category as kalshi_event_sub_category,
                    ke.close_time as kalshi_event_close_time,
                    ke.status as kalshi_event_status,
                    -- Polymarket market fields
                    pm.market_id as polymarket_market_id,
                    pm.title as polymarket_market_title,
                    pm.yes_price as polymarket_yes_price,
                    pm.no_price as polymarket_no_price,
                    pm.liquidity as polymarket_liquidity,
                    pm.volume as polymarket_volume,
                    pm.status as polymarket_market_status,
                    pm.description as polymarket_description,
                    pm.slug as polymarket_slug,
                    pm.extracted_text as polymarket_extracted_text,
                    -- Polymarket event fields
                    pe.event_id as polymarket_event_id,
                    pe.title as polymarket_event_title,
                    pe.category as polymarket_event_category,
                    pe.close_time as polymarket_event_close_time,
                    pe.status as polymarket_event_status
                FROM market_matching mm
                LEFT JOIN kalshi_markets km ON mm.kalshi_market_id = km.market_id
                LEFT JOIN kalshi_events ke ON km.event_id = ke.event_id
                LEFT JOIN polymarket_markets pm ON mm.polymarket_market_id = pm.market_id
                LEFT JOIN polymarket_events pe ON pm.event_id = pe.event_id
                WHERE mm.id = %s
            """
            
            # Execute query
            cursor.execute(query, (match_id,))
            result = cursor.fetchone()
            
            if not result:
                cursor.close()
                return jsonify({
                    'success': False,
                    'error': f'Market match with id {match_id} not found'
                }), 404
            
            # Structure the response using the helper function
            market_match = structure_opportunity(result)
            # Add additional fields that might not be in structure_opportunity
            market_match['show'] = result.get('show', None)
            market_match['verified'] = result.get('verified', None)
            market_match['featured'] = result.get('featured', None)
            
            cursor.close()
            
            return jsonify({
                'success': True,
                'market_match': market_match
            }), 200
            
    except ValueError as e:
        return jsonify({'error': f'Invalid parameter: {str(e)}'}), 400
    except OperationalError as e:
        logger.error(f"Database connection error: {e}")
        return jsonify({'error': 'Database connection error. Please try again.'}), 503
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        if db_pool is None:
            return jsonify({'status': 'unhealthy', 'message': 'Database pool not initialized'}), 503
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return jsonify({'status': 'healthy'}), 200
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'message': str(e)}), 503


if __name__ == '__main__':
    # Initialize database pool
    init_db_pool()
    
    # Run Flask app
    port = int(os.getenv('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=True)




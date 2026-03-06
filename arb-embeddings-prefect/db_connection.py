"""Database connection module for PostgreSQL."""
import os
from typing import Optional, List
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values


class DatabaseConnection:
    """Handles PostgreSQL database connections."""
    
    def __init__(self, db_url: Optional[str] = None):
        """
        Initialize database connection.
        
        Args:
            db_url: PostgreSQL connection URL. If None, reads from DB_URL environment variable.
        """
        self.db_url = db_url or os.getenv('DB_URL')
        if not self.db_url:
            raise ValueError("Database URL must be provided either as argument or DB_URL environment variable")
        self.connection = None
    
    def connect(self):
        """Establish database connection."""
        try:
            # Add keepalive settings to prevent connection timeout
            self.connection = psycopg2.connect(
                self.db_url,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            return self.connection
        except psycopg2.Error as e:
            raise ConnectionError(f"Failed to connect to database: {e}")
    
    def ensure_connected(self):
        """Ensure database connection is alive, reconnect if needed."""
        if not self.connection or self.connection.closed:
            self.connect()
        else:
            # Test connection with a simple query
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
            except (psycopg2.InterfaceError, psycopg2.OperationalError):
                # Connection is dead, reconnect
                self.disconnect()
                self.connect()
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
    
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """
        Execute a SELECT query and return results.
        
        Args:
            query: SQL query string
            params: Optional query parameters for parameterized queries
            
        Returns:
            List of dictionaries representing rows
        """
        self.ensure_connected()
        
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except psycopg2.Error as e:
            raise RuntimeError(f"Query execution failed: {e}")
    
    def execute_update(self, query: str, params: Optional[tuple] = None):
        """
        Execute an UPDATE, INSERT, or DELETE query.
        
        Args:
            query: SQL query string
            params: Optional query parameters for parameterized queries
            
        Returns:
            Number of rows affected
        """
        self.ensure_connected()
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                self.connection.commit()
                return cursor.rowcount
        except psycopg2.Error as e:
            self.connection.rollback()
            raise RuntimeError(f"Update execution failed: {e}")
    
    def batch_update_embedding_status(self, table_name: str, market_ids: List[str], id_column: str = 'id'):
        """
        Update embedding_created flag to True for multiple market IDs using temporary table with bulk insert.
        
        Args:
            table_name: Name of the table to update ('kalshi_markets' or 'polymarket_markets')
            market_ids: List of market IDs to update
            id_column: Name of the ID column (default: 'id')
            
        Returns:
            Number of rows updated
        """
        if not market_ids:
            return 0
        
        # Ensure connection is alive before using it
        self.ensure_connected()
        
        temp_table_name = f"temp_embedding_ids_{id_column}"
        
        try:
            with self.connection.cursor() as cursor:
                # Create temporary table
                cursor.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table_name} (
                        id_value TEXT
                    )
                """)
                
                # Bulk insert all IDs into temporary table using execute_values (no batching)
                values = [(str(mid),) for mid in market_ids]
                execute_values(
                    cursor,
                    f"INSERT INTO {temp_table_name} (id_value) VALUES %s",
                    values
                )
                
                # Update main table using JOIN with temporary table
                update_query = f"""
                    UPDATE {table_name} AS t
                    SET embedding_created = TRUE
                    FROM {temp_table_name} AS v
                    WHERE t.{id_column}::text = v.id_value
                """
                cursor.execute(update_query)
                
                rows_updated = cursor.rowcount
                
                # Drop temporary table (will be dropped automatically, but explicit is cleaner)
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                
                self.connection.commit()
                return rows_updated
        except psycopg2.Error as e:
            self.connection.rollback()
            raise RuntimeError(f"Bulk update failed for {table_name} with column {id_column}: {e}")
    
    def fetch_markets_with_null_extracted_text(self, table_name: str, events_table: str) -> List[dict]:
        """
        Fetch markets where extracted_text is NULL.
        
        Args:
            table_name: Name of the markets table ('kalshi_markets' or 'polymarket_markets')
            events_table: Name of the events table ('kalshi_events' or 'polymarket_events')
            
        Returns:
            List of dictionaries containing market data with event details
        """
        self.ensure_connected()
        
        if 'kalshi' in table_name.lower():
            query = f"""
                SELECT 
                    m.*,
                    e.title,
                    m.rules_primary as description,
                    e.category
                FROM {table_name} m
                INNER JOIN {events_table} e ON m.event_id = e.event_id
                WHERE m.extracted_text IS NULL
                AND m.status = 'active'
            """
        else:
            query = f"""
                SELECT 
                    m.*,
                    e.title,
                    e.category
                FROM {table_name} m
                INNER JOIN {events_table} e ON m.event_id = e.event_id
                WHERE m.extracted_text IS NULL
                AND m.status = 'open'
            """
        
        return self.execute_query(query)
    
    def batch_update_extracted_text(self, table_name: str, updates: List[tuple], id_column: str = 'id') -> int:
        """
        Update extracted_text for multiple market IDs using temporary table with bulk insert.
        
        Args:
            table_name: Name of the table to update ('kalshi_markets' or 'polymarket_markets')
            updates: List of tuples, each containing (market_id, extracted_text)
            id_column: Name of the ID column (default: 'id')
            
        Returns:
            Number of rows updated
        """
        if not updates:
            return 0
        
        self.ensure_connected()
        
        temp_table_name = f"temp_extracted_text_{id_column}"
        
        try:
            with self.connection.cursor() as cursor:
                # Create temporary table
                cursor.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table_name} (
                        id_value TEXT,
                        extracted_text TEXT
                    )
                """)
                
                # Bulk insert all updates into temporary table using execute_values
                execute_values(
                    cursor,
                    f"INSERT INTO {temp_table_name} (id_value, extracted_text) VALUES %s",
                    updates
                )
                
                # Update main table using JOIN with temporary table
                update_query = f"""
                    UPDATE {table_name} AS t
                    SET extracted_text = v.extracted_text
                    FROM {temp_table_name} AS v
                    WHERE t.{id_column}::text = v.id_value
                """
                cursor.execute(update_query)
                
                rows_updated = cursor.rowcount
                
                # Drop temporary table
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                
                self.connection.commit()
                return rows_updated
        except psycopg2.Error as e:
            self.connection.rollback()
            raise RuntimeError(f"Bulk update of extracted_text failed for {table_name}: {e}")

    def batch_insert_market_matches(self, matches: List[tuple]):
        """
        Insert market matches into the market_matching table using temporary table with bulk insert.
        
        Args:
            matches: List of tuples, each containing (kalshi_market_id, polymarket_market_id, confidence)
                     or (kalshi_market_id, polymarket_market_id) if confidence is not provided
                     
        Returns:
            Number of rows inserted
        """
        if not matches:
            return 0
        
        # Ensure connection is alive before using it
        self.ensure_connected()
        
        temp_table_name = "temp_market_matches"
        
        try:
            with self.connection.cursor() as cursor:
                # Check if matches include confidence (3-tuple) or not (2-tuple)
                has_confidence = len(matches[0]) == 3 if matches else False
                
                # Create temporary table with appropriate columns
                if has_confidence:
                    try:
                        # Try creating temp table with confidence column
                        cursor.execute(f"""
                            CREATE TEMPORARY TABLE {temp_table_name} (
                                kalshi_market_id TEXT,
                                polymarket_market_id TEXT,
                                confidence FLOAT
                            )
                        """)
                        
                        # Bulk insert all matches into temporary table using execute_values (no batching)
                        execute_values(
                            cursor,
                            f"INSERT INTO {temp_table_name} (kalshi_market_id, polymarket_market_id, confidence) VALUES %s",
                            matches
                        )
                        
                        # Upsert from temporary table to main table
                        insert_query = f"""
                            INSERT INTO market_matching (kalshi_market_id, polymarket_market_id, confidence)
                            SELECT kalshi_market_id, polymarket_market_id, confidence
                            FROM {temp_table_name}
                            ON CONFLICT (kalshi_market_id, polymarket_market_id) 
                            DO UPDATE SET confidence = EXCLUDED.confidence
                        """
                        cursor.execute(insert_query)
                        
                        rows_inserted = cursor.rowcount
                        
                        # Drop temporary table
                        cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                        
                        self.connection.commit()
                        return rows_inserted
                    except psycopg2.Error as e:
                        # If confidence column doesn't exist, fall back to 2-column insert
                        error_str = str(e).lower()
                        if 'column "confidence" does not exist' in error_str or 'column "confidence"' in error_str:
                            print("Warning: confidence column not found, inserting without confidence")
                            self.connection.rollback()
                            
                            # Drop temp table if it exists
                            cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                            
                            # Strip confidence and retry with 2-column insert
                            matches_2col = [(k, p) for k, p, _ in matches]
                            
                            # Create temp table without confidence
                            cursor.execute(f"""
                                CREATE TEMPORARY TABLE {temp_table_name} (
                                    kalshi_market_id TEXT,
                                    polymarket_market_id TEXT
                                )
                            """)
                            
                            # Bulk insert matches into temporary table using execute_values (no batching)
                            execute_values(
                                cursor,
                                f"INSERT INTO {temp_table_name} (kalshi_market_id, polymarket_market_id) VALUES %s",
                                matches_2col
                            )
                            
                            # Upsert from temporary table to main table
                            insert_query = f"""
                                INSERT INTO market_matching (kalshi_market_id, polymarket_market_id)
                                SELECT kalshi_market_id, polymarket_market_id
                                FROM {temp_table_name}
                                ON CONFLICT (kalshi_market_id, polymarket_market_id) DO NOTHING
                            """
                            cursor.execute(insert_query)
                            
                            rows_inserted = cursor.rowcount
                            
                            # Drop temporary table
                            cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                            
                            self.connection.commit()
                            return rows_inserted
                        else:
                            raise
                else:
                    # Create temp table without confidence
                    cursor.execute(f"""
                        CREATE TEMPORARY TABLE {temp_table_name} (
                            kalshi_market_id TEXT,
                            polymarket_market_id TEXT
                        )
                    """)
                    
                    # Bulk insert all matches into temporary table using execute_values (no batching)
                    execute_values(
                        cursor,
                        f"INSERT INTO {temp_table_name} (kalshi_market_id, polymarket_market_id) VALUES %s",
                        matches
                    )
                    
                    # Upsert from temporary table to main table
                    insert_query = f"""
                        INSERT INTO market_matching (kalshi_market_id, polymarket_market_id)
                        SELECT kalshi_market_id, polymarket_market_id
                        FROM {temp_table_name}
                        ON CONFLICT (kalshi_market_id, polymarket_market_id) DO NOTHING
                    """
                    cursor.execute(insert_query)
                    
                    rows_inserted = cursor.rowcount
                    
                    # Drop temporary table
                    cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                    
                    self.connection.commit()
                    return rows_inserted
        except psycopg2.Error as e:
            self.connection.rollback()
            raise RuntimeError(f"Bulk insert failed for market_matching: {e}")


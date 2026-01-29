import psycopg2
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, Dict, Any, Tuple
import logging

logger = logging.getLogger(__name__)


@contextmanager
def get_db_connection(db_config):
    """
    Context manager for database connections with automatic commit/rollback
    
    Args:
        db_config: DatabaseConfig object
        
    Yields:
        psycopg2 connection object
        
    Example:
        with get_db_connection(DB_CONFIGS['operational']) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM customers")
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config.get_psycopg2_params())
        logger.info(f"Connected to database: {db_config.database}")
        yield conn
        conn.commit()
        logger.info(f"Transaction committed successfully")
    except Exception as e:
        if conn:
            conn.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info(f"Database connection closed: {db_config.database}")


def log_etl_start(process_name: str, dwh_conn, additional_info: Optional[Dict] = None) -> int:
    """
    Log the start of an ETL process
    
    Args:
        process_name: Name of the ETL process
        dwh_conn: Connection to DWH database
        additional_info: Optional dictionary with additional metadata
        
    Returns:
        log_id: ID of the created log entry
    """
    cursor = dwh_conn.cursor()
    
    try:
        import json
        additional_info_json = json.dumps(additional_info) if additional_info else None
        
        cursor.execute(
            """
            INSERT INTO etl_logs (process_name, start_time, status, additional_info)
            VALUES (%s, %s, %s, %s::jsonb)
            RETURNING log_id
            """,
            (process_name, datetime.now(), 'RUNNING', additional_info_json)
        )
        
        log_id = cursor.fetchone()[0]
        dwh_conn.commit()
        
        logger.info(f"ETL process '{process_name}' started with log_id: {log_id}")
        return log_id
        
    except Exception as e:
        logger.error(f"Failed to log ETL start: {e}")
        raise
    finally:
        cursor.close()


def log_etl_end(
    log_id: int,
    status: str,
    dwh_conn,
    rows_processed: int = 0,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    rows_deleted: int = 0,
    error_message: Optional[str] = None,
    additional_info: Optional[Dict] = None
):
    """
    Log the completion of an ETL process
    
    Args:
        log_id: ID of the log entry to update
        status: Status of the process (SUCCESS, FAILED, WARNING)
        dwh_conn: Connection to DWH database
        rows_processed: Total number of rows processed
        rows_inserted: Number of rows inserted
        rows_updated: Number of rows updated
        rows_deleted: Number of rows deleted
        error_message: Error message if status is FAILED
        additional_info: Optional dictionary with additional metadata
    """
    cursor = dwh_conn.cursor()
    
    try:
        import json
        additional_info_json = json.dumps(additional_info) if additional_info else None
        
        cursor.execute(
            """
            UPDATE etl_logs
            SET 
                end_time = %s,
                status = %s,
                rows_processed = %s,
                rows_inserted = %s,
                rows_updated = %s,
                rows_deleted = %s,
                error_message = %s,
                additional_info = COALESCE(%s::jsonb, additional_info)
            WHERE log_id = %s
            """,
            (
                datetime.now(),
                status,
                rows_processed,
                rows_inserted,
                rows_updated,
                rows_deleted,
                error_message,
                additional_info_json,
                log_id
            )
        )
        
        dwh_conn.commit()
        logger.info(f"ETL process log_id {log_id} completed with status: {status}")
        
    except Exception as e:
        logger.error(f"Failed to log ETL end: {e}")
        raise
    finally:
        cursor.close()


def get_high_water_mark(table_name: str, op_conn) -> datetime:
    """
    Get the high water mark (last extraction timestamp) for a table
    
    Args:
        table_name: Name of the table
        op_conn: Connection to operational database
        
    Returns:
        datetime: Last extraction timestamp
    """
    cursor = op_conn.cursor()
    
    try:
        cursor.execute(
            "SELECT last_updated FROM high_water_mark WHERE table_name = %s",
            (table_name,)
        )
        
        result = cursor.fetchone()
        
        if result is None:
            logger.warning(f"No high water mark found for table '{table_name}', using epoch")
            return datetime(1900, 1, 1)
        
        hwm = result[0]
        logger.info(f"High water mark for '{table_name}': {hwm}")
        return hwm
        
    except Exception as e:
        logger.error(f"Failed to get high water mark: {e}")
        raise
    finally:
        cursor.close()


def update_high_water_mark(table_name: str, new_hwm: datetime, op_conn):
    """
    Update the high water mark for a table
    
    Args:
        table_name: Name of the table
        new_hwm: New high water mark timestamp
        op_conn: Connection to operational database
    """
    cursor = op_conn.cursor()
    
    try:
        cursor.execute(
            """
            UPDATE high_water_mark
            SET last_updated = %s, updated_at = CURRENT_TIMESTAMP
            WHERE table_name = %s
            """,
            (new_hwm, table_name)
        )
        
        if cursor.rowcount == 0:
            # Insert if not exists
            cursor.execute(
                """
                INSERT INTO high_water_mark (table_name, last_updated)
                VALUES (%s, %s)
                """,
                (table_name, new_hwm)
            )
        
        op_conn.commit()
        logger.info(f"High water mark for '{table_name}' updated to: {new_hwm}")
        
    except Exception as e:
        logger.error(f"Failed to update high water mark: {e}")
        raise
    finally:
        cursor.close()


def execute_query(conn, query: str, params: Optional[Tuple] = None) -> int:
    """
    Execute a query and return number of affected rows
    
    Args:
        conn: Database connection
        query: SQL query to execute
        params: Optional query parameters
        
    Returns:
        int: Number of affected rows
    """
    cursor = conn.cursor()
    
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        affected_rows = cursor.rowcount
        logger.info(f"Query executed successfully. Rows affected: {affected_rows}")
        return affected_rows
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise
    finally:
        cursor.close()


def fetch_all(conn, query: str, params: Optional[Tuple] = None) -> list:
    """
    Execute a SELECT query and return all results
    
    Args:
        conn: Database connection
        query: SQL SELECT query
        params: Optional query parameters
        
    Returns:
        list: List of result rows
    """
    cursor = conn.cursor()
    
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        results = cursor.fetchall()
        logger.info(f"Query executed successfully. Rows fetched: {len(results)}")
        return results
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise
    finally:
        cursor.close()

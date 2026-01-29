from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add utils path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

from utils.config import DB_CONFIGS
from utils.db_utils import get_db_connection, log_etl_start, log_etl_end

# DAG defaults
default_args = {
    'owner': 'yessen',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def transform_customers_to_stg(**context):
    """Transform customers from MRR to STG"""
    process_name = 'mrr_to_stg_customers'
    
    with get_db_connection(DB_CONFIGS['mrr']) as mrr_conn, \
         get_db_connection(DB_CONFIGS['stg']) as stg_conn, \
         get_db_connection(DB_CONFIGS['dwh']) as dwh_conn:
        
        # Start logging
        log_id = log_etl_start(process_name, dwh_conn)
        
        try:
            # Extract from MRR
            mrr_cursor = mrr_conn.cursor()
            mrr_cursor.execute("SELECT id, name, country FROM mrr_dim_customers")
            records = mrr_cursor.fetchall()
            mrr_cursor.close()
            
            if not records:
                print("No customers to transform")
                log_etl_end(log_id, 'SUCCESS', dwh_conn, 0, 0)
                return
            
            # Transform and load
            stg_cursor = stg_conn.cursor()
            stg_cursor.executemany("""
                INSERT INTO stg_dim_customers (
                    customer_id, customer_name, country, country_normalized
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE
                SET customer_name = EXCLUDED.customer_name,
                    country = EXCLUDED.country,
                    country_normalized = EXCLUDED.country_normalized,
                    loaded_at = CURRENT_TIMESTAMP
            """, [
                (r[0], r[1].strip() if r[1] else '', r[2], r[2].upper().strip() if r[2] else '')
                for r in records
            ])
            
            rows_affected = stg_cursor.rowcount
            stg_cursor.close()
            
            # Log success
            log_etl_end(log_id, 'SUCCESS', dwh_conn, rows_affected, rows_affected)
            print(f"Transformed {rows_affected} customers to STG")
            
        except Exception as e:
            # Log failure
            log_etl_end(log_id, 'FAILED', dwh_conn, 0, 0, error_message=str(e))
            raise


def transform_products_to_stg(**context):
    """Transform products from MRR to STG"""
    process_name = 'mrr_to_stg_products'
    
    with get_db_connection(DB_CONFIGS['mrr']) as mrr_conn, \
         get_db_connection(DB_CONFIGS['stg']) as stg_conn, \
         get_db_connection(DB_CONFIGS['dwh']) as dwh_conn:
        
        log_id = log_etl_start(process_name, dwh_conn)
        
        try:
            # Extract from MRR
            mrr_cursor = mrr_conn.cursor()
            mrr_cursor.execute("SELECT id, name, group_name FROM mrr_dim_products")
            records = mrr_cursor.fetchall()
            mrr_cursor.close()
            
            if not records:
                print("No products to transform")
                log_etl_end(log_id, 'SUCCESS', dwh_conn, 0, 0)
                return
            
            # Transform and load
            stg_cursor = stg_conn.cursor()
            stg_cursor.executemany("""
                INSERT INTO stg_dim_products (
                    product_id, product_name, product_group, product_group_normalized
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE
                SET product_name = EXCLUDED.product_name,
                    product_group = EXCLUDED.product_group,
                    product_group_normalized = EXCLUDED.product_group_normalized,
                    loaded_at = CURRENT_TIMESTAMP
            """, [
                (r[0], r[1].strip() if r[1] else '', r[2], r[2].upper().strip() if r[2] else '')
                for r in records
            ])
            
            rows_affected = stg_cursor.rowcount
            stg_cursor.close()
            
            log_etl_end(log_id, 'SUCCESS', dwh_conn, rows_affected, rows_affected)
            print(f"Transformed {rows_affected} products to STG")
            
        except Exception as e:
            log_etl_end(log_id, 'FAILED', dwh_conn, 0, 0, error_message=str(e))
            raise


def transform_sales_to_stg(**context):
    """Transform sales from MRR to STG with derived date columns"""
    process_name = 'mrr_to_stg_sales'
    
    with get_db_connection(DB_CONFIGS['mrr']) as mrr_conn, \
         get_db_connection(DB_CONFIGS['stg']) as stg_conn, \
         get_db_connection(DB_CONFIGS['dwh']) as dwh_conn:
        
        log_id = log_etl_start(process_name, dwh_conn)
        
        try:
            # Extract from MRR
            mrr_cursor = mrr_conn.cursor()
            mrr_cursor.execute("""
                SELECT id, customer_id, product_id, qty, transaction_date
                FROM mrr_fact_sales
            """)
            records = mrr_cursor.fetchall()
            mrr_cursor.close()
            
            if not records:
                print("No sales to transform")
                log_etl_end(log_id, 'SUCCESS', dwh_conn, 0, 0)
                return
            
            # Transform with dates
            stg_cursor = stg_conn.cursor()
            stg_cursor.executemany("""
                INSERT INTO stg_fact_sales (
                    sale_id, customer_id, product_id, quantity, transaction_date,
                    transaction_year, transaction_month, transaction_day,
                    transaction_quarter, day_of_week
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    EXTRACT(YEAR FROM %s::TIMESTAMP),
                    EXTRACT(MONTH FROM %s::TIMESTAMP),
                    EXTRACT(DAY FROM %s::TIMESTAMP),
                    EXTRACT(QUARTER FROM %s::TIMESTAMP),
                    EXTRACT(DOW FROM %s::TIMESTAMP)
                )
                ON CONFLICT (sale_id) DO UPDATE
                SET customer_id = EXCLUDED.customer_id,
                    product_id = EXCLUDED.product_id,
                    quantity = EXCLUDED.quantity,
                    transaction_date = EXCLUDED.transaction_date,
                    transaction_year = EXCLUDED.transaction_year,
                    transaction_month = EXCLUDED.transaction_month,
                    transaction_day = EXCLUDED.transaction_day,
                    transaction_quarter = EXCLUDED.transaction_quarter,
                    day_of_week = EXCLUDED.day_of_week,
                    loaded_at = CURRENT_TIMESTAMP
            """, [
                (r[0], r[1], r[2], r[3], r[4], r[4], r[4], r[4], r[4], r[4])
                for r in records
            ])
            
            rows_affected = stg_cursor.rowcount
            stg_cursor.close()
            
            log_etl_end(log_id, 'SUCCESS', dwh_conn, rows_affected, rows_affected)
            print(f"Transformed {rows_affected} sales to STG")
            
        except Exception as e:
            log_etl_end(log_id, 'FAILED', dwh_conn, 0, 0, error_message=str(e))
            raise


# Define DAG
with DAG(
    'etl_mrr_to_stg',
    default_args=default_args,
    description='Transform data from MRR to STG layer',
    schedule_interval='15 * * * *', 
    catchup=False,
    tags=['etl', 'mrr', 'stg', 'transformation'],
) as dag:
    
    
    transform_customers = PythonOperator(
        task_id='transform_customers_to_stg',
        python_callable=transform_customers_to_stg,
    )
    
    transform_products = PythonOperator(
        task_id='transform_products_to_stg',
        python_callable=transform_products_to_stg,
    )
    
    transform_sales = PythonOperator(
        task_id='transform_sales_to_stg',
        python_callable=transform_sales_to_stg,
    )
    
    
    # DAG dependencies
    start >> [transform_customers, transform_products] >> transform_sales >> end
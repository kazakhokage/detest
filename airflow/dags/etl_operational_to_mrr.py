from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import sys
import os


sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

from utils.config import DB_CONFIGS
from utils.db_utils import (
    get_db_connection,
    log_etl_start,
    log_etl_end,
    get_high_water_mark,
    update_high_water_mark
)

# DAG default arguments
default_args = {
    'owner': 'yessen',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def extract_customers_to_mrr(**context):
    """Extract customers from operational to MRR using delta load"""
    process_name = 'operational_to_mrr_customers'
    
    with get_db_connection(DB_CONFIGS['operational']) as op_conn, \
         get_db_connection(DB_CONFIGS['mrr']) as mrr_conn, \
         get_db_connection(DB_CONFIGS['dwh']) as dwh_conn:
        
        log_id = log_etl_start(process_name, dwh_conn)
        
        try:
            # Get high water mark
            hwm = get_high_water_mark('customers', op_conn)
            
            # Extract delta 
            op_cursor = op_conn.cursor()
            op_cursor.execute(
                """
                SELECT id, name, country, created_at
                FROM customers
                WHERE created_at > %s
                ORDER BY created_at
                """,
                (hwm,)
            )
            
            records = op_cursor.fetchall()
            op_cursor.close()
            
            if not records:
                print(f"No new customers to load (HWM: {hwm})")
                log_etl_end(log_id, 'SUCCESS', dwh_conn, 0, 0)
                return
            
            # Insert into MRR 
            mrr_cursor = mrr_conn.cursor()
            mrr_cursor.executemany(
                """
                INSERT INTO mrr_dim_customers (id, name, country)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET name = EXCLUDED.name,
                    country = EXCLUDED.country,
                    loaded_at = CURRENT_TIMESTAMP
                """,
                [(r[0], r[1], r[2]) for r in records]
            )
            
            rows_affected = mrr_cursor.rowcount
            mrr_cursor.close()
            
            # Store new HWM 
            new_hwm = max(r[3] for r in records)
            context['ti'].xcom_push(key='customers_hwm', value=new_hwm.isoformat())
            
            # Log success
            log_etl_end(log_id, 'SUCCESS', dwh_conn, rows_affected, rows_affected)
            
            print(f"Successfully loaded {rows_affected} customers to MRR")
            
        except Exception as e:
            log_etl_end(log_id, 'FAILED', dwh_conn, 0, 0, error_message=str(e))
            raise


def extract_products_to_mrr(**context):
    """Extract products from operational to MRR using delta load"""
    process_name = 'operational_to_mrr_products'
    
    with get_db_connection(DB_CONFIGS['operational']) as op_conn, \
         get_db_connection(DB_CONFIGS['mrr']) as mrr_conn, \
         get_db_connection(DB_CONFIGS['dwh']) as dwh_conn:
        
        log_id = log_etl_start(process_name, dwh_conn)
        
        try:
            # Get high water mark
            hwm = get_high_water_mark('products', op_conn)
            
            # Extract delta 
            op_cursor = op_conn.cursor()
            op_cursor.execute(
                """
                SELECT id, name, group_name, created_at
                FROM products
                WHERE created_at > %s
                ORDER BY created_at
                """,
                (hwm,)
            )
            
            records = op_cursor.fetchall()
            op_cursor.close()
            
            if not records:
                print(f"No new products to load (HWM: {hwm})")
                log_etl_end(log_id, 'SUCCESS', dwh_conn, 0, 0)
                return
            
            # Insert into MRR 
            mrr_cursor = mrr_conn.cursor()
            mrr_cursor.executemany(
                """
                INSERT INTO mrr_dim_products (id, name, group_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET name = EXCLUDED.name,
                    group_name = EXCLUDED.group_name,
                    loaded_at = CURRENT_TIMESTAMP
                """,
                [(r[0], r[1], r[2]) for r in records]
            )
            
            rows_affected = mrr_cursor.rowcount
            mrr_cursor.close()
            
            # Store new HWM 
            new_hwm = max(r[3] for r in records)
            context['ti'].xcom_push(key='products_hwm', value=new_hwm.isoformat())
            
            # Log success
            log_etl_end(log_id, 'SUCCESS', dwh_conn, rows_affected, rows_affected)
            
            print(f"Successfully loaded {rows_affected} products to MRR")
            
        except Exception as e:
            log_etl_end(log_id, 'FAILED', dwh_conn, 0, 0, error_message=str(e))
            raise


def extract_sales_to_mrr(**context):
    """Extract sales from operational to MRR using delta load"""
    process_name = 'operational_to_mrr_sales'
    
    with get_db_connection(DB_CONFIGS['operational']) as op_conn, \
         get_db_connection(DB_CONFIGS['mrr']) as mrr_conn, \
         get_db_connection(DB_CONFIGS['dwh']) as dwh_conn:
        
        log_id = log_etl_start(process_name, dwh_conn)
        
        try:
            # Get high water mark
            hwm = get_high_water_mark('sales', op_conn)
            
            # Extract delta
            op_cursor = op_conn.cursor()
            op_cursor.execute(
                """
                SELECT id, customer_id, product_id, qty, transaction_date
                FROM sales
                WHERE transaction_date > %s
                ORDER BY transaction_date
                """,
                (hwm,)
            )
            
            records = op_cursor.fetchall()
            op_cursor.close()
            
            if not records:
                print(f"No new sales to load (HWM: {hwm})")
                log_etl_end(log_id, 'SUCCESS', dwh_conn, 0, 0)
                return
            
            # Insert into MRR
            mrr_cursor = mrr_conn.cursor()
            mrr_cursor.executemany(
                """
                INSERT INTO mrr_fact_sales (id, customer_id, product_id, qty, transaction_date)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET customer_id = EXCLUDED.customer_id,
                    product_id = EXCLUDED.product_id,
                    qty = EXCLUDED.qty,
                    transaction_date = EXCLUDED.transaction_date,
                    loaded_at = CURRENT_TIMESTAMP
                """,
                records
            )
            
            rows_affected = mrr_cursor.rowcount
            mrr_cursor.close()
            
            # Store new HWM 
            new_hwm = max(r[4] for r in records)
            context['ti'].xcom_push(key='sales_hwm', value=new_hwm.isoformat())
            
            # Log success
            log_etl_end(log_id, 'SUCCESS', dwh_conn, rows_affected, rows_affected)
            
            print(f"Successfully loaded {rows_affected} sales to MRR")
            
        except Exception as e:
            log_etl_end(log_id, 'FAILED', dwh_conn, 0, 0, error_message=str(e))
            raise


# Define DAG
with DAG(
    'etl_operational_to_mrr',
    default_args=default_args,
    description='Extract data from Operational DB to MRR layer with delta loading',
    schedule_interval='0 * * * *',  # Every hour
    catchup=False,
    tags=['etl', 'operational', 'mrr', 'delta-load'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    extract_customers = PythonOperator(
        task_id='extract_customers_to_mrr',
        python_callable=extract_customers_to_mrr,
        provide_context=True,
    )
    
    extract_products = PythonOperator(
        task_id='extract_products_to_mrr',
        python_callable=extract_products_to_mrr,
        provide_context=True,
    )
    
    extract_sales = PythonOperator(
        task_id='extract_sales_to_mrr',
        python_callable=extract_sales_to_mrr,
        provide_context=True,
    )
    
    end = DummyOperator(task_id='end')
    
    # Set task dependencies
    start >> [extract_customers, extract_products] >> extract_sales >> end

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
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


def load_date_dimension(**context):
    """Populate date dimension table"""
    process_name = 'stg_to_dwh_date_dimension'
    
    with get_db_connection(DB_CONFIGS['dwh']) as dwh_conn:
        # Start logging
        log_id = log_etl_start(process_name, dwh_conn)
        
        try:
            cursor = dwh_conn.cursor()
            
            # Generate date range
            cursor.execute("""
                INSERT INTO dwh_dim_date (
                    date_key, date, year, quarter, month, month_name,
                    week, day_of_month, day_of_week, day_name, is_weekend
                )
                SELECT
                    TO_CHAR(d, 'YYYYMMDD')::INTEGER as date_key,
                    d::DATE as date,
                    EXTRACT(YEAR FROM d)::INTEGER as year,
                    EXTRACT(QUARTER FROM d)::INTEGER as quarter,
                    EXTRACT(MONTH FROM d)::INTEGER as month,
                    TO_CHAR(d, 'Month') as month_name,
                    EXTRACT(WEEK FROM d)::INTEGER as week,
                    EXTRACT(DAY FROM d)::INTEGER as day_of_month,
                    EXTRACT(DOW FROM d)::INTEGER as day_of_week,
                    TO_CHAR(d, 'Day') as day_name,
                    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend
                FROM generate_series(
                    CURRENT_DATE - INTERVAL '2 years',
                    CURRENT_DATE + INTERVAL '1 year',
                    '1 day'::INTERVAL
                ) AS d
                ON CONFLICT (date_key) DO NOTHING
            """)
            
            rows_affected = cursor.rowcount
            cursor.close()
            
            # Log success
            log_etl_end(log_id, 'SUCCESS', dwh_conn, rows_affected, rows_affected)
            print(f"Loaded {rows_affected} dates to DWH")
            
        except Exception as e:
            # Log failure
            log_etl_end(log_id, 'FAILED', dwh_conn, 0, 0, error_message=str(e))
            raise


def load_customers_to_dwh(**context):
    """Load customers to DWH with SCD Type 2"""
    process_name = 'stg_to_dwh_customers'
    
    with get_db_connection(DB_CONFIGS['stg']) as stg_conn, \
         get_db_connection(DB_CONFIGS['dwh']) as dwh_conn:
        
        log_id = log_etl_start(process_name, dwh_conn)
        
        try:
            # Extract from STG
            stg_cursor = stg_conn.cursor()
            stg_cursor.execute("""
                SELECT customer_id, customer_name, country
                FROM stg_dim_customers
            """)
            customers = stg_cursor.fetchall()
            stg_cursor.close()
            
            dwh_cursor = dwh_conn.cursor()
            
            for customer_id, name, country in customers:
                # Check existing record
                dwh_cursor.execute("""
                    SELECT customer_key, customer_name, country
                    FROM dwh_dim_customers
                    WHERE customer_id = %s AND is_current = TRUE
                """, (customer_id,))
                
                existing = dwh_cursor.fetchone()
                
                if existing is None:
                    # Insert new customer
                    dwh_cursor.execute("""
                        INSERT INTO dwh_dim_customers (
                            customer_id, customer_name, country, valid_from
                        )
                        VALUES (%s, %s, %s, %s)
                    """, (customer_id, name, country, date.today()))
                    
                elif existing[1] != name or existing[2] != country:
                    # SCD Type 2: Close old
                    dwh_cursor.execute("""
                        UPDATE dwh_dim_customers
                        SET valid_to = %s, is_current = FALSE
                        WHERE customer_id = %s AND is_current = TRUE
                    """, (date.today(), customer_id))
                    
                    # Insert new version
                    dwh_cursor.execute("""
                        INSERT INTO dwh_dim_customers (
                            customer_id, customer_name, country, valid_from
                        )
                        VALUES (%s, %s, %s, %s)
                    """, (customer_id, name, country, date.today()))
            
            rows_affected = len(customers)
            dwh_cursor.close()
            
            log_etl_end(log_id, 'SUCCESS', dwh_conn, rows_affected, rows_affected)
            print(f"Loaded {rows_affected} customers to DWH")
            
        except Exception as e:
            log_etl_end(log_id, 'FAILED', dwh_conn, 0, 0, error_message=str(e))
            raise


def load_products_to_dwh(**context):
    """Load products to DWH with SCD Type 2"""
    process_name = 'stg_to_dwh_products'
    
    with get_db_connection(DB_CONFIGS['stg']) as stg_conn, \
         get_db_connection(DB_CONFIGS['dwh']) as dwh_conn:
        
        log_id = log_etl_start(process_name, dwh_conn)
        
        try:
            # Extract from STG
            stg_cursor = stg_conn.cursor()
            stg_cursor.execute("""
                SELECT product_id, product_name, product_group
                FROM stg_dim_products
            """)
            products = stg_cursor.fetchall()
            stg_cursor.close()
            
            dwh_cursor = dwh_conn.cursor()
            
            for product_id, name, group in products:
                # Check existing record
                dwh_cursor.execute("""
                    SELECT product_key, product_name, product_group
                    FROM dwh_dim_products
                    WHERE product_id = %s AND is_current = TRUE
                """, (product_id,))
                
                existing = dwh_cursor.fetchone()
                
                if existing is None:
                    # Insert new product
                    dwh_cursor.execute("""
                        INSERT INTO dwh_dim_products (
                            product_id, product_name, product_group, valid_from
                        )
                        VALUES (%s, %s, %s, %s)
                    """, (product_id, name, group, date.today()))
                    
                elif existing[1] != name or existing[2] != group:
                    # SCD Type 2: Close old
                    dwh_cursor.execute("""
                        UPDATE dwh_dim_products
                        SET valid_to = %s, is_current = FALSE
                        WHERE product_id = %s AND is_current = TRUE
                    """, (date.today(), product_id))
                    
                    # Insert new version
                    dwh_cursor.execute("""
                        INSERT INTO dwh_dim_products (
                            product_id, product_name, product_group, valid_from
                        )
                        VALUES (%s, %s, %s, %s)
                    """, (product_id, name, group, date.today()))
            
            rows_affected = len(products)
            dwh_cursor.close()
            
            log_etl_end(log_id, 'SUCCESS', dwh_conn, rows_affected, rows_affected)
            print(f"Loaded {rows_affected} products to DWH")
            
        except Exception as e:
            log_etl_end(log_id, 'FAILED', dwh_conn, 0, 0, error_message=str(e))
            raise


def load_sales_to_dwh(**context):
    """Load sales fact table to DWH with surrogate keys"""
    process_name = 'stg_to_dwh_sales'
    
    with get_db_connection(DB_CONFIGS['stg']) as stg_conn, \
         get_db_connection(DB_CONFIGS['dwh']) as dwh_conn:
        
        log_id = log_etl_start(process_name, dwh_conn)
        
        try:
            # Extract from STG
            stg_cursor = stg_conn.cursor()
            stg_cursor.execute("""
                SELECT sale_id, customer_id, product_id, quantity, transaction_date
                FROM stg_fact_sales
            """)
            sales = stg_cursor.fetchall()
            stg_cursor.close()
            
            dwh_cursor = dwh_conn.cursor()
            
            for sale_id, customer_id, product_id, quantity, trans_date in sales:
                # Get surrogate keys
                dwh_cursor.execute("""
                    SELECT customer_key FROM dwh_dim_customers
                    WHERE customer_id = %s AND is_current = TRUE
                """, (customer_id,))
                customer_key = dwh_cursor.fetchone()
                
                dwh_cursor.execute("""
                    SELECT product_key FROM dwh_dim_products
                    WHERE product_id = %s AND is_current = TRUE
                """, (product_id,))
                product_key = dwh_cursor.fetchone()
                
                if customer_key and product_key:
                    date_key = int(trans_date.strftime('%Y%m%d'))
                    
                    # Load fact record
                    dwh_cursor.execute("""
                        INSERT INTO dwh_fact_sales (
                            customer_key, product_key, date_key, original_sale_id,
                            quantity, transaction_date, transaction_timestamp
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (original_sale_id) DO UPDATE
                        SET quantity = EXCLUDED.quantity,
                            loaded_at = CURRENT_TIMESTAMP
                    """, (
                        customer_key[0], product_key[0], date_key, sale_id,
                        quantity, trans_date.date(), trans_date
                    ))
            
            rows_affected = len(sales)
            dwh_cursor.close()
            
            log_etl_end(log_id, 'SUCCESS', dwh_conn, rows_affected, rows_affected)
            print(f"Loaded {rows_affected} sales to DWH")
            
        except Exception as e:
            log_etl_end(log_id, 'FAILED', dwh_conn, 0, 0, error_message=str(e))
            raise


def update_hwm_after_dwh_load(**context):
    """Update High Water Mark after successful DWH load"""
    process_name = 'update_high_water_mark'
    
    with get_db_connection(DB_CONFIGS['operational']) as op_conn, \
         get_db_connection(DB_CONFIGS['dwh']) as dwh_conn:
        
        log_id = log_etl_start(process_name, dwh_conn)
        
        try:
            from datetime import datetime
            from utils.db_utils import update_high_water_mark
            
            ti = context['ti']
            cursor = op_conn.cursor()
            
            # Get max from DWH
            with get_db_connection(DB_CONFIGS['dwh']) as dwh_conn_check:
                dwh_cursor = dwh_conn_check.cursor()
                
                # Update sales HWM
                dwh_cursor.execute("""
                    SELECT MAX(transaction_timestamp) FROM dwh_fact_sales
                """)
                max_sales_date = dwh_cursor.fetchone()[0]
                
                if max_sales_date:
                    update_high_water_mark('sales', max_sales_date, op_conn)
                    print(f"Updated HWM for sales: {max_sales_date}")
                
                dwh_cursor.close()
            
            cursor.close()
            
            log_etl_end(log_id, 'SUCCESS', dwh_conn, 1, 1)
            print("High Water Marks updated successfully")
            
        except Exception as e:
            log_etl_end(log_id, 'FAILED', dwh_conn, 0, 0, error_message=str(e))
            raise


# Define DAG
with DAG(
    'etl_stg_to_dwh',
    default_args=default_args,
    description='Load final DWH from STG with surrogate keys and SCD',
    schedule_interval='30 * * * *',
    catchup=False,
    tags=['etl', 'stg', 'dwh', 'final'],
) as dag:
    
    
    load_date_dim = PythonOperator(
        task_id='load_date_dimension',
        python_callable=load_date_dimension,
    )
    
    load_customers = PythonOperator(
        task_id='load_customers_to_dwh',
        python_callable=load_customers_to_dwh,
    )
    
    load_products = PythonOperator(
        task_id='load_products_to_dwh',
        python_callable=load_products_to_dwh,
    )
    
    load_sales = PythonOperator(
        task_id='load_sales_to_dwh',
        python_callable=load_sales_to_dwh,
    )
    
    update_hwm = PythonOperator(
        task_id='update_high_water_mark',
        python_callable=update_hwm_after_dwh_load,
    )
    
    
    # Task dependencies
    start >> load_date_dim >> [load_customers, load_products] >> load_sales >> update_hwm >> end
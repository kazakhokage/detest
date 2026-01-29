-- ============================================
-- Stored Procedures for DWH
-- Purpose: Business logic and data aggregation with error handling
-- ============================================

-- ============================================
-- Procedure: Aggregate Sales Report
-- Purpose: Create aggregated sales report using cursor for row-by-row processing
-- Features: Cursor, TRY-CATCH (exception handling), ETL logging
-- ============================================
CREATE OR REPLACE PROCEDURE sp_aggregate_sales_report(
    p_start_date DATE,
    p_end_date DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_log_id INTEGER;
    v_process_name VARCHAR(255) := 'sp_aggregate_sales_report';
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_processed INTEGER := 0;
    v_error_message TEXT;
    
    -- Cursor for row-by-row processing of sales data
    sales_cursor CURSOR FOR
        SELECT 
            c.customer_name,
            c.country,
            p.product_group,
            DATE_TRUNC('month', s.transaction_date)::DATE as sales_month,
            SUM(s.quantity) as total_quantity,
            COUNT(DISTINCT s.sale_key) as transaction_count,
            COUNT(DISTINCT s.product_key) as unique_products
        FROM dwh_fact_sales s
        INNER JOIN dwh_dim_customers c ON s.customer_key = c.customer_key AND c.is_current = TRUE
        INNER JOIN dwh_dim_products p ON s.product_key = p.product_key AND p.is_current = TRUE
        WHERE s.transaction_date BETWEEN p_start_date AND p_end_date
        GROUP BY c.customer_name, c.country, p.product_group, DATE_TRUNC('month', s.transaction_date)
        ORDER BY sales_month, c.customer_name, p.product_group;
        
    v_record RECORD;
    
BEGIN
    -- Log process start
    v_log_id := fn_log_etl_start(
        v_process_name, 
        jsonb_build_object(
            'start_date', p_start_date,
            'end_date', p_end_date
        )
    );
    
    RAISE NOTICE 'Starting sales aggregation for period: % to %', p_start_date, p_end_date;
    
    -- Create temporary table for aggregated results
    DROP TABLE IF EXISTS temp_sales_report;
    CREATE TEMP TABLE temp_sales_report (
        customer_name VARCHAR(255),
        country VARCHAR(100),
        product_group VARCHAR(100),
        sales_month DATE,
        total_quantity INTEGER,
        transaction_count INTEGER,
        unique_products INTEGER,
        avg_quantity_per_transaction NUMERIC(10,2)
    );
    
    -- Open cursor and process each row
    OPEN sales_cursor;
    
    LOOP
        -- Fetch next row
        FETCH sales_cursor INTO v_record;
        EXIT WHEN NOT FOUND;
        
        -- Insert aggregated data into temp table
        INSERT INTO temp_sales_report (
            customer_name,
            country,
            product_group,
            sales_month,
            total_quantity,
            transaction_count,
            unique_products,
            avg_quantity_per_transaction
        ) VALUES (
            v_record.customer_name,
            v_record.country,
            v_record.product_group,
            v_record.sales_month,
            v_record.total_quantity,
            v_record.transaction_count,
            v_record.unique_products,
            ROUND(v_record.total_quantity::NUMERIC / v_record.transaction_count, 2)
        );
        
        v_rows_processed := v_rows_processed + 1;
        
        -- Log progress every 1000 rows
        IF v_rows_processed % 1000 = 0 THEN
            RAISE NOTICE 'Processed % rows...', v_rows_processed;
        END IF;
        
    END LOOP;
    
    -- Close cursor
    CLOSE sales_cursor;
    
    RAISE NOTICE 'Aggregation complete. Total rows processed: %', v_rows_processed;
    
    -- Log successful completion
    PERFORM fn_log_etl_end(
        v_log_id,
        'SUCCESS',
        v_rows_processed,
        v_rows_processed,
        0,
        0,
        NULL,
        jsonb_build_object('output_table', 'temp_sales_report')
    );
    
    -- Display summary
    RAISE NOTICE '===== SALES REPORT SUMMARY =====';
    RAISE NOTICE 'Report saved to: temp_sales_report';
    RAISE NOTICE 'Total aggregated rows: %', v_rows_processed;
    RAISE NOTICE 'Date range: % to %', p_start_date, p_end_date;
    
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
        -- Capture error message
        GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
        
        RAISE NOTICE 'ERROR: %', v_error_message;
        
        -- Log failure
        PERFORM fn_log_etl_end(
            v_log_id,
            'FAILED',
            v_rows_processed,
            0,
            0,
            0,
            v_error_message,
            NULL
        );
        
        -- Re-raise the exception
        RAISE EXCEPTION 'Procedure failed: %', v_error_message;
        
        ROLLBACK;
END;
$$;

COMMENT ON PROCEDURE sp_aggregate_sales_report IS 'Aggregates sales data using cursor with error handling and logging';

-- ============================================
-- Procedure: Refresh Customer Metrics
-- Purpose: Calculate and update customer-level metrics
-- Features: Bulk processing, error handling, logging
-- ============================================
CREATE OR REPLACE PROCEDURE sp_refresh_customer_metrics()
LANGUAGE plpgsql
AS $$
DECLARE
    v_log_id INTEGER;
    v_process_name VARCHAR(255) := 'sp_refresh_customer_metrics';
    v_rows_processed INTEGER := 0;
    v_error_message TEXT;
BEGIN
    -- Log process start
    v_log_id := fn_log_etl_start(v_process_name, NULL);
    
    RAISE NOTICE 'Refreshing customer metrics...';
    
    -- Create or replace metrics table
    DROP TABLE IF EXISTS customer_metrics;
    CREATE TABLE customer_metrics AS
    SELECT 
        c.customer_key,
        c.customer_id,
        c.customer_name,
        c.country,
        COUNT(DISTINCT s.sale_key) as total_transactions,
        SUM(s.quantity) as total_quantity_purchased,
        COUNT(DISTINCT s.product_key) as unique_products_purchased,
        MIN(s.transaction_date) as first_purchase_date,
        MAX(s.transaction_date) as last_purchase_date,
        ROUND(AVG(s.quantity), 2) as avg_quantity_per_transaction,
        CURRENT_TIMESTAMP as calculated_at
    FROM dwh_dim_customers c
    LEFT JOIN dwh_fact_sales s ON c.customer_key = s.customer_key
    WHERE c.is_current = TRUE
    GROUP BY c.customer_key, c.customer_id, c.customer_name, c.country;
    
    GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
    
    -- Create indexes
    CREATE INDEX idx_customer_metrics_customer_id ON customer_metrics(customer_id);
    CREATE INDEX idx_customer_metrics_country ON customer_metrics(country);
    
    RAISE NOTICE 'Customer metrics refreshed. Total customers: %', v_rows_processed;
    
    -- Log success
    PERFORM fn_log_etl_end(
        v_log_id,
        'SUCCESS',
        v_rows_processed,
        v_rows_processed,
        0,
        0,
        NULL,
        jsonb_build_object('output_table', 'customer_metrics')
    );
    
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
        
        RAISE NOTICE 'ERROR: %', v_error_message;
        
        PERFORM fn_log_etl_end(
            v_log_id,
            'FAILED',
            v_rows_processed,
            0,
            0,
            0,
            v_error_message,
            NULL
        );
        
        RAISE EXCEPTION 'Procedure failed: %', v_error_message;
        ROLLBACK;
END;
$$;

COMMENT ON PROCEDURE sp_refresh_customer_metrics IS 'Refreshes customer-level aggregated metrics';

-- ============================================
-- Usage Examples:
--
-- 1. Run sales aggregation report:
--    CALL sp_aggregate_sales_report('2025-01-01', '2025-01-31');
--    SELECT * FROM temp_sales_report ORDER BY sales_month, customer_name;
--
-- 2. Refresh customer metrics:
--    CALL sp_refresh_customer_metrics();
--    SELECT * FROM customer_metrics ORDER BY total_transactions DESC LIMIT 10;
--
-- 3. Check execution logs:
--    SELECT * FROM vw_recent_etl_runs WHERE process_name LIKE 'sp_%';
-- ============================================

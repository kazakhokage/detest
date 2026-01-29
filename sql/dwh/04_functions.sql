-- ============================================
-- Analytical Functions for DWH
-- Purpose: Reusable calculation functions for metrics and KPIs
-- ============================================

-- ============================================
-- Function: Calculate Average Quantity Per Customer
-- Purpose: Returns average purchase quantity for a specific customer
-- Parameters: 
--   - p_customer_id: Customer natural key
--   - p_start_date: Optional start date for filtering
--   - p_end_date: Optional end date for filtering
-- ============================================
CREATE OR REPLACE FUNCTION fn_avg_quantity_per_customer(
    p_customer_id INTEGER,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
RETURNS NUMERIC
LANGUAGE plpgsql
AS $$
DECLARE
    v_avg_qty NUMERIC;
BEGIN
    SELECT COALESCE(ROUND(AVG(s.quantity), 2), 0)
    INTO v_avg_qty
    FROM dwh_fact_sales s
    INNER JOIN dwh_dim_customers c ON s.customer_key = c.customer_key
    WHERE c.customer_id = p_customer_id
        AND c.is_current = TRUE
        AND (p_start_date IS NULL OR s.transaction_date >= p_start_date)
        AND (p_end_date IS NULL OR s.transaction_date <= p_end_date);
    
    RETURN v_avg_qty;
END;
$$;

COMMENT ON FUNCTION fn_avg_quantity_per_customer IS 'Calculates average purchase quantity for a customer within optional date range';

-- ============================================
-- Function: Calculate Product Sales Velocity
-- Purpose: Returns average daily sales quantity for a product
-- Parameters:
--   - p_product_id: Product natural key
--   - p_days: Number of days to look back (default 30)
-- ============================================
CREATE OR REPLACE FUNCTION fn_product_sales_velocity(
    p_product_id INTEGER,
    p_days INTEGER DEFAULT 30
)
RETURNS NUMERIC
LANGUAGE plpgsql
AS $$
DECLARE
    v_velocity NUMERIC;
    v_start_date DATE;
BEGIN
    v_start_date := CURRENT_DATE - p_days;
    
    SELECT COALESCE(ROUND(SUM(s.quantity)::NUMERIC / p_days, 2), 0)
    INTO v_velocity
    FROM dwh_fact_sales s
    INNER JOIN dwh_dim_products p ON s.product_key = p.product_key
    WHERE p.product_id = p_product_id
        AND p.is_current = TRUE
        AND s.transaction_date >= v_start_date;
    
    RETURN v_velocity;
END;
$$;

COMMENT ON FUNCTION fn_product_sales_velocity IS 'Calculates average daily sales velocity for a product over specified days';

-- ============================================
-- Function: Get Customer Lifetime Value
-- Purpose: Calculate total quantity purchased by customer (simplified LTV)
-- Parameters:
--   - p_customer_id: Customer natural key
-- ============================================
CREATE OR REPLACE FUNCTION fn_customer_lifetime_value(
    p_customer_id INTEGER
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_ltv INTEGER;
BEGIN
    SELECT COALESCE(SUM(s.quantity), 0)
    INTO v_ltv
    FROM dwh_fact_sales s
    INNER JOIN dwh_dim_customers c ON s.customer_key = c.customer_key
    WHERE c.customer_id = p_customer_id
        AND c.is_current = TRUE;
    
    RETURN v_ltv;
END;
$$;

COMMENT ON FUNCTION fn_customer_lifetime_value IS 'Calculates total lifetime quantity purchased by customer';

-- ============================================
-- Function: Get Customer Recency (days since last purchase)
-- Purpose: Calculate how many days since customer's last purchase
-- Parameters:
--   - p_customer_id: Customer natural key
-- ============================================
CREATE OR REPLACE FUNCTION fn_customer_recency(
    p_customer_id INTEGER
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_days_since_last_purchase INTEGER;
    v_last_purchase_date DATE;
BEGIN
    SELECT MAX(s.transaction_date)
    INTO v_last_purchase_date
    FROM dwh_fact_sales s
    INNER JOIN dwh_dim_customers c ON s.customer_key = c.customer_key
    WHERE c.customer_id = p_customer_id
        AND c.is_current = TRUE;
    
    IF v_last_purchase_date IS NULL THEN
        RETURN NULL; -- Customer has no purchases
    END IF;
    
    v_days_since_last_purchase := CURRENT_DATE - v_last_purchase_date;
    
    RETURN v_days_since_last_purchase;
END;
$$;

COMMENT ON FUNCTION fn_customer_recency IS 'Returns number of days since customer last purchase (NULL if no purchases)';

-- ============================================
-- Function: Get Product Group Performance
-- Purpose: Returns aggregated metrics for a product group
-- Parameters:
--   - p_product_group: Product group name
--   - p_start_date: Optional start date
--   - p_end_date: Optional end date
-- Returns: JSON object with metrics
-- ============================================
CREATE OR REPLACE FUNCTION fn_product_group_performance(
    p_product_group VARCHAR(100),
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_result JSONB;
BEGIN
    SELECT jsonb_build_object(
        'product_group', p_product_group,
        'total_quantity', COALESCE(SUM(s.quantity), 0),
        'total_transactions', COALESCE(COUNT(DISTINCT s.sale_key), 0),
        'unique_customers', COALESCE(COUNT(DISTINCT s.customer_key), 0),
        'unique_products', COALESCE(COUNT(DISTINCT s.product_key), 0),
        'avg_quantity_per_transaction', COALESCE(ROUND(AVG(s.quantity), 2), 0),
        'start_date', COALESCE(p_start_date, MIN(s.transaction_date)),
        'end_date', COALESCE(p_end_date, MAX(s.transaction_date))
    )
    INTO v_result
    FROM dwh_fact_sales s
    INNER JOIN dwh_dim_products p ON s.product_key = p.product_key
    WHERE p.product_group = p_product_group
        AND p.is_current = TRUE
        AND (p_start_date IS NULL OR s.transaction_date >= p_start_date)
        AND (p_end_date IS NULL OR s.transaction_date <= p_end_date);
    
    RETURN v_result;
END;
$$;

COMMENT ON FUNCTION fn_product_group_performance IS 'Returns JSON object with aggregated performance metrics for product group';

-- ============================================
-- Function: Calculate Month-over-Month Growth
-- Purpose: Calculate MoM growth percentage for sales quantity
-- Parameters:
--   - p_year: Year
--   - p_month: Month (1-12)
-- ============================================
CREATE OR REPLACE FUNCTION fn_month_over_month_growth(
    p_year INTEGER,
    p_month INTEGER
)
RETURNS NUMERIC
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_month_total INTEGER;
    v_previous_month_total INTEGER;
    v_growth_rate NUMERIC;
    v_current_date DATE;
    v_previous_date DATE;
BEGIN
    -- Calculate first day of current and previous month
    v_current_date := make_date(p_year, p_month, 1);
    v_previous_date := v_current_date - INTERVAL '1 month';
    
    -- Get current month total
    SELECT COALESCE(SUM(quantity), 0)
    INTO v_current_month_total
    FROM dwh_fact_sales
    WHERE transaction_date >= v_current_date
        AND transaction_date < v_current_date + INTERVAL '1 month';
    
    -- Get previous month total
    SELECT COALESCE(SUM(quantity), 0)
    INTO v_previous_month_total
    FROM dwh_fact_sales
    WHERE transaction_date >= v_previous_date
        AND transaction_date < v_current_date;
    
    -- Calculate growth rate
    IF v_previous_month_total = 0 THEN
        RETURN NULL; -- Cannot calculate growth from zero
    END IF;
    
    v_growth_rate := ROUND(
        ((v_current_month_total - v_previous_month_total)::NUMERIC / v_previous_month_total) * 100,
        2
    );
    
    RETURN v_growth_rate;
END;
$$;

COMMENT ON FUNCTION fn_month_over_month_growth IS 'Calculates month-over-month sales growth percentage';

-- ============================================
-- Function: Get Top N Customers by Quantity
-- Purpose: Returns top N customers ordered by total quantity purchased
-- Parameters:
--   - p_limit: Number of top customers to return
--   - p_start_date: Optional start date
--   - p_end_date: Optional end date
-- Returns: Table of customer info with metrics
-- ============================================
CREATE OR REPLACE FUNCTION fn_top_customers(
    p_limit INTEGER DEFAULT 10,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
RETURNS TABLE (
    customer_id INTEGER,
    customer_name VARCHAR(255),
    country VARCHAR(100),
    total_quantity BIGINT,
    total_transactions BIGINT,
    avg_quantity_per_transaction NUMERIC
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.customer_id,
        c.customer_name,
        c.country,
        SUM(s.quantity) as total_quantity,
        COUNT(DISTINCT s.sale_key) as total_transactions,
        ROUND(AVG(s.quantity), 2) as avg_quantity_per_transaction
    FROM dwh_dim_customers c
    INNER JOIN dwh_fact_sales s ON c.customer_key = s.customer_key
    WHERE c.is_current = TRUE
        AND (p_start_date IS NULL OR s.transaction_date >= p_start_date)
        AND (p_end_date IS NULL OR s.transaction_date <= p_end_date)
    GROUP BY c.customer_id, c.customer_name, c.country
    ORDER BY total_quantity DESC
    LIMIT p_limit;
END;
$$;

COMMENT ON FUNCTION fn_top_customers IS 'Returns top N customers by total quantity purchased';

-- ============================================
-- Usage Examples:
--
-- 1. Average quantity for customer #5:
--    SELECT fn_avg_quantity_per_customer(5);
--
-- 2. Average quantity for customer #5 in January 2025:
--    SELECT fn_avg_quantity_per_customer(5, '2025-01-01', '2025-01-31');
--
-- 3. Product sales velocity (last 30 days):
--    SELECT fn_product_sales_velocity(10, 30);
--
-- 4. Customer lifetime value:
--    SELECT fn_customer_lifetime_value(5);
--
-- 5. Days since last purchase:
--    SELECT fn_customer_recency(5);
--
-- 6. Product group performance:
--    SELECT fn_product_group_performance('Electronics', '2025-01-01', '2025-01-31');
--
-- 7. Month-over-month growth:
--    SELECT fn_month_over_month_growth(2025, 1);
--
-- 8. Top 10 customers:
--    SELECT * FROM fn_top_customers(10);
--
-- 9. Top 5 customers in date range:
--    SELECT * FROM fn_top_customers(5, '2025-01-01', '2025-01-31');
-- ============================================

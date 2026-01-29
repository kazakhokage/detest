-- ============================================
-- MRR (Mirror/Raw) Database Schema
-- Purpose: Raw data storage with minimal transformation
-- Layer: Bronze/Raw layer in data warehouse architecture
-- ============================================

-- Drop tables if they exist (for clean re-runs)
DROP TABLE IF EXISTS mrr_fact_sales CASCADE;
DROP TABLE IF EXISTS mrr_dim_customers CASCADE;
DROP TABLE IF EXISTS mrr_dim_products CASCADE;

-- ============================================
-- MRR Fact Sales Table
-- Naming Convention: mrr_fact_<table_name>
-- Purpose: Store raw sales transactions from operational DB
-- ============================================
CREATE TABLE mrr_fact_sales (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    qty INTEGER NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE mrr_fact_sales IS 'Raw sales fact table - mirrors operational sales with minimal transformation';
COMMENT ON COLUMN mrr_fact_sales.id IS 'Original transaction ID from operational database';
COMMENT ON COLUMN mrr_fact_sales.loaded_at IS 'Timestamp when record was loaded into MRR';

-- ============================================
-- MRR Dimension Customers Table
-- Naming Convention: mrr_dim_<table_name>
-- Purpose: Store customer dimension data
-- ============================================
CREATE TABLE mrr_dim_customers (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    country VARCHAR(100) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE mrr_dim_customers IS 'Raw customer dimension - mirrors operational customers';
COMMENT ON COLUMN mrr_dim_customers.id IS 'Original customer ID from operational database';

-- ============================================
-- MRR Dimension Products Table
-- Naming Convention: mrr_dim_<table_name>
-- Purpose: Store product dimension data
-- ============================================
CREATE TABLE mrr_dim_products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    group_name VARCHAR(100) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE mrr_dim_products IS 'Raw product dimension - mirrors operational products';
COMMENT ON COLUMN mrr_dim_products.id IS 'Original product ID from operational database';

-- Create indexes for query performance
CREATE INDEX idx_mrr_sales_transaction_date ON mrr_fact_sales(transaction_date);
CREATE INDEX idx_mrr_sales_customer_id ON mrr_fact_sales(customer_id);
CREATE INDEX idx_mrr_sales_product_id ON mrr_fact_sales(product_id);
CREATE INDEX idx_mrr_sales_loaded_at ON mrr_fact_sales(loaded_at);

-- ============================================
-- Concept Explanation: Fact Tables
-- Fact tables contain measurable, quantitative data (metrics)
-- Examples: sales amounts, quantities, counts
-- Characteristics:
--   - Large volume of records
--   - Contains foreign keys to dimension tables
--   - Contains numeric measures (qty, amount, etc.)
--   - Typically append-only or slowly updated
-- ============================================

-- ============================================
-- Concept Explanation: Dimension Tables
-- Dimension tables contain descriptive attributes (context)
-- Examples: customer names, product categories, dates
-- Characteristics:
--   - Smaller volume of records
--   - Contains descriptive text fields
--   - Used for filtering, grouping, and labeling
--   - Can be slowly changing (SCD Type 1, 2, 3)
-- ============================================

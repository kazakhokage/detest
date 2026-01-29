-- ============================================
-- STG (Staging) Database Schema
-- Purpose: Cleaned, standardized data with business rules applied
-- Layer: Silver layer in data warehouse architecture
-- ============================================

-- Drop tables if they exist (for clean re-runs)
DROP TABLE IF EXISTS stg_fact_sales CASCADE;
DROP TABLE IF EXISTS stg_dim_customers CASCADE;
DROP TABLE IF EXISTS stg_dim_products CASCADE;

-- ============================================
-- STG Fact Sales Table
-- Naming Convention: stg_fact_<table_name>
-- Purpose: Transformed sales with additional derived columns
-- ============================================
CREATE TABLE stg_fact_sales (
    sale_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    transaction_year INTEGER NOT NULL,
    transaction_month INTEGER NOT NULL,
    transaction_day INTEGER NOT NULL,
    transaction_quarter INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE stg_fact_sales IS 'Staging sales fact table with derived date dimensions';
COMMENT ON COLUMN stg_fact_sales.transaction_year IS 'Extracted year from transaction_date';
COMMENT ON COLUMN stg_fact_sales.transaction_month IS 'Extracted month (1-12) from transaction_date';
COMMENT ON COLUMN stg_fact_sales.transaction_day IS 'Extracted day (1-31) from transaction_date';
COMMENT ON COLUMN stg_fact_sales.transaction_quarter IS 'Calculated quarter (1-4) from transaction_date';
COMMENT ON COLUMN stg_fact_sales.day_of_week IS 'Day of week (0=Sunday, 6=Saturday)';

-- ============================================
-- STG Dimension Customers Table
-- Naming Convention: stg_dim_<table_name>
-- Purpose: Cleaned customer data with standardization
-- ============================================
CREATE TABLE stg_dim_customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    country VARCHAR(100) NOT NULL,
    country_normalized VARCHAR(100),
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE stg_dim_customers IS 'Staging customer dimension with data cleaning applied';
COMMENT ON COLUMN stg_dim_customers.customer_name IS 'Standardized customer name (trimmed, proper case)';
COMMENT ON COLUMN stg_dim_customers.country_normalized IS 'Normalized country name for consistency';
COMMENT ON COLUMN stg_dim_customers.valid_from IS 'Timestamp when this record became valid';

-- ============================================
-- STG Dimension Products Table
-- Naming Convention: stg_dim_<table_name>
-- Purpose: Cleaned product data with standardization
-- ============================================
CREATE TABLE stg_dim_products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    product_group VARCHAR(100) NOT NULL,
    product_group_normalized VARCHAR(100),
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE stg_dim_products IS 'Staging product dimension with data cleaning applied';
COMMENT ON COLUMN stg_dim_products.product_name IS 'Standardized product name';
COMMENT ON COLUMN stg_dim_products.product_group_normalized IS 'Normalized product group for consistency';
COMMENT ON COLUMN stg_dim_products.valid_from IS 'Timestamp when this record became valid';

-- Create indexes for performance
CREATE INDEX idx_stg_sales_transaction_date ON stg_fact_sales(transaction_date);
CREATE INDEX idx_stg_sales_customer_id ON stg_fact_sales(customer_id);
CREATE INDEX idx_stg_sales_product_id ON stg_fact_sales(product_id);
CREATE INDEX idx_stg_sales_year_month ON stg_fact_sales(transaction_year, transaction_month);

-- ============================================
-- Purpose of STG Layer:
-- 1. Data Cleaning: Remove duplicates, handle nulls, fix data types
-- 2. Standardization: Consistent formats, naming conventions
-- 3. Business Rules: Apply domain-specific transformations
-- 4. Derived Attributes: Calculate additional fields for analysis
-- 5. Data Quality: Validate and flag quality issues
-- ============================================

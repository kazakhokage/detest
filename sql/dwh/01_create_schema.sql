-- ============================================
-- DWH (Data Warehouse) Database Schema
-- Purpose: Final analytical warehouse optimized for reporting and analysis
-- Layer: Gold layer in data warehouse architecture
-- ============================================

-- Drop tables if they exist (for clean re-runs)
DROP TABLE IF EXISTS dwh_fact_sales CASCADE;
DROP TABLE IF EXISTS dwh_dim_customers CASCADE;
DROP TABLE IF EXISTS dwh_dim_products CASCADE;
DROP TABLE IF EXISTS dwh_dim_date CASCADE;

-- ============================================
-- DWH Fact Sales Table
-- Naming Convention: dwh_fact_<table_name>
-- Purpose: Final fact table with surrogate keys for optimal analytics
-- ============================================
CREATE TABLE dwh_fact_sales (
    sale_key SERIAL PRIMARY KEY,
    customer_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    date_key INTEGER NOT NULL,
    original_sale_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_timestamp TIMESTAMP NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(original_sale_id)
);

COMMENT ON TABLE dwh_fact_sales IS 'Final sales fact table with surrogate keys for optimal analytics';
COMMENT ON COLUMN dwh_fact_sales.sale_key IS 'Surrogate key - unique identifier in DWH';
COMMENT ON COLUMN dwh_fact_sales.customer_key IS 'Foreign key to dwh_dim_customers';
COMMENT ON COLUMN dwh_fact_sales.product_key IS 'Foreign key to dwh_dim_products';
COMMENT ON COLUMN dwh_fact_sales.date_key IS 'Foreign key to dwh_dim_date (YYYYMMDD format)';
COMMENT ON COLUMN dwh_fact_sales.original_sale_id IS 'Original transaction ID from source system';

-- ============================================
-- DWH Dimension Customers Table (SCD Type 2)
-- Naming Convention: dwh_dim_<table_name>
-- Purpose: Customer dimension with history tracking
-- ============================================
CREATE TABLE dwh_dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    country VARCHAR(100) NOT NULL,
    region VARCHAR(100),
    valid_from DATE NOT NULL,
    valid_to DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dwh_dim_customers IS 'Customer dimension with SCD Type 2 (historical tracking)';
COMMENT ON COLUMN dwh_dim_customers.customer_key IS 'Surrogate key - unique for each version of customer';
COMMENT ON COLUMN dwh_dim_customers.customer_id IS 'Natural key - business identifier from source';
COMMENT ON COLUMN dwh_dim_customers.valid_from IS 'Date when this version became effective';
COMMENT ON COLUMN dwh_dim_customers.valid_to IS 'Date when this version expired (9999-12-31 for current)';
COMMENT ON COLUMN dwh_dim_customers.is_current IS 'Flag indicating if this is the current version';

-- ============================================
-- DWH Dimension Products Table (SCD Type 2)
-- Naming Convention: dwh_dim_<table_name>
-- Purpose: Product dimension with history tracking
-- ============================================
CREATE TABLE dwh_dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    product_group VARCHAR(100) NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dwh_dim_products IS 'Product dimension with SCD Type 2 (historical tracking)';
COMMENT ON COLUMN dwh_dim_products.product_key IS 'Surrogate key - unique for each version of product';
COMMENT ON COLUMN dwh_dim_products.product_id IS 'Natural key - business identifier from source';
COMMENT ON COLUMN dwh_dim_products.valid_from IS 'Date when this version became effective';
COMMENT ON COLUMN dwh_dim_products.valid_to IS 'Date when this version expired (9999-12-31 for current)';
COMMENT ON COLUMN dwh_dim_products.is_current IS 'Flag indicating if this is the current version';

-- ============================================
-- DWH Dimension Date Table
-- Purpose: Date dimension for time-based analysis
-- ============================================
CREATE TABLE dwh_dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE
);

COMMENT ON TABLE dwh_dim_date IS 'Date dimension for time-based analysis';
COMMENT ON COLUMN dwh_dim_date.date_key IS 'Surrogate key in YYYYMMDD format';
COMMENT ON COLUMN dwh_dim_date.is_weekend IS 'Flag indicating if date falls on weekend';

-- Create comprehensive indexes for optimal query performance
CREATE INDEX idx_dwh_sales_customer_key ON dwh_fact_sales(customer_key);
CREATE INDEX idx_dwh_sales_product_key ON dwh_fact_sales(product_key);
CREATE INDEX idx_dwh_sales_date_key ON dwh_fact_sales(date_key);
CREATE INDEX idx_dwh_sales_transaction_date ON dwh_fact_sales(transaction_date);
CREATE INDEX idx_dwh_customers_customer_id ON dwh_dim_customers(customer_id);
CREATE INDEX idx_dwh_customers_is_current ON dwh_dim_customers(is_current);
CREATE INDEX idx_dwh_products_product_id ON dwh_dim_products(product_id);
CREATE INDEX idx_dwh_products_is_current ON dwh_dim_products(is_current);

-- ============================================
-- Add foreign key constraints for referential integrity
-- ============================================
ALTER TABLE dwh_fact_sales
    ADD CONSTRAINT fk_sales_customer FOREIGN KEY (customer_key) 
        REFERENCES dwh_dim_customers(customer_key),
    ADD CONSTRAINT fk_sales_product FOREIGN KEY (product_key) 
        REFERENCES dwh_dim_products(product_key),
    ADD CONSTRAINT fk_sales_date FOREIGN KEY (date_key) 
        REFERENCES dwh_dim_date(date_key);

-- ============================================
-- Purpose of DWH Layer:
-- 1. Surrogate Keys: Use system-generated keys for stability
-- 2. Historical Tracking: Maintain history of dimension changes (SCD Type 2)
-- 3. Optimized Structure: Denormalized for query performance
-- 4. Business-Friendly: Easy to understand and query
-- 5. Referential Integrity: Enforce data quality through constraints
-- ============================================

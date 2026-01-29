-- ============================================
-- Operational Database Schema (OLTP)
-- Purpose: Transactional database for daily operations
-- ============================================

-- Drop tables if they exist (for clean re-runs)
DROP TABLE IF EXISTS sales CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS high_water_mark CASCADE;

-- ============================================
-- Customers Table
-- ============================================
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    country VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE customers IS 'Customer master data';
COMMENT ON COLUMN customers.id IS 'Unique customer identifier';
COMMENT ON COLUMN customers.name IS 'Customer full name';
COMMENT ON COLUMN customers.country IS 'Customer country of residence';

-- ============================================
-- Products Table
-- ============================================
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    group_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE products IS 'Product master data';
COMMENT ON COLUMN products.id IS 'Unique product identifier';
COMMENT ON COLUMN products.name IS 'Product name';
COMMENT ON COLUMN products.group_name IS 'Product category/group';

-- ============================================
-- Sales Table (Fact Table)
-- ============================================
CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    qty INTEGER NOT NULL CHECK (qty > 0),
    transaction_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

COMMENT ON TABLE sales IS 'Sales transactions fact table';
COMMENT ON COLUMN sales.id IS 'Unique transaction identifier';
COMMENT ON COLUMN sales.customer_id IS 'Reference to customer who made purchase';
COMMENT ON COLUMN sales.product_id IS 'Reference to product purchased';
COMMENT ON COLUMN sales.qty IS 'Quantity purchased (must be positive)';
COMMENT ON COLUMN sales.transaction_date IS 'Timestamp of transaction (used for delta loading)';

-- Create indexes for performance
CREATE INDEX idx_sales_transaction_date ON sales(transaction_date);
CREATE INDEX idx_sales_customer_id ON sales(customer_id);
CREATE INDEX idx_sales_product_id ON sales(product_id);

-- ============================================
-- High Water Mark Table
-- Purpose: Track last loaded timestamp for delta loading
-- ============================================
CREATE TABLE high_water_mark (
    table_name VARCHAR(100) PRIMARY KEY,
    last_updated TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE high_water_mark IS 'Tracks last extraction timestamp for each table to enable delta loading';
COMMENT ON COLUMN high_water_mark.table_name IS 'Name of the source table';
COMMENT ON COLUMN high_water_mark.last_updated IS 'Last successful extraction timestamp';

-- Initialize high water mark values (start from epoch to load all data on first run)
INSERT INTO high_water_mark (table_name, last_updated) VALUES
    ('sales', '1900-01-01 00:00:00'),
    ('customers', '1900-01-01 00:00:00'),
    ('products', '1900-01-01 00:00:00');

-- ============================================
-- Verification Queries
-- ============================================
-- Uncomment to verify schema creation:
-- SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
-- SELECT * FROM high_water_mark;

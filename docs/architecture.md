# Data Warehouse Architecture Documentation

## Overview
This document describes the multi-layer data warehouse architecture implemented in this project, following modern data engineering best practices.

## Architecture Layers

### Layer 1: Operational Database (OLTP)
**Purpose**: Source transactional system

**Characteristics**:
- Optimized for transactional workloads (CRUD operations)
- Normalized schema for data integrity
- Real-time updates from applications
- Supports concurrent transactions

**Tables**:
- `customers`: Customer master data
- `products`: Product catalog
- `sales`: Transaction records
- `high_water_mark`: Tracks extraction timestamps for delta loading

**Design Decisions**:
- Foreign key constraints ensure referential integrity
- Indexes on transaction_date support efficient delta extraction
- Small, focused tables minimize write conflicts

---

### Layer 2: MRR (Mirror/Raw Layer) - Bronze
**Purpose**: Raw data storage with minimal transformation

**Characteristics**:
- Nearly identical copy of source data
- Preserves original data types and values
- Adds metadata (loaded_at timestamps)
- Enables data recovery and auditing

**Tables**:
- `mrr_fact_sales`: Raw sales transactions
- `mrr_dim_customers`: Raw customer data
- `mrr_dim_products`: Raw product data

**Naming Convention**:
- Facts: `mrr_fact_<table_name>`
- Dimensions: `mrr_dim_<table_name>`

**Why MRR?**
1. **Data Lake Principles**: Keep raw data unchanged
2. **Reprocessing**: Can rebuild downstream layers if transformations change
3. **Audit Trail**: Compare source vs processed data
4. **Debugging**: Troubleshoot transformation issues

---

### Layer 3: STG (Staging Layer) - Silver
**Purpose**: Cleaned and standardized data with business rules applied

**Characteristics**:
- Data quality improvements (trimming, standardization)
- Derived columns (date parts, calculations)
- Business rule application
- Data type conversions

**Tables**:
- `stg_fact_sales`: Sales with derived date dimensions
- `stg_dim_customers`: Cleaned customer data
- `stg_dim_products`: Standardized product data

**Transformations Applied**:
```sql
-- Example transformations
- TRIM(name)                           -- Remove whitespace
- UPPER(country)                       -- Standardize case
- EXTRACT(YEAR FROM transaction_date)  -- Derive year
- EXTRACT(QUARTER FROM date)           -- Derive quarter
```

**Why STG?**
1. **Separation of Concerns**: Transform data without affecting raw layer
2. **Validation Layer**: Apply data quality checks
3. **Business Logic**: Implement domain-specific rules
4. **Preparation**: Ready data for final warehouse structure

---

### Layer 4: DWH (Data Warehouse) - Gold
**Purpose**: Final analytical warehouse optimized for reporting

**Characteristics**:
- Surrogate keys for stability
- Slowly Changing Dimensions (SCD Type 2)
- Denormalized for query performance
- Business-friendly structure
- Comprehensive indexing

**Tables**:
- `dwh_fact_sales`: Final fact table with surrogate keys
- `dwh_dim_customers`: Customer dimension (SCD Type 2)
- `dwh_dim_products`: Product dimension (SCD Type 2)
- `dwh_dim_date`: Date dimension for time analysis
- `etl_logs`: ETL process monitoring
- `customer_metrics`: Pre-aggregated metrics

**Key Features**:

#### Surrogate Keys
```sql
-- Natural Key vs Surrogate Key
Natural Key:  customer_id = 5 (from source system)
Surrogate Key: customer_key = 123 (DWH-generated)

Why?
- Source keys can change or be reused
- Surrogate keys are immutable
- Enables SCD Type 2 (multiple versions)
```

#### Slowly Changing Dimensions (SCD Type 2)
```sql
-- Example: Customer moves countries
customer_key | customer_id | name | country | valid_from | valid_to   | is_current
-------------|-------------|------|---------|------------|------------|------------
1            | 5           | John | USA     | 2020-01-01 | 2024-06-30 | FALSE
2            | 5           | John | Canada  | 2024-07-01 | 9999-12-31 | TRUE

-- Benefits:
- Maintain historical accuracy
- Point-in-time reporting
- Trend analysis over time
```

#### Date Dimension
```sql
-- Pre-computed date attributes for fast queries
date_key | date       | year | quarter | month | week | is_weekend
---------|------------|------|---------|-------|------|------------
20250115 | 2025-01-15 | 2025 | 1       | 1     | 3    | FALSE

-- Enables queries like:
SELECT SUM(quantity) FROM sales WHERE is_weekend = TRUE;
```

---

## Fact vs Dimension Tables

### Fact Tables
**Definition**: Tables containing measurable, quantitative data

**Characteristics**:
- Large number of rows (millions/billions)
- Numeric measures (quantities, amounts, counts)
- Foreign keys to dimension tables
- Typically append-only or slowly updated
- Grain: One row per business event

**Example**:
```sql
CREATE TABLE dwh_fact_sales (
    sale_key INTEGER PRIMARY KEY,      -- Surrogate key
    customer_key INTEGER,              -- FK to dimension
    product_key INTEGER,               -- FK to dimension
    date_key INTEGER,                  -- FK to dimension
    quantity INTEGER,                  -- MEASURE
    -- Grain: One row per sale transaction
);
```

**Questions Facts Answer**:
- How many units were sold?
- What was the revenue?
- How many transactions occurred?

### Dimension Tables
**Definition**: Tables containing descriptive, contextual attributes

**Characteristics**:
- Smaller number of rows (thousands/millions)
- Descriptive text fields
- Used for filtering, grouping, labeling
- Can be slowly changing
- Provide context to facts

**Example**:
```sql
CREATE TABLE dwh_dim_customers (
    customer_key INTEGER PRIMARY KEY,  -- Surrogate key
    customer_id INTEGER,               -- Natural key
    customer_name VARCHAR(255),        -- ATTRIBUTE
    country VARCHAR(100),              -- ATTRIBUTE
    region VARCHAR(100),               -- ATTRIBUTE
    valid_from DATE,                   -- SCD support
    valid_to DATE,                     -- SCD support
    is_current BOOLEAN                 -- SCD support
);
```

**Questions Dimensions Answer**:
- Who made the purchase?
- What product was purchased?
- When did it happen?
- Where did it occur?

---

## ETL Design Patterns

### High Water Mark Pattern
**Purpose**: Efficiently extract only new/changed records

**Implementation**:
```python
1. Read last extraction timestamp from high_water_mark table
2. Query: SELECT * FROM source WHERE transaction_date > last_hwm
3. Load extracted records through all layers (MRR → STG → DWH)
4. AFTER successful DWH load: Update high_water_mark = MAX(transaction_date)
```

**Why update after DWH, not after MRR?**
- Ensures all layers are synchronized
- Prevents data loss if STG or DWH load fails
- If we updated HWM after MRR and DWH failed, we'd lose track of those records
- Provides transaction-like semantics: all-or-nothing across layers

**Benefits**:
- Reduces data transfer volume
- Faster extraction
- Lower database load
- Supports incremental loading
- Ensures data consistency across all layers

### Idempotency Pattern
**Purpose**: Make ETL processes safe to rerun

**Implementation**:
```sql
-- Use UPSERT pattern
INSERT INTO target (id, name, value)
VALUES (1, 'John', 100)
ON CONFLICT (id) DO UPDATE
SET name = EXCLUDED.name,
    value = EXCLUDED.value,
    updated_at = CURRENT_TIMESTAMP;
```

**Benefits**:
- Safe reruns after failures
- No duplicate data
- Deterministic results
- Easier debugging

### Logging Pattern
**Purpose**: Track ETL execution for monitoring and debugging

**Implementation**:
```python
1. log_start(process_name) → returns log_id
2. Execute ETL logic
3. log_end(log_id, status, metrics)
4. Exception handling logs failures
```

**Benefits**:
- Performance monitoring
- Failure detection
- Audit trail
- Troubleshooting data

---

## Database Selection Rationale

### Why PostgreSQL?
1. **Open Source**: No licensing costs
2. **ACID Compliant**: Data integrity guaranteed
3. **Rich Features**: 
   - Window functions
   - CTEs
   - JSON support
   - Full-text search
4. **Scalability**: Handles large datasets efficiently
5. **Extensions**: PostGIS, pg_partman, etc.

### Why Separate Databases?
1. **Isolation**: Failures in one layer don't affect others
2. **Performance**: Each layer can be optimized independently
3. **Security**: Different access controls per layer
4. **Scalability**: Can move layers to separate servers
5. **Development**: Teams can work on different layers

---

## Data Flow Summary

```
┌──────────────┐  Delta Load    ┌──────────┐
│ Operational  │───────────────>│   MRR    │
│   (Source)   │  (HWM based)   │  (Raw)   │
└──────────────┘                └─────┬────┘
                                      │
                                      │ Clean &
                                      │ Transform
                                      ↓
                                ┌──────────┐
                                │   STG    │
                                │ (Staging)│
                                └─────┬────┘
                                      │
                                      │ Surrogate
                                      │ Keys & SCD
                                      ↓
                                ┌──────────┐
                                │   DWH    │
                                │(Analytics)│
                                └──────────┘
```

## Performance Considerations

### Indexing Strategy
```sql
-- Fact tables: Index on dimension keys
CREATE INDEX idx_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_sales_date ON fact_sales(date_key);

-- Dimension tables: Index on natural keys
CREATE INDEX idx_customer_id ON dim_customers(customer_id);
CREATE INDEX idx_current ON dim_customers(is_current);
```

### Partitioning (Future Enhancement)
```sql
-- Partition fact table by date for better performance
CREATE TABLE fact_sales_2025_01 
PARTITION OF fact_sales 
FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
```

### Materialized Views (Future Enhancement)
```sql
-- Pre-aggregate common queries
CREATE MATERIALIZED VIEW mv_monthly_sales AS
SELECT 
    year, month, product_group,
    SUM(quantity) as total_quantity
FROM fact_sales
GROUP BY year, month, product_group;
```

---

## Security & Access Control

### Recommended Approach
```sql
-- Read-only user for analysts
CREATE USER analyst WITH PASSWORD 'xxx';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst;

-- ETL user with write permissions
CREATE USER etl_user WITH PASSWORD 'xxx';
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO etl_user;

-- Admin user
-- Full access (already postgres user)
```

---

## Monitoring & Alerting

### Key Metrics to Monitor
1. **ETL Duration**: Track execution time trends
2. **Row Counts**: Validate data volume expectations
3. **Failure Rate**: Alert on failed processes
4. **Data Freshness**: Monitor high water mark age
5. **Disk Space**: Track database size growth

### Queries for Monitoring
```sql
-- ETL performance trends
SELECT 
    process_name,
    DATE_TRUNC('day', start_time) as day,
    AVG(duration_seconds) as avg_duration,
    MAX(duration_seconds) as max_duration
FROM etl_logs
WHERE status = 'SUCCESS'
GROUP BY process_name, DATE_TRUNC('day', start_time)
ORDER BY day DESC;

-- Data freshness check
SELECT 
    table_name,
    last_updated,
    CURRENT_TIMESTAMP - last_updated as age
FROM high_water_mark
ORDER BY age DESC;
```

---

## Future Enhancements

1. **Data Quality Framework**: Add data validation rules
2. **Incremental Updates**: Implement CDC (Change Data Capture)
3. **Partitioning**: Partition large fact tables by date
4. **Aggregation Tables**: Pre-compute common queries
5. **Data Lineage**: Track data flow through layers
6. **Alerting**: Set up email/Slack notifications for failures
7. **Testing**: Add unit tests for transformations
8. **CI/CD**: Automate deployment with GitHub Actions

---

## References

- [Kimball Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Apache Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Data Warehouse Toolkit by Ralph Kimball](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/)

##  Architecture

```
┌─────────────────┐
│  Operational DB │  ← OLTP (Source System)
│    (PostgreSQL) │
└────────┬────────┘
         │ Delta Load (High Water Mark)
         ↓
┌─────────────────┐
│      MRR        │  ← Bronze/Raw Layer
│    (PostgreSQL) │     (Minimal transformation)
└────────┬────────┘
         │ Data Cleaning & Transformation
         ↓
┌─────────────────┐
│      STG        │  ← Silver/Staging Layer
│    (PostgreSQL) │     (Business rules applied)
└────────┬────────┘
         │ Surrogate Keys & SCD Type 2
         ↓
┌─────────────────┐
│      DWH        │  ← Gold/Analytics Layer
│    (PostgreSQL) │     (Final warehouse)
└─────────────────┘
```

### Data Flow
1. **Operational** → Extract via delta loading (HWM)
2. **MRR** → Mirror raw data with timestamps
3. **STG** → Clean, standardize, derive attributes
4. **DWH** → Add surrogate keys, implement SCD, optimize for analytics


## Prerequisites

- **Docker** 20.10+
- **Docker Compose** 2.0+
- **Python** 3.10+ (for scripts)
- **PostgreSQL Client** (optional, for manual queries)

##  Quick Start

### 1. Clone and Navigate
```bash
cd detest
```

### 2. Start All Services
```bash
docker-compose up -d
```

Wait for all containers to be healthy (~2 minutes):
```bash
docker-compose ps
```

### 3. Initialize Database Schemas
```bash
# Operational DB
docker exec -i operational-db psql -U postgres -d operational < sql/operational/01_create_schema.sql

# MRR DB
docker exec -i mrr-db psql -U postgres -d mrr < sql/mrr/01_create_schema.sql

# STG DB
docker exec -i stg-db psql -U postgres -d stg < sql/stg/01_create_schema.sql

# DWH DB (all scripts)
docker exec -i dwh-db psql -U postgres -d dwh < sql/dwh/01_create_schema.sql
docker exec -i dwh-db psql -U postgres -d dwh < sql/dwh/02_create_etl_logs.sql
docker exec -i dwh-db psql -U postgres -d dwh < sql/dwh/03_stored_procedures.sql
docker exec -i dwh-db psql -U postgres -d dwh < sql/dwh/04_functions.sql
```

### 4. Generate Sample Data
```bash
# Install Python dependencies
pip install psycopg2-binary faker

# Run data seeding script
python scripts/seed_data.py
```

### 5. Access Airflow UI
```bash
# Open browser to:
http://localhost:8080

# Login credentials:
Username: admin
Password: admin
```

### 6. Run ETL Pipeline

**Run Master Pipeline**
- Enable and trigger: `master_etl_pipeline`
- This runs all stages sequentially

### 7. Verify Results
```bash
# Check DWH data
docker exec -it dwh-db psql -U postgres -d dwh

# Run verification queries
SELECT COUNT(*) FROM dwh_fact_sales;
SELECT COUNT(*) FROM dwh_dim_customers WHERE is_current = TRUE;
SELECT COUNT(*) FROM dwh_dim_products WHERE is_current = TRUE;

# Check ETL logs
SELECT * FROM vw_recent_etl_runs;
```
### 8. For DB instances:
```bash
Operational DB:
  Host: localhost
  Port: 5432
  Database: operational
  User: postgres
  Password: postgres

MRR DB:
  Host: localhost
  Port: 5433
  Database: mrr
  User: postgres
  Password: postgres

STG DB:
  Host: localhost
  Port: 5434
  Database: stg
  User: postgres
  Password: postgres

DWH DB:
  Host: localhost
  Port: 5435
  Database: dwh
  User: postgres
  Password: postgres
```

## Database Layers

### 1. Operational DB (OLTP)
**Purpose**: Transactional source system
- **Tables**: customers, products, sales
- **Pattern**: Normalized for transactional efficiency
- **Updates**: Real-time via applications

### 2. MRR (Mirror/Raw)
**Purpose**: Raw data mirror with minimal transformation
- **Tables**: mrr_fact_sales, mrr_dim_customers, mrr_dim_products
- **Pattern**: Fact/Dimension naming convention
- **Updates**: Delta loads via High Water Mark

### 3. STG (Staging)
**Purpose**: Cleaned and standardized data
- **Tables**: stg_fact_sales, stg_dim_customers, stg_dim_products
- **Transformations**:
  - Data cleaning (trim, standardize)
  - Derived columns (date parts)
  - Business rule application

### 4. DWH (Data Warehouse)
**Purpose**: Final analytical warehouse
- **Tables**: dwh_fact_sales, dwh_dim_*, dwh_dim_date
- **Features**:
  - Surrogate keys
  - SCD Type 2 for dimensions
  - Optimized for analytics
  - ETL logging infrastructure

##  ETL Pipeline

### High Water Mark Mechanism
```python
# Track last extraction timestamp
1. Read HWM from high_water_mark table
2. Extract records WHERE transaction_date > HWM
3. Load to MRR → STG → DWH
4. After successful DWH load: Update HWM = MAX(transaction_date)
```


### Idempotency
```sql
INSERT INTO table VALUES (...)
ON CONFLICT (id) DO UPDATE
SET column = EXCLUDED.column;
```

### Pipeline Flow
```
┌─────────────────────────────────────┐
│  Master ETL Pipeline (every 6 hrs) │
└────────────────┬────────────────────┘
                 │
       ┌─────────┴─────────┐
       │                   │
       ↓                   ↓
┌──────────────┐    ┌──────────────┐
│ Operational  │    │ Monitor logs │
│  → MRR       │    │ in DWH       │
└──────┬───────┘    └──────────────┘
       │
       ↓
┌──────────────┐
│ MRR → STG    │
└──────┬───────┘
       │
       ↓
┌──────────────┐
│ STG → DWH    │
└──────────────┘
```

## Usage

### Running Stored Procedures
```sql
-- Aggregate sales report
CALL sp_aggregate_sales_report('2025-01-01', '2025-01-31');
SELECT * FROM temp_sales_report;

-- Refresh customer metrics
CALL sp_refresh_customer_metrics();
SELECT * FROM customer_metrics LIMIT 10;
```

### Using Analytical Functions
```sql
-- Customer average quantity
SELECT fn_avg_quantity_per_customer(5);

-- Product sales velocity
SELECT fn_product_sales_velocity(10, 30);

-- Top customers
SELECT * FROM fn_top_customers(10);

-- Product group performance
SELECT fn_product_group_performance('Electronics', '2025-01-01', '2025-01-31');
```

### Monitoring ETL
```sql
-- Recent ETL runs
SELECT * FROM vw_recent_etl_runs;

-- Performance summary
SELECT * FROM vw_etl_performance_summary;

-- Failed processes
SELECT * FROM etl_logs WHERE status = 'FAILED' ORDER BY start_time DESC;
```

## Monitoring

### Airflow UI
- **DAG Runs**: Monitor execution history
- **Task Instances**: Debug individual tasks
- **Logs**: View detailed execution logs

### Database Monitoring
```sql
-- Check data freshness
SELECT table_name, last_updated 
FROM high_water_mark 
ORDER BY last_updated DESC;

-- Row counts by layer
SELECT 'operational' as layer, COUNT(*) FROM sales
UNION ALL
SELECT 'mrr', COUNT(*) FROM mrr_fact_sales
UNION ALL
SELECT 'stg', COUNT(*) FROM stg_fact_sales
UNION ALL
SELECT 'dwh', COUNT(*) FROM dwh_fact_sales;
```

### Health Checks
```bash
# All containers running
docker-compose ps

# Database connections
docker exec operational-db pg_isready
docker exec dwh-db pg_isready

# Airflow status
curl http://localhost:8080/health
```


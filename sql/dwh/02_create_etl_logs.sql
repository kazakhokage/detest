-- ============================================
-- ETL Logging Infrastructure
-- Purpose: Track ETL process execution, performance, and errors
-- Location: DWH database (centralized logging)
-- ============================================

-- Drop table if exists (for clean re-runs)
DROP TABLE IF EXISTS etl_logs CASCADE;

-- ============================================
-- ETL Logs Table
-- Purpose: Audit trail for all ETL processes
-- ============================================
CREATE TABLE etl_logs (
    log_id SERIAL PRIMARY KEY,
    process_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds NUMERIC GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (end_time - start_time))
    ) STORED,
    status VARCHAR(50) NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'WARNING')),
    rows_processed INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,
    rows_updated INTEGER DEFAULT 0,
    rows_deleted INTEGER DEFAULT 0,
    error_message TEXT,
    additional_info JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE etl_logs IS 'Centralized ETL process logging and monitoring';
COMMENT ON COLUMN etl_logs.log_id IS 'Unique log entry identifier';
COMMENT ON COLUMN etl_logs.process_name IS 'Name of the ETL process/DAG/task';
COMMENT ON COLUMN etl_logs.start_time IS 'Process start timestamp';
COMMENT ON COLUMN etl_logs.end_time IS 'Process end timestamp (NULL if still running)';
COMMENT ON COLUMN etl_logs.duration_seconds IS 'Calculated duration in seconds';
COMMENT ON COLUMN etl_logs.status IS 'Process status: RUNNING, SUCCESS, FAILED, WARNING';
COMMENT ON COLUMN etl_logs.rows_processed IS 'Total number of rows processed';
COMMENT ON COLUMN etl_logs.rows_inserted IS 'Number of rows inserted';
COMMENT ON COLUMN etl_logs.rows_updated IS 'Number of rows updated';
COMMENT ON COLUMN etl_logs.rows_deleted IS 'Number of rows deleted';
COMMENT ON COLUMN etl_logs.error_message IS 'Error details if status is FAILED';
COMMENT ON COLUMN etl_logs.additional_info IS 'Additional metadata in JSON format';

-- Create indexes for query performance
CREATE INDEX idx_etl_logs_process_name ON etl_logs(process_name);
CREATE INDEX idx_etl_logs_start_time ON etl_logs(start_time);
CREATE INDEX idx_etl_logs_status ON etl_logs(status);
CREATE INDEX idx_etl_logs_process_start ON etl_logs(process_name, start_time);

-- ============================================
-- Helper Function: Log ETL Start
-- ============================================
CREATE OR REPLACE FUNCTION fn_log_etl_start(
    p_process_name VARCHAR(255),
    p_additional_info JSONB DEFAULT NULL
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_log_id INTEGER;
BEGIN
    INSERT INTO etl_logs (process_name, start_time, status, additional_info)
    VALUES (p_process_name, CURRENT_TIMESTAMP, 'RUNNING', p_additional_info)
    RETURNING log_id INTO v_log_id;
    
    RETURN v_log_id;
END;
$$;

COMMENT ON FUNCTION fn_log_etl_start IS 'Logs the start of an ETL process and returns log_id';

-- ============================================
-- Helper Function: Log ETL End
-- ============================================
CREATE OR REPLACE FUNCTION fn_log_etl_end(
    p_log_id INTEGER,
    p_status VARCHAR(50),
    p_rows_processed INTEGER DEFAULT 0,
    p_rows_inserted INTEGER DEFAULT 0,
    p_rows_updated INTEGER DEFAULT 0,
    p_rows_deleted INTEGER DEFAULT 0,
    p_error_message TEXT DEFAULT NULL,
    p_additional_info JSONB DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE etl_logs
    SET 
        end_time = CURRENT_TIMESTAMP,
        status = p_status,
        rows_processed = p_rows_processed,
        rows_inserted = p_rows_inserted,
        rows_updated = p_rows_updated,
        rows_deleted = p_rows_deleted,
        error_message = p_error_message,
        additional_info = COALESCE(p_additional_info, additional_info)
    WHERE log_id = p_log_id;
END;
$$;

COMMENT ON FUNCTION fn_log_etl_end IS 'Updates ETL log entry with completion details';

-- ============================================
-- View: Recent ETL Runs
-- Purpose: Quick overview of recent ETL executions
-- ============================================
CREATE OR REPLACE VIEW vw_recent_etl_runs AS
SELECT 
    log_id,
    process_name,
    start_time,
    end_time,
    ROUND(duration_seconds, 2) as duration_seconds,
    status,
    rows_processed,
    error_message,
    created_at
FROM etl_logs
ORDER BY start_time DESC
LIMIT 100;

COMMENT ON VIEW vw_recent_etl_runs IS 'Most recent 100 ETL process executions';

-- ============================================
-- View: ETL Performance Summary
-- Purpose: Aggregate statistics by process name
-- ============================================
CREATE OR REPLACE VIEW vw_etl_performance_summary AS
SELECT 
    process_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
    ROUND(AVG(duration_seconds), 2) as avg_duration_seconds,
    ROUND(MAX(duration_seconds), 2) as max_duration_seconds,
    ROUND(MIN(duration_seconds), 2) as min_duration_seconds,
    SUM(rows_processed) as total_rows_processed,
    MAX(start_time) as last_run_time
FROM etl_logs
WHERE end_time IS NOT NULL
GROUP BY process_name
ORDER BY last_run_time DESC;

COMMENT ON VIEW vw_etl_performance_summary IS 'Aggregated performance metrics by ETL process';

-- ============================================
-- Usage Examples:
-- 
-- 1. Log ETL start:
--    SELECT fn_log_etl_start('my_etl_process', '{"batch_id": 123}'::jsonb);
--
-- 2. Log ETL end (success):
--    SELECT fn_log_etl_end(1, 'SUCCESS', 1000, 1000, 0, 0);
--
-- 3. Log ETL end (failure):
--    SELECT fn_log_etl_end(1, 'FAILED', 0, 0, 0, 0, 'Connection timeout');
--
-- 4. View recent runs:
--    SELECT * FROM vw_recent_etl_runs;
--
-- 5. View performance summary:
--    SELECT * FROM vw_etl_performance_summary;
-- ============================================

-- Performance Tuning Scripts for EDI Claims Processor
-- ===================================================
-- These scripts optimize both PostgreSQL and SQL Server databases for high-performance claims processing
-- Target: 100,000 records in 15 seconds (~6,667 records/second)

-- ===========================
-- POSTGRESQL PERFORMANCE TUNING
-- ===========================

-- Connect to PostgreSQL database
\c edi_staging;

-- 1. ADVANCED INDEXING STRATEGIES
-- ===============================

-- Composite indexes for common query patterns
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_claims_status_facility_date 
ON staging.claims(processing_status, facility_id, service_date) 
WHERE exported_to_production = FALSE;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_claims_export_status_date 
ON staging.claims(exported_to_production, processing_status, created_date)
WHERE processing_status IN ('COMPLETED', 'VALIDATED');

-- Partial indexes for specific processing states
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_claims_pending_validation 
ON staging.claims(facility_id, financial_class_id, created_date)
WHERE processing_status = 'PENDING' AND exported_to_production = FALSE;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_claims_validation_failed 
ON staging.claims(facility_id, validation_errors, created_date)
WHERE array_length(validation_errors, 1) > 0;

-- Covering indexes to avoid table lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_claims_processing_covering 
ON staging.claims(claim_id, processing_status, facility_id, total_charge_amount, service_date)
WHERE processing_status IN ('PENDING', 'PROCESSING', 'VALIDATED');

-- Expression indexes for common calculations
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_claims_age_hours 
ON staging.claims(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_date))/3600)
WHERE processing_status = 'PENDING';

-- Indexes for validation results
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_validation_results_claim_type_status 
ON staging.validation_results(claim_id, validation_type, validation_status, created_date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_validation_results_performance 
ON staging.validation_results(validation_type, processing_time, ml_inference_time)
WHERE processing_time IS NOT NULL;

-- Indexes for batch processing
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_claim_batches_batch_status 
ON staging.claim_batches(batch_id, assigned_date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_processing_batches_status_date 
ON staging.processing_batches(status, start_time, end_time)
WHERE status IN ('PROCESSING', 'COMPLETED');

-- 2. PARTITIONING FOR LARGE TABLES
-- =================================

-- Partition staging.claims by service_date for better query performance
-- Note: This requires data migration for existing tables

-- Create partitioned table structure (example for new implementation)
/*
CREATE TABLE staging.claims_partitioned (
    LIKE staging.claims INCLUDING ALL
) PARTITION BY RANGE (service_date);

-- Create monthly partitions for the last 2 years and next year
DO $$
DECLARE
    start_date DATE := DATE_TRUNC('month', CURRENT_DATE - INTERVAL '24 months');
    end_date DATE := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '12 months');
    partition_start DATE;
    partition_end DATE;
    partition_name TEXT;
BEGIN
    WHILE start_date < end_date LOOP
        partition_start := start_date;
        partition_end := start_date + INTERVAL '1 month';
        partition_name := 'claims_' || TO_CHAR(partition_start, 'YYYY_MM');
        
        EXECUTE format(
            'CREATE TABLE staging.%I PARTITION OF staging.claims_partitioned 
             FOR VALUES FROM (%L) TO (%L)',
            partition_name, partition_start, partition_end
        );
        
        start_date := partition_end;
    END LOOP;
END $$;
*/

-- 3. MATERIALIZED VIEWS FOR COMPLEX AGGREGATIONS
-- ===============================================

-- Materialized view for validation summary statistics
CREATE MATERIALIZED VIEW staging.mv_validation_summary AS
SELECT 
    DATE_TRUNC('hour', created_date) as hour,
    validation_type,
    validation_status,
    COUNT(*) as count,
    AVG(processing_time) as avg_processing_time,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY processing_time) as p95_processing_time,
    COUNT(*) FILTER (WHERE validation_status = 'FAILED') as failed_count
FROM staging.validation_results 
WHERE created_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE_TRUNC('hour', created_date), validation_type, validation_status;

CREATE UNIQUE INDEX ON staging.mv_validation_summary (hour, validation_type, validation_status);

-- Materialized view for facility processing metrics
CREATE MATERIALIZED VIEW staging.mv_facility_metrics AS
SELECT 
    facility_id,
    DATE_TRUNC('day', service_date) as service_day,
    processing_status,
    COUNT(*) as claim_count,
    SUM(total_charge_amount) as total_charges,
    AVG(total_charge_amount) as avg_charge,
    COUNT(*) FILTER (WHERE facility_validated = TRUE) as validated_claims,
    COUNT(*) FILTER (WHERE array_length(validation_errors, 1) > 0) as error_claims
FROM staging.claims 
WHERE service_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY facility_id, DATE_TRUNC('day', service_date), processing_status;

CREATE UNIQUE INDEX ON staging.mv_facility_metrics (facility_id, service_day, processing_status);

-- 4. PERFORMANCE TUNING FUNCTIONS
-- ================================

-- Function to refresh materialized views efficiently
CREATE OR REPLACE FUNCTION staging.refresh_performance_views()
RETURNS TEXT AS $$
DECLARE
    result TEXT := '';
BEGIN
    -- Refresh validation summary
    REFRESH MATERIALIZED VIEW CONCURRENTLY staging.mv_validation_summary;
    result := result || 'Refreshed validation summary. ';
    
    -- Refresh facility metrics
    REFRESH MATERIALIZED VIEW CONCURRENTLY staging.mv_facility_metrics;
    result := result || 'Refreshed facility metrics. ';
    
    -- Update table statistics
    ANALYZE staging.claims;
    ANALYZE staging.validation_results;
    result := result || 'Updated table statistics. ';
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Function for bulk claim status updates (optimized for batch processing)
CREATE OR REPLACE FUNCTION staging.bulk_update_claim_status(
    p_claim_ids TEXT[],
    p_new_status VARCHAR(30),
    p_error_messages TEXT[] DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    -- Use a single UPDATE with unnest for better performance
    UPDATE staging.claims 
    SET 
        processing_status = p_new_status,
        updated_date = CURRENT_TIMESTAMP,
        validation_errors = COALESCE(p_error_messages, validation_errors)
    FROM unnest(p_claim_ids) AS t(claim_id)
    WHERE staging.claims.claim_id = t.claim_id;
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- Function for efficient batch insertion with conflict resolution
CREATE OR REPLACE FUNCTION staging.insert_claims_batch(
    p_claims JSONB
)
RETURNS TABLE(inserted_count INTEGER, updated_count INTEGER, skipped_count INTEGER) AS $$
DECLARE
    claim_record RECORD;
    insert_count INTEGER := 0;
    update_count INTEGER := 0;
    skip_count INTEGER := 0;
BEGIN
    -- Use INSERT ... ON CONFLICT for efficient upsert
    FOR claim_record IN 
        SELECT * FROM jsonb_populate_recordset(null::staging.claims, p_claims)
    LOOP
        BEGIN
            INSERT INTO staging.claims SELECT (claim_record).*;
            insert_count := insert_count + 1;
        EXCEPTION WHEN unique_violation THEN
            -- Handle duplicate claim_id
            UPDATE staging.claims 
            SET updated_date = CURRENT_TIMESTAMP
            WHERE claim_id = claim_record.claim_id;
            
            IF FOUND THEN
                update_count := update_count + 1;
            ELSE
                skip_count := skip_count + 1;
            END IF;
        END;
    END LOOP;
    
    RETURN QUERY SELECT insert_count, update_count, skip_count;
END;
$$ LANGUAGE plpgsql;

-- 5. POSTGRESQL CONFIGURATION RECOMMENDATIONS
-- ==========================================

-- Display current configuration and recommendations
CREATE OR REPLACE FUNCTION staging.analyze_pg_performance()
RETURNS TABLE(
    setting_name TEXT,
    current_value TEXT,
    recommended_value TEXT,
    description TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.name::TEXT,
        s.setting::TEXT,
        r.recommended::TEXT,
        r.description::TEXT
    FROM pg_settings s
    JOIN (VALUES
        ('shared_buffers', '4GB', 'Set to 25% of available RAM for dedicated server'),
        ('effective_cache_size', '12GB', 'Set to 75% of available RAM'),
        ('work_mem', '256MB', 'Increase for complex queries and batch operations'),
        ('maintenance_work_mem', '1GB', 'For VACUUM, CREATE INDEX operations'),
        ('checkpoint_timeout', '15min', 'Reduce checkpoint frequency'),
        ('checkpoint_completion_target', '0.9', 'Spread checkpoint I/O over time'),
        ('wal_buffers', '64MB', 'Increase for high write workloads'),
        ('max_wal_size', '4GB', 'Allow larger WAL for batch operations'),
        ('random_page_cost', '1.1', 'Lower for SSD storage'),
        ('effective_io_concurrency', '200', 'Increase for SSD storage'),
        ('max_worker_processes', '16', 'Increase for parallel operations'),
        ('max_parallel_workers_per_gather', '4', 'Enable parallel queries'),
        ('max_parallel_workers', '8', 'Total parallel workers'),
        ('max_parallel_maintenance_workers', '4', 'For parallel index creation')
    ) r(name, recommended, description) ON s.name = r.name
    WHERE s.name IN (
        'shared_buffers', 'effective_cache_size', 'work_mem', 'maintenance_work_mem',
        'checkpoint_timeout', 'checkpoint_completion_target', 'wal_buffers', 'max_wal_size',
        'random_page_cost', 'effective_io_concurrency', 'max_worker_processes',
        'max_parallel_workers_per_gather', 'max_parallel_workers', 'max_parallel_maintenance_workers'
    );
END;
$$ LANGUAGE plpgsql;
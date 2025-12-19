-- ============================================================================
-- COMPREHENSIVE DATA QUALITY TEST SUITE
-- ============================================================================
-- Database: PostgreSQL localhost:5435/postgres (postgres/postgres)
-- Purpose: Automated data quality validation for insurance pipeline
-- Created: 2025-12-19
-- ============================================================================

-- ============================================================================
-- SECTION 1: NULL CHECKS (All Columns)
-- ============================================================================

-- 1.1: NULL CHECK - Gold Layer (Complete)
-- Identifies all columns with NULL values in gold.fact_insurance_performance
-- BASELINE RESULTS NEEDED - Run this to establish quality metrics

SELECT 
    'GOLD LAYER NULL ANALYSIS' as test_suite,
    'fact_insurance_performance' as table_name,
    col.column_name,
    COUNT(*) as null_count,
    ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM gold.fact_insurance_performance)::NUMERIC * 100, 2) as null_percentage,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS - No NULLs'
        WHEN ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM gold.fact_insurance_performance)::NUMERIC * 100, 2) < 5 THEN '⚠️  WARNING - <5% NULLs'
        ELSE '🚨 FAIL - >5% NULLs'
    END as quality_status
FROM (
    SELECT 'GEO_id' as column_name UNION ALL
    SELECT 'gender' UNION ALL
    SELECT 'birthdate' UNION ALL
    SELECT 'state_province_code' UNION ALL
    SELECT 'zipcode' UNION ALL
    SELECT 'county_code' UNION ALL
    SELECT 'fips_county_code' UNION ALL
    SELECT 'division_number' UNION ALL
    SELECT 'rating_level' UNION ALL
    SELECT 'current_premium' UNION ALL
    SELECT 'indicated_premium' UNION ALL
    SELECT 'territory_label' UNION ALL
    SELECT 'territory_area' UNION ALL
    SELECT 'territory_town' UNION ALL
    SELECT 'territory_county' UNION ALL
    SELECT 'acs03_total_population' UNION ALL
    SELECT 'acs05_total_population'
) col
LEFT JOIN gold.fact_insurance_performance f ON (
    (col.column_name = 'GEO_id' AND f."GEO_id" IS NULL) OR
    (col.column_name = 'gender' AND f.gender IS NULL) OR
    (col.column_name = 'birthdate' AND f.birthdate IS NULL) OR
    (col.column_name = 'state_province_code' AND f.state_province_code IS NULL) OR
    (col.column_name = 'zipcode' AND f.zipcode IS NULL) OR
    (col.column_name = 'county_code' AND f.county_code IS NULL) OR
    (col.column_name = 'fips_county_code' AND f.fips_county_code IS NULL) OR
    (col.column_name = 'division_number' AND f.division_number IS NULL) OR
    (col.column_name = 'rating_level' AND f.rating_level IS NULL) OR
    (col.column_name = 'current_premium' AND f.current_premium IS NULL) OR
    (col.column_name = 'indicated_premium' AND f.indicated_premium IS NULL) OR
    (col.column_name = 'territory_label' AND f.territory_label IS NULL) OR
    (col.column_name = 'territory_area' AND f.territory_area IS NULL) OR
    (col.column_name = 'territory_town' AND f.territory_town IS NULL) OR
    (col.column_name = 'territory_county' AND f.territory_county IS NULL) OR
    (col.column_name = 'acs03_total_population' AND f.acs03_total_population IS NULL) OR
    (col.column_name = 'acs05_total_population' AND f.acs05_total_population IS NULL)
)
GROUP BY col.column_name
ORDER BY null_percentage DESC;

-- 1.2: NULL CHECK - Silver Layer (stg_premiums)
-- Check source data quality in silver layer

SELECT 
    'SILVER LAYER NULL ANALYSIS' as test_suite,
    'stg_premiums' as table_name,
    col.column_name,
    COUNT(*) as null_count,
    ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM silver.stg_premiums)::NUMERIC * 100, 2) as null_percentage
FROM (
    SELECT 'GEO_id' as column_name UNION ALL
    SELECT 'gender' UNION ALL
    SELECT 'birthdate' UNION ALL
    SELECT 'zipcode'
) col
LEFT JOIN silver.stg_premiums p ON (
    (col.column_name = 'GEO_id' AND p."GEO_id" IS NULL) OR
    (col.column_name = 'gender' AND p.gender IS NULL) OR
    (col.column_name = 'birthdate' AND p.birthdate IS NULL) OR
    (col.column_name = 'zipcode' AND p.zipcode IS NULL)
)
GROUP BY col.column_name
ORDER BY null_percentage DESC;

-- 1.3: CRITICAL NULL CHECK - Required Fields
-- Alerts if critical fields have NULLs

SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '🚨 FAIL - Critical fields have NULLs'
    END as status,
    COUNT(*) as problematic_records,
    STRING_AGG(DISTINCT col_name, ', ') as affected_columns
FROM (
    SELECT 
        'GEO_id' as col_name
    FROM gold.fact_insurance_performance
    WHERE "GEO_id" IS NULL
    UNION ALL
    SELECT 'gender'
    FROM gold.fact_insurance_performance
    WHERE gender IS NULL AND gender NOT IN ('Unknown', '')
    UNION ALL
    SELECT 'birthdate'
    FROM gold.fact_insurance_performance
    WHERE birthdate IS NULL
) t
GROUP BY 1;

-- ============================================================================
-- SECTION 2: DUPLICATE DETECTION
-- ============================================================================

-- 2.1: Exact Duplicate Detection - Gold Layer
-- Identifies completely identical records
SELECT 
    'Exact Duplicates' as test_type,
    COUNT(*) as duplicate_sets,
    SUM(record_count - 1) as total_duplicate_records,
    CASE 
        WHEN SUM(record_count - 1) = 0 THEN '✅ PASS - No exact duplicates'
        ELSE '🚨 FAIL - Exact duplicates found'
    END as status
FROM (
    SELECT 
        MD5(CONCAT_WS('||', "GEO_id", gender, birthdate, zipcode, state_province_code, 
                      rating_level, current_premium, indicated_premium)) as record_hash,
        COUNT(*) as record_count
    FROM gold.fact_insurance_performance
    GROUP BY record_hash
    HAVING COUNT(*) > 1
) duplicates;

-- 2.2: Key-based Duplicate Detection - Gold Layer
-- Checks for duplicate combinations of key fields
SELECT 
    "GEO_id",
    gender,
    birthdate,
    zipcode,
    COUNT(*) as duplicate_count,
    COUNT(*) - 1 as extra_records,
    STRING_AGG(CAST(ingested_at AS TEXT), ', ') as ingestion_dates,
    CASE 
        WHEN COUNT(*) > 1 THEN '⚠️  DUPLICATE'
        ELSE '✅ OK'
    END as status
FROM gold.fact_insurance_performance
GROUP BY "GEO_id", gender, birthdate, zipcode
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 100;

-- 2.3: Duplicate Count Summary
-- Provides aggregate duplicate statistics
WITH duplicates AS (
    SELECT 
        "GEO_id",
        gender,
        birthdate,
        zipcode,
        COUNT(*) as record_count
    FROM gold.fact_insurance_performance
    GROUP BY "GEO_id", gender, birthdate, zipcode
    HAVING COUNT(*) > 1
)
SELECT 
    'Key-based Duplicates' as test_type,
    COUNT(*) as duplicate_groups,
    SUM(record_count - 1) as total_extra_records,
    ROUND(SUM(record_count - 1)::NUMERIC / (SELECT COUNT(*) FROM gold.fact_insurance_performance)::NUMERIC * 100, 4) as percentage_of_total,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS - No key duplicates'
        WHEN ROUND(SUM(record_count - 1)::NUMERIC / (SELECT COUNT(*) FROM gold.fact_insurance_performance)::NUMERIC * 100, 2) < 0.1 THEN '⚠️  WARNING - <0.1% duplicates'
        ELSE '🚨 FAIL - >0.1% duplicates'
    END as status
FROM duplicates;

-- 2.4: Silver Layer Duplicate Detection
-- Check for duplicates in source data

WITH premiums_dupes AS (
    SELECT 
        "GEO_id",
        gender,
        birthdate,
        COUNT(*) as record_count
    FROM silver.stg_premiums
    GROUP BY "GEO_id", gender, birthdate
    HAVING COUNT(*) > 1
)
SELECT 
    'stg_premiums Duplicates' as table_name,
    COUNT(*) as duplicate_groups,
    SUM(record_count - 1) as extra_records,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '⚠️  Check source data'
    END as status
FROM premiums_dupes;

-- ============================================================================
-- SECTION 3: DATA CONSISTENCY CHECKS
-- ============================================================================

-- 3.1: Referential Integrity - Territory Matches
-- Verify all records have valid territory mappings
SELECT 
    'Territory Mapping' as test_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN territory_label != 'Unknown' THEN 1 END) as matched,
    COUNT(CASE WHEN territory_label = 'Unknown' THEN 1 END) as unmatched,
    ROUND(COUNT(CASE WHEN territory_label != 'Unknown' THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 2) as match_percentage,
    CASE 
        WHEN ROUND(COUNT(CASE WHEN territory_label != 'Unknown' THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 2) >= 99 THEN '✅ PASS'
        WHEN ROUND(COUNT(CASE WHEN territory_label != 'Unknown' THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 2) >= 95 THEN '⚠️  WARNING'
        ELSE '🚨 FAIL'
    END as status
FROM gold.fact_insurance_performance;

-- 3.2: Referential Integrity - Census Data Coverage
-- Verify census table joins
SELECT 
    'Census Data Coverage' as test_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN acs03_total_population IS NOT NULL THEN 1 END) as acs03_matched,
    COUNT(CASE WHEN acs05_total_population IS NOT NULL THEN 1 END) as acs05_matched,
    ROUND(COUNT(CASE WHEN acs03_total_population IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 2) as acs03_coverage_pct,
    ROUND(COUNT(CASE WHEN acs05_total_population IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 2) as acs05_coverage_pct
FROM gold.fact_insurance_performance;

-- 3.3: Date Validation - Birthdate Reasonableness
-- Check if birthdates are within reasonable range
SELECT 
    'Date Validation' as test_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN birthdate >= '1920-01-01' AND birthdate <= CURRENT_DATE - INTERVAL '18 years' THEN 1 END) as valid_dates,
    COUNT(CASE WHEN birthdate < '1920-01-01' OR birthdate > CURRENT_DATE - INTERVAL '18 years' THEN 1 END) as invalid_dates,
    CASE 
        WHEN COUNT(CASE WHEN birthdate < '1920-01-01' OR birthdate > CURRENT_DATE - INTERVAL '18 years' THEN 1 END) = 0 THEN '✅ PASS'
        ELSE '⚠️  Invalid dates detected'
    END as status
FROM gold.fact_insurance_performance
WHERE birthdate IS NOT NULL;

-- 3.4: Numeric Field Validation - Premiums
-- Check for negative or zero premiums
SELECT 
    'Premium Validation' as test_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN CAST(current_premium AS NUMERIC) > 0 THEN 1 END) as positive_current,
    COUNT(CASE WHEN CAST(current_premium AS NUMERIC) <= 0 THEN 1 END) as zero_or_negative_current,
    COUNT(CASE WHEN CAST(indicated_premium AS NUMERIC) > 0 THEN 1 END) as positive_indicated,
    COUNT(CASE WHEN CAST(indicated_premium AS NUMERIC) <= 0 THEN 1 END) as zero_or_negative_indicated
FROM gold.fact_insurance_performance
WHERE current_premium IS NOT NULL OR indicated_premium IS NOT NULL;

-- ============================================================================
-- SECTION 4: BATCH CONSISTENCY TESTS
-- ============================================================================

-- 4.1: Batch Completeness
-- Verify each batch has expected data
SELECT 
    'Batch Completeness' as test_type,
    batch_id,
    COUNT(*) as record_count,
    COUNT(DISTINCT "GEO_id") as unique_locations,
    COUNT(DISTINCT gender) as gender_variations,
    MIN(ingested_at) as batch_start,
    MAX(ingested_at) as batch_end,
    CASE 
        WHEN COUNT(*) > 1000 THEN '✅ PASS'
        WHEN COUNT(*) > 100 THEN '⚠️  WARNING - Small batch'
        ELSE '🚨 FAIL - Minimal records'
    END as status
FROM gold.fact_insurance_performance
GROUP BY batch_id
ORDER BY batch_id DESC
LIMIT 10;

-- 4.2: Batch Consistency - Record Distribution
-- Check if batches have similar record counts
WITH batch_stats AS (
    SELECT 
        batch_id,
        COUNT(*) as record_count
    FROM gold.fact_insurance_performance
    GROUP BY batch_id
)
SELECT 
    'Batch Distribution' as test_type,
    COUNT(DISTINCT batch_id) as total_batches,
    AVG(record_count)::BIGINT as avg_records,
    MIN(record_count) as min_records,
    MAX(record_count) as max_records,
    STDDEV(record_count)::NUMERIC as std_dev,
    CASE 
        WHEN STDDEV(record_count) / AVG(record_count) < 0.1 THEN '✅ PASS - Consistent batches'
        WHEN STDDEV(record_count) / AVG(record_count) < 0.2 THEN '⚠️  WARNING - Some variation'
        ELSE '🚨 FAIL - Inconsistent batch sizes'
    END as status
FROM batch_stats;

-- ============================================================================
-- SECTION 5: AGGREGATE DATA QUALITY SCORE
-- ============================================================================

-- 5.1: Overall Data Quality Report Card
-- Comprehensive quality metric across all tests
WITH quality_checks AS (
    SELECT 
        'NULL Check: GEO_id' as check_name,
        CASE 
            WHEN COUNT(*) = 0 THEN 100 
            ELSE GREATEST(0, 100 - (COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM gold.fact_insurance_performance)::NUMERIC * 100)::INT)
        END as score
    FROM gold.fact_insurance_performance
    WHERE "GEO_id" IS NULL
    
    UNION ALL
    
    SELECT 'NULL Check: gender',
        CASE 
            WHEN COUNT(*) = 0 THEN 100 
            ELSE GREATEST(0, 100 - (COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM gold.fact_insurance_performance)::NUMERIC * 100)::INT)
        END
    FROM gold.fact_insurance_performance
    WHERE gender IS NULL
    
    UNION ALL
    
    SELECT 'NULL Check: birthdate',
        CASE 
            WHEN COUNT(*) = 0 THEN 100 
            ELSE GREATEST(0, 100 - (COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM gold.fact_insurance_performance)::NUMERIC * 100)::INT)
        END
    FROM gold.fact_insurance_performance
    WHERE birthdate IS NULL
    
    UNION ALL
    
    SELECT 'Duplicate Detection',
        CASE 
            WHEN (SELECT COUNT(*) FROM (
                SELECT "GEO_id", gender, birthdate, zipcode, COUNT(*) as cnt
                FROM gold.fact_insurance_performance
                GROUP BY "GEO_id", gender, birthdate, zipcode
                HAVING COUNT(*) > 1
            ) d) = 0 THEN 100
            ELSE 85
        END
    
    UNION ALL
    
    SELECT 'Territory Mapping',
        CASE 
            WHEN (SELECT ROUND(COUNT(CASE WHEN territory_label != 'Unknown' THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 0)
                  FROM gold.fact_insurance_performance) >= 99 THEN 100
            WHEN (SELECT ROUND(COUNT(CASE WHEN territory_label != 'Unknown' THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 0)
                  FROM gold.fact_insurance_performance) >= 95 THEN 80
            ELSE 60
        END
    
    UNION ALL
    
    SELECT 'Data Freshness',
        CASE 
            WHEN MAX(ingested_at) >= CURRENT_TIMESTAMP - INTERVAL '24 hours' THEN 100
            ELSE 50
        END
    FROM gold.fact_insurance_performance
)
SELECT 
    'OVERALL DATA QUALITY SCORE' as report_title,
    ROUND(AVG(score), 1) as overall_score,
    COUNT(*) as tests_run,
    SUM(CASE WHEN score >= 90 THEN 1 ELSE 0 END) as passing_tests,
    SUM(CASE WHEN score < 90 THEN 1 ELSE 0 END) as failing_tests,
    CASE 
        WHEN ROUND(AVG(score), 1) >= 95 THEN '🟢 EXCELLENT'
        WHEN ROUND(AVG(score), 1) >= 85 THEN '🟡 GOOD'
        WHEN ROUND(AVG(score), 1) >= 75 THEN '🟠 FAIR'
        ELSE '🔴 POOR'
    END as quality_grade
FROM quality_checks;

-- 5.2: Data Quality Summary by Category
-- Breakdown of quality metrics by category
SELECT 
    'Summary Report' as report_type,
    COUNT(*) as total_records,
    COUNT(DISTINCT batch_id) as total_batches,
    COUNT(DISTINCT "GEO_id") as unique_locations,
    ROUND(COUNT(CASE WHEN gender IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 2) as gender_completeness_pct,
    ROUND(COUNT(CASE WHEN birthdate IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 2) as birthdate_completeness_pct,
    ROUND(COUNT(CASE WHEN territory_label != 'Unknown' THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 2) as territory_match_pct,
    DATE(MAX(ingested_at)) as latest_data_date,
    CASE 
        WHEN DATE(MAX(ingested_at)) = CURRENT_DATE THEN 'Fresh (Today)'
        WHEN DATE(MAX(ingested_at)) = CURRENT_DATE - INTERVAL '1 day' THEN 'Fresh (Yesterday)'
        ELSE 'Stale'
    END as data_freshness
FROM gold.fact_insurance_performance;

-- ============================================================================
-- SECTION 6: MONITORING & ALERTING QUERIES
-- ============================================================================

-- 6.1: Alert - Data Quality Degradation
-- Trigger alert if quality drops significantly
SELECT 
    CASE 
        WHEN curr_gender_pct < 95 THEN '🚨 ALERT: Gender completeness dropped to ' || curr_gender_pct || '%'
        WHEN curr_dupes > 1000 THEN '🚨 ALERT: ' || curr_dupes || ' duplicate records detected'
        WHEN curr_stale > 0 THEN '⚠️  WARNING: Data is stale (>24 hours)'
        ELSE '✅ All systems nominal'
    END as alert_status,
    curr_gender_pct,
    curr_dupes,
    curr_stale
FROM (
    SELECT 
        ROUND(COUNT(CASE WHEN gender IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 2) as curr_gender_pct,
        (SELECT COUNT(*) FROM (
            SELECT "GEO_id", gender, birthdate, zipcode, COUNT(*) as cnt
            FROM gold.fact_insurance_performance
            GROUP BY "GEO_id", gender, birthdate, zipcode
            HAVING COUNT(*) > 1
        ) d) as curr_dupes,
        CASE WHEN MAX(ingested_at) < CURRENT_TIMESTAMP - INTERVAL '24 hours' THEN 1 ELSE 0 END as curr_stale
    FROM gold.fact_insurance_performance
) alerts;

-- 6.2: Row Count Trend
-- Track row count changes over time
SELECT 
    DATE(ingested_at) as ingestion_date,
    COUNT(*) as daily_record_count,
    LAG(COUNT(*)) OVER (ORDER BY DATE(ingested_at)) as previous_day_count,
    COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY DATE(ingested_at)) as record_change,
    ROUND((COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY DATE(ingested_at)))::NUMERIC 
          / LAG(COUNT(*)) OVER (ORDER BY DATE(ingested_at))::NUMERIC * 100, 2) as pct_change
FROM gold.fact_insurance_performance
GROUP BY DATE(ingested_at)
ORDER BY ingestion_date DESC
LIMIT 10;

-- ============================================================================
-- SECTION 7: TEST EXECUTION COMMANDS
-- ============================================================================

/*
HOW TO RUN THESE TESTS:
=======================

1. NULL CHECKS (Run daily at 6:15 AM UTC):
   psql -h localhost -p 5435 -U postgres -d postgres -f DATA_QUALITY_TESTS.sql \
     --section 1 > quality_report_nulls.txt

2. DUPLICATE DETECTION (Run daily at 6:30 AM UTC):
   psql -h localhost -p 5435 -U postgres -d postgres \
     -c "PGPASSWORD=postgres psql -h localhost -p 5435 -U postgres -d postgres < DATA_QUALITY_TESTS.sql" \
     --section 2

3. OVERALL QUALITY SCORE (Run daily at 6:45 AM UTC):
   psql -h localhost -p 5435 -U postgres -d postgres \
     -c "SELECT * FROM quality_checks LIMIT 1;"

4. QUICK HEALTH CHECK (Run every 2 hours):
   psql -h localhost -p 5435 -U postgres -d postgres \
     -c "$(cat DATA_QUALITY_TESTS.sql | grep -A 20 '5.2: Data Quality Summary')"

POWERSHELL SCHEDULE (Windows Task Scheduler):
=============================================

$env:PGPASSWORD = "postgres"
psql -h localhost -p 5435 -U postgres -d postgres < "C:\path\to\DATA_QUALITY_TESTS.sql" | Out-File "C:\logs\quality_$(Get-Date -Format 'yyyy-MM-dd').txt"
*/

-- ============================================================================
-- END OF DATA QUALITY TEST SUITE
-- ============================================================================

-- ============================================================================
-- SAMPLE QUERIES FOR E2E INSURANCE DATA PIPELINE
-- ============================================================================
-- Database: postgresql://localhost:5435/postgres
-- Created: 2025-12-19
-- Purpose: Reporting, Data Quality, Dashboard Preparation
-- ============================================================================

-- ============================================================================
-- SECTION 1: BASIC REPORTING QUERIES
-- ============================================================================

-- 1.1: Summary Statistics - Total Records and Coverage
-- Shows overall data volume and coverage
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT "GEO_id") as unique_geo_ids,
    COUNT(DISTINCT territory_label) as unique_territories,
    MIN(ingested_at) as earliest_record,
    MAX(ingested_at) as latest_record
FROM gold.fact_insurance_performance;

-- 1.2: Territory Performance Summary
-- Shows performance metrics by territory
SELECT 
    territory_label,
    COUNT(*) as record_count,
    AVG(CAST(current_premium AS NUMERIC)) as avg_premium,
    MAX(CAST(current_premium AS NUMERIC)) as max_premium,
    MIN(CAST(current_premium AS NUMERIC)) as min_premium
FROM gold.fact_insurance_performance
WHERE territory_label IS NOT NULL
GROUP BY territory_label
ORDER BY record_count DESC
LIMIT 20;

-- 1.3: Join Success Rate
-- Shows how many records successfully joined with dimensions
SELECT 
    SUM(CASE WHEN territory_label = 'Unknown' THEN 1 ELSE 0 END) as unmatched_territory,
    SUM(CASE WHEN territory_label != 'Unknown' THEN 1 ELSE 0 END) as matched_territory,
    ROUND(
        SUM(CASE WHEN territory_label != 'Unknown' THEN 1 ELSE 0 END)::NUMERIC 
        / COUNT(*)::NUMERIC * 100, 
        2
    ) as match_percentage
FROM gold.fact_insurance_performance;

-- 1.4: Census Demographic Coverage
-- Shows how many records have census data
SELECT 
    SUM(CASE WHEN acs03_total_population IS NOT NULL THEN 1 ELSE 0 END) as acs03_coverage,
    SUM(CASE WHEN acs05_total_population IS NOT NULL THEN 1 ELSE 0 END) as acs05_coverage,
    COUNT(*) as total_records
FROM gold.fact_insurance_performance;

-- ============================================================================
-- SECTION 2: DATA QUALITY TESTS
-- ============================================================================

-- 2.1: NULL Value Check - Gold Layer
-- Identifies columns with NULL values
SELECT 
    'GEO_id' as column_name,
    COUNT(*) as null_count,
    ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM gold.fact_insurance_performance)::NUMERIC * 100, 2) as null_percentage
FROM gold.fact_insurance_performance
WHERE "GEO_id" IS NULL
UNION ALL
SELECT 'gender', COUNT(*), ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM gold.fact_insurance_performance)::NUMERIC * 100, 2)
FROM gold.fact_insurance_performance WHERE gender IS NULL
UNION ALL
SELECT 'territory_label', COUNT(*), ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM gold.fact_insurance_performance)::NUMERIC * 100, 2)
FROM gold.fact_insurance_performance WHERE territory_label IS NULL
UNION ALL
SELECT 'acs03_total_population', COUNT(*), ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM gold.fact_insurance_performance)::NUMERIC * 100, 2)
FROM gold.fact_insurance_performance WHERE acs03_total_population IS NULL;

-- 2.2: Duplicate Detection - Gold Layer
-- Checks for duplicate records based on primary key candidates
SELECT 
    "GEO_id",
    gender,
    birthdate,
    COUNT(*) as duplicate_count
FROM gold.fact_insurance_performance
GROUP BY "GEO_id", gender, birthdate
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20;

-- 2.3: Duplicate Detection - Silver Layer (stg_premiums)
-- Verify no duplicates in source data
SELECT 
    "GEO_id",
    gender,
    birthdate,
    COUNT(*) as duplicate_count
FROM silver.stg_premiums
GROUP BY "GEO_id", gender, birthdate
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 2.4: Data Type Validation
-- Check for data quality issues in numeric fields
SELECT 
    'current_premium' as field,
    COUNT(*) as non_numeric_count,
    COUNT(CASE WHEN CAST(current_premium AS NUMERIC) IS NOT NULL THEN 1 END) as valid_numeric
FROM gold.fact_insurance_performance
WHERE current_premium IS NOT NULL
UNION ALL
SELECT 'indicated_premium', COUNT(*), COUNT(CASE WHEN CAST(indicated_premium AS NUMERIC) IS NOT NULL THEN 1 END)
FROM gold.fact_insurance_performance
WHERE indicated_premium IS NOT NULL;

-- 2.5: Data Freshness Check
-- Verify ingestion timestamps
SELECT 
    DATE(ingested_at) as ingestion_date,
    COUNT(*) as record_count,
    MIN(ingested_at) as earliest_time,
    MAX(ingested_at) as latest_time
FROM gold.fact_insurance_performance
GROUP BY DATE(ingested_at)
ORDER BY ingestion_date DESC
LIMIT 10;

-- 2.6: Batch ID Validation
-- Check batch processing consistency
SELECT 
    batch_id,
    COUNT(*) as record_count,
    COUNT(DISTINCT "GEO_id") as unique_geo_ids
FROM gold.fact_insurance_performance
GROUP BY batch_id
ORDER BY batch_id DESC;

-- ============================================================================
-- SECTION 3: DASHBOARD PREPARATION QUERIES
-- ============================================================================

-- 3.1: Territory Dimension Summary (for Dashboard)
-- Aggregated metrics by territory for dashboard cards
SELECT 
    territory_label::TEXT as territory,
    COUNT(*) as total_records,
    COUNT(DISTINCT "GEO_id") as unique_locations,
        ROUND(AVG(CAST(current_premium AS NUMERIC)), 2) as avg_premium,
        ROUND(SUM(CAST(current_premium AS NUMERIC)), 2) as total_premium,
    COUNT(CASE WHEN gender = 'M' THEN 1 END) as male_count,
    COUNT(CASE WHEN gender = 'F' THEN 1 END) as female_count
FROM gold.fact_insurance_performance
WHERE territory_label IS NOT NULL
GROUP BY territory_label
ORDER BY total_records DESC
LIMIT 20;

-- 3.2: Demographics Dashboard - Age Groups
-- Territory distribution for demographic analysis
SELECT 
    territory_label::TEXT as territory,
    COUNT(*) as record_count,
        ROUND(AVG(CAST(current_premium AS NUMERIC)), 2) as avg_premium
FROM gold.fact_insurance_performance
WHERE territory_label IS NOT NULL
GROUP BY territory_label
ORDER BY record_count DESC
LIMIT 15;

-- 3.3: Gender Distribution Dashboard
-- Gender-based metrics for dashboard
SELECT 
    gender,
    COUNT(*) as record_count,
    ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM gold.fact_insurance_performance)::NUMERIC * 100, 2) as percentage,
    ROUND(AVG(CAST(current_premium AS NUMERIC)), 2) as avg_premium,
    ROUND(SUM(CAST(current_premium AS NUMERIC)), 2) as total_premium
FROM gold.fact_insurance_performance
WHERE gender IS NOT NULL
GROUP BY gender
ORDER BY record_count DESC;

-- 3.4: Census Data Analysis - Population Density
-- Use census data to analyze records with census data
SELECT 
    'Census Records' as metric,
    COUNT(*) as record_count,
    ROUND(AVG(acs03_total_population)) as avg_population,
    ROUND(AVG(acs03_median_age), 1) as avg_median_age,
    COUNT(DISTINCT territory_label) as territories_with_census
FROM gold.fact_insurance_performance
WHERE acs03_total_population IS NOT NULL;

-- 3.5: Premium Distribution - For Dashboard Visualizations
-- Premium statistics across all records
SELECT 
    COUNT(*) as total_records,
        ROUND(AVG(CAST(current_premium AS NUMERIC)), 2) as avg_premium,
        ROUND(MAX(CAST(current_premium AS NUMERIC)), 2) as max_premium,
        ROUND(MIN(CAST(current_premium AS NUMERIC)), 2) as min_premium,
        ROUND(STDDEV(CAST(current_premium AS NUMERIC)), 2) as stddev_premium
FROM gold.fact_insurance_performance;

-- 3.6: Time Series - Ingestion by Batch (for Time-based Dashboard)
SELECT 
    MAX(ingested_at) as time,
    batch_id as batch,
    COUNT(*) as records_per_batch,
    ROUND(SUM(CAST(current_premium AS NUMERIC)), 2) as batch_premium_total,
    ROUND(AVG(CAST(current_premium AS NUMERIC)), 2) as batch_avg_premium
FROM gold.fact_insurance_performance
GROUP BY batch_id
ORDER BY batch_id DESC;

-- ============================================================================
-- SECTION 4: SUMMARY STATISTICS & KPIs
-- ============================================================================

-- 4.1: Overall Pipeline Health Summary
-- KPI dashboard showing pipeline health
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT "GEO_id") as unique_geos,
    COUNT(DISTINCT territory_label) as unique_territories,
    COUNT(DISTINCT batch_id) as total_batches
FROM gold.fact_insurance_performance;

-- 4.2: Data Quality Score
-- Overall data quality assessment
WITH quality_metrics AS (
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN "GEO_id" IS NOT NULL THEN 1 END) as geo_populated,
        COUNT(CASE WHEN gender IS NOT NULL THEN 1 END) as gender_populated,
        COUNT(CASE WHEN territory_label IS NOT NULL THEN 1 END) as territory_matched,
        COUNT(CASE WHEN acs03_total_population IS NOT NULL THEN 1 END) as census_matched
    FROM gold.fact_insurance_performance
)
SELECT 
    'GEO_ID Completeness' as metric,
    ROUND(geo_populated::NUMERIC / total_records * 100, 2)::TEXT || '%' as score
FROM quality_metrics
UNION ALL
SELECT 'Gender Completeness', ROUND(gender_populated::NUMERIC / total_records * 100, 2)::TEXT || '%'
FROM quality_metrics
UNION ALL
SELECT 'Territory Join Success', ROUND(territory_matched::NUMERIC / total_records * 100, 2)::TEXT || '%'
FROM quality_metrics
UNION ALL
SELECT 'Census Data Coverage', ROUND(census_matched::NUMERIC / total_records * 100, 2)::TEXT || '%'
FROM quality_metrics;

-- ============================================================================
-- SECTION 5: EXPORT QUERIES (For BI Tools)
-- ============================================================================

-- 5.1: Flattened Fact Table Export
-- Complete enriched dataset ready for export to dashboards
SELECT 
    "GEO_id",
    gender,
    birthdate,
    territory_label,
    territory_area,
    territory_town,
    territory_county,
    current_premium,
    indicated_premium,
    cgr_factor,
    acs03_total_population,
    acs03_median_age,
    acs03_married_percent,
    acs05_total_population,
    acs05_speak_english_only,
    ingested_at,
    batch_id
FROM gold.fact_insurance_performance
WHERE territory_label IS NOT NULL
LIMIT 1000;  -- Change LIMIT as needed for export size

-- ============================================================================
-- END OF SAMPLE QUERIES
-- ============================================================================

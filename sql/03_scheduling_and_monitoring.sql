-- ============================================================================
-- DAG SCHEDULING & QUERY TEMPLATE
-- ============================================================================
-- Database: PostgreSQL
-- Host: localhost
-- Port: 5435
-- Username: postgres
-- Password: postgres
-- Database: postgres
-- ============================================================================

-- ============================================================================
-- SECTION 1: DAG SCHEDULING INFORMATION
-- ============================================================================

/*
SCHEDULED PIPELINES
===================

DAG 1: 1_ingestion_raw_to_bronze
  Schedule:  Daily at 2:00 AM UTC (0 2 * * *)
  Status:    ✅ SCHEDULED
  Owner:     senior_de
  Retries:   2 (max)
  Timeout:   3 minutes between retries
  
  Purpose:   Reads CSV files from landing zone, converts to Parquet, moves to archive
  Input:     CSV files in /include/landing/
  Output:    Parquet files in /include/bronze/
  
  SLA:       Must complete within 1 hour


DAG 2: 2_bronze_to_silver_v3
  Schedule:  Daily at 3:00 AM UTC (0 3 * * *)
  Status:    ✅ SCHEDULED (runs after DAG 1)
  Owner:     senior_de
  Retries:   2 (max)
  
  Purpose:   Loads Parquet files from Bronze, creates/updates Silver tables
  Input:     Parquet files from /include/bronze/
  Output:    6 tables in silver schema
  Tables:    stg_premiums, stg_ACS_MD_15_5YR_DP03_20251218, 
             stg_ACS_MD_15_5YR_DP05_20251218, stg_territory_definitions_table_20251218,
             stg_cgr_definitions_table_20251218, stg_cgr_premiums_table_20251218
  
  SLA:       Must complete within 1.5 hours
  Features:  Schema evolution, dynamic table creation


DAG 3: 3_gold_transformation
  Schedule:  Daily at 4:00 AM UTC (0 4 * * *)
  Status:    ✅ SCHEDULED (runs after DAG 2)
  Owner:     senior_de
  Retries:   2 (max)
  
  Purpose:   Joins Silver tables, creates enriched Gold fact table
  Input:     Tables from silver schema
  Output:    fact_insurance_performance table in gold schema
  Joins:     - stg_premiums (base)
             - stg_territory_definitions_table_20251218 (GEO_id → zipcode)
             - stg_ACS_MD_15_5YR_DP03_20251218 (GEO_id demographics)
             - stg_ACS_MD_15_5YR_DP05_20251218 (GEO_id ancestry/language)
  
  SLA:       Must complete within 2 hours
  Expected Rows: ~95k records

TOTAL PIPELINE TIME: 4 hours (2 AM → 6 AM UTC)
AVAILABILITY:       Ready for analytics at 6 AM UTC daily
*/

-- ============================================================================
-- SECTION 2: SCHEDULED QUERY TEMPLATES
-- ============================================================================

-- ============================================================================
-- 2.1 DAILY HEALTH CHECK (Run after 6 AM UTC daily)
-- ============================================================================

-- Health Check Query Template
-- Purpose: Verify all DAGs completed successfully
-- Add this to your monitoring/alerting system

PSQL_COMMAND_TEMPLATE_1:
psql -h localhost -p 5435 -U postgres -d postgres -c "
SELECT 
    'fact_insurance_performance' as table_name,
    COUNT(*) as current_row_count,
    DATE(MAX(ingested_at)) as latest_ingestion_date,
    CASE WHEN COUNT(*) > 95000 THEN '✅ HEALTHY' ELSE '⚠️ CHECK' END as status
FROM gold.fact_insurance_performance
GROUP BY DATE(ingested_at)
ORDER BY latest_ingestion_date DESC
LIMIT 1;
"

-- ============================================================================
-- 2.2 WEEKLY SUMMARY REPORT (Run Sundays at 6 AM UTC)
-- ============================================================================

PSQL_COMMAND_TEMPLATE_2:
psql -h localhost -p 5435 -U postgres -d postgres -c "
WITH weekly_stats AS (
    SELECT 
        DATE_TRUNC('week', ingested_at)::DATE as week_starting,
        COUNT(*) as records_processed,
        COUNT(DISTINCT territory_label) as territories_covered,
        COUNT(CASE WHEN territory_label != 'Unknown' THEN 1 END) as matched_territories
    FROM gold.fact_insurance_performance
    WHERE ingested_at >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY DATE_TRUNC('week', ingested_at)::DATE
)
SELECT * FROM weekly_stats ORDER BY week_starting DESC;
"

-- ============================================================================
-- 2.3 MONTHLY DATA QUALITY CHECK (Run 1st of each month at 6 AM UTC)
-- ============================================================================

PSQL_COMMAND_TEMPLATE_3:
psql -h localhost -p 5435 -U postgres -d postgres -c "
WITH monthly_quality AS (
    SELECT 
        TO_CHAR(MAX(ingested_at), 'YYYY-MM') as month,
        COUNT(*) as total_records,
        COUNT(CASE WHEN gender IS NOT NULL THEN 1 END) as gender_populated,
        COUNT(CASE WHEN territory_label != 'Unknown' THEN 1 END) as territory_matched,
        ROUND(COUNT(CASE WHEN gender IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 2) as gender_completeness_pct,
        ROUND(COUNT(CASE WHEN territory_label != 'Unknown' THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 2) as territory_match_pct
    FROM gold.fact_insurance_performance
    WHERE ingested_at >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
)
SELECT * FROM monthly_quality;
"

-- ============================================================================
-- 2.4 REAL-TIME MONITORING QUERIES
-- ============================================================================

-- Check if latest DAG run completed successfully
PSQL_COMMAND_TEMPLATE_4:
psql -h localhost -p 5435 -U postgres -d postgres -c "
SELECT 
    'DAG 1: Ingestion' as dag_name,
    MAX(ingested_at) as last_run,
    COUNT(*) as records_from_last_batch
FROM gold.fact_insurance_performance
WHERE DATE(ingested_at) = CURRENT_DATE
GROUP BY DATE(ingested_at);
"

-- Check for null values in critical columns
PSQL_COMMAND_TEMPLATE_5:
psql -h localhost -p 5435 -U postgres -d postgres -c "
SELECT 
    'GEO_id' as column_name,
    COUNT(*) as null_count,
    ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM gold.fact_insurance_performance)::NUMERIC * 100, 2) as null_pct
FROM gold.fact_insurance_performance
WHERE \"GEO_id\" IS NULL
UNION ALL
SELECT 'gender', COUNT(*), ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM gold.fact_insurance_performance)::NUMERIC * 100, 2)
FROM gold.fact_insurance_performance WHERE gender IS NULL
UNION ALL
SELECT 'territory_label', COUNT(*), ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM gold.fact_insurance_performance)::NUMERIC * 100, 2)
FROM gold.fact_insurance_performance WHERE territory_label IS NULL;
"

-- ============================================================================
-- SECTION 3: POWERSHELL SCHEDULE EXAMPLES (Windows Task Scheduler)
-- ============================================================================

/*
To schedule reports on Windows, create a PowerShell script:

FILE: C:\scheduled-reports\daily_health_check.ps1
============================================================

# Daily Health Check - 6:30 AM UTC (after all DAGs complete)
psql -h localhost -p 5435 -U postgres -d postgres -c "
SELECT 
    'Daily Health Check' as report_name,
    NOW() as check_time,
    COUNT(*) as total_records
FROM gold.fact_insurance_performance
LIMIT 1;
" | Out-File "C:\scheduled-reports\logs\$(Get-Date -Format 'yyyy-MM-dd')_health_check.log" -Append

# Send email if needed
Send-MailMessage -From "airflow@company.com" -To "team@company.com" `
  -Subject "Daily Pipeline Health Check - $(Get-Date -Format 'yyyy-MM-dd')" `
  -Body "Check the attached log file for details" `
  -Attachments "C:\scheduled-reports\logs\$(Get-Date -Format 'yyyy-MM-dd')_health_check.log" `
  -SmtpServer "smtp.company.com"

Then add to Windows Task Scheduler:
- Trigger: Daily at 6:30 AM
- Action: Run PowerShell -File "C:\scheduled-reports\daily_health_check.ps1"
- Run with highest privileges
*/

-- ============================================================================
-- SECTION 4: SCHEDULE REFERENCE
-- ============================================================================

/*
CRON SYNTAX REFERENCE (Used in Airflow schedule_interval)

Format: minute hour day month weekday

Common Examples:
================

0 2 * * *       - Daily at 2:00 AM UTC
0 */6 * * *     - Every 6 hours
0 9 * * 1       - Mondays at 9:00 AM UTC
0 0 1 * *       - 1st of each month at midnight
0 12 * * 1-5    - Weekdays at noon
*/30 * * * *    - Every 30 minutes
0 */12 * * *    - Every 12 hours (0:00 and 12:00 UTC)

AIRFLOW SCHEDULE PARAMETERS:
============================

schedule_interval='0 2 * * *'     # Cron expression
schedule='@daily'                  # Preset: Once per day
schedule='@hourly'                 # Preset: Every hour
schedule='@weekly'                 # Preset: Once per week
schedule_interval=timedelta(hours=1)  # Interval-based

MONITORING CHECKLIST:
=====================

Daily:
  ☐ Verify all 3 DAGs completed without errors
  ☐ Check row counts in gold.fact_insurance_performance
  ☐ Monitor data freshness (ingested_at timestamps)

Weekly:
  ☐ Review SLA compliance
  ☐ Check for data quality degradation
  ☐ Verify join success rates

Monthly:
  ☐ Full data quality audit
  ☐ Backlog review
  ☐ Performance optimization review

*/

-- ============================================================================
-- SECTION 5: ALERTING QUERIES
-- ============================================================================

-- Alert if DAG hasn't run in last 24 hours
PSQL_COMMAND_TEMPLATE_6:
psql -h localhost -p 5435 -U postgres -d postgres -c "
SELECT 
    CASE 
        WHEN MAX(ingested_at) < CURRENT_TIMESTAMP - INTERVAL '24 hours' 
        THEN '🚨 ALERT: Pipeline not run in 24 hours!'
        ELSE '✅ Pipeline ran recently'
    END as status,
    MAX(ingested_at) as last_run_time
FROM gold.fact_insurance_performance;
"

-- Alert if data quality drops
PSQL_COMMAND_TEMPLATE_7:
psql -h localhost -p 5435 -U postgres -d postgres -c "
SELECT 
    CASE 
        WHEN ROUND(COUNT(CASE WHEN gender IS NOT NULL THEN 1 END)::NUMERIC 
             / COUNT(*)::NUMERIC * 100, 2) < 95 
        THEN '⚠️  WARNING: Gender completeness below 95%'
        ELSE '✅ Gender data quality OK'
    END as quality_status,
    ROUND(COUNT(CASE WHEN gender IS NOT NULL THEN 1 END)::NUMERIC 
          / COUNT(*)::NUMERIC * 100, 2) as gender_completeness_pct
FROM gold.fact_insurance_performance
WHERE ingested_at >= CURRENT_DATE;
"

-- ============================================================================
-- END OF SCHEDULING DOCUMENT
-- ============================================================================

IMPORTANT NOTES:
================

1. All credentials are hardcoded above. In production, use environment variables:
   psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME

2. Always include password for automated queries:
   PGPASSWORD=postgres psql -h localhost -p 5435 -U postgres -d postgres -c "SELECT 1"

3. Time zone: All schedules use UTC. Adjust as needed for your time zone.

4. Catchup behavior: Set to False prevents backfill when DAGs are paused/resumed.

5. SLA times are estimates based on current data volume. Monitor and adjust as needed.

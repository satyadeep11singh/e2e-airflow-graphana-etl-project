from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook # We know this works!
from datetime import datetime

def build_gold_layer():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sql = """
        CREATE SCHEMA IF NOT EXISTS gold;

        -- 1. Drop existing fact table
        DROP TABLE IF EXISTS gold.fact_insurance_performance;

        -- 2. Build comprehensive fact table with all dimension data
        -- Joins: Territory (GEO_id), Census DP03 (GEO_id), Census DP05 (GEO_id)
        CREATE TABLE gold.fact_insurance_performance AS
        SELECT 
            -- Premium base data
            p.*,
            
            -- Territory/Geography dimensions
            COALESCE(t.territory::TEXT, 'Unknown') AS territory_label,
            t.area AS territory_area,
            t.town AS territory_town,
            t.county AS territory_county,
            
            -- Census demographic dimensions (ACS DP03 - Social/Economic)
            acs03."HC01_VC03" AS acs03_total_population,
            acs03."HC01_VC04" AS acs03_median_age,
            acs03."HC01_VC05" AS acs03_married_percent,
            
            -- Census demographic dimensions (ACS DP05 - Ancestry/Language)
            acs05."HC01_VC03" AS acs05_total_population,
            acs05."HC01_VC04" AS acs05_speak_english_only
            
        FROM silver.stg_premiums p
        
        -- Territory/Geography lookup (join on GEO_id -> zipcode)
        LEFT JOIN silver."stg_territory_definitions_table_20251218" t 
            ON CAST(SUBSTRING(p."GEO_id"::TEXT FROM LENGTH(p."GEO_id"::TEXT)-4) AS INTEGER) = CAST(t.zipcode AS INTEGER)
        
        -- Census data - Social and Economic (DP03) - join on GEO_id
        LEFT JOIN silver."stg_ACS_MD_15_5YR_DP03_20251218" acs03
            ON p."GEO_id" = acs03."GEO_id"
        
        -- Census data - Ancestry and Language (DP05) - join on GEO_id
        LEFT JOIN silver."stg_ACS_MD_15_5YR_DP05_20251218" acs05
            ON p."GEO_id" = acs05."GEO_id";
    """
    
    pg_hook.run(sql)
    
    # Get row count
    result = pg_hook.get_records("SELECT COUNT(*) FROM gold.fact_insurance_performance;")
    row_count = result[0][0] if result else 0
    
    print(f"\n🚀 Gold layer successfully rebuilt!")
    print(f"   Total rows in fact table: {row_count:,}")
    print(f"   Dimensions included:")
    print(f"   - Premium base data (stg_premiums)")
    print(f"   - Territory/Geography mappings")
    print(f"   - Census demographics (ACS DP03 & DP05)")

with DAG(
    dag_id='3_gold_transformation',
    start_date=datetime(2025, 1, 1),
    schedule_interval='0 4 * * *',  # Run daily at 4 AM UTC (1 hour after DAG 2)
    catchup=False,
    tags=['transformation', 'gold'],
    description='Daily transformation: Join Silver tables to create Gold fact tables for analytics'
) as dag:

    run_gold_sql = PythonOperator(
        task_id='build_final_reporting_table',
        python_callable=build_gold_layer
    )
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def get_latest_silver_tables():
    """Dynamically fetch latest silver table names from database."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Query to find the latest versions of required tables
    query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'silver' 
        AND table_name LIKE 'stg_%'
        ORDER BY table_name DESC;
    """
    
    tables = pg_hook.get_records(query)
    
    if not tables:
        raise ValueError("Required silver layer tables not found. Ensure bronze_to_silver_transformation DAG has completed.")
    
    table_names = [row[0] for row in tables]
    logger.info(f"Found {len(table_names)} silver tables: {table_names}")
    
    return table_names

def build_gold_layer():
    """Build gold layer fact table by joining silver dimension tables."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Fetch latest table names dynamically
        logger.info("Fetching latest silver layer tables...")
        tables = get_latest_silver_tables()
        
        if len(tables) < 4:
            raise ValueError(f"Expected at least 4 silver tables (premiums, territory, DP03, DP05), found {len(tables)}")
        
        # Organize tables by type - use exact matching
        premiums_tables = [t for t in tables if 'premiums' in t.lower()]
        territory_tables = [t for t in tables if 'territory' in t.lower()]
        dp03_tables = [t for t in tables if 'dp03' in t.lower()]
        dp05_tables = [t for t in tables if 'dp05' in t.lower()]
        
        if not all([premiums_tables, territory_tables, dp03_tables, dp05_tables]):
            raise ValueError(f"Missing required tables. Found: premiums={len(premiums_tables)}, "
                           f"territory={len(territory_tables)}, dp03={len(dp03_tables)}, dp05={len(dp05_tables)}")
        
        # Take the first (most recent) of each type
        premiums_table = premiums_tables[0]
        territory_table = territory_tables[0]
        dp03_table = dp03_tables[0]
        dp05_table = dp05_tables[0]
        
        logger.info(f"Using tables: {premiums_table}, {territory_table}, {dp03_table}, {dp05_table}")
        
        # Validate table names to prevent SQL injection (alphanumeric, underscore, hyphen only)
        for tbl in [premiums_table, territory_table, dp03_table, dp05_table]:
            if not all(c.isalnum() or c in '_-' for c in tbl):
                raise ValueError(f"Invalid table name: {tbl}")
        
        # Validate required columns exist before building fact table
        logger.info("Validating required columns in silver tables...")
        
        # Check territory table has required columns (using LOWER for case-insensitive matching)
        check_cols_query = f"""
            SELECT LOWER(column_name) FROM information_schema.columns 
            WHERE table_schema = 'silver' AND table_name = '{territory_table}'
            AND LOWER(column_name) IN ('territory', 'zipcode', 'area', 'town', 'county')
        """
        territory_cols = {row[0] for row in pg_hook.get_records(check_cols_query)}
        required_territory_cols = {'territory', 'zipcode', 'area', 'town', 'county'}
        
        if not required_territory_cols.issubset(territory_cols):
            missing = required_territory_cols - territory_cols
            raise ValueError(f"Territory table missing required columns: {missing}")
        
        # Check premiums table has required columns (territory for joining)
        check_premiums_query = f"""
            SELECT LOWER(column_name) FROM information_schema.columns 
            WHERE table_schema = 'silver' AND table_name = '{premiums_table}'
            AND LOWER(column_name) = 'territory'
        """
        premiums_cols = {row[0] for row in pg_hook.get_records(check_premiums_query)}
        
        if 'territory' not in premiums_cols:
            raise ValueError(f"Premiums table missing required 'territory' column for joining with territory table")
        
        # Check census DP03 table has required columns (case-insensitive)
        check_dp03_query = f"""
            SELECT LOWER(column_name) FROM information_schema.columns 
            WHERE table_schema = 'silver' AND table_name = '{dp03_table}'
            AND LOWER(column_name) IN ('geo_id2', 'hc01_vc03', 'hc01_vc04', 'hc01_vc05')
        """
        dp03_cols = {row[0] for row in pg_hook.get_records(check_dp03_query)}
        required_dp03_cols = {'geo_id2', 'hc01_vc03', 'hc01_vc04', 'hc01_vc05'}
        
        if not required_dp03_cols.issubset(dp03_cols):
            missing = required_dp03_cols - dp03_cols
            raise ValueError(f"DP03 table missing required columns: {missing}")
        
        # Check census DP05 table has required columns (case-insensitive)
        check_dp05_query = f"""
            SELECT LOWER(column_name) FROM information_schema.columns 
            WHERE table_schema = 'silver' AND table_name = '{dp05_table}'
            AND LOWER(column_name) IN ('geo_id2', 'hc01_vc03', 'hc01_vc04')
        """
        dp05_cols = {row[0] for row in pg_hook.get_records(check_dp05_query)}
        required_dp05_cols = {'geo_id2', 'hc01_vc03', 'hc01_vc04'}
        
        if not required_dp05_cols.issubset(dp05_cols):
            missing = required_dp05_cols - dp05_cols
            raise ValueError(f"DP05 table missing required columns: {missing}")
        
        logger.info("All required columns validated successfully")
        
        # Build SQL with dynamically selected tables (without explicit transaction control)
        # PostgreSQL auto-commits individual statements; wrap in Python exception handling instead
        sql_drop = f"""
            DROP TABLE IF EXISTS gold.fact_insurance_performance CASCADE;
        """
        
        sql_create = f"""
            CREATE SCHEMA IF NOT EXISTS gold;
            
            CREATE TABLE gold.fact_insurance_performance AS
            SELECT 
                -- Premium base data from raw premiums table (excluding territory - from dimension table)
                p."gender",
                p."birthdate",
                p."ypc",
                p."current_premium",
                p."indicated_premium",
                p."selected_premium",
                p."underlying_premium",
                p."fixed_expenses",
                p."underlying_total_premium",
                p."cgr_factor",
                p."cgr",
                p."ingested_at",
                p."batch_id",
                
                -- Territory/Geography dimensions (preserve NULLs as is)
                t."territory",
                t."area",
                t."town",
                t."county",
                t."zipcode",
                
                -- Census demographic dimensions (ACS DP03 - Social/Economic)
                -- Note: Joined through territory->zipcode to GEO_id2 (zipcode-based identifier)
                acs03."HC01_VC03" AS acs03_total_population,
                acs03."HC01_VC04" AS acs03_median_age,
                acs03."HC01_VC05" AS acs03_married_percent,
                
                -- Census demographic dimensions (ACS DP05 - Ancestry/Language)
                acs05."HC01_VC03" AS acs05_total_population,
                acs05."HC01_VC04" AS acs05_speak_english_only
                
            FROM silver."{premiums_table}" p
            
            -- INNER JOIN Territory (required - fact table only makes sense with territory data)
            INNER JOIN silver."{territory_table}" t 
                ON CAST(p."territory" AS INTEGER) = CAST(t."territory" AS INTEGER)
            
            -- LEFT JOIN Census DP03 (optional - some territories may not have census data)
            LEFT JOIN silver."{dp03_table}" acs03
                ON CAST(acs03."GEO_id2" AS VARCHAR) = CAST(t."zipcode" AS VARCHAR)
            
            -- LEFT JOIN Census DP05 (optional - some territories may not have census data)
            LEFT JOIN silver."{dp05_table}" acs05
                ON CAST(acs05."GEO_id2" AS VARCHAR) = CAST(t."zipcode" AS VARCHAR);
        """
        
        # Execute in order: drop, then create
        try:
            pg_hook.run(sql_drop)
            logger.info("Dropped existing fact table")
        except Exception as e:
            logger.warning(f"Could not drop table (may not exist): {str(e)}")
        
        pg_hook.run(sql_create)
        logger.info("Created new fact table")
        
        # Get row count
        result = pg_hook.get_records("SELECT COUNT(*) FROM gold.fact_insurance_performance;")
        row_count = result[0][0] if result else 0
        
        logger.info(f"Gold layer successfully rebuilt with {row_count:,} rows")
        print(f"\n🚀 Gold layer successfully rebuilt!")
        print(f"   Total rows in fact table: {row_count:,}")
        print(f"   Dimensions included:")
        print(f"   - Premium base data ({premiums_table})")
        print(f"   - Territory/Geography mappings ({territory_table})")
        print(f"   - Census demographics ACS DP03 ({dp03_table})")
        print(f"   - Census demographics ACS DP05 ({dp05_table})")
        
    except Exception as e:
        logger.error(f"Failed to build gold layer: {str(e)}")
        raise

with DAG(
    dag_id='gold_transformation_enrichment',
    start_date=datetime(2025, 1, 1),
    schedule='0 4 * * *',  # Run daily at 4 AM UTC (1 hour after DAG 2)
    catchup=False,
    tags=['transformation', 'gold'],
    description='Daily transformation: Join Silver tables to create Gold fact tables for analytics'
) as dag:

    run_gold_sql = PythonOperator(
        task_id='build_final_reporting_table',
        python_callable=build_gold_layer
    )
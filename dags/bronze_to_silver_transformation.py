from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import text

BRONZE_LAYER = "/usr/local/airflow/include/bronze"

def get_inferred_sql_type(dtype):
    """Infer PostgreSQL data type from pandas dtype."""
    dtype_str = str(dtype).lower()
    if 'int' in dtype_str:
        return 'INTEGER'
    elif 'float' in dtype_str:
        return 'NUMERIC'
    elif 'datetime' in dtype_str or 'timestamp' in dtype_str:
        return 'TIMESTAMP'
    elif 'bool' in dtype_str:
        return 'BOOLEAN'
    else:
        return 'TEXT'

def load_parquet_to_silver():
    """Load parquet files from bronze layer to silver schema in PostgreSQL."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Validate bronze layer exists
    if not os.path.exists(BRONZE_LAYER):
        raise FileNotFoundError(f"Bronze layer directory not found: {BRONZE_LAYER}")
    
    pg_hook.run("CREATE SCHEMA IF NOT EXISTS silver;")
    
    files = [f for f in os.listdir(BRONZE_LAYER) if f.endswith('.parquet')]
    files.sort()  # Process files in consistent order
    
    processed_tables = []
    failed_tables = []
    
    # Reuse a single connection to avoid resource leaks
    with engine.connect() as conn:
        for file_name in files:
            try:
                file_path = os.path.join(BRONZE_LAYER, file_name)
                df = pd.read_parquet(file_path)
                
                # 1. Generate table name from filename (remove .parquet, add stg_ prefix)
                base_filename = file_name.replace('.parquet', '')
                table_name = f"stg_{base_filename}"
                
                # 2. Clean data and add metadata
                df['ingested_at'] = datetime.now()
                df['batch_id'] = datetime.now().strftime('%Y%m%d')
                df.columns = [c.replace('.', '_').replace('-', '_') for c in df.columns]
                
                # 3. Check if table already exists (using parameterized query)
                check_table_exists = text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'silver' AND table_name = :table_name
                    );
                """)
                
                result = conn.execute(check_table_exists, {"table_name": table_name})
                table_exists = result.fetchone()[0]
                
                if not table_exists:
                    # Create the table for the first time
                    df.to_sql(table_name, engine, schema='silver', if_exists='fail', index=False)
                    print(f"✅ Created new table: {table_name}")
                else:
                    # SCHEMA EVOLUTION: Check for new columns with proper type inference
                    get_existing_cols = text("""
                        SELECT column_name FROM information_schema.columns 
                        WHERE table_schema = 'silver' AND table_name = :table_name;
                    """)
                    
                    result = conn.execute(get_existing_cols, {"table_name": table_name})
                    existing_cols = [row[0] for row in result.fetchall()]
                    
                    # Add missing columns with inferred types
                    for col in df.columns:
                        if col not in existing_cols:
                            sql_type = get_inferred_sql_type(df[col].dtype)
                            print(f"  ➕ Adding missing column '{col}' ({sql_type}) to {table_name}")
                            add_col_query = f'ALTER TABLE silver."{table_name}" ADD COLUMN "{col}" {sql_type};'
                            pg_hook.run(add_col_query)
                    
                    # Append new data to existing table
                    df.to_sql(table_name, engine, schema='silver', if_exists='append', index=False)
                    print(f"✅ Appended data to: {table_name}")
                
                processed_tables.append(table_name)
                
            except Exception as e:
                print(f"❌ Failed to process {file_name}: {str(e)}")
                failed_tables.append((file_name, str(e)))
    
    print(f"\n🚀 Loaded {len(processed_tables)} tables to Silver layer:")
    for tbl in processed_tables:
        print(f"   - {tbl}")
    
    if failed_tables:
        print(f"\n⚠️ {len(failed_tables)} tables failed to load:")
        for file_name, error in failed_tables:
            print(f"   - {file_name}: {error}")

with DAG(
    dag_id='bronze_to_silver_transformation',
    start_date=datetime(2023, 1, 1),
    schedule='0 3 * * *',  # Run daily at 3 AM UTC (1 hour after DAG 1)
    catchup=False,
    tags=['transformation', 'silver'],
    description='Daily transformation: Load Bronze Parquet files to Silver PostgreSQL tables'
) as dag:

    load_task = PythonOperator(
        task_id='parquet_to_sql_silver',
        python_callable=load_parquet_to_silver
    )
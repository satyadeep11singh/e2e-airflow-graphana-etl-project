from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

BRONZE_LAYER = "/usr/local/airflow/include/bronze"

def load_parquet_to_silver():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    pg_hook.run("CREATE SCHEMA IF NOT EXISTS silver;")
    
    files = [f for f in os.listdir(BRONZE_LAYER) if f.endswith('.parquet')]
    files.sort()  # Process files in consistent order
    
    processed_tables = []

    for file_name in files:
        file_path = os.path.join(BRONZE_LAYER, file_name)
        df = pd.read_parquet(file_path)
        
        # 1. Generate table name from filename (remove .parquet, add stg_ prefix)
        base_filename = file_name.replace('.parquet', '')
        table_name = f"stg_{base_filename}"
        
        # 2. Clean data and add metadata
        df['ingested_at'] = datetime.now()
        df['batch_id'] = '20251218'
        df.columns = [c.replace('.', '_').replace('-', '_') for c in df.columns]
        
        # 3. Check if table already exists
        table_exists_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'silver' AND table_name = '{table_name}'
            );
        """
        table_exists = pg_hook.get_records(table_exists_query)[0][0]
        
        if not table_exists:
            # Create the table for the first time
            df.to_sql(table_name, engine, schema='silver', if_exists='replace', index=False)
            print(f"✅ Created new table: {table_name}")
        else:
            # SCHEMA EVOLUTION: Check for new columns
            existing_cols_query = f"""
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = 'silver' AND table_name = '{table_name}';
            """
            existing_cols = [row[0] for row in pg_hook.get_records(existing_cols_query)]
            
            # Add missing columns
            for col in df.columns:
                if col not in existing_cols:
                    print(f"  ➕ Adding missing column '{col}' to {table_name}")
                    pg_hook.run(f'ALTER TABLE silver."{table_name}" ADD COLUMN "{col}" TEXT;')
            
            # Append data
            df.to_sql(table_name, engine, schema='silver', if_exists='append', index=False)
            print(f"✅ Appended data to: {table_name}")
        
        processed_tables.append(table_name)
    
    print(f"\n🚀 Loaded {len(processed_tables)} tables to Silver layer:")
    for tbl in processed_tables:
        print(f"   - {tbl}")

with DAG(
    dag_id='2_bronze_to_silver_v3',
    start_date=datetime(2023, 1, 1),
    schedule_interval='0 3 * * *',  # Run daily at 3 AM UTC (1 hour after DAG 1)
    catchup=False,
    tags=['transformation', 'silver'],
    description='Daily transformation: Load Bronze Parquet files to Silver PostgreSQL tables'
) as dag:

    load_task = PythonOperator(
        task_id='parquet_to_sql_silver',
        python_callable=load_parquet_to_silver
    )
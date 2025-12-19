from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import pandas as pd
import shutil
import os

LANDING_ZONE = "/usr/local/airflow/include/landing"
BRONZE_LAYER = "/usr/local/airflow/include/bronze"

default_args = {
    'owner': 'senior_de',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def process_to_bronze(**context):
    logical_date = context['ds_nodash']
    landing_path = "/usr/local/airflow/include/landing"
    bronze_path = "/usr/local/airflow/include/bronze"
    archive_path = os.path.join(landing_path, "archive")

    # Ensure directories exist
    os.makedirs(bronze_path, exist_ok=True)
    os.makedirs(archive_path, exist_ok=True)

    # 1. List files
    files = [f for f in os.listdir(landing_path) if f.endswith('.csv')]
    
    if not files:
        print("No files found to process.")
        return

    for file_name in files:
        source_file_path = os.path.join(landing_path, file_name)
        
        # --- THE MISSING LOGIC ---
        try:
            # 2. READ CSV
            df = pd.read_csv(source_file_path)
            
            # 3. ADD METADATA (The Senior Difference)
            df['ingested_at'] = datetime.now()
            df['batch_id'] = logical_date
            
            # 4. WRITE TO BRONZE (Using the bronze_path!)
            parquet_file_name = file_name.replace('.csv', f'_{logical_date}.parquet')
            target_bronze_path = os.path.join(bronze_path, parquet_file_name)
            
            # engine='pyarrow' is faster and standard for big data
            df.to_parquet(target_bronze_path, index=False, engine='pyarrow')
            print(f"Successfully created Bronze file: {target_bronze_path}")

            # 5. THE MOVE (Only after successful write)
            target_archive_path = os.path.join(archive_path, file_name)
            if os.path.exists(target_archive_path):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                target_archive_path = os.path.join(archive_path, f"{timestamp}_{file_name}")

            shutil.move(source_file_path, target_archive_path)
            print(f"Archived {file_name} to {target_archive_path}")

        except Exception as e:
            print(f"Failed to process {file_name}: {e}")
            # In a real job, you might send an alert here (Slack/Email)
            raise e

with DAG(
    dag_id='1_ingestion_raw_to_bronze',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='0 2 * * *',  # Run daily at 2 AM UTC
    catchup=False,
    tags=['ingestion', 'bronze'],
    description='Daily ingestion: CSV files from landing zone to Parquet in bronze layer'
) as dag:

    # 1. The Gatekeeper: Wait for the file
    wait_for_batch = FileSensor(
        task_id='check_for_landing_files',
        fs_conn_id='fs_default',
        filepath='', # An empty string looks at the base path of the connection
        poke_interval=10,
        timeout=300,
        mode='reschedule'
    )

    # 2. The Processor: Load, Add Metadata, and Convert
    ingest_task = PythonOperator(
        task_id='csv_to_parquet_bronze',
        python_callable=process_to_bronze
    )

    wait_for_batch >> ingest_task
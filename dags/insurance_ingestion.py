from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import pandas as pd
import shutil
import os
import logging

logger = logging.getLogger(__name__)

LANDING_ZONE = "/usr/local/airflow/include/landing"
BRONZE_LAYER = "/usr/local/airflow/include/bronze"
CSV_PATTERN = "*.csv"

default_args = {
    'owner': 'senior_de',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def check_directories_exist():
    """Validate that required directories exist."""
    for directory in [LANDING_ZONE, BRONZE_LAYER]:
        if not os.path.exists(directory):
            raise FileNotFoundError(f"Required directory does not exist: {directory}")
    logger.info(f"Directory validation passed: {LANDING_ZONE}, {BRONZE_LAYER}")

def process_to_bronze(**context):
    """Process CSV files from landing zone to Bronze layer as Parquet."""
    logical_date = context['ds_nodash']
    landing_path = LANDING_ZONE
    bronze_path = BRONZE_LAYER
    archive_path = os.path.join(landing_path, "archive")

    # Ensure directories exist
    os.makedirs(bronze_path, exist_ok=True)
    os.makedirs(archive_path, exist_ok=True)
    
    logger.info(f"Processing files for logical date: {logical_date}")

    # 1. List CSV files with validation
    try:
        all_files = os.listdir(landing_path)
        files = [f for f in all_files if f.endswith('.csv') and not f.startswith('_')]
        
        if not files:
            logger.warning("No CSV files found to process in landing zone.")
            return
        
        logger.info(f"Found {len(files)} CSV file(s) to process: {files}")
    except Exception as e:
        logger.error(f"Failed to list landing zone files: {str(e)}")
        raise

    processed_files = []
    failed_files = []

    for file_name in files:
        source_file_path = os.path.join(landing_path, file_name)
        
        # Skip if file is locked or currently being written
        if source_file_path.endswith('.tmp') or source_file_path.endswith('.lock'):
            logger.info(f"Skipping file in progress: {file_name}")
            continue
        
        try:
            logger.info(f"Processing file: {file_name}")
            
            # 2. READ CSV
            df = pd.read_csv(source_file_path)
            logger.info(f"  Loaded {len(df)} rows from {file_name}")
            
            # 3. ADD METADATA (The Senior Difference)
            df['ingested_at'] = datetime.now()
            df['batch_id'] = logical_date
            
            # Validate data quality
            if df.empty:
                logger.warning(f"  Skipping {file_name}: File contains no data rows")
                continue
            
            # 4. WRITE TO BRONZE (Using the bronze_path!)
            parquet_file_name = file_name.replace('.csv', f'_{logical_date}.parquet')
            target_bronze_path = os.path.join(bronze_path, parquet_file_name)
            
            # engine='pyarrow' is faster and standard for big data
            df.to_parquet(target_bronze_path, index=False, engine='pyarrow', compression='snappy')
            logger.info(f"  ✅ Created Bronze file: {target_bronze_path}")

            # 5. ARCHIVE (Only after successful write to prevent data loss)
            target_archive_path = os.path.join(archive_path, file_name)
            
            # Handle duplicate file names with timestamp
            if os.path.exists(target_archive_path):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                base_name, ext = os.path.splitext(file_name)
                target_archive_path = os.path.join(archive_path, f"{timestamp}_{base_name}{ext}")

            shutil.move(source_file_path, target_archive_path)
            logger.info(f"  📦 Archived to: {target_archive_path}")
            processed_files.append(file_name)

        except pd.errors.EmptyDataError:
            logger.error(f"❌ {file_name}: File is empty or malformed CSV")
            failed_files.append((file_name, "Empty or malformed CSV"))
        except pd.errors.ParserError as e:
            logger.error(f"❌ {file_name}: CSV parsing error - {str(e)}")
            failed_files.append((file_name, f"Parse error: {str(e)}"))
        except Exception as e:
            logger.error(f"❌ {file_name}: Processing failed - {str(e)}")
            failed_files.append((file_name, str(e)))
            # Do NOT re-raise - continue processing other files

    # Summary reporting
    logger.info(f"\n{'='*60}")
    logger.info(f"Ingestion Summary for {logical_date}:")
    logger.info(f"  ✅ Successfully processed: {len(processed_files)}")
    for f in processed_files:
        logger.info(f"     - {f}")
    
    if failed_files:
        logger.warning(f"  ❌ Failed to process: {len(failed_files)}")
        for f, error in failed_files:
            logger.warning(f"     - {f}: {error}")
    
    logger.info(f"{'='*60}\n")

with DAG(
    dag_id='insurance_ingestion_raw_to_bronze',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule='0 2 * * *',  # Run daily at 2 AM UTC
    catchup=False,
    tags=['ingestion', 'bronze'],
    description='Daily ingestion: CSV files from landing zone to Parquet in bronze layer'
) as dag:

    # 0. Validate directories exist before processing
    validate_dirs = PythonOperator(
        task_id='validate_directories',
        python_callable=check_directories_exist
    )

    # 1. The Gatekeeper: Wait for CSV files in landing zone
    # Use poke mode to avoid task slot starvation with reschedule mode
    # soft_fail=True allows DAG to proceed even if no files are found (graceful degradation)
    wait_for_batch = FileSensor(
        task_id='check_for_landing_files',
        fs_conn_id='fs_default',
        filepath='/usr/local/airflow/include/landing/*.csv',
        poke_interval=60,  # Check every 60 seconds instead of 10 (reduce load)
        timeout=1800,  # 30 minutes instead of 5 (more reasonable for daily batch)
        mode='poke',  # Use 'poke' mode to avoid infinite rescheduling
        soft_fail=True  # Allow DAG to continue if no files found (graceful handling)
    )

    # 2. The Processor: Load, Add Metadata, and Convert
    # Depends on FileSensor but can proceed even if sensor skips (soft_fail=True)
    ingest_task = PythonOperator(
        task_id='csv_to_parquet_bronze',
        python_callable=process_to_bronze,
        trigger_rule='all_done'  # Run even if previous task was skipped
    )

    validate_dirs >> wait_for_batch >> ingest_task
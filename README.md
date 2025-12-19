# E2E Insurance Data Pipeline

A complete end-to-end (E2E) data pipeline built with Apache Airflow on Astronomer Runtime. This project demonstrates a modern data lakehouse architecture with Landing → Bronze → Silver → Gold layers.

## 🎯 Project Overview

This project implements a three-stage data transformation pipeline for insurance data:

```
Landing Zone (CSV) 
    ↓
Bronze Layer (Parquet) [Raw/Immutable Data]
    ↓
Silver Layer (SQL Tables) [Cleaned/Standardized]
    ↓
Gold Layer (Fact Tables) [Business-Ready Analytics]
```

## 📊 Architecture

### **Data Layers**

| Layer | Technology | Purpose |
|---|---|---|
| **Landing** | CSV Files | Raw incoming data |
| **Bronze** | Parquet Files | Immutable raw copy with metadata |
| **Silver** | PostgreSQL Tables | Cleaned, deduplicated, standardized data |
| **Gold** | PostgreSQL Fact Tables | Enriched, joined, business-ready data |

### **Data Warehouse**

- **Database:** PostgreSQL 12.6
- **Host:** localhost
- **Port:** 5435
- **Credentials:** postgres / postgres
- **Schemas:** silver, gold

## 🚀 Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.8+
- Astronomer CLI (optional)

### Start the Pipeline

```bash
cd c:\Users\satya\OneDrive\Desktop\e2e-project
astro dev start
```

This spins up:
- Airflow Scheduler (monitors DAGs)
- Airflow API Server (UI at http://localhost:8080)
- Airflow Triggerer (manages async tasks)
- DAG Processor (parses DAG files)
- PostgreSQL Database (port 5435)

### Access Airflow UI

```
URL: http://localhost:8080
Username: admin
Password: admin
```

## 📁 Project Structure

```
e2e-project/
├── dags/
│   ├── 1_insurance_ingestion.py           # Landing → Bronze
│   ├── 2_bronze_to_silver.py              # Bronze → Silver
│   ├── 3_gold_transformation.py           # Silver → Gold
│   └── .airflowignore
├── include/
│   ├── landing/                           # CSV input files
│   │   └── archive/                       # Processed files
│   └── bronze/                            # Parquet files
├── sql/                                   # SQL queries & tests
│   ├── 01_reporting_queries.sql           # 66 reporting queries
│   ├── 02_quality_tests.sql               # Data quality tests
│   └── 03_scheduling_and_monitoring.sql   # Monitoring queries
├── docs/                                  # Documentation
├── scripts/                               # Utility scripts
├── config/                                # Configuration files
├── plugins/                               # Custom operators
├── tests/                                 # Test suites
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
├── packages.txt
└── README.md
```

## 🔄 DAGs (Data Pipelines)

### **DAG 1: `1_insurance_ingestion` (Landing → Bronze)**

**Purpose:** Read CSV files, add metadata, convert to Parquet

**Schedule:** Daily at 2:00 AM UTC (0 2 * * *)

**Output:** Parquet files in `/include/bronze/` with `ingested_at` and `batch_id` columns

---

### **DAG 2: `2_bronze_to_silver` (Bronze → Silver)**

**Purpose:** Load Parquet files into PostgreSQL with dynamic table creation

**Schedule:** Daily at 3:00 AM UTC (0 3 * * *) - 1 hour after DAG 1

**Features:**
- Dynamically creates separate tables for each parquet file
- Handles schema evolution (adds new columns as needed)

**Silver Tables Created (6 total):**
- `stg_premiums` (657 columns, 95,308 rows)
- `stg_acs_md_15_5yr_dp03_20251218` (556 columns)
- `stg_acs_md_15_5yr_dp05_20251218` (344 columns)
- `stg_territory_definitions_table_20251218` (8 columns)
- `stg_cgr_definitions_table_20251218` (10 columns)
- `stg_cgr_premiums_table_20251218` (14 columns)

---

### **DAG 3: `3_gold_transformation` (Silver → Gold)**

**Purpose:** Create enriched fact table by joining all silver tables

**Schedule:** Daily at 4:00 AM UTC (0 4 * * *) - 1 hour after DAG 2

**Gold Table:**
- `fact_insurance_performance` (666 columns, 95,536 rows)

**Joins:** stg_premiums → territory_definitions → ACS_DP03 → ACS_DP05

---

## 📈 Data Quality Metrics

| Metric | Value | Status |
|---|---|---|
| Total Records (Gold) | 95,536 | ✅ |
| Gender Completeness | 97.13% | ✅ |
| Batch Consistency | 100% | ✅ |
| Territory Matches | 1.20% | ⚠️ Limited |
| Census Coverage | 1.22% | ⚠️ Limited |

## 🔍 Reporting & Analytics

SQL files located in `sql/` directory:

### **01_reporting_queries.sql**
66 comprehensive queries for:
- Basic reporting (summary stats, territory performance)
- Data quality (NULL checks, duplicate detection)
- Dashboard preparation (demographics, revenue analysis)
- KPIs (pipeline health, data quality scores)
- Data exports

### **02_quality_tests.sql**
Comprehensive data quality test suite:
- NULL checks (all 17 columns)
- Duplicate detection (exact & key-based)
- Data consistency validation
- Batch processing tests
- Automated alerting & monitoring
- Overall quality scorecards

### **03_scheduling_and_monitoring.sql**
DAG scheduling & monitoring:
- Cron expression reference
- Health check templates
- Weekly/monthly report queries
- Real-time monitoring
- PowerShell scheduling examples

### Quick Query Examples

```sql
-- Overall summary
SELECT COUNT(*) as total_records, 
       COUNT(DISTINCT territory_label) as unique_territories
FROM gold.fact_insurance_performance;

-- Territory performance
SELECT territory_label, COUNT(*) as record_count, 
       AVG(CAST(current_premium AS NUMERIC)) as avg_premium
FROM gold.fact_insurance_performance
WHERE territory_label != 'Unknown'
GROUP BY territory_label
ORDER BY record_count DESC;

-- Data quality check
SELECT 'Gender Completeness' as metric,
       ROUND(COUNT(CASE WHEN gender IS NOT NULL THEN 1 END)::NUMERIC 
       / COUNT(*)::NUMERIC * 100, 2)::TEXT || '%' as score
FROM gold.fact_insurance_performance;
```

## 🗄️ Database Connection

### Connection Details

```
Host: localhost
Port: 5435
Username: postgres
Password: postgres
Database: postgres
```

### Connect with psql

```bash
PGPASSWORD=postgres psql -h localhost -p 5435 -U postgres -d postgres
```

### Schemas

- **silver:** Staging tables (6 tables, 95k+ rows)
- **gold:** Fact tables (1 table, 95.5k rows)
- **public:** Default schema

## 🔧 Common Tasks

### Run a DAG

1. Go to Airflow UI: http://localhost:8080
2. Find the DAG (e.g., `2_bronze_to_silver`)
3. Click "Trigger DAG" (play button)
4. Monitor in Logs tab

### Query the Database

```bash
# Connect
PGPASSWORD=postgres psql -h localhost -p 5435 -U postgres -d postgres

# List silver tables
\dt silver.*

# Query gold fact table
SELECT * FROM gold.fact_insurance_performance LIMIT 5;
```

### Run Data Quality Tests

```bash
PGPASSWORD=postgres psql -h localhost -p 5435 -U postgres -d postgres -f sql/02_quality_tests.sql
```

## 🚨 Troubleshooting

### PostgreSQL Connection Issues

```bash
# Check if container is running
docker ps | grep postgres

# Test connection
PGPASSWORD=postgres psql -h localhost -p 5435 -U postgres -d postgres -c "SELECT version();"
```

### DAG Not Showing Up

- Check `dags/` for Python files
- Verify syntax: `python -m py_compile dags/*.py`
- Restart scheduler: `astro dev restart`

### Memory/Resource Issues

```bash
astro dev stop
astro dev start --no-cache
```

## 📚 Resources

- [Airflow Documentation](https://airflow.apache.org/)
- [Astronomer Documentation](https://www.astronomer.io/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Parquet Format](https://parquet.apache.org/)

## ✅ Project Status

| Component | Status | Notes |
|---|---|---|
| Bronze Layer | ✅ Operational | CSV → Parquet conversion |
| Silver Layer | ✅ Operational | Parquet → SQL with schema evolution |
| Gold Layer | ✅ Operational | Enriched fact table with joins |
| Data Validation | ✅ Complete | Quality metrics established |
| Reporting Queries | ✅ Complete | 66 queries ready for BI tools |
| Data Quality Tests | ✅ Complete | Automated testing & monitoring |
| DAG Scheduling | ✅ Configured | Daily runs, staggered execution |
| Dashboards | 🔄 Pending | Ready for BI tool integration |

---

**Last Updated:** December 19, 2025 | **Status:** ✅ Fully Operational | **File Structure:** ✅ Standardized

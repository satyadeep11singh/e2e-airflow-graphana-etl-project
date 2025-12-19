# E2E Insurance Data Pipeline

Apache Airflow-based ETL pipeline for insurance data with PostgreSQL data warehouse and Grafana dashboards.

## Architecture

```
Landing Zone (CSV)
    ↓ DAG 1: insurance_ingestion
Bronze Layer (Parquet)
    ↓ DAG 2: bronze_to_silver
Silver Layer (PostgreSQL)
    ↓ DAG 3: gold_transformation
Gold Layer (Fact Tables)
    ↓
Grafana Dashboards
```

## Quick Start

### 1. Start Airflow & PostgreSQL
```bash
astro dev start
```

### 2. Start Grafana
```bash
docker run -d -p 3000:3000 --name grafana grafana/grafana:latest
```

### 3. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| PostgreSQL | localhost:5435 | postgres / postgres |

## Project Structure

```
e2e-project/
├── dags/
│   ├── 1_insurance_ingestion.py       (CSV → Parquet)
│   ├── 2_bronze_to_silver.py          (Parquet → SQL)
│   └── 3_gold_transformation.py       (Joins & enrichment)
├── sql/
│   ├── 01_reporting_queries.sql       (66 reporting queries)
│   ├── 02_quality_tests.sql           (Data quality checks)
│   └── 03_scheduling_and_monitoring.sql
├── include/
│   ├── landing/                       (CSV input)
│   └── bronze/                        (Parquet files)
├── config/
│   └── airflow_settings.yaml
├── .astro/                            (Astronomer runtime config)
└── README.md                          (This file)
```

## Database

### PostgreSQL Connection
```
Host: localhost
Port: 5435
Database: postgres
User: postgres
Password: postgres
```

### Schemas
- **silver:** Staging tables (6 tables, 95k+ rows)
- **gold:** Fact tables (1 table, 95.5k rows)

### Test Connection
```bash
PGPASSWORD=postgres psql -h localhost -p 5435 -U postgres -d postgres -c "SELECT COUNT(*) FROM gold.fact_insurance_performance;"
```

## DAGs

### DAG 1: insurance_ingestion
- Schedule: Daily 2:00 AM UTC (0 2 * * *)
- Action: Converts CSV files to Parquet with metadata
- Output: `/include/bronze/` parquet files

### DAG 2: bronze_to_silver
- Schedule: Daily 3:00 AM UTC (0 3 * * *)
- Action: Loads Parquet files into PostgreSQL silver schema
- Output: 6 staging tables

### DAG 3: gold_transformation
- Schedule: Daily 4:00 AM UTC (0 4 * * *)
- Action: Creates enriched fact table from joined silver tables
- Output: `gold.fact_insurance_performance` (95,536 rows)

## SQL Queries

All available SQL queries are in the `sql/` directory:

```bash
# View reporting queries (66 total)
cat sql/01_reporting_queries.sql

# View quality tests
cat sql/02_quality_tests.sql

# View monitoring queries
cat sql/03_scheduling_and_monitoring.sql
```

## Grafana Setup

### Add PostgreSQL Data Source

1. Open http://localhost:3000
2. Settings → Data Sources
3. Add PostgreSQL data source:
   - Name: `PostgreSQL-Gold`
   - Host: `host.docker.internal:5435` (Docker) or `localhost:5435` (Local)
   - Database: `postgres`
   - User: `postgres`
   - Password: `postgres`

### Create Dashboards

Example query to start:
```sql
SELECT COUNT(*) as total_records
FROM gold.fact_insurance_performance
```

See `sql/01_reporting_queries.sql` for 60+ additional queries.

## Troubleshooting

### PostgreSQL not connecting in Grafana
- Ensure PostgreSQL is running: `docker ps | Select-String postgres`
- Use `host.docker.internal:5435` if Grafana runs in Docker
- Test: `PGPASSWORD=postgres psql -h localhost -p 5435 -U postgres -d postgres -c "SELECT 1"`

### DAG not showing in Airflow
- Check for syntax errors: `python -m py_compile dags/*.py`
- Restart scheduler: `astro dev restart`

### Data quality metrics
- Gender completeness: 97.13%
- Territory join success: 1.20%
- Census coverage: 1.22%

## Files

| File | Purpose |
|------|---------|
| `dags/*.py` | Airflow DAG definitions |
| `sql/*.sql` | SQL queries for reporting & QA |
| `config/airflow_settings.yaml` | Airflow configuration |
| `.astro/` | Astronomer runtime config |
| `include/landing/` | CSV input files |
| `include/bronze/` | Parquet output from DAG 1 |
| `docker-compose.yaml` | Docker compose configuration |
| `Dockerfile` | Astronomer Runtime image |
| `requirements.txt` | Python dependencies (pandas, pyarrow) |
| `packages.txt` | OS-level dependencies |

## Status

✅ **Operational**
- Bronze layer: CSV → Parquet ✓
- Silver layer: Parquet → SQL tables ✓
- Gold layer: Enriched fact table ✓
- Data validation: Complete ✓
- SQL queries: 66 available ✓
- Grafana: Ready for dashboards ✓

## GitHub

Repository: https://github.com/satyadeep11singh/e2e-airflow-graphana-etl-project

## Next Steps

1. Review DAGs in Airflow UI (http://localhost:8080)
2. Trigger DAGs manually to verify pipeline
3. Connect Grafana to PostgreSQL
4. Create dashboards using SQL queries
5. Set up monitoring and alerts

---

**Last Updated:** December 19, 2025  
**Status:** ✅ Production Ready

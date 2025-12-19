# E2E Insurance Data Pipeline

Apache Airflow-based ETL pipeline that processes insurance data from CSV → Parquet → SQL → Grafana dashboards.

**Data Source**: [Kaggle - Insurance Companies Secret Sauce](https://www.kaggle.com/datasets/thedevastator/insurance-companies-secret-sauce-finally-exposed)

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.9+

### 1. Start Services
```bash
astro dev start          # Airflow + PostgreSQL
docker run -d -p 3000:3000 --name grafana grafana/grafana:latest  # Grafana
```

### 2. Access Dashboards

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| **Grafana** | http://localhost:3000 | admin / admin |
| PostgreSQL | localhost:5435 | postgres / postgres |

**Grafana Reports**: Automated dashboard with 19 analytics panels (reporting queries, KPIs, data quality scores)

---

## Architecture

```
CSV Files (Landing) 
  ↓ [DAG 1: insurance_ingestion]
Parquet (Bronze)
  ↓ [DAG 2: bronze_to_silver_transformation]
SQL Tables (Silver)
  ↓ [DAG 3: gold_transformation_enrichment]
Fact Table (Gold: 95,536 rows)
  ↓
Grafana Dashboards (19 analytics panels)
```

---

## Project Structure

```
e2e-project/
├── dags/
│   ├── insurance_ingestion.py
│   ├── bronze_to_silver_transformation.py
│   └── gold_transformation_enrichment.py
├── scripts/
│   └── generate_grafana_dashboards.py           # Auto-generates dashboards
├── sql/
│   ├── 01_reporting_queries.sql                 # 19 reporting queries
│   ├── 02_quality_tests.sql
│   └── 03_scheduling_and_monitoring.sql
├── include/
│   ├── landing/                                 # CSV input (from Kaggle)
│   └── bronze/                                  # Parquet files
└── docker-compose.yaml, Dockerfile, requirements.txt
```

---

## Data Pipeline

### DAG 1: insurance_ingestion (2 AM UTC)
- Reads CSV files from `include/landing/`
- Converts to Parquet format
- Stores in `include/bronze/`

### DAG 2: bronze_to_silver_transformation (3 AM UTC)
- Loads Parquet files into PostgreSQL
- Creates 6 staging tables in `silver` schema
- ~95,500 records

### DAG 3: gold_transformation_enrichment (4 AM UTC)
- Joins silver tables with census data
- Creates `gold.fact_insurance_performance`
- 95,536 rows with enriched demographics

### Grafana Reports
- 19 auto-generated analytics panels
- Reporting queries, KPIs, data quality metrics
- Connect to PostgreSQL-Gold datasource
- Dashboard: http://localhost:3000/d/[dashboard-id]

---

## PostgreSQL

**Connection**:
```
Host: localhost
Port: 5435
User: postgres
Password: postgres
Database: postgres
```

**Test**:
```bash
PGPASSWORD=postgres psql -h localhost -p 5435 -U postgres -d postgres -c "SELECT COUNT(*) FROM gold.fact_insurance_performance;"
```

**Schemas**:
- `silver`: 6 staging tables
- `gold`: 1 fact table (95,536 rows)

---

## Grafana Setup

1. Open http://localhost:3000 (admin / admin)
2. Add PostgreSQL data source:
   - **Name**: PostgreSQL-Gold
   - **Host**: `host.docker.internal:5435` (Docker) or `localhost:5435`
   - **Database**: postgres
   - **User**: postgres
   - **Password**: postgres

3. Auto-generated dashboard includes:
   - Summary statistics & KPIs
   - Territory performance metrics
   - Gender distribution charts
   - Premium analysis
   - Data quality scores
   - Ingestion timeline

---

## SQL Queries

66 reporting queries in `sql/01_reporting_queries.sql`:
- Summary statistics
- Territory analysis
- Demographics & census data
- Premium distribution
- Data quality validation

Example:
```sql
SELECT COUNT(*) as total, COUNT(DISTINCT "GEO_id") as locations
FROM gold.fact_insurance_performance;
```

---

## Status

✅ **All Components Operational**
- CSV ingestion (94,394 "Unknown" territory records + 4 mapped territories)
- Parquet conversion
- PostgreSQL tables (6 silver + 1 gold)
- Grafana dashboards (19 panels auto-generated)
- Data quality: 97% gender completeness, 100% territory join success

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| PostgreSQL not connecting | Use `host.docker.internal:5435` in Grafana or `localhost:5435` locally |
| DAG syntax error | Run `python -m py_compile dags/*.py` |
| Grafana shows "No data" | Verify PostgreSQL datasource UID matches queries |
| DAG not discovered | Restart scheduler: `astro dev restart` |

---

## Files

| File | Purpose |
|------|---------|
| `dags/` | Airflow DAG definitions |
| `sql/` | SQL queries (reporting, QA, monitoring) |
| `scripts/generate_grafana_dashboards.py` | Auto-generates Grafana dashboard from SQL |
| `include/landing/` | Input CSV files (download from Kaggle) |
| `include/bronze/` | Parquet files (output of DAG 1) |
| `docker-compose.yaml` | Docker services config |
| `requirements.txt` | Python dependencies |

---

## References

- **Data Source**: https://www.kaggle.com/datasets/thedevastator/insurance-companies-secret-sauce-finally-exposed
- **Airflow**: https://airflow.apache.org/
- **Grafana**: https://grafana.com/

---

**Last Updated**: December 19, 2025  
**Status:** ✅ Production Ready

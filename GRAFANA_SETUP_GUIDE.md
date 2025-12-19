# Grafana Dashboard Setup Guide - Step by Step

**Project:** E2E Insurance Data Pipeline  
**Date:** December 19, 2025  
**Data Source:** PostgreSQL (localhost:5435) - Gold Layer  

---

## Prerequisites Checklist

✅ **Have Ready:**
- [ ] Grafana account created
- [ ] PostgreSQL running (localhost:5435)
- [ ] Database credentials: postgres / postgres
- [ ] Gold layer table: `gold.fact_insurance_performance` (95,536 rows)
- [ ] SQL queries from `sql/01_reporting_queries.sql`

---

## STEP 1: Access Grafana

### Option A: Grafana Cloud (If using cloud version)
```
URL: https://grafana.com/auth/sign-in
Login with your GitHub account
```

### Option B: Self-Hosted Grafana (If installed locally)
```
URL: http://localhost:3000
Default credentials: admin / admin
```

**ACTION:** Log into Grafana with your account

---

## STEP 2: Add PostgreSQL Data Source

### Navigate to Data Sources

1. Click **Settings** (gear icon) → **Data Sources**
2. Click **Add data source** (blue button)
3. Search for and select **PostgreSQL**

### Configure PostgreSQL Connection

Fill in the following fields:

```
Name:                PostgreSQL-Gold
Host:                localhost:5435
Database:            postgres
User:                postgres
Password:            postgres
SSL Mode:            disable (or require, depending on setup)
```

**Important Settings:**
- **Host:** `localhost:5435` (Not 5432!)
- **Port:** Should be included in host or separate field: `5435`
- **User:** `postgres`
- **Password:** `postgres`
- **Database:** `postgres`

### Test Connection

1. Scroll down to **Database Health**
2. Click **Save & Test**
3. You should see: **"Database connection ok"** ✅

**If Error:** Check PostgreSQL is running
```bash
docker ps | grep postgres
# Should show postgres container running on port 5435
```

---

## STEP 3: Create First Dashboard - KPI Overview

### Create New Dashboard

1. Click **+** (Create) → **Dashboard**
2. Click **Add a new panel**
3. You're now in the Dashboard Builder

### Panel 1: Total Records Count

**Query:**
```sql
SELECT COUNT(*) as total_records
FROM gold.fact_insurance_performance
```

**Configuration:**
- Data Source: PostgreSQL-Gold
- Query Name: Total Records
- Visualization: Stat (big number)
- Unit: short (default)
- Value field: total_records

**Panel Title:** "Total Records in Gold Layer"

### Panel 2: Gender Completeness %

**Query:**
```sql
SELECT 
  ROUND(COUNT(CASE WHEN gender IS NOT NULL THEN 1 END)::NUMERIC 
  / COUNT(*)::NUMERIC * 100, 2) as gender_pct
FROM gold.fact_insurance_performance
```

**Configuration:**
- Visualization: Stat
- Color: Green if > 95%, Yellow if > 85%
- Value field: gender_pct
- Unit: percent (0-100)

**Panel Title:** "Gender Completeness %"

### Panel 3: Territory Match Rate %

**Query:**
```sql
SELECT 
  ROUND(COUNT(CASE WHEN territory_label != 'Unknown' THEN 1 END)::NUMERIC 
  / COUNT(*)::NUMERIC * 100, 2) as territory_match_pct
FROM gold.fact_insurance_performance
```

**Configuration:**
- Visualization: Stat
- Unit: percent (0-100)

**Panel Title:** "Territory Match Rate %"

### Save Dashboard

1. Click **Save dashboard** (top right)
2. Name: `Insurance Pipeline - KPI Overview`
3. Click **Save**

---

## STEP 4: Create Second Dashboard - Territory Analysis

### Create New Dashboard

1. Click **+** → **Dashboard**
2. Name it: `Insurance Pipeline - Territory Analysis`

### Panel 1: Top 10 Territories by Record Count

**Query:**
```sql
SELECT 
  territory_label,
  COUNT(*) as record_count
FROM gold.fact_insurance_performance
WHERE territory_label != 'Unknown'
GROUP BY territory_label
ORDER BY record_count DESC
LIMIT 10
```

**Configuration:**
- Visualization: Bar chart (horizontal)
- X-axis: record_count
- Y-axis: territory_label
- Legend: Hide

**Panel Title:** "Top 10 Territories by Volume"

### Panel 2: Average Premium by Territory

**Query:**
```sql
SELECT 
  territory_label,
  ROUND(AVG(CAST(current_premium AS NUMERIC)), 2) as avg_premium
FROM gold.fact_insurance_performance
WHERE territory_label != 'Unknown'
GROUP BY territory_label
ORDER BY avg_premium DESC
LIMIT 10
```

**Configuration:**
- Visualization: Bar chart
- Unit: Currency (USD)

**Panel Title:** "Average Premium by Territory (Top 10)"

### Panel 3: Territory Distribution (Pie Chart)

**Query:**
```sql
SELECT 
  CASE 
    WHEN territory_label = 'Unknown' THEN 'Unknown'
    ELSE 'Mapped'
  END as status,
  COUNT(*) as record_count
FROM gold.fact_insurance_performance
GROUP BY status
```

**Configuration:**
- Visualization: Pie chart
- Legend: Show
- Display: Percent

**Panel Title:** "Territory Mapping Distribution"

### Save Dashboard

Click **Save dashboard**

---

## STEP 5: Create Third Dashboard - Data Quality Monitoring

### Create New Dashboard

Name: `Insurance Pipeline - Data Quality`

### Panel 1: NULL Values Check (Gauge)

**Query:**
```sql
SELECT 
  ROUND(COUNT(CASE WHEN gender IS NULL THEN 1 END)::NUMERIC 
  / COUNT(*)::NUMERIC * 100, 2) as null_gender_pct
FROM gold.fact_insurance_performance
```

**Configuration:**
- Visualization: Gauge
- Max value: 5
- Thresholds: Red > 2%, Yellow > 1%
- Unit: percent

**Panel Title:** "Gender NULL %"

### Panel 2: Data Freshness

**Query:**
```sql
SELECT 
  MAX(ingested_at) as latest_data
FROM gold.fact_insurance_performance
```

**Configuration:**
- Visualization: Stat
- Format: Timestamp (human readable)

**Panel Title:** "Latest Data Ingestion"

### Panel 3: Batch Count

**Query:**
```sql
SELECT 
  COUNT(DISTINCT batch_id) as total_batches
FROM gold.fact_insurance_performance
```

**Configuration:**
- Visualization: Stat
- Unit: none

**Panel Title:** "Total Batches Processed"

### Panel 4: Record Count Trend (Optional)

**Query:**
```sql
SELECT 
  DATE(ingested_at) as date,
  COUNT(*) as daily_records
FROM gold.fact_insurance_performance
GROUP BY DATE(ingested_at)
ORDER BY date DESC
LIMIT 10
```

**Configuration:**
- Visualization: Time series (line chart)
- Legend: Show
- Tooltip: Multi-series

**Panel Title:** "Daily Record Count Trend"

---

## STEP 6: Create Fourth Dashboard - Premium Analysis

### Create New Dashboard

Name: `Insurance Pipeline - Premium Analysis`

### Panel 1: Current Premium Distribution (Histogram)

**Query:**
```sql
SELECT 
  CAST(current_premium AS NUMERIC) as premium,
  COUNT(*) as freq
FROM gold.fact_insurance_performance
WHERE current_premium IS NOT NULL
GROUP BY CAST(current_premium AS NUMERIC)
ORDER BY premium
```

**Configuration:**
- Visualization: Bar chart
- Unit: Currency

**Panel Title:** "Current Premium Distribution"

### Panel 2: Average Premium Statistics

**Query:**
```sql
SELECT 
  ROUND(AVG(CAST(current_premium AS NUMERIC)), 2) as avg_premium,
  ROUND(MIN(CAST(current_premium AS NUMERIC)), 2) as min_premium,
  ROUND(MAX(CAST(current_premium AS NUMERIC)), 2) as max_premium,
  COUNT(*) as record_count
FROM gold.fact_insurance_performance
WHERE current_premium IS NOT NULL
```

**Configuration:**
- Visualization: Table
- Columns: avg_premium, min_premium, max_premium, record_count

**Panel Title:** "Premium Statistics"

### Panel 3: Total Premium by Territory (Top 10)

**Query:**
```sql
SELECT 
  territory_label,
  ROUND(SUM(CAST(current_premium AS NUMERIC)), 2) as total_premium
FROM gold.fact_insurance_performance
WHERE territory_label != 'Unknown'
GROUP BY territory_label
ORDER BY total_premium DESC
LIMIT 10
```

**Configuration:**
- Visualization: Bar chart
- Unit: Currency
- Sort: Descending

**Panel Title:** "Total Premium Revenue by Territory"

---

## STEP 7: Create Fifth Dashboard - Demographics

### Create New Dashboard

Name: `Insurance Pipeline - Demographics`

### Panel 1: Gender Distribution

**Query:**
```sql
SELECT 
  gender,
  COUNT(*) as count
FROM gold.fact_insurance_performance
WHERE gender IS NOT NULL
GROUP BY gender
```

**Configuration:**
- Visualization: Pie chart
- Legend: Show
- Display: Percent + Value

**Panel Title:** "Gender Distribution"

### Panel 2: Age Analysis (From Census Data)

**Query:**
```sql
SELECT 
  acs03_total_population as population,
  COUNT(*) as records
FROM gold.fact_insurance_performance
WHERE acs03_total_population IS NOT NULL
GROUP BY acs03_total_population
ORDER BY population DESC
LIMIT 20
```

**Configuration:**
- Visualization: Table

**Panel Title:** "Population Statistics"

### Panel 3: Record Count by State

**Query:**
```sql
SELECT 
  state_province_code,
  COUNT(*) as record_count
FROM gold.fact_insurance_performance
WHERE state_province_code IS NOT NULL
GROUP BY state_province_code
ORDER BY record_count DESC
LIMIT 10
```

**Configuration:**
- Visualization: Bar chart

**Panel Title:** "Records by State (Top 10)"

---

## STEP 8: Create Master Dashboard (Overview)

### Create New Dashboard

Name: `Insurance Pipeline - Master Dashboard`

### Add Existing Panels

1. Click **Add panels from a library**
2. Select previous dashboards and add key panels:
   - KPI Overview (all 3 stats panels)
   - Territory Analysis (top territories)
   - Data Quality (freshness & NULL checks)

### Arrange Layout

Organize panels in logical order:
```
Row 1: KPIs (Total Records, Gender %, Territory %)
Row 2: Territory Analysis (Volume, Premium)
Row 3: Data Quality (Freshness, NULL checks)
Row 4: Premium Analysis (Distribution, Statistics)
```

---

## STEP 9: Set Up Auto-Refresh

For each dashboard:

1. Click the **dashboard settings** (gear icon)
2. Under **General**, set:
   - **Refresh interval:** 1 minute
   - **Time range:** Last 24 hours (or Last 7 days)
3. Click **Save**

This ensures data updates automatically

---

## STEP 10: Variable Setup (Optional - Advanced)

For dynamic filtering:

1. Dashboard Settings → **Variables**
2. Add new variable:
   - Name: `territory_filter`
   - Type: Query
   - Query: `SELECT DISTINCT territory_label FROM gold.fact_insurance_performance ORDER BY 1`
3. Use in queries: `WHERE territory_label = $territory_filter`

---

## Troubleshooting

### "Database connection failed"

**Solution:** 
```bash
# Verify PostgreSQL is running
docker ps | grep postgres

# Test connection manually
PGPASSWORD=postgres psql -h localhost -p 5435 -U postgres -d postgres -c "SELECT COUNT(*) FROM gold.fact_insurance_performance;"
```

### "No data in dashboard"

**Solution:**
1. Check SQL query in Grafana query editor
2. Verify table name: `gold.fact_insurance_performance`
3. Run query manually in psql to confirm data exists
4. Check data source credentials in Grafana

### "Field not found" error

**Solution:**
1. Ensure column name matches exactly (case-sensitive in PostgreSQL)
2. Remove unnecessary casts or functions
3. Test query in psql first

### Dashboard not auto-refreshing

**Solution:**
1. Check refresh interval is set (not "off")
2. Verify data source is healthy
3. Check browser console for errors (F12)

---

## Quick Command Reference

### Start Grafana (if self-hosted)
```bash
docker run -d -p 3000:3000 grafana/grafana
```

### Test PostgreSQL Connection
```bash
PGPASSWORD=postgres psql -h localhost -p 5435 -U postgres -d postgres -c "SELECT COUNT(*) FROM gold.fact_insurance_performance;"
```

### Check if PostgreSQL is Running
```bash
docker ps | grep postgres
```

---

## Next Steps After Dashboard Setup

1. **Share Dashboards:** Click share icon to get shareable links
2. **Set Alerts:** Add alert conditions for data quality metrics
3. **Create Annotations:** Mark important events on dashboards
4. **Backup Dashboards:** Export as JSON for version control
5. **CI/CD Integration:** Push dashboard definitions to GitHub

---

## Dashboard Summary

| Dashboard | Panels | Purpose |
|---|---|---|
| KPI Overview | 3 | High-level metrics & data quality |
| Territory Analysis | 3 | Territory performance & distribution |
| Data Quality | 4 | Monitoring & freshness checks |
| Premium Analysis | 3 | Revenue & premium statistics |
| Demographics | 3 | Gender, age, geographic analysis |
| Master | 9+ | Consolidated overview |

**Total Panels:** 25+ visualizations  
**Data Source:** PostgreSQL (localhost:5435)  
**Refresh:** Automatic (1 minute)  
**Update Frequency:** Daily (from DAG scheduling)  

---

## Support SQL Queries

All SQL queries used in these dashboards are from:
- `sql/01_reporting_queries.sql` (Reporting queries)
- `sql/02_quality_tests.sql` (Quality checks)

You can find additional queries there for custom panels!

---

**Setup Time:** ~30-45 minutes for full dashboard suite  
**Complexity:** Beginner-friendly  
**Next Review:** After first week of data collection  

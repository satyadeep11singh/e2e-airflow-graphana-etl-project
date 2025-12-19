# Grafana Dashboard Setup - Quick Checklist

**Project:** E2E Insurance Data Pipeline  
**Data Source:** PostgreSQL Gold Layer  
**Date:** December 19, 2025  

---

## 🎯 Quick Setup Summary

You're setting up 5 dashboards to visualize the insurance data pipeline with **25+ panels** total.

---

## ✅ STEP-BY-STEP CHECKLIST

### Phase 1: Initial Setup (5 minutes)

- [ ] Open Grafana in browser
- [ ] Log in with GitHub account (if Grafana Cloud)
- [ ] Navigate to **Settings** → **Data Sources**
- [ ] Click **Add data source**
- [ ] Select **PostgreSQL**

### Phase 2: PostgreSQL Connection (5 minutes)

**Fill these fields exactly:**

```
Field Name         | Value
-------------------|------------------
Name               | PostgreSQL-Gold
Host               | localhost:5435
Database           | postgres
User               | postgres
Password           | postgres
SSL Mode           | disable
```

- [ ] Fill in all connection details
- [ ] Click **Save & Test**
- [ ] See **"Database connection ok"** ✅

### Phase 3: Create Dashboard 1 - KPI Overview (10 minutes)

**Three simple metrics panels:**

| Panel Name | Query | Visualization |
|---|---|---|
| Total Records | `SELECT COUNT(*) as total_records FROM gold.fact_insurance_performance` | Stat (big number) |
| Gender % | `SELECT ROUND(COUNT(CASE WHEN gender IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 2) as gender_pct FROM gold.fact_insurance_performance` | Stat (percent) |
| Territory % | `SELECT ROUND(COUNT(CASE WHEN territory_label != 'Unknown' THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 2) as territory_match_pct FROM gold.fact_insurance_performance` | Stat (percent) |

**Actions:**
- [ ] Create new dashboard
- [ ] Add 3 panels (one for each query above)
- [ ] Set visualization types as shown
- [ ] Save as: **"Insurance Pipeline - KPI Overview"**

### Phase 4: Create Dashboard 2 - Territory Analysis (10 minutes)

**Three territory-focused panels:**

- [ ] **Panel 1:** Top 10 Territories by Volume (Bar chart)
  - Query: Top territories by record count
  - Visualization: Horizontal bar chart
  
- [ ] **Panel 2:** Average Premium by Territory (Bar chart)
  - Query: Average premium per territory
  - Visualization: Bar chart (USD currency)
  
- [ ] **Panel 3:** Territory Distribution (Pie chart)
  - Query: Mapped vs Unknown split
  - Visualization: Pie chart

- [ ] Save as: **"Insurance Pipeline - Territory Analysis"**

### Phase 5: Create Dashboard 3 - Data Quality (10 minutes)

**Four monitoring panels:**

- [ ] **Panel 1:** Gender NULL % (Gauge)
- [ ] **Panel 2:** Latest Data Ingestion (Stat - timestamp)
- [ ] **Panel 3:** Total Batches Processed (Stat - number)
- [ ] **Panel 4:** Daily Record Count Trend (Time series line chart)

- [ ] Save as: **"Insurance Pipeline - Data Quality"**

### Phase 6: Create Dashboard 4 - Premium Analysis (10 minutes)

**Three revenue-focused panels:**

- [ ] **Panel 1:** Current Premium Distribution (Bar chart)
- [ ] **Panel 2:** Premium Statistics (Table: avg, min, max)
- [ ] **Panel 3:** Total Premium by Territory (Bar chart - USD)

- [ ] Save as: **"Insurance Pipeline - Premium Analysis"**

### Phase 7: Create Dashboard 5 - Demographics (10 minutes)

**Three demographic panels:**

- [ ] **Panel 1:** Gender Distribution (Pie chart - percent)
- [ ] **Panel 2:** Population Statistics (Table)
- [ ] **Panel 3:** Records by State (Bar chart - top 10)

- [ ] Save as: **"Insurance Pipeline - Demographics"**

### Phase 8: Configure Auto-Refresh (5 minutes)

**For each dashboard:**

- [ ] Click dashboard settings (gear icon)
- [ ] Set **Refresh interval** to **1 minute**
- [ ] Set **Time range** to **Last 24 hours**
- [ ] Click **Save**

### Phase 9: Create Master Dashboard (Optional - 10 minutes)

- [ ] Create new dashboard: **"Insurance Pipeline - Master Dashboard"**
- [ ] Add key panels from all 5 dashboards
- [ ] Arrange in logical order (KPIs → Territory → Quality → Premium → Demographics)
- [ ] Save

---

## 🔍 Validation Checklist

**For each dashboard, verify:**

- [ ] At least one panel shows data (no "No data")
- [ ] Colors are visible and readable
- [ ] Panel titles are descriptive
- [ ] Units are correct (%, numbers, currency, timestamps)
- [ ] Auto-refresh is enabled

---

## 🚨 Common Issues & Fixes

| Issue | Fix |
|---|---|
| "Database connection failed" | Check PostgreSQL is running: `docker ps \| grep postgres` |
| "No data in dashboard" | Run query manually in psql to verify data exists |
| "Field not found" error | Check column names match exactly (case-sensitive) |
| Dashboard not refreshing | Verify refresh interval is set (not "off") |

---

## 📊 Dashboard Details Summary

| # | Dashboard Name | Panels | Purpose |
|---|---|---|---|
| 1 | KPI Overview | 3 | High-level metrics |
| 2 | Territory Analysis | 3 | Territory performance |
| 3 | Data Quality | 4 | Monitoring & freshness |
| 4 | Premium Analysis | 3 | Revenue insights |
| 5 | Demographics | 3 | Geographic analysis |
| 6 | Master (Optional) | 9+ | Consolidated view |

**Total:** 25+ visualizations | **Time:** 60-90 minutes

---

## 🔗 Database Connection Details

```
Host:           localhost:5435
Database:       postgres
Username:       postgres
Password:       postgres
Table:          gold.fact_insurance_performance
Records:        95,536 rows
Columns:        666 columns
```

---

## 📝 PostgreSQL Test Command

```bash
# Test connection manually
PGPASSWORD=postgres psql -h localhost -p 5435 -U postgres -d postgres \
  -c "SELECT COUNT(*) as record_count FROM gold.fact_insurance_performance;"

# Should output: 95536
```

---

## 💾 Query Reference

All SQL queries are in your project:
- `sql/01_reporting_queries.sql` - 66 reporting queries
- `sql/02_quality_tests.sql` - Data quality tests

You can copy queries from these files directly into Grafana!

---

## ✨ Pro Tips

1. **Create variables** for dynamic filtering by territory or state
2. **Set thresholds** on gauges (red/yellow/green zones)
3. **Use annotations** to mark important dates (e.g., DAG schedule changes)
4. **Export dashboards** as JSON for backup and version control
5. **Share dashboards** via unique links for team access

---

## 🎉 Success Criteria

✅ **You're done when:**
- [ ] All 5 dashboards created and showing data
- [ ] Data updates automatically (1-minute refresh)
- [ ] Can navigate between dashboards smoothly
- [ ] All panels display correctly (no errors)
- [ ] Dashboards match the structure above

---

## 📞 Need Help?

**Common Resources:**
- [Grafana Documentation](https://grafana.com/docs)
- [PostgreSQL Datasource Guide](https://grafana.com/docs/grafana/latest/datasources/postgres/)
- Full setup guide: See `GRAFANA_SETUP_GUIDE.md` in project

---

**Estimated Time:** 60-90 minutes  
**Difficulty:** Beginner-friendly  
**Result:** Professional data visualization dashboard suite  

---

Start with **Step 1: Access Grafana** above! 🚀

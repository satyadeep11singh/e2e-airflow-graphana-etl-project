<!-- PROJECT STRUCTURE ANALYSIS & STANDARDIZATION REPORT -->
<!-- E2E Insurance Data Pipeline - Apache Airflow Project -->
<!-- Generated: 2025-12-19 -->

# Project Structure Standardization Report

## Current Structure Analysis

```
e2e-project/
├── .astro/                                      ✅ Standard Astro Runtime config
├── .dockerignore                                ✅ Standard Docker config
├── .env                                         ✅ Environment variables
├── .gitignore                                   ✅ Git ignore rules
├── airflow_settings.yaml                        ✅ Airflow settings
├── docker-compose.yaml                          ✅ Standard name (YAML format)
├── Dockerfile                                   ✅ Standard name
├── packages.txt                                 ✅ OS dependencies
├── requirements.txt                             ✅ Python dependencies
├── README.md                                    ⚠️  OLD - See issues below
├── README_UPDATED.md                            ⚠️  DUPLICATE - Should be consolidated
├── DAG_SCHEDULING_AND_QUERIES.sql              ⚠️  Non-standard location
├── DATA_QUALITY_TESTS.sql                      ⚠️  Non-standard location
├── sample_queries.sql                          ⚠️  Non-standard location
│
├── dags/                                        ✅ Standard Airflow directory
│   ├── .airflowignore
│   ├── insurance_ingestion.py                  ⚠️  Inconsistent naming (1_xxx.py expected)
│   ├── 2_bronze_to_silver.py                   ⚠️  Missing 1_ prefix for consistency
│   ├── 3_gold_transformation.py                ✅ Numbered correctly
│   └── __pycache__/
│
├── include/                                     ✅ Standard Airflow directory
│   ├── landing/
│   │   └── archive/                            ✅ Processed files storage
│   └── bronze/
│       ├── ACS_MD_15_5YR_DP03_20251218.parquet  ⚠️  Inconsistent naming (hyphens)
│       ├── ACS_MD_15_5YR_DP05_20251218.parquet  ⚠️  Inconsistent naming (hyphens)
│       ├── cgr-definitions-table_20251218.parquet    ⚠️  Mixed hyphens/underscores
│       ├── cgr-premiums-table_20251218.parquet       ⚠️  Mixed hyphens/underscores
│       └── territory-definitions-table_20251218.parquet  ⚠️  Mixed hyphens/underscores
│
├── plugins/                                     ✅ Standard Airflow directory
├── tests/                                       ✅ Standard test directory
│   └── dags/
└── sql/                                         ❌ MISSING - Should be created
    ├── 01_ddl_schemas.sql                      (To be created)
    ├── 02_ddl_silver_tables.sql                (To be created)
    ├── 03_ddl_gold_tables.sql                  (To be created)
    └── 04_functions_procedures.sql             (To be created)
```

---

## Issues Identified

### 🔴 CRITICAL

1. **SQL Files in Root Directory**
   - Location: Root project directory
   - Issue: SQL files should be in `sql/` subdirectory
   - Files: `DAG_SCHEDULING_AND_QUERIES.sql`, `DATA_QUALITY_TESTS.sql`, `sample_queries.sql`
   - Impact: Pollutes root directory, violates Airflow best practices

2. **Duplicate README Files**
   - Files: `README.md` (old) and `README_UPDATED.md` (new)
   - Issue: Two versions create confusion
   - Impact: Users unsure which is current

3. **DAG File Naming Inconsistency**
   - Issue: `insurance_ingestion.py` should be `1_insurance_ingestion.py`
   - Current: 1_xxx.py, 2_xxx.py, 3_xxx.py (mixed with unumbered)
   - Impact: Execution order not clear; Airflow alphabetical sorting may reorder

### 🟡 MEDIUM

4. **Inconsistent File Naming in Bronze Layer**
   - Issue: Mix of hyphens and underscores in parquet files
   - Examples:
     - `ACS_MD_15_5YR_DP03_20251218.parquet` (underscores)
     - `cgr-definitions-table_20251218.parquet` (hyphens + underscores)
   - Impact: Hard to parse programmatically; naming conventions not followed

5. **Missing SQL Organization**
   - Issue: No `sql/` directory for schema definitions
   - Current: All queries as .sql files in root
   - Impact: No clear separation of DDL/queries/tests

### 🟢 MINOR

6. **Missing git tracking**
   - `__pycache__/` directories should be ignored
   - `.astro/` should potentially be ignored

---

## Recommended Standardization

### Structure After Standardization

```
e2e-project/
├── .github/
│   └── workflows/                              (NEW) CI/CD pipelines
├── .astro/
├── .dockerignore
├── .env
├── .gitignore
├── config/                                     (NEW) Configuration files
│   └── airflow_settings.yaml
├── dags/                                       (Fixed naming)
│   ├── .airflowignore
│   ├── 1_insurance_ingestion.py                (RENAMED)
│   ├── 2_bronze_to_silver.py                   (OK)
│   ├── 3_gold_transformation.py                (OK)
│   └── __pycache__/
├── include/
│   ├── bronze/                                 (Rename parquet files)
│   │   ├── acs_md_15_5yr_dp03_20251218.parquet
│   │   ├── acs_md_15_5yr_dp05_20251218.parquet
│   │   ├── cgr_definitions_table_20251218.parquet
│   │   ├── cgr_premiums_table_20251218.parquet
│   │   └── territory_definitions_table_20251218.parquet
│   ├── landing/
│   │   └── archive/
│   └── sql/                                    (NEW)
│       ├── 00_setup.sql
│       ├── 01_bronze_to_silver.sql
│       ├── 02_silver_to_gold.sql
│       └── 03_quality_assurance.sql
├── docs/                                       (NEW) Documentation
│   ├── ARCHITECTURE.md
│   ├── SETUP_GUIDE.md
│   ├── DAG_DOCUMENTATION.md
│   └── TROUBLESHOOTING.md
├── plugins/
├── scripts/                                    (NEW) Utility scripts
│   ├── data_quality_check.sh
│   ├── backup_database.sh
│   └── run_scheduled_reports.ps1
├── tests/
│   ├── dags/
│   └── sql/                                    (NEW)
│       └── test_data_quality.sql
├── docker-compose.yaml
├── Dockerfile
├── packages.txt
├── requirements.txt
├── README.md                                   (CONSOLIDATE - merge both)
└── .gitignore
```

---

## Implementation Plan

### Step 1: Create SQL Directory Structure
```bash
mkdir sql/
mkdir include/sql/
mkdir docs/
mkdir scripts/
mkdir config/
```

### Step 2: Move and Rename SQL Files
```bash
# Move to sql/ directory
mv sample_queries.sql sql/01_reporting_queries.sql
mv DATA_QUALITY_TESTS.sql sql/02_quality_tests.sql
mv DAG_SCHEDULING_AND_QUERIES.sql sql/03_scheduling_and_monitoring.sql

# Create schema files
touch sql/00_setup_schemas.sql
touch sql/04_functions_procedures.sql
```

### Step 3: Rename DAG Files
```bash
mv dags/insurance_ingestion.py dags/1_insurance_ingestion.py
# Verify DAG IDs remain consistent in code
```

### Step 4: Consolidate README Files
```bash
# Merge README_UPDATED.md content into README.md
# Delete README_UPDATED.md
rm README_UPDATED.md

# Move airflow_settings.yaml to config/
mv airflow_settings.yaml config/
```

### Step 5: Standardize Parquet File Names
```bash
# Rename in bronze/ directory
ACS_MD_15_5YR_DP03_20251218.parquet → acs_md_15_5yr_dp03_20251218.parquet
ACS_MD_15_5YR_DP05_20251218.parquet → acs_md_15_5yr_dp05_20251218.parquet
cgr-definitions-table_20251218.parquet → cgr_definitions_table_20251218.parquet
cgr-premiums-table_20251218.parquet → cgr_premiums_table_20251218.parquet
territory-definitions-table_20251218.parquet → territory_definitions_table_20251218.parquet
```

### Step 6: Update DAG Code References
Update all file references in DAG files:
- `dags/1_insurance_ingestion.py` - Update file paths
- `dags/2_bronze_to_silver.py` - Update file paths
- `dags/3_gold_transformation.py` - Update file paths

---

## Naming Conventions

### Python Files (DAGs)
```
Format: {number}_{descriptive_name}.py
Examples:
  ✅ 1_insurance_ingestion.py
  ✅ 2_bronze_to_silver.py
  ✅ 3_gold_transformation.py
  ✅ 4_data_quality_checks.py (if added)
```

### SQL Files
```
Format: {number}_{description}.sql
Examples:
  ✅ 00_setup_schemas.sql
  ✅ 01_reporting_queries.sql
  ✅ 02_quality_tests.sql
  ✅ 03_monitoring_queries.sql
```

### Data Files (Parquet)
```
Format: {source_system}_{table_type}_{date}.parquet
Rules:
  - All lowercase
  - Use underscores (no hyphens)
  - Include date suffix
  
Examples:
  ✅ acs_md_15_5yr_dp03_20251218.parquet
  ✅ cgr_definitions_table_20251218.parquet
  ✅ territory_definitions_table_20251218.parquet
```

### Directory Names
```
✅ dags/          - Airflow DAGs
✅ include/       - Includes (data files, dependencies)
✅ tests/         - Test suites
✅ plugins/       - Custom Airflow plugins
✅ docs/          - Documentation
✅ scripts/       - Utility scripts
✅ config/        - Configuration files
✅ sql/           - SQL scripts
```

---

## Benefits of Standardization

1. **Consistency**: Predictable file locations and naming
2. **Maintainability**: Easier to find and update files
3. **Scalability**: Framework supports adding new DAGs/queries
4. **Version Control**: Clear organization in git
5. **Documentation**: Self-documenting file structure
6. **Automation**: Scripts can parse file names reliably
7. **Team Onboarding**: New developers understand structure immediately

---

## Action Items

| Priority | Task | Impact | Effort |
|----------|------|--------|--------|
| 🔴 HIGH | Move SQL files to sql/ directory | Critical | 10 min |
| 🔴 HIGH | Consolidate README files | Critical | 15 min |
| 🔴 HIGH | Rename insurance_ingestion.py to 1_* | Critical | 5 min + testing |
| 🟡 MEDIUM | Standardize parquet file names | High | 15 min + DAG updates |
| 🟡 MEDIUM | Create docs/ directory | Medium | 20 min |
| 🟡 MEDIUM | Create scripts/ directory | Medium | 10 min |
| 🟢 LOW | Update .gitignore entries | Low | 5 min |

**Total Estimated Time: 80 minutes**

---

## Next Steps

1. Review this standardization plan
2. Approve changes
3. Execute implementation
4. Update DAG code to reference new file locations
5. Test end-to-end pipeline
6. Update documentation


-- ============================================================================
-- STAKEHOLDER ANALYSIS QUERIES - TOP 10 BUSINESS QUESTIONS
-- ============================================================================
-- Database: postgresql://localhost:5435/postgres
-- Created: 2025-12-20
-- Purpose: Answer key business questions for executives, finance, marketing, risk
-- ============================================================================
-- Note: These queries support the top 10 stakeholder questions documented in
--       STAKEHOLDER_QUESTIONS_ANALYSIS.md with Grafana visualization guidance
-- ============================================================================

-- ============================================================================
-- QUESTION 1: PREMIUM PERFORMANCE BY TERRITORY
-- "Which territories are generating the most premium revenue and what's the 
--  average premium per policy?"
-- Visualization: Horizontal Bar Chart + Detailed Table
-- ============================================================================

-- 1.1: Territory Revenue Ranking (Top 20)
-- Best for: Bar Chart visualization showing territory performance
SELECT 
    territory as territory_id,
    town,
    county,
    COUNT(*) as policy_count,
    ROUND(AVG(current_premium)::NUMERIC, 2) as avg_premium,
    ROUND(SUM(current_premium)::NUMERIC, 2) as total_revenue,
    ROUND(SUM(underlying_total_premium)::NUMERIC, 2) as total_cost,
    ROUND(
        (SUM(current_premium) - SUM(underlying_total_premium))::NUMERIC,
        2
    ) as gross_margin,
    ROUND(
        ((SUM(current_premium) - SUM(underlying_total_premium)) / 
        SUM(current_premium) * 100)::NUMERIC,
        2
    ) as margin_percentage
FROM gold.fact_insurance_performance
GROUP BY territory, town, county
ORDER BY total_revenue DESC
LIMIT 20;

-- 1.2: Territory Summary Statistics
-- Best for: Dashboard stat cards showing key metrics
SELECT 
    COUNT(DISTINCT territory) as total_territories,
    ROUND(AVG(policy_count), 0)::INT as avg_policies_per_territory,
    MAX(policy_count) as max_policies_in_territory,
    ROUND(AVG(avg_premium)::NUMERIC, 2) as company_wide_avg_premium,
    ROUND(SUM(total_revenue)::NUMERIC, 2) as company_total_revenue
FROM (
    SELECT 
        territory,
        COUNT(*) as policy_count,
        AVG(current_premium) as avg_premium,
        SUM(current_premium) as total_revenue
    FROM gold.fact_insurance_performance
    GROUP BY territory
) territory_metrics;

-- ============================================================================
-- QUESTION 2: GENDER-BASED RISK & PREMIUM ANALYSIS
-- "Are there significant premium differences between male and female policyholders?
--  What's the risk profile by gender?"
-- Visualization: Pie Chart + Side-by-side Stat Panels + Table
-- ============================================================================

-- 2.1: Gender Distribution & Premium Comparison
-- Best for: Side-by-side comparison and pie chart
SELECT 
    gender,
    COUNT(*) as policy_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()::NUMERIC, 2) as percentage_of_total,
    ROUND(AVG(current_premium)::NUMERIC, 2) as avg_current_premium,
    ROUND(AVG(indicated_premium)::NUMERIC, 2) as avg_indicated_premium,
    ROUND(AVG(underlying_total_premium)::NUMERIC, 2) as avg_underlying_premium,
    ROUND(AVG(ypc)::NUMERIC, 2) as avg_prior_claims,
    ROUND(AVG(cgr_factor)::NUMERIC, 2) as avg_cgr_factor
FROM gold.fact_insurance_performance
WHERE gender IS NOT NULL
GROUP BY gender
ORDER BY policy_count DESC;

-- 2.2: Gender Premium Variance Analysis
-- Best for: Detailed comparison metrics
SELECT 
    'Premium Variance by Gender' as metric,
    ROUND(
        ((MAX(avg_premium) - MIN(avg_premium)) / MIN(avg_premium) * 100)::NUMERIC,
        2
    )::TEXT || '%' as variance_percentage,
    ROUND((MAX(avg_premium) - MIN(avg_premium))::NUMERIC, 2) as absolute_difference
FROM (
    SELECT gender, AVG(current_premium) as avg_premium
    FROM gold.fact_insurance_performance
    WHERE gender IS NOT NULL
    GROUP BY gender
) gender_avg;

-- 2.3: Gender Risk Profile (Prior Claims & Loss Ratio)
-- Best for: Risk underwriters and pricing teams
SELECT 
    territory,
    town,
    COUNT(*) as policy_count,
    ROUND(AVG(current_premium)::NUMERIC, 2) as avg_premium,
    ROUND(AVG(acs03_total_population), 0)::INT as total_population,
    ROUND(AVG(acs03_median_age)::NUMERIC, 1) as median_age,
    ROUND(AVG(acs03_married_percent)::NUMERIC, 1) as percent_married,
    ROUND(AVG(acs05_total_population), 0)::INT as diverse_population_estimate
FROM gold.fact_insurance_performance
WHERE acs03_total_population IS NOT NULL
GROUP BY territory, town
ORDER BY policy_count DESC
LIMIT 50;

-- ============================================================================
-- QUESTION 3: DEMOGRAPHIC-ADJUSTED PREMIUM INSIGHTS
-- "How do community demographics (population, age, income, employment) correlate
--  with insurance premiums and claims?"
-- Visualization: Scatter Plot + Heatmap + Correlation Stats
-- ============================================================================

-- 3.1: Territory Demographics & Premium Correlation
-- Best for: Scatter plot showing premium vs demographics
SELECT 
    territory,
    town,
    COUNT(*) as policy_count,
    ROUND(AVG(current_premium)::NUMERIC, 2) as avg_premium,
    ROUND(AVG(acs03_total_population), 0)::INT as total_population,
    ROUND(AVG(acs03_median_age)::NUMERIC, 1) as median_age,
    ROUND(AVG(acs03_married_percent)::NUMERIC, 1) as percent_married,
    ROUND(AVG(acs05_total_population), 0)::INT as diverse_population_estimate
FROM gold.fact_insurance_performance
WHERE acs03_total_population IS NOT NULL
GROUP BY territory, town
ORDER BY policy_count DESC
LIMIT 50;

-- 3.2: Age-Premium Correlation Analysis
-- Best for: Understanding how median age affects premiums
SELECT 
    CASE 
        WHEN acs03_median_age < 30 THEN 'Under 30'
        WHEN acs03_median_age < 40 THEN '30-40'
        WHEN acs03_median_age < 50 THEN '40-50'
        WHEN acs03_median_age < 60 THEN '50-60'
        ELSE '60+'
    END as age_group,
    COUNT(*) as policy_count,
    ROUND(AVG(current_premium)::NUMERIC, 2) as avg_premium,
    ROUND(AVG(ypc)::NUMERIC, 2) as avg_prior_claims,
    ROUND(AVG(cgr_factor)::NUMERIC, 2) as avg_cgr_factor
FROM gold.fact_insurance_performance
WHERE acs03_median_age IS NOT NULL
GROUP BY 
    CASE 
        WHEN acs03_median_age < 30 THEN 'Under 30'
        WHEN acs03_median_age < 40 THEN '30-40'
        WHEN acs03_median_age < 50 THEN '40-50'
        WHEN acs03_median_age < 60 THEN '50-60'
        ELSE '60+'
    END
ORDER BY age_group ASC;

-- ============================================================================
-- QUESTION 4: POLICY LOSS RATIO & PROFITABILITY ANALYSIS
-- "Which territories are most profitable? What's our loss ratio (claims vs.
--  premiums) by geography?"
-- Visualization: Grouped Bar Chart + Gauge + Table
-- ============================================================================

-- 4.1: Territory Profitability & Loss Ratio
-- Best for: Identifying profitable vs unprofitable territories
SELECT 
    territory,
    town,
    county,
    COUNT(*) as policy_count,
    ROUND(AVG(current_premium)::NUMERIC, 2) as avg_current_premium,
    ROUND(AVG(underlying_premium)::NUMERIC, 2) as avg_underlying_premium,
    ROUND(AVG(underlying_total_premium)::NUMERIC, 2) as avg_total_cost,
    ROUND(
        (SUM(current_premium) / NULLIF(SUM(underlying_total_premium), 0))::NUMERIC,
        4
    ) as loss_ratio,
    CASE 
        WHEN SUM(current_premium) / NULLIF(SUM(underlying_total_premium), 0) < 0.75 
            THEN 'Highly Profitable'
        WHEN SUM(current_premium) / NULLIF(SUM(underlying_total_premium), 0) < 1.0 
            THEN 'Profitable'
        WHEN SUM(current_premium) / NULLIF(SUM(underlying_total_premium), 0) < 1.25 
            THEN 'Acceptable Risk'
        ELSE 'Loss-Making'
    END as profitability_tier,
    ROUND(
        ((1 - (SUM(underlying_total_premium) / NULLIF(SUM(current_premium), 0))) * 100)::NUMERIC,
        2
    ) as margin_percentage
FROM gold.fact_insurance_performance
GROUP BY territory, town, county
ORDER BY margin_percentage DESC
LIMIT 30;

-- 4.2: Company-Wide Loss Ratio Summary (Gauge metric)
-- Best for: KPI dashboard gauge
SELECT 
    ROUND(
        (SUM(current_premium) / NULLIF(SUM(underlying_total_premium), 0))::NUMERIC,
        4
    ) as overall_loss_ratio,
    ROUND(
        ((1 - (SUM(underlying_total_premium) / NULLIF(SUM(current_premium), 0))) * 100)::NUMERIC,
        2
    ) as overall_margin_percent
FROM gold.fact_insurance_performance;

-- 4.3: CGR Category Performance (Claims/Growth Ratio)
-- Best for: Understanding claims impact by rating category
SELECT 
    cgr as cgr_category,
    COUNT(*) as policy_count,
    ROUND(AVG(current_premium)::NUMERIC, 2) as avg_premium,
    ROUND(AVG(cgr_factor)::NUMERIC, 3) as avg_cgr_factor,
    ROUND(AVG(ypc)::NUMERIC, 2) as avg_prior_claims
FROM gold.fact_insurance_performance
WHERE cgr IS NOT NULL
GROUP BY cgr
ORDER BY policy_count DESC;

-- ============================================================================
-- QUESTION 5: AGE-BASED PREMIUM SEGMENTATION
-- "How do insurance premiums vary by customer age groups or age-related risk
--  factors?"
-- Visualization: Bar Chart (Vertical) + Trend Line + Table
-- ============================================================================

-- 5.1: Premium by Age Cohort
-- Best for: Age-based segmentation analysis
WITH age_cohorts AS (
    SELECT 
        CASE 
            WHEN EXTRACT(YEAR FROM AGE(CAST(birthdate AS DATE))) < 25 THEN '18-25'
            WHEN EXTRACT(YEAR FROM AGE(CAST(birthdate AS DATE))) < 35 THEN '25-35'
            WHEN EXTRACT(YEAR FROM AGE(CAST(birthdate AS DATE))) < 45 THEN '35-45'
            WHEN EXTRACT(YEAR FROM AGE(CAST(birthdate AS DATE))) < 55 THEN '45-55'
            WHEN EXTRACT(YEAR FROM AGE(CAST(birthdate AS DATE))) < 65 THEN '55-65'
            ELSE '65+'
        END as age_group,
        CASE 
            WHEN EXTRACT(YEAR FROM AGE(CAST(birthdate AS DATE))) < 25 THEN 1
            WHEN EXTRACT(YEAR FROM AGE(CAST(birthdate AS DATE))) < 35 THEN 2
            WHEN EXTRACT(YEAR FROM AGE(CAST(birthdate AS DATE))) < 45 THEN 3
            WHEN EXTRACT(YEAR FROM AGE(CAST(birthdate AS DATE))) < 55 THEN 4
            WHEN EXTRACT(YEAR FROM AGE(CAST(birthdate AS DATE))) < 65 THEN 5
            ELSE 6
        END as sort_order,
        current_premium,
        ypc
    FROM gold.fact_insurance_performance
    WHERE birthdate IS NOT NULL 
        AND birthdate != ''
        AND EXTRACT(YEAR FROM AGE(CAST(birthdate AS DATE))) BETWEEN 18 AND 100
)
SELECT 
    age_group,
    COUNT(*) as policy_count,
    ROUND(AVG(current_premium)::NUMERIC, 2) as avg_premium,
    ROUND(MIN(current_premium)::NUMERIC, 2) as min_premium,
    ROUND(MAX(current_premium)::NUMERIC, 2) as max_premium,
    ROUND(STDDEV(current_premium)::NUMERIC, 2) as stddev_premium,
    ROUND(AVG(ypc)::NUMERIC, 2) as avg_prior_claims
FROM age_cohorts
GROUP BY age_group, sort_order
ORDER BY sort_order;

-- ============================================================================
-- QUESTION 6: YEAR-PRIOR CLAIMS IMPACT ANALYSIS
-- "What's the relationship between prior claims (YPC) and current premium
--  pricing?"
-- Visualization: Scatter Plot + Box Plot + Correlation Stats
-- ============================================================================

-- 6.1: Premium by Prior Claims Bucket
-- Best for: Scatter plot and box plot visualization
WITH claims_buckets AS (
    SELECT 
        CASE 
            WHEN ypc = 0 THEN 'No Claims'
            WHEN ypc = 1 THEN '1 Claim'
            WHEN ypc = 2 THEN '2 Claims'
            WHEN ypc = 3 THEN '3 Claims'
            WHEN ypc >= 4 THEN '4+ Claims'
        END as prior_claims_bucket,
        CASE 
            WHEN ypc = 0 THEN 1
            WHEN ypc = 1 THEN 2
            WHEN ypc = 2 THEN 3
            WHEN ypc = 3 THEN 4
            WHEN ypc >= 4 THEN 5
        END as sort_order,
        current_premium
    FROM gold.fact_insurance_performance
    WHERE ypc IS NOT NULL
)
SELECT 
    prior_claims_bucket,
    COUNT(*) as policy_count,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ())::NUMERIC, 2) as percent_of_portfolio,
    ROUND(AVG(current_premium)::NUMERIC, 2) as avg_premium,
    ROUND(STDDEV(current_premium)::NUMERIC, 2) as stddev_premium,
    ROUND(MIN(current_premium)::NUMERIC, 2) as min_premium,
    ROUND(MAX(current_premium)::NUMERIC, 2) as max_premium,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY current_premium)::NUMERIC, 2) as median_premium
FROM claims_buckets
GROUP BY prior_claims_bucket, sort_order
ORDER BY sort_order;

-- 6.2: Premium Increase by Claims Count
-- Best for: Understanding pricing impact of claims
SELECT 
    ypc as prior_claims_count,
    COUNT(*) as policy_count,
    ROUND(AVG(current_premium)::NUMERIC, 2) as avg_premium,
    ROUND(AVG(indicated_premium)::NUMERIC, 2) as avg_indicated,
    ROUND(
        ((AVG(current_premium) - AVG(indicated_premium)) / 
        NULLIF(AVG(indicated_premium), 0) * 100)::NUMERIC,
        2
    ) as percent_adjustment_from_indicated
FROM gold.fact_insurance_performance
WHERE ypc IS NOT NULL AND ypc BETWEEN 0 AND 10
GROUP BY ypc
ORDER BY ypc ASC;

-- ============================================================================
-- QUESTION 7: GEOGRAPHIC MARKET PENETRATION & OPPORTUNITIES
-- "Which towns/counties have highest/lowest policy density? Where should we
--  focus marketing?"
-- Visualization: Geo Map + Table (County/Town ranking)
-- ============================================================================

-- 7.1: County-Level Market Penetration
-- Best for: Geographic map and county-based analysis
SELECT 
    county,
    COUNT(DISTINCT town) as num_towns,
    COUNT(*) as total_policies,
    COUNT(DISTINCT territory) as coverage_territories,
    ROUND(AVG(current_premium)::NUMERIC, 2) as avg_premium,
    ROUND(SUM(current_premium)::NUMERIC, 2) as county_total_revenue,
    ROUND(AVG(acs03_total_population)::NUMERIC, 0)::INT as est_county_population,
    ROUND(
        (COUNT(*) * 1000.0 / NULLIF(AVG(acs03_total_population), 0))::NUMERIC,
        2
    ) as policies_per_1000_population
FROM gold.fact_insurance_performance
WHERE county IS NOT NULL
GROUP BY county
ORDER BY total_policies DESC;

-- 7.2: Town-Level Market Penetration (Top 50)
-- Best for: Identifying high-opportunity towns
SELECT 
    town,
    county,
    COUNT(*) as total_policies,
    ROUND(AVG(current_premium)::NUMERIC, 2) as avg_premium,
    ROUND(SUM(current_premium)::NUMERIC, 2) as town_total_revenue,
    COUNT(DISTINCT gender) as demographic_diversity,
    COUNT(CASE WHEN ypc > 0 THEN 1 END) as policies_with_claims,
    ROUND(
        (COUNT(CASE WHEN ypc > 0 THEN 1 END)::NUMERIC / COUNT(*) * 100)::NUMERIC,
        2
    ) as claim_frequency_percent
FROM gold.fact_insurance_performance
WHERE town IS NOT NULL AND town != ''
GROUP BY town, county
ORDER BY total_policies DESC
LIMIT 50;

-- ============================================================================
-- QUESTION 8: PREMIUM ACCURACY & VARIANCE ANALYSIS
-- "How often do indicated premiums differ from selected/current premiums?
--  What's the discount/markup pattern?"
-- Visualization: Area Chart (3-tier) + Histogram + Stat Cards
-- ============================================================================

-- 8.1: Premium Variance Pattern (Indicated vs Selected vs Current)
-- Best for: Area chart and three-tier comparison
SELECT 
    COUNT(*) as total_records,
    ROUND(AVG(indicated_premium)::NUMERIC, 2) as avg_indicated_premium,
    ROUND(AVG(selected_premium)::NUMERIC, 2) as avg_selected_premium,
    ROUND(AVG(current_premium)::NUMERIC, 2) as avg_current_premium,
    ROUND(
        (AVG(selected_premium) / NULLIF(AVG(indicated_premium), 0))::NUMERIC,
        4
    ) as selection_factor,
    ROUND(
        (AVG(current_premium) / NULLIF(AVG(indicated_premium), 0))::NUMERIC,
        4
    ) as current_to_indicated_ratio
FROM gold.fact_insurance_performance
WHERE indicated_premium IS NOT NULL 
    AND selected_premium IS NOT NULL 
    AND current_premium IS NOT NULL;

-- 8.2: Premium Discount Distribution
-- Best for: Histogram showing how often company deviates from indicated
WITH variance_data AS (
    SELECT 
        ((current_premium - indicated_premium) / 
         NULLIF(indicated_premium, 0)) * 100 as percent_variance
    FROM gold.fact_insurance_performance
    WHERE indicated_premium IS NOT NULL AND indicated_premium > 0
),
variance_buckets AS (
    SELECT 
        CASE 
            WHEN percent_variance < -20 THEN 'Discount >20%'
            WHEN percent_variance < -10 THEN 'Discount 10-20%'
            WHEN percent_variance < 0 THEN 'Discount 0-10%'
            WHEN percent_variance < 10 THEN 'Markup 0-10%'
            WHEN percent_variance < 20 THEN 'Markup 10-20%'
            ELSE 'Markup >20%'
        END as variance_bucket,
        CASE 
            WHEN percent_variance < -20 THEN 1
            WHEN percent_variance < -10 THEN 2
            WHEN percent_variance < 0 THEN 3
            WHEN percent_variance < 10 THEN 4
            WHEN percent_variance < 20 THEN 5
            ELSE 6
        END as sort_order
    FROM variance_data
)
SELECT 
    variance_bucket,
    COUNT(*) as policy_count,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ())::NUMERIC, 2) as percentage
FROM variance_buckets
GROUP BY variance_bucket, sort_order
ORDER BY sort_order;

-- ============================================================================
-- QUESTION 9: EXPENSE RATIO & LOSS-ADJUSTED METRICS
-- "What percentage of premium goes to actual coverage vs. operational
--  expenses? What's our expense ratio by territory?"
-- Visualization: Donut Chart + Gauge + Table
-- ============================================================================

-- 9.1: Company-Wide Expense Breakdown
-- Best for: Pie/donut chart showing premium allocation
SELECT 
    ROUND(SUM(current_premium)::NUMERIC, 2) as total_premium,
    ROUND(SUM(underlying_total_premium)::NUMERIC, 2) as total_underlying_cost,
    ROUND(SUM(fixed_expenses)::NUMERIC, 2) as total_fixed_expenses,
    ROUND(
        (SUM(underlying_premium) - SUM(fixed_expenses))::NUMERIC,
        2
    ) as total_claims_cost,
    ROUND(
        ((SUM(underlying_premium) - SUM(fixed_expenses)) / 
        NULLIF(SUM(current_premium), 0) * 100)::NUMERIC,
        2
    ) as claims_percentage,
    ROUND(
        (SUM(fixed_expenses) / NULLIF(SUM(current_premium), 0) * 100)::NUMERIC,
        2
    ) as expense_percentage,
    ROUND(
        ((SUM(current_premium) - SUM(underlying_total_premium)) / 
        NULLIF(SUM(current_premium), 0) * 100)::NUMERIC,
        2
    ) as profit_margin_percent
FROM gold.fact_insurance_performance;

-- 9.2: Expense Ratio by Territory
-- Best for: Territory-level expense analysis
SELECT 
    territory,
    town,
    county,
    COUNT(*) as policy_count,
    ROUND(SUM(current_premium)::NUMERIC, 2) as total_premium,
    ROUND(SUM(fixed_expenses)::NUMERIC, 2) as total_expenses,
    ROUND(AVG(fixed_expenses)::NUMERIC, 2) as avg_expense_per_policy,
    ROUND(
        (SUM(fixed_expenses) / NULLIF(SUM(current_premium), 0) * 100)::NUMERIC,
        2
    ) as expense_ratio_percent
FROM gold.fact_insurance_performance
GROUP BY territory, town, county
ORDER BY expense_ratio_percent DESC
LIMIT 30;

-- 9.3: Premium Allocation Waterfall Data
-- Best for: Understanding cost structure progression
SELECT 
    'Current Premium' as metric,
    ROUND(AVG(current_premium)::NUMERIC, 2) as value,
    0 as cumulative_order
FROM gold.fact_insurance_performance
UNION ALL
SELECT 
    'Less: Fixed Expenses',
    ROUND(AVG(fixed_expenses)::NUMERIC, 2),
    1
FROM gold.fact_insurance_performance
UNION ALL
SELECT 
    'Less: Underlying Premium',
    ROUND(AVG(underlying_premium)::NUMERIC, 2),
    2
FROM gold.fact_insurance_performance
UNION ALL
SELECT 
    'Gross Profit',
    ROUND(AVG(current_premium - underlying_total_premium)::NUMERIC, 2),
    3
FROM gold.fact_insurance_performance
ORDER BY cumulative_order;

-- ============================================================================
-- QUESTION 10: DATA QUALITY & COMPLETENESS ASSESSMENT
-- "How complete is our data? Are there territories or demographics missing
--  census information?"
-- Visualization: Stat Cards Grid + Bar Chart (Column Health)
-- ============================================================================

-- 10.1: Overall Data Quality Scorecard
-- Best for: Stat panel cards showing key metrics
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT territory) as unique_territories,
    COUNT(CASE WHEN gender IS NOT NULL AND gender != '' THEN 1 END) as records_with_gender,
    COUNT(CASE WHEN birthdate IS NOT NULL AND birthdate != '' THEN 1 END) as records_with_birthdate,
    COUNT(CASE WHEN "acs03_total_population" IS NOT NULL THEN 1 END) as records_with_acs03,
    COUNT(CASE WHEN "acs05_total_population" IS NOT NULL THEN 1 END) as records_with_acs05,
    ROUND(
        (COUNT(CASE WHEN "acs03_total_population" IS NOT NULL THEN 1 END)::NUMERIC / 
        COUNT(*) * 100)::NUMERIC,
        2
    ) as census_coverage_percent,
    DATE(MAX(ingested_at)) as latest_data_date
FROM gold.fact_insurance_performance;

-- 10.2: Column-by-Column Data Completeness
-- Best for: Bar chart showing completeness by field
SELECT 
    'Territory' as column_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN territory IS NOT NULL THEN 1 END) as non_null_count,
    ROUND(
        COUNT(CASE WHEN territory IS NOT NULL THEN 1 END)::NUMERIC / 
        COUNT(*) * 100,
        2
    ) as completeness_percent
FROM gold.fact_insurance_performance
UNION ALL
SELECT 'Gender', COUNT(*), COUNT(CASE WHEN gender IS NOT NULL THEN 1 END),
    ROUND(COUNT(CASE WHEN gender IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*) * 100, 2)
FROM gold.fact_insurance_performance
UNION ALL
SELECT 'Birthdate', COUNT(*), COUNT(CASE WHEN birthdate IS NOT NULL AND birthdate != '' THEN 1 END),
    ROUND(COUNT(CASE WHEN birthdate IS NOT NULL AND birthdate != '' THEN 1 END)::NUMERIC / COUNT(*) * 100, 2)
FROM gold.fact_insurance_performance
UNION ALL
SELECT 'Current Premium', COUNT(*), COUNT(CASE WHEN "current_premium" IS NOT NULL THEN 1 END),
    ROUND(COUNT(CASE WHEN "current_premium" IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*) * 100, 2)
FROM gold.fact_insurance_performance
UNION ALL
SELECT 'ACS DP03 (Census)', COUNT(*), COUNT(CASE WHEN "acs03_total_population" IS NOT NULL THEN 1 END),
    ROUND(COUNT(CASE WHEN "acs03_total_population" IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*) * 100, 2)
FROM gold.fact_insurance_performance
UNION ALL
SELECT 'ACS DP05 (Census)', COUNT(*), COUNT(CASE WHEN "acs05_total_population" IS NOT NULL THEN 1 END),
    ROUND(COUNT(CASE WHEN "acs05_total_population" IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*) * 100, 2)
FROM gold.fact_insurance_performance
UNION ALL
SELECT 'YPC (Prior Claims)', COUNT(*), COUNT(CASE WHEN "ypc" IS NOT NULL THEN 1 END),
    ROUND(COUNT(CASE WHEN "ypc" IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*) * 100, 2)
FROM gold.fact_insurance_performance
UNION ALL
SELECT 'CGR Factor', COUNT(*), COUNT(CASE WHEN "cgr_factor" IS NOT NULL THEN 1 END),
    ROUND(COUNT(CASE WHEN "cgr_factor" IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*) * 100, 2)
FROM gold.fact_insurance_performance;

-- 10.3: Census Data Coverage by Territory
-- Best for: Identifying data gaps by geography
SELECT 
    territory,
    town,
    county,
    COUNT(*) as total_policies,
    COUNT(CASE WHEN acs03_total_population IS NOT NULL THEN 1 END) as with_acs03,
    COUNT(CASE WHEN acs05_total_population IS NOT NULL THEN 1 END) as with_acs05,
    COUNT(CASE WHEN acs03_total_population IS NULL THEN 1 END) as missing_census,
    ROUND(
        COUNT(CASE WHEN acs03_total_population IS NOT NULL THEN 1 END)::NUMERIC / 
        COUNT(*) * 100,
        2
    ) as census_coverage_percent
FROM gold.fact_insurance_performance
GROUP BY territory, town, county
HAVING COUNT(CASE WHEN acs03_total_population IS NULL THEN 1 END) > 0
ORDER BY missing_census DESC;

-- ============================================================================
-- END OF STAKEHOLDER QUERIES
-- ============================================================================
-- These queries are designed for Grafana dashboard integration.
-- Each query includes comments indicating optimal visualization types.
-- Copy-paste query results into corresponding Grafana panels.
-- ============================================================================

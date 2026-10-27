-- Sample payer / PBM questions answered from Gold.
-- ${catalog} is healthcare_dev locally in documentation; on Databricks it is
-- the Unity Catalog name.

-- PMPM by line of business and month
SELECT
    year_month,
    line_of_business,
    member_months,
    medical_paid,
    pharmacy_paid,
    total_paid,
    pmpm_medical,
    pmpm_pharmacy,
    pmpm_total
FROM ${catalog}.gold.fact_pmpm
ORDER BY year_month, line_of_business;

-- Denial rate and top reasons
SELECT
    year_month,
    claim_domain,
    adjudicated_claims,
    denied_claims,
    denial_rate
FROM ${catalog}.gold.fact_denial_summary
ORDER BY year_month, claim_domain;

SELECT
    denial_reason_code,
    SUM(denied_claims) AS denied_claims,
    SUM(denied_billed_amount) AS denied_billed_amount
FROM ${catalog}.gold.fact_denial_reason
GROUP BY denial_reason_code
ORDER BY denied_claims DESC;

-- Generic dispensing rate
SELECT
    year_month,
    generic_fills,
    total_fills,
    generic_dispensing_rate,
    paid_amount
FROM ${catalog}.gold.fact_generic_dispensing
ORDER BY year_month;

-- Top drugs by plan paid
SELECT spend_rank, ndc, drug_name, brand_generic_flag, paid_amount, fill_count
FROM ${catalog}.gold.fact_top_drugs
ORDER BY spend_rank;

-- Pharmacy reimbursement variance versus contracted MAC/WAC/AWP
SELECT
    pharmacy_name,
    network_tier,
    year_month,
    fill_count,
    total_reimbursement,
    contracted_amount,
    variance_amount,
    variance_pct
FROM ${catalog}.gold.fact_reimbursement_variance
ORDER BY ABS(variance_amount) DESC;

-- Prescribing patterns
SELECT
    prescriber_specialty,
    SUM(fill_count) AS fills,
    SUM(generic_fills) AS generic_fills,
    SUM(paid_amount) AS paid_amount
FROM ${catalog}.gold.fact_provider_prescribing
GROUP BY prescriber_specialty
ORDER BY paid_amount DESC;

-- High-utilizing members
SELECT
    member_id,
    line_of_business,
    SUM(medical_claims) AS medical_claims,
    SUM(pharmacy_claims) AS pharmacy_claims,
    SUM(er_visits) AS er_visits,
    SUM(total_paid) AS total_paid
FROM ${catalog}.gold.fact_member_utilization
GROUP BY member_id, line_of_business
ORDER BY total_paid DESC
LIMIT 25;

-- Reconciliation failures for the latest run
SELECT *
FROM ${catalog}.audit.reconciliation
WHERE status = 'FAIL'
ORDER BY check_name;

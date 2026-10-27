-- Gold views Power BI connects to.
-- Replace ${catalog} with healthcare_dev or healthcare_prod.
-- Local Spark creates the same projections as gold_<table> views over delta paths.

CREATE OR REPLACE VIEW ${catalog}.gold.v_fact_medical_claim AS
SELECT * FROM ${catalog}.gold.fact_medical_claim;

CREATE OR REPLACE VIEW ${catalog}.gold.v_fact_pharmacy_claim AS
SELECT * FROM ${catalog}.gold.fact_pharmacy_claim;

CREATE OR REPLACE VIEW ${catalog}.gold.v_fact_pmpm AS
SELECT * FROM ${catalog}.gold.fact_pmpm;

CREATE OR REPLACE VIEW ${catalog}.gold.v_fact_denial_summary AS
SELECT * FROM ${catalog}.gold.fact_denial_summary;

CREATE OR REPLACE VIEW ${catalog}.gold.v_fact_denial_reason AS
SELECT
    r.*,
    d.denial_reason_description
FROM ${catalog}.gold.fact_denial_reason r
LEFT JOIN ${catalog}.gold.dim_denial_reason d
    ON r.denial_reason_code = d.denial_reason_code;

CREATE OR REPLACE VIEW ${catalog}.gold.v_fact_generic_dispensing AS
SELECT * FROM ${catalog}.gold.fact_generic_dispensing;

CREATE OR REPLACE VIEW ${catalog}.gold.v_fact_top_drugs AS
SELECT * FROM ${catalog}.gold.fact_top_drugs;

CREATE OR REPLACE VIEW ${catalog}.gold.v_fact_reimbursement_variance AS
SELECT * FROM ${catalog}.gold.fact_reimbursement_variance;

CREATE OR REPLACE VIEW ${catalog}.gold.v_fact_provider_prescribing AS
SELECT * FROM ${catalog}.gold.fact_provider_prescribing;

CREATE OR REPLACE VIEW ${catalog}.gold.v_fact_member_utilization AS
SELECT * FROM ${catalog}.gold.fact_member_utilization;

CREATE OR REPLACE VIEW ${catalog}.gold.v_dim_provider_history AS
SELECT * FROM ${catalog}.gold.dim_provider_history;

CREATE OR REPLACE VIEW ${catalog}.gold.v_dim_pharmacy_history AS
SELECT * FROM ${catalog}.gold.dim_pharmacy_history;

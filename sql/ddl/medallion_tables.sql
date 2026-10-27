-- Local-equivalent DDL for the medallion tables.
-- On Databricks with Unity Catalog, replace delta.`...` paths with
-- catalog.schema.table names from sql/unity_catalog/01_catalog.sql.
-- The pipeline creates these tables on first write; this script documents
-- the grain and the columns Power BI and reconciliation depend on.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

-- Bronze tables are all-string plus ingest metadata. prior_auth_number is
-- added by the 2024-06-06 batch (mergeSchema / addNewColumns).
-- Silver current claims: one row per claim_id + claim_line_number.
-- Silver *_version tables: one row per claim_id + line + claim_version.
-- Silver provider, pharmacy, drug_pricing: SCD2 with effective_start,
-- effective_end, is_current.

-- Audit
-- audit.pipeline_run (run_id, status, start_ts, end_ts, row counts)
-- audit.reconciliation
-- audit.dq_metric
-- audit.processed_file
-- audit.schema_change
-- audit.alert
-- silver.quarantine

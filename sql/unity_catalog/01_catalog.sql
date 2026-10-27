-- Unity Catalog setup for the healthcare claims platform.
-- Run as a metastore admin, then grant the job service principal.
-- Storage credential and external location assume the access connector
-- created by infra/main.bicep (databricks-access-connector).

CREATE STORAGE CREDENTIAL IF NOT EXISTS hc_adls_credential
WITH (AZURE_MANAGED_IDENTITY = 'databricks-access-connector');

CREATE EXTERNAL LOCATION IF NOT EXISTS hc_lake_dev
URL 'abfss://claims@sthcclaimsdev.dfs.core.windows.net/lake'
WITH (STORAGE CREDENTIAL hc_adls_credential)
COMMENT 'Dev medallion root';

CREATE EXTERNAL LOCATION IF NOT EXISTS hc_landing_dev
URL 'abfss://claims@sthcclaimsdev.dfs.core.windows.net/landing'
WITH (STORAGE CREDENTIAL hc_adls_credential)
COMMENT 'Dev landing zone produced by ADF';

CREATE CATALOG IF NOT EXISTS healthcare_dev
MANAGED LOCATION 'abfss://claims@sthcclaimsdev.dfs.core.windows.net/lake/uc';

CREATE SCHEMA IF NOT EXISTS healthcare_dev.bronze;
CREATE SCHEMA IF NOT EXISTS healthcare_dev.silver;
CREATE SCHEMA IF NOT EXISTS healthcare_dev.gold;
CREATE SCHEMA IF NOT EXISTS healthcare_dev.audit;

GRANT USE CATALOG ON CATALOG healthcare_dev TO `hc-data-engineers`;
GRANT USE SCHEMA ON SCHEMA healthcare_dev.bronze TO `hc-data-engineers`;
GRANT USE SCHEMA ON SCHEMA healthcare_dev.silver TO `hc-data-engineers`;
GRANT USE SCHEMA ON SCHEMA healthcare_dev.gold TO `hc-data-engineers`;
GRANT USE SCHEMA ON SCHEMA healthcare_dev.audit TO `hc-data-engineers`;
GRANT SELECT, MODIFY ON SCHEMA healthcare_dev.bronze TO `hc-data-engineers`;
GRANT SELECT, MODIFY ON SCHEMA healthcare_dev.silver TO `hc-data-engineers`;
GRANT SELECT, MODIFY ON SCHEMA healthcare_dev.gold TO `hc-data-engineers`;
GRANT SELECT, MODIFY ON SCHEMA healthcare_dev.audit TO `hc-data-engineers`;

-- Analysts read gold only.
GRANT USE CATALOG ON CATALOG healthcare_dev TO `hc-analysts`;
GRANT USE SCHEMA ON SCHEMA healthcare_dev.gold TO `hc-analysts`;
GRANT SELECT ON SCHEMA healthcare_dev.gold TO `hc-analysts`;

-- Prod catalog. Repeat after the prod storage account exists.
CREATE EXTERNAL LOCATION IF NOT EXISTS hc_lake_prod
URL 'abfss://claims@sthcclaimsprod.dfs.core.windows.net/lake'
WITH (STORAGE CREDENTIAL hc_adls_credential);

CREATE CATALOG IF NOT EXISTS healthcare_prod
MANAGED LOCATION 'abfss://claims@sthcclaimsprod.dfs.core.windows.net/lake/uc';

CREATE SCHEMA IF NOT EXISTS healthcare_prod.bronze;
CREATE SCHEMA IF NOT EXISTS healthcare_prod.silver;
CREATE SCHEMA IF NOT EXISTS healthcare_prod.gold;
CREATE SCHEMA IF NOT EXISTS healthcare_prod.audit;

-- Key Vault-backed secret scopes are created outside SQL:
--   databricks secrets create-scope --scope hc-kv-dev --scope-backend-type AZURE_KEYVAULT \
--     --resource-id <key-vault-id> --dns-name https://kv-hc-claims-dev.vault.azure.net/
-- Notebooks call dbutils.secrets.get("hc-kv-dev", "databricks-token").

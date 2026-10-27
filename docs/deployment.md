# Azure deployment

Nothing in this guide was executed against a live subscription. The JSON, SQL, bundle, and Bicep are the artifacts a dev environment would apply. Validate them in a sandbox before production.

## 1. Infrastructure

```bash
az group create --name rg-hc-claims-dev --location eastus
az deployment group create \
  --resource-group rg-hc-claims-dev \
  --template-file infra/main.bicep \
  --parameters infra/parameters/dev.bicepparam
```

`infra/main.bicep` creates:

- ADLS Gen2 storage with hierarchical namespace and containers `claims` and `source-landing`
- Key Vault with RBAC
- Data Factory with a system-assigned identity
- Storage Blob Data Contributor for the factory and for the Databricks access connector
- a premium Databricks workspace

Prod uses `infra/parameters/prod.bicepparam`. Storage account names must be globally unique; change them before deploying.

## 2. Secrets

Create these Key Vault secrets:

- `adls-account-key` (or prefer the managed identity and drop the account key)
- `databricks-token`

Create a Databricks secret scope backed by that vault:

```bash
databricks secrets create-scope --scope hc-kv-dev \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id "$KEY_VAULT_ID" \
  --dns-name "https://kv-hc-claims-dev.vault.azure.net/"
```

Notebooks call `dbutils.secrets.get` when `HC_SECRET_MODE=databricks`. Locally, `secrets.mode=env` reads process environment variables and never calls `dbutils`.

## 3. Unity Catalog

Run `sql/unity_catalog/01_catalog.sql` as a metastore admin after the access connector can read the lake. Replace storage account names if you changed the Bicep parameters. Grant `hc-data-engineers` modify on bronze, silver, gold, and audit. Grant `hc-analysts` select on gold.

## 4. Databricks job

Install the Databricks CLI, set `DATABRICKS_HOST` and `DATABRICKS_TOKEN`, then:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

`databricks.yml` defines the medallion job (Auto Loader notebooks, then `02_silver_gold.py`) and a serverless Lakeflow pipeline pointing at `dlt/claims_dlt_pipeline.py`. The cluster environment sets `HC_STORAGE_MODE=adls` and `HC_CATALOG_MODE=unity`.

Upload this repo to `/Workspace/Shared/hc_platform` or install the wheel on the cluster so `import hc_platform` resolves. The job cluster in the bundle does not install the package automatically; add a workspace library or an init script in the sandbox.

## 5. Data Factory

Import the files under `adf/`:

- linked services `LS_KeyVault`, `LS_ADLS_Gen2`, `LS_Databricks`
- datasets `DS_Landing_Delimited`, `DS_Raw_Delimited`
- pipeline `PL_Ingest_Landing_To_Raw`
- trigger `TR_Daily_Batch` (left `Stopped`)

Parameters: `batchDate`, storage account, containers, Databricks job id, workspace URL, cluster id. The copy activity retries three times. The tumbling-window trigger passes `batchDate` from the window start.

Point the source container at whoever drops the daily files (the generator output is the stand-in). The sink path is `claims/batch_date=<date>/<entity>.csv`, which is what Auto Loader and the local runner both expect.

## 6. Power BI

There is no `.pbix` in this repo. Connect Power BI Desktop to the SQL warehouse and the views in `sql/analytics/gold_views.sql`. Measures are in `docs/powerbi.md`. The screenshots in the README are the local Streamlit dashboard on the same Gold tables.

## 7. GitHub environments

The CD workflow (`.github/workflows/cd.yml`) uses GitHub environments `dev` and `prod`. It skips cleanly when these secrets are empty:

- dev: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`, `AZURE_CLIENT_SECRET`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`
- prod: `AZURE_CLIENT_ID_PROD`, `AZURE_CLIENT_SECRET_PROD`, `DATABRICKS_HOST_PROD`, `DATABRICKS_TOKEN_PROD`

Push to `main` attempts dev only. Prod is `workflow_dispatch` with target `prod`.

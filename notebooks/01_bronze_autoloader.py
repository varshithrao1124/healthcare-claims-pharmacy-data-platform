# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Auto Loader
# MAGIC
# MAGIC Ingests the ADF landing zone with `cloudFiles`. Schema location, rescued
# MAGIC data, and `addNewColumns` match the local parser in
# MAGIC `hc_platform.ingestion.parser`. This notebook is the Azure path. The local
# MAGIC runner does not execute `cloudFiles`.

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("entity", "medical_claims")

environment = dbutils.widgets.get("environment")
entity = dbutils.widgets.get("entity")

# COMMAND ----------

import sys

sys.path.append("/Workspace/Shared/hc_platform/src")

from hc_platform.config import load_settings
from hc_platform.schemas import BRONZE_TABLES
from hc_platform.secrets import get_secret

settings = load_settings(environment)
# Touch the Key Vault-backed scope so a missing secret fails this task early.
# The local secret mode reads the environment and ignores the scope.
_ = get_secret(spark, settings, "storage-account-name", default="")

landing = f"{settings.landing_dir}/entity={entity}"
schema_location = f"{settings.schema_dir}/{entity}"
target = settings.table_id("bronze", BRONZE_TABLES[entity]) if settings.use_unity_catalog else settings.table_path("bronze", BRONZE_TABLES[entity])
checkpoint = f"{settings.checkpoint_dir}/autoloader/{entity}"

reader = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes", "false")
)

stream = reader.load(landing).selectExpr(
    "*",
    "regexp_extract(input_file_name(), 'batch_date=([0-9-]+)', 1) AS batch_date",
    "input_file_name() AS source_file",
    f"'{entity}' AS entity",
    "current_timestamp() AS ingest_ts",
)

if settings.use_unity_catalog:
    query = (
        stream.writeStream.format("delta")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(target)
    )
else:
    query = (
        stream.writeStream.format("delta")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .start(target)
    )

query.awaitTermination()

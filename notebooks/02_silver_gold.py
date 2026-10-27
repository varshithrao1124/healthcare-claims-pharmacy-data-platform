# Databricks notebook source
# MAGIC %md
# MAGIC # Silver and Gold
# MAGIC
# MAGIC Calls the same `run_pipeline` module exercised by local PySpark and CI.
# MAGIC Set `HC_STORAGE_MODE=adls` and `HC_CATALOG_MODE=unity` on the cluster
# MAGIC (or in the job environment) before running.

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("fail_after", "")

import sys

sys.path.append("/Workspace/Shared/hc_platform/src")

from hc_platform.config import load_settings
from hc_platform.pipeline.runner import run_pipeline

environment = dbutils.widgets.get("environment")
fail_after = dbutils.widgets.get("fail_after") or None
settings = load_settings(environment)
summary = run_pipeline(settings, spark=spark, fail_after=fail_after, stop_spark=False)
dbutils.notebook.exit(str(summary.get("status")))

"""Pipeline run metadata, DQ metrics, schema-change events, and alert hooks."""

from __future__ import annotations

import json
import urllib.request
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from hc_platform.config import Settings
from hc_platform.io import delta_exists, read_delta, write_delta

RUN_COLUMNS = [
    "run_id",
    "pipeline_name",
    "environment",
    "start_ts",
    "end_ts",
    "status",
    "batches_processed",
    "batches_skipped",
    "bronze_rows",
    "silver_rows",
    "quarantine_rows",
    "gold_rows",
    "reconciliation_status",
    "error_message",
]


def _string_df(spark: SparkSession, rows: list[dict[str, Any]], columns: list[str]) -> DataFrame:
    schema = StructType([StructField(column, StringType(), True) for column in columns])
    payload = [
        tuple(None if row.get(column) is None else str(row.get(column)) for column in columns)
        for row in rows
    ]
    return spark.createDataFrame(payload, schema)


def upsert_run(spark: SparkSession, settings: Settings, row: dict[str, Any]) -> None:
    current = _string_df(spark, [row], RUN_COLUMNS)
    if not delta_exists(spark, settings, "audit", "pipeline_run"):
        write_delta(
            current, settings, "audit", "pipeline_run", mode="overwrite", overwrite_schema=True
        )
        return
    existing = read_delta(spark, settings, "audit", "pipeline_run").filter(
        F.col("run_id") != row["run_id"]
    )
    write_delta(
        existing.unionByName(current, allowMissingColumns=True),
        settings,
        "audit",
        "pipeline_run",
        mode="overwrite",
        overwrite_schema=True,
    )


def append_rows(
    spark: SparkSession,
    settings: Settings,
    table: str,
    rows: list[dict[str, Any]],
    columns: list[str],
) -> None:
    if not rows:
        return
    frame = _string_df(spark, rows, columns)
    if delta_exists(spark, settings, "audit", table):
        write_delta(frame, settings, "audit", table, mode="append", merge_schema=True)
    else:
        write_delta(frame, settings, "audit", table, mode="overwrite", overwrite_schema=True)


def post_webhook(url: str | None, payload: dict[str, Any]) -> None:
    """Best-effort alert sink. Failures are logged by the caller and do not fail the run."""

    if not url:
        return
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        response.read()

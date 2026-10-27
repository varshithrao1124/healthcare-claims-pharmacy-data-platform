"""Idempotent claim and dimension merges.

Current-state claims keep one row per claim id and line. A higher
``claim_version`` replaces that row (adjustments and reversals). An older
version is a late or stale replay and does not roll the current row back.
An equal version with the same hash is a duplicate and is a no-op.

Every accepted version is also merged into a version history table so
reconciliation can balance bronze amounts back to silver.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from hc_platform.config import Settings
from hc_platform.io import delta_exists, delta_table, write_delta


def _dedupe_versions(claims: DataFrame) -> DataFrame:
    keys = ["claim_id", "claim_line_number", "claim_version"]
    return claims.dropDuplicates(keys)


def current_candidates(claims: DataFrame) -> DataFrame:
    """Keep the highest version of each claim line inside one batch."""

    versions = _dedupe_versions(claims)
    window = Window.partitionBy("claim_id", "claim_line_number").orderBy(
        F.col("claim_version").desc()
    )
    return (
        versions.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def merge_type1(
    spark: SparkSession,
    settings: Settings,
    incoming: DataFrame,
    table: str,
    keys: list[str],
) -> int:
    incoming = incoming.dropDuplicates(keys)
    if len(incoming.take(1)) == 0:
        return 0
    if not delta_exists(spark, settings, "silver", table):
        write_delta(incoming, settings, "silver", table, mode="overwrite", overwrite_schema=True)
        return incoming.count()
    condition = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    (
        delta_table(spark, settings, "silver", table)
        .alias("t")
        .merge(incoming.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return incoming.count()


def merge_claim_history(
    spark: SparkSession,
    settings: Settings,
    versions: DataFrame,
    table: str,
) -> int:
    versions = _dedupe_versions(versions)
    if len(versions.take(1)) == 0:
        return 0
    if not delta_exists(spark, settings, "silver", table):
        write_delta(versions, settings, "silver", table, mode="overwrite", overwrite_schema=True)
        return versions.count()
    condition = (
        "t.claim_id = s.claim_id AND t.claim_line_number = s.claim_line_number "
        "AND t.claim_version = s.claim_version"
    )
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    (
        delta_table(spark, settings, "silver", table)
        .alias("t")
        .merge(versions.alias("s"), condition)
        .whenNotMatchedInsertAll()
        .execute()
    )
    return versions.count()


def merge_claim_current(
    spark: SparkSession,
    settings: Settings,
    updates: DataFrame,
    table: str,
) -> int:
    updates = current_candidates(updates)
    if len(updates.take(1)) == 0:
        return 0
    if not delta_exists(spark, settings, "silver", table):
        write_delta(updates, settings, "silver", table, mode="overwrite", overwrite_schema=True)
        return updates.count()
    condition = (
        "s.claim_version > t.claim_version OR "
        "(s.claim_version = t.claim_version AND s.record_hash <> t.record_hash "
        "AND s.source_batch_date >= t.source_batch_date)"
    )
    assignments = {
        column: f"s.{column}"
        for column in updates.columns
        if column not in {"claim_id", "claim_line_number"}
    }
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    (
        delta_table(spark, settings, "silver", table)
        .alias("t")
        .merge(
            updates.alias("s"),
            "t.claim_id = s.claim_id AND t.claim_line_number = s.claim_line_number",
        )
        .whenMatchedUpdate(condition=condition, set=assignments)
        .whenNotMatchedInsertAll()
        .execute()
    )
    return updates.count()

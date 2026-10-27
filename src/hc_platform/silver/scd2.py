"""SCD Type 2 for providers, pharmacies, and drug pricing.

The first snapshot opens at the configured epoch so claims with service
dates before the first batch still resolve a dimension row. Later changes
close the current row on the day before the batch date and open a new
current row. A same-day correction replaces the open row instead of
creating an effective range that ends before it starts.

The write is a full overwrite of the dimension. Delta commits that
overwrite atomically. Replaying the current snapshot does not add
versions because the attribute hash matches. Replaying an older batch
whose effective date is before the open row does not close that row:
the later version already won, same as a stale claim version.
"""

from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from hc_platform.config import Settings
from hc_platform.io import delta_exists, read_delta, write_delta

END_OF_TIME = date(9999, 12, 31)

SCD2_SPEC: dict[str, dict[str, list[str] | str]] = {
    "provider": {
        "entity": "providers",
        "keys": ["provider_npi"],
        "tracked": [
            "provider_name",
            "specialty",
            "taxonomy_code",
            "network_status",
            "credential_status",
            "city",
            "state",
            "postal_code",
        ],
    },
    "pharmacy": {
        "entity": "pharmacies",
        "keys": ["pharmacy_npi"],
        "tracked": [
            "ncpdp_id",
            "pharmacy_name",
            "chain_name",
            "pharmacy_type",
            "network_tier",
            "city",
            "state",
            "postal_code",
        ],
    },
    "drug_pricing": {
        "entity": "drug_pricing",
        "keys": ["ndc", "price_type"],
        "tracked": ["unit_price", "contract_id"],
    },
}


def _as_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _with_hash(df: DataFrame, tracked: list[str]) -> DataFrame:
    if "attr_hash" in df.columns:
        return df
    parts = [F.coalesce(F.col(column).cast("string"), F.lit("")) for column in tracked]
    return df.withColumn("attr_hash", F.sha2(F.concat_ws("||", *parts), 256))


def build_scd2(
    current_full: DataFrame | None,
    incoming: DataFrame,
    keys: list[str],
    tracked: list[str],
    as_of: date | str,
    epoch: date | str,
) -> DataFrame:
    """Return the full dimension, including closed history."""

    as_of_date = _as_date(as_of)
    epoch_date = _as_date(epoch)
    business = list(keys) + list(tracked)
    incoming = incoming.dropDuplicates(keys)
    incoming = _with_hash(incoming, tracked)
    open_end = F.lit(END_OF_TIME).cast("date")

    def project_open(frame: DataFrame, start_column: F.Column) -> DataFrame:
        return frame.select(
            *[F.col(column) for column in business],
            start_column.alias("effective_start"),
            open_end.alias("effective_end"),
            F.lit(True).alias("is_current"),
            F.col("attr_hash"),
            F.col("source_batch_date").cast("date").alias("source_batch_date"),
        )

    if current_full is None or len(current_full.take(1)) == 0:
        return project_open(incoming, F.lit(epoch_date).cast("date"))

    history = current_full.filter(~F.col("is_current"))
    current = current_full.filter(F.col("is_current"))
    current_pref = current.select(
        *[F.col(column).alias(f"c_{column}") for column in current.columns]
    )
    incoming_pref = incoming.select(
        *[F.col(column).alias(f"i_{column}") for column in incoming.columns]
    )
    join_condition = [F.col(f"c_{key}") == F.col(f"i_{key}") for key in keys]
    joined = current_pref.join(incoming_pref, join_condition, "fullouter")
    both_present = F.col(f"c_{keys[0]}").isNotNull() & F.col(f"i_{keys[0]}").isNotNull()
    unchanged_join = joined.filter(both_present & (F.col("c_attr_hash") == F.col("i_attr_hash")))
    changed_join = joined.filter(both_present & (F.col("c_attr_hash") != F.col("i_attr_hash")))
    new_join = joined.filter(F.col(f"c_{keys[0]}").isNull() & F.col(f"i_{keys[0]}").isNotNull())
    retained_join = joined.filter(
        F.col(f"c_{keys[0]}").isNotNull() & F.col(f"i_{keys[0]}").isNull()
    )

    def from_current(frame: DataFrame, end_column: F.Column, is_current: bool) -> DataFrame:
        return frame.select(
            *[F.col(f"c_{column}").alias(column) for column in business],
            F.col("c_effective_start").alias("effective_start"),
            end_column.alias("effective_end"),
            F.lit(is_current).alias("is_current"),
            F.col("c_attr_hash").alias("attr_hash"),
            F.col("c_source_batch_date").alias("source_batch_date"),
        )

    def from_incoming(frame: DataFrame, start_column: F.Column) -> DataFrame:
        return frame.select(
            *[F.col(f"i_{column}").alias(column) for column in business],
            start_column.alias("effective_start"),
            open_end.alias("effective_end"),
            F.lit(True).alias("is_current"),
            F.col("i_attr_hash").alias("attr_hash"),
            F.col("i_source_batch_date").cast("date").alias("source_batch_date"),
        )

    as_of_column = F.lit(as_of_date).cast("date")
    unchanged = from_current(unchanged_join, F.col("c_effective_end"), True)
    retained = from_current(retained_join, F.col("c_effective_end"), True)
    opened_new = from_incoming(new_join, as_of_column)
    same_day = changed_join.filter(F.col("c_effective_start") == as_of_column)
    forward = changed_join.filter(F.col("c_effective_start") < as_of_column)
    # A snapshot from before the open row is a replay of history. Keep the
    # open row. Closing it would set effective_end before effective_start.
    stale = changed_join.filter(F.col("c_effective_start") > as_of_column)
    kept_stale = from_current(stale, F.col("c_effective_end"), True)
    replaced = from_incoming(same_day, F.col("c_effective_start"))
    closed = from_current(forward, F.date_sub(as_of_column, 1), False)
    opened = from_incoming(forward, as_of_column)
    result = history
    for part in (unchanged, retained, opened_new, replaced, kept_stale, closed, opened):
        result = result.unionByName(part)
    return result


def apply_scd2(
    spark: SparkSession,
    settings: Settings,
    incoming: DataFrame,
    table: str,
    as_of: date | str,
) -> dict[str, int]:
    spec = SCD2_SPEC[table]
    keys = list(spec["keys"])
    tracked = list(spec["tracked"])
    current = (
        read_delta(spark, settings, "silver", table)
        if delta_exists(spark, settings, "silver", table)
        else None
    )
    result = build_scd2(current, incoming, keys, tracked, as_of, settings.scd2_epoch)
    write_delta(result, settings, "silver", table, mode="overwrite", overwrite_schema=True)
    current_rows = result.filter(F.col("is_current")).count()
    closed_rows = result.filter(~F.col("is_current")).count()
    return {
        "current_rows": int(current_rows),
        "closed_rows": int(closed_rows),
        "rows": int(current_rows + closed_rows),
    }

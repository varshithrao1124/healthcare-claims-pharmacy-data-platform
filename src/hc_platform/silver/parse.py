"""Cast bronze strings into typed silver rows.

Hashes are computed from the raw strings, not from the cast values, so a
replay of the same file produces the same hash. ``prior_auth_number`` is
always projected. It is null until the schema-change batch supplies it.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

MONEY = DecimalType(18, 2)
PRICE = DecimalType(18, 4)

MEDICAL_HASH = [
    "claim_id",
    "claim_line_number",
    "claim_version",
    "adjustment_type",
    "original_claim_id",
    "member_id",
    "billing_provider_npi",
    "rendering_provider_npi",
    "service_from_date",
    "service_to_date",
    "claim_type",
    "place_of_service",
    "procedure_code",
    "diagnosis_code",
    "billed_amount",
    "allowed_amount",
    "paid_amount",
    "member_responsibility",
    "claim_status",
    "denial_reason_code",
    "received_date",
    "paid_date",
    "prior_auth_number",
]

PHARMACY_HASH = [
    "claim_id",
    "claim_line_number",
    "claim_version",
    "adjustment_type",
    "original_claim_id",
    "member_id",
    "pharmacy_npi",
    "prescriber_npi",
    "ndc",
    "rx_id",
    "fill_date",
    "days_supply",
    "quantity",
    "daw_code",
    "generic_indicator",
    "ingredient_cost",
    "dispensing_fee",
    "billed_amount",
    "paid_amount",
    "member_copay",
    "claim_status",
    "denial_reason_code",
    "received_date",
    "paid_date",
    "prior_auth_number",
]


def _raw(name: str) -> F.Column:
    return F.when(F.col(name).isNull() | (F.trim(F.col(name)) == ""), F.lit(None)).otherwise(
        F.trim(F.col(name))
    )


def _ensure(df: DataFrame, column: str) -> DataFrame:
    if column in df.columns:
        return df
    return df.withColumn(column, F.lit(None).cast("string"))


def _hash(df: DataFrame, columns: list[str], target: str) -> DataFrame:
    parts = [F.coalesce(F.col(column).cast("string"), F.lit("")) for column in columns]
    return df.withColumn(target, F.sha2(F.concat_ws("||", *parts), 256))


def _money(name: str) -> F.Column:
    return _raw(name).cast(MONEY).alias(name)


def _date(name: str) -> F.Column:
    return F.to_date(_raw(name)).alias(name)


def parse_members(df: DataFrame) -> DataFrame:
    return df.select(
        _raw("member_id").alias("member_id"),
        _raw("subscriber_id").alias("subscriber_id"),
        _raw("first_name").alias("first_name"),
        _raw("last_name").alias("last_name"),
        _date("date_of_birth"),
        _raw("gender").alias("gender"),
        _raw("plan_id").alias("plan_id"),
        _raw("line_of_business").alias("line_of_business"),
        _raw("group_id").alias("group_id"),
        _date("coverage_start"),
        _date("coverage_end"),
        _raw("state").alias("state"),
        _raw("postal_code").alias("postal_code"),
        _raw("pcp_npi").alias("pcp_npi"),
        F.to_date(F.col("batch_date")).alias("source_batch_date"),
    )


def parse_providers(df: DataFrame) -> DataFrame:
    tracked = [
        "provider_name",
        "specialty",
        "taxonomy_code",
        "network_status",
        "credential_status",
        "city",
        "state",
        "postal_code",
    ]
    hashed = _hash(df, tracked, "attr_hash")
    return hashed.select(
        _raw("provider_npi").alias("provider_npi"),
        *[_raw(column).alias(column) for column in tracked],
        F.col("attr_hash"),
        F.to_date(F.col("batch_date")).alias("source_batch_date"),
    )


def parse_pharmacies(df: DataFrame) -> DataFrame:
    tracked = [
        "pharmacy_name",
        "chain_name",
        "pharmacy_type",
        "network_tier",
        "city",
        "state",
        "postal_code",
    ]
    hashed = _hash(df, ["ncpdp_id", *tracked], "attr_hash")
    return hashed.select(
        _raw("pharmacy_npi").alias("pharmacy_npi"),
        _raw("ncpdp_id").alias("ncpdp_id"),
        *[_raw(column).alias(column) for column in tracked],
        F.col("attr_hash"),
        F.to_date(F.col("batch_date")).alias("source_batch_date"),
    )


def parse_drugs(df: DataFrame) -> DataFrame:
    return df.select(
        _raw("ndc").alias("ndc"),
        _raw("drug_name").alias("drug_name"),
        _raw("generic_name").alias("generic_name"),
        _raw("brand_generic_flag").alias("brand_generic_flag"),
        _raw("therapeutic_class").alias("therapeutic_class"),
        _raw("dosage_form").alias("dosage_form"),
        _raw("strength").alias("strength"),
        _raw("route").alias("route"),
        _raw("manufacturer").alias("manufacturer"),
        F.to_date(F.col("batch_date")).alias("source_batch_date"),
    )


def parse_drug_pricing(df: DataFrame) -> DataFrame:
    hashed = _hash(df, ["unit_price", "contract_id"], "attr_hash")
    return hashed.select(
        _raw("ndc").alias("ndc"),
        _raw("price_type").alias("price_type"),
        _raw("unit_price").cast(PRICE).alias("unit_price"),
        _raw("contract_id").alias("contract_id"),
        F.col("attr_hash"),
        F.to_date(F.col("batch_date")).alias("source_batch_date"),
    )


def parse_prescriptions(df: DataFrame) -> DataFrame:
    return df.select(
        _raw("rx_id").alias("rx_id"),
        _raw("member_id").alias("member_id"),
        _raw("prescriber_npi").alias("prescriber_npi"),
        _raw("ndc").alias("ndc"),
        _date("written_date"),
        _raw("quantity").cast(MONEY).alias("quantity"),
        _raw("refills_authorized").cast("int").alias("refills_authorized"),
        _raw("days_supply").cast("int").alias("days_supply"),
        _raw("sig").alias("sig"),
        F.to_date(F.col("batch_date")).alias("source_batch_date"),
    )


def parse_medical_claims(df: DataFrame) -> DataFrame:
    prepared = _ensure(df, "prior_auth_number")
    prepared = _hash(prepared, MEDICAL_HASH, "record_hash")
    return prepared.select(
        _raw("claim_id").alias("claim_id"),
        _raw("claim_line_number").cast("int").alias("claim_line_number"),
        _raw("claim_version").cast("int").alias("claim_version"),
        _raw("adjustment_type").alias("adjustment_type"),
        _raw("original_claim_id").alias("original_claim_id"),
        _raw("member_id").alias("member_id"),
        _raw("billing_provider_npi").alias("billing_provider_npi"),
        _raw("rendering_provider_npi").alias("rendering_provider_npi"),
        _date("service_from_date"),
        _date("service_to_date"),
        _raw("claim_type").alias("claim_type"),
        _raw("place_of_service").alias("place_of_service"),
        _raw("procedure_code").alias("procedure_code"),
        _raw("diagnosis_code").alias("diagnosis_code"),
        _money("billed_amount"),
        _money("allowed_amount"),
        _money("paid_amount"),
        _money("member_responsibility"),
        _raw("claim_status").alias("claim_status"),
        _raw("denial_reason_code").alias("denial_reason_code"),
        _date("received_date"),
        _date("paid_date"),
        _raw("prior_auth_number").alias("prior_auth_number"),
        F.col("record_hash"),
        F.col("ingest_record_id"),
        F.col("source_file"),
        F.to_date(F.col("batch_date")).alias("source_batch_date"),
        (F.col("adjustment_type") == F.lit("Reversal")).alias("is_reversed"),
        F.current_timestamp().alias("updated_at"),
    )


def parse_pharmacy_claims(df: DataFrame) -> DataFrame:
    prepared = _ensure(df, "prior_auth_number")
    prepared = _hash(prepared, PHARMACY_HASH, "record_hash")
    return prepared.select(
        _raw("claim_id").alias("claim_id"),
        _raw("claim_line_number").cast("int").alias("claim_line_number"),
        _raw("claim_version").cast("int").alias("claim_version"),
        _raw("adjustment_type").alias("adjustment_type"),
        _raw("original_claim_id").alias("original_claim_id"),
        _raw("member_id").alias("member_id"),
        _raw("pharmacy_npi").alias("pharmacy_npi"),
        _raw("prescriber_npi").alias("prescriber_npi"),
        _raw("ndc").alias("ndc"),
        _raw("rx_id").alias("rx_id"),
        _date("fill_date"),
        _raw("days_supply").cast("int").alias("days_supply"),
        _raw("quantity").cast(MONEY).alias("quantity"),
        _raw("daw_code").alias("daw_code"),
        _raw("generic_indicator").alias("generic_indicator"),
        _money("ingredient_cost"),
        _money("dispensing_fee"),
        _money("billed_amount"),
        _money("paid_amount"),
        _money("member_copay"),
        _raw("claim_status").alias("claim_status"),
        _raw("denial_reason_code").alias("denial_reason_code"),
        _date("received_date"),
        _date("paid_date"),
        _raw("prior_auth_number").alias("prior_auth_number"),
        F.col("record_hash"),
        F.col("ingest_record_id"),
        F.col("source_file"),
        F.to_date(F.col("batch_date")).alias("source_batch_date"),
        (F.col("adjustment_type") == F.lit("Reversal")).alias("is_reversed"),
        F.current_timestamp().alias("updated_at"),
    )


def parse_remittances(df: DataFrame) -> DataFrame:
    return df.select(
        _raw("remittance_id").alias("remittance_id"),
        _raw("claim_id").alias("claim_id"),
        _raw("claim_domain").alias("claim_domain"),
        _date("payment_date"),
        _money("paid_amount"),
        _money("adjustment_amount"),
        _raw("adjustment_reason_code").alias("adjustment_reason_code"),
        _raw("payment_method").alias("payment_method"),
        _raw("trace_number").alias("trace_number"),
        F.col("ingest_record_id"),
        F.col("source_file"),
        F.to_date(F.col("batch_date")).alias("source_batch_date"),
    )

"""DQ, SCD2, claim MERGE idempotency, and reconciliation arithmetic."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from hc_platform.audit.reconcile import judge
from hc_platform.identifiers import is_valid_npi
from hc_platform.io import read_delta
from hc_platform.silver.claims import merge_claim_current, merge_claim_history
from hc_platform.silver.dq import apply_dq
from hc_platform.silver.parse import parse_medical_claims
from hc_platform.silver.scd2 import build_scd2

NPI = "1234567893"
assert is_valid_npi(NPI)


def _strings(spark, rows: list[dict]):
    keys = list(rows[0].keys())
    schema = StructType([StructField(key, StringType(), True) for key in keys])
    payload = [
        tuple(None if row.get(key) is None else str(row.get(key)) for key in keys) for row in rows
    ]
    return spark.createDataFrame(payload, schema)


def _medical_row(**overrides):
    row = {
        "claim_id": "MC2024060300001",
        "claim_line_number": "1",
        "claim_version": "1",
        "adjustment_type": "Original",
        "original_claim_id": None,
        "member_id": "M00000001",
        "billing_provider_npi": NPI,
        "rendering_provider_npi": NPI,
        "service_from_date": "2024-05-28",
        "service_to_date": "2024-05-28",
        "claim_type": "Professional",
        "place_of_service": "11",
        "procedure_code": "99213",
        "diagnosis_code": "E119",
        "billed_amount": "180.00",
        "allowed_amount": "140.00",
        "paid_amount": "120.00",
        "member_responsibility": "20.00",
        "claim_status": "Paid",
        "denial_reason_code": None,
        "received_date": "2024-06-03",
        "paid_date": "2024-06-03",
        "batch_date": "2024-06-03",
        "source_file": "batch_date=2024-06-03/medical_claims.csv",
        "source_row_num": "1",
        "ingest_record_id": "abc",
        "_is_rescued": "false",
        "_rescued_data": None,
    }
    row.update(overrides)
    return row


def test_dq_quarantines_bad_npi_and_keeps_good_row(spark):
    rows = [
        _medical_row(),
        _medical_row(
            claim_id="MC2024060300002",
            billing_provider_npi="1234567890",
            paid_amount="-5.00",
            ingest_record_id="bad",
            source_row_num="2",
        ),
        _medical_row(
            claim_id=None,
            ingest_record_id="rescued",
            source_row_num="3",
            _is_rescued="true",
            _rescued_data="##CORRUPT##",
        ),
    ]
    frame = _strings(spark, rows)
    good, quarantine = apply_dq(
        frame, "medical_claims", run_id="r1", quarantined_at="2024-06-03T00:00:00Z"
    )
    assert good.count() == 1
    rule_ids = {row.rule_id for row in quarantine.collect()}
    assert "MED_BILL_NPI_CHECK" in rule_ids
    assert "MED_PAID" in rule_ids
    assert "RESCUED_RECORD" in rule_ids
    assert quarantine.filter(F.col("rule_id") == "RESCUED_RECORD").count() == 1


def test_scd2_opens_closes_and_is_idempotent(spark):
    first = spark.createDataFrame(
        [
            {
                "provider_npi": NPI,
                "provider_name": "Rivera",
                "specialty": "Family Medicine",
                "attr_hash": "a",
                "source_batch_date": "2024-06-03",
            }
        ]
    )
    opened = build_scd2(
        None,
        first,
        ["provider_npi"],
        ["provider_name", "specialty"],
        "2024-06-03",
        "2020-01-01",
    )
    current = opened.filter(F.col("is_current")).collect()
    assert len(current) == 1
    assert current[0].effective_start == date(2020, 1, 1)
    assert current[0].specialty == "Family Medicine"

    second = spark.createDataFrame(
        [
            {
                "provider_npi": NPI,
                "provider_name": "Rivera",
                "specialty": "Internal Medicine",
                "attr_hash": "b",
                "source_batch_date": "2024-06-04",
            }
        ]
    )
    changed = build_scd2(
        opened,
        second,
        ["provider_npi"],
        ["provider_name", "specialty"],
        "2024-06-04",
        "2020-01-01",
    )
    rows = changed.orderBy("effective_start").collect()
    assert len(rows) == 2
    assert rows[0].is_current is False
    assert rows[0].effective_end == date(2024, 6, 3)
    assert rows[1].is_current is True
    assert rows[1].specialty == "Internal Medicine"
    replay = build_scd2(
        changed,
        second,
        ["provider_npi"],
        ["provider_name", "specialty"],
        "2024-06-04",
        "2020-01-01",
    )
    assert replay.count() == 2
    assert replay.filter(F.col("is_current")).count() == 1
    stale = build_scd2(
        changed,
        first,
        ["provider_npi"],
        ["provider_name", "specialty"],
        "2024-06-03",
        "2020-01-01",
    )
    stale_rows = stale.orderBy("effective_start").collect()
    assert len(stale_rows) == 2
    assert stale_rows[0].is_current is False
    assert stale_rows[1].is_current is True
    assert stale_rows[1].specialty == "Internal Medicine"
    assert stale.filter(F.col("effective_start") > F.col("effective_end")).count() == 0


def test_claim_merge_is_idempotent_and_ignores_stale_versions(spark, local_settings):
    def frame(version, paid, batch, hash_value, adjustment="Original", status="Paid"):
        raw = _strings(
            spark,
            [
                _medical_row(
                    claim_version=str(version),
                    paid_amount=paid,
                    batch_date=batch,
                    adjustment_type=adjustment,
                    claim_status=status,
                    ingest_record_id=f"id-{version}-{batch}",
                )
            ],
        )
        parsed = parse_medical_claims(raw).withColumn("record_hash", F.lit(hash_value))
        return parsed

    first = frame(1, "120.00", "2024-06-03", "h1")
    assert merge_claim_current(spark, local_settings, first, "medical_claim") == 1
    assert merge_claim_history(spark, local_settings, first, "medical_claim_version") == 1
    merge_claim_current(spark, local_settings, first, "medical_claim")
    merge_claim_history(spark, local_settings, first, "medical_claim_version")
    current = read_delta(spark, local_settings, "silver", "medical_claim")
    assert current.count() == 1
    assert current.collect()[0].paid_amount == Decimal("120.00")
    assert read_delta(spark, local_settings, "silver", "medical_claim_version").count() == 1

    adjusted = frame(2, "138.00", "2024-06-05", "h2", adjustment="Adjustment", status="Adjusted")
    merge_claim_current(spark, local_settings, adjusted, "medical_claim")
    merge_claim_history(spark, local_settings, adjusted, "medical_claim_version")
    current = read_delta(spark, local_settings, "silver", "medical_claim")
    assert current.collect()[0].claim_version == 2
    assert current.collect()[0].paid_amount == Decimal("138.00")

    stale = frame(1, "120.00", "2024-06-06", "h1")
    merge_claim_current(spark, local_settings, stale, "medical_claim")
    merge_claim_history(spark, local_settings, stale, "medical_claim_version")
    current = read_delta(spark, local_settings, "silver", "medical_claim")
    assert current.collect()[0].claim_version == 2
    assert current.collect()[0].paid_amount == Decimal("138.00")
    assert read_delta(spark, local_settings, "silver", "medical_claim_version").count() == 2


def test_reconciliation_tolerance():
    assert judge(Decimal("10.00"), Decimal("10.00"), Decimal("0.01")) == "PASS"
    assert judge(Decimal("10.00"), Decimal("10.009"), Decimal("0.01")) == "PASS"
    assert judge(Decimal("10.00"), Decimal("10.02"), Decimal("0.01")) == "FAIL"
    assert judge(Decimal("3"), Decimal("3"), Decimal("0")) == "PASS"

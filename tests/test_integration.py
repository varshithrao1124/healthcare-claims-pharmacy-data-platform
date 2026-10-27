"""End-to-end local medallion run on the tiny synthetic dataset."""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import functions as F

from hc_platform.generator.generate import generate_dataset
from hc_platform.ingestion.tracker import load_checkpoint
from hc_platform.io import read_delta
from hc_platform.pipeline.errors import SimulatedFailure
from hc_platform.pipeline.runner import run_pipeline


def test_pipeline_reconciles_evolves_and_reruns_without_duplicates(local_settings, tmp_path):
    generate_dataset(local_settings.landing_dir, scale="tiny", seed=42)

    try:
        run_pipeline(local_settings, fail_after="silver", stop_spark=False)
        raise AssertionError("simulated silver failure was not raised")
    except SimulatedFailure:
        pass

    failed_runs = read_delta(
        local_settings_spark_safe(local_settings), local_settings, "audit", "pipeline_run"
    )
    assert failed_runs.filter(F.col("status") == "FAILED").count() >= 1
    checkpoint = load_checkpoint(local_settings.checkpoint_dir, "local")
    assert checkpoint["completed_batches"] == [
        "2024-06-03",
        "2024-06-04",
        "2024-06-05",
        "2024-06-06",
    ]
    claims_after_failure = read_delta(
        local_settings_spark_safe(local_settings), local_settings, "silver", "medical_claim"
    ).count()

    summary = run_pipeline(local_settings, stop_spark=False)
    assert summary["status"] == "SUCCESS"
    assert summary["reconciliation_status"] == "PASS"
    assert summary["reconciliation_failed"] == 0
    assert summary["batches_processed"] == []
    assert summary["batches_skipped"] == ["2024-06-03", "2024-06-04", "2024-06-05", "2024-06-06"]
    assert Decimal(str(summary["total_paid"])) > 0

    spark = local_settings_spark_safe(local_settings)
    medical = read_delta(spark, local_settings, "silver", "medical_claim")
    pharmacy = read_delta(spark, local_settings, "silver", "pharmacy_claim")
    assert medical.count() == claims_after_failure
    assert medical.select("claim_id", "claim_line_number").distinct().count() == medical.count()
    assert pharmacy.select("claim_id", "claim_line_number").distinct().count() == pharmacy.count()

    bronze_medical = read_delta(spark, local_settings, "bronze", "medical_claim")
    assert "prior_auth_number" in bronze_medical.columns
    evolved = bronze_medical.filter(F.col("batch_date") == "2024-06-06")
    assert evolved.filter(F.col("prior_auth_number").isNotNull()).count() > 0
    assert (
        bronze_medical.filter(F.col("batch_date") == "2024-06-03")
        .filter(F.col("prior_auth_number").isNotNull())
        .count()
        == 0
    )
    assert medical.filter(F.col("prior_auth_number").isNotNull()).count() > 0

    providers = read_delta(spark, local_settings, "silver", "provider")
    multi = (
        providers.groupBy("provider_npi").count().filter(F.col("count") > 1).select("provider_npi")
    )
    assert multi.count() >= 1
    history = providers.join(multi, "provider_npi").orderBy("provider_npi", "effective_start")
    current_flags = history.filter(F.col("is_current")).groupBy("provider_npi").count()
    assert current_flags.filter(F.col("count") != 1).count() == 0

    quarantine = read_delta(spark, local_settings, "silver", "quarantine")
    assert quarantine.select("ingest_record_id").distinct().count() > 0
    assert quarantine.filter(F.col("rule_id") == "RESCUED_RECORD").count() > 0

    checks = read_delta(spark, local_settings, "audit", "reconciliation")
    assert checks.filter(F.col("status") == "FAIL").count() == 0
    runs = read_delta(spark, local_settings, "audit", "pipeline_run")
    assert runs.filter(F.col("status") == "SUCCESS").count() == 1

    replay = run_pipeline(local_settings, force=True, stop_spark=False)
    assert replay["status"] == "SUCCESS"
    assert replay["reconciliation_status"] == "PASS"
    medical_after = read_delta(spark, local_settings, "silver", "medical_claim")
    pharmacy_after = read_delta(spark, local_settings, "silver", "pharmacy_claim")
    assert medical_after.count() == medical.count()
    assert pharmacy_after.count() == pharmacy.count()
    assert (
        medical_after.select("claim_id", "claim_line_number").distinct().count()
        == medical_after.count()
    )


def local_settings_spark_safe(settings):
    from hc_platform.spark_session import build_spark

    return build_spark(settings)

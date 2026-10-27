"""Local and cluster entry point for the medallion pipeline.

Databricks notebooks call ``run_pipeline`` with an environment whose config
points at ADLS and Unity Catalog. Laptops and CI call it with the dev or
test config, which defaults to the local filesystem. Transformation code is
shared; only the IO paths change.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

from hc_platform.audit.monitoring import append_rows, post_webhook, upsert_run
from hc_platform.audit.reconcile import failed_checks, reconcile
from hc_platform.config import Settings
from hc_platform.gold.build import build_gold
from hc_platform.ingestion.parser import load_schema, parse_landing_file, save_schema
from hc_platform.ingestion.tracker import load_checkpoint, save_checkpoint
from hc_platform.io import delta_exists, delta_table, read_delta, write_delta
from hc_platform.logging_utils import configure_logging, log_event
from hc_platform.pipeline.errors import ReconciliationError, SimulatedFailure
from hc_platform.schemas import ALL_ENTITIES, BRONZE_TABLES
from hc_platform.silver.claims import merge_claim_current, merge_claim_history, merge_type1
from hc_platform.silver.dq import apply_dq, referential_rule, with_reference_flag
from hc_platform.silver.parse import (
    parse_drug_pricing,
    parse_drugs,
    parse_medical_claims,
    parse_members,
    parse_pharmacies,
    parse_pharmacy_claims,
    parse_prescriptions,
    parse_providers,
    parse_remittances,
)
from hc_platform.silver.scd2 import apply_scd2
from hc_platform.spark_session import build_spark

DQ_COLUMNS = [
    "run_id",
    "entity",
    "batch_date",
    "rule_id",
    "rows_in",
    "rows_pass",
    "rows_fail",
    "recorded_at",
]
FILE_COLUMNS = [
    "run_id",
    "source_file",
    "entity",
    "batch_date",
    "physical_rows",
    "ingested_at",
]
SCHEMA_COLUMNS = ["run_id", "entity", "batch_date", "new_columns", "created_at"]
ALERT_COLUMNS = ["alert_id", "run_id", "severity", "alert_code", "message", "created_at"]
CHECK_COLUMNS = [
    "run_id",
    "check_name",
    "entity",
    "batch_date",
    "metric_name",
    "source_value",
    "target_value",
    "difference",
    "tolerance",
    "status",
    "detail",
]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


def discover_batches(landing_dir: str) -> list[tuple[str, dict[str, Path]]]:
    root = Path(landing_dir)
    if not root.exists():
        raise FileNotFoundError(f"Landing directory does not exist: {root}")
    batches: list[tuple[str, dict[str, Path]]] = []
    for batch_dir in sorted(path for path in root.glob("batch_date=*") if path.is_dir()):
        batch_date = batch_dir.name.split("=", 1)[1]
        files: dict[str, Path] = {}
        for entity in ALL_ENTITIES:
            file_path = batch_dir / f"{entity}.csv"
            if not file_path.exists():
                raise FileNotFoundError(f"Missing {file_path}")
            files[entity] = file_path
        batches.append((batch_date, files))
    if not batches:
        raise FileNotFoundError(f"No batch_date partitions under {root}")
    return batches


def _bronze_df(
    spark: SparkSession,
    rows: list[dict[str, str | None]],
    source_columns: list[str],
    *,
    batch_date: str,
    entity: str,
    source_file: str,
    ingest_ts: str,
) -> DataFrame:
    columns = list(source_columns)
    for extra in (
        "ingest_record_id",
        "source_file",
        "batch_date",
        "entity",
        "ingest_ts",
        "_rescued_data",
        "_is_rescued",
    ):
        if extra not in columns:
            columns.append(extra)
    schema = StructType([StructField(column, StringType(), True) for column in columns])
    payload = []
    for row in rows:
        values: list[str | None] = []
        for column in columns:
            if column == "source_file":
                values.append(source_file)
            elif column == "batch_date":
                values.append(batch_date)
            elif column == "entity":
                values.append(entity)
            elif column == "ingest_ts":
                values.append(ingest_ts)
            else:
                value = row.get(column)
                values.append(None if value is None else str(value))
        payload.append(tuple(values))
    return spark.createDataFrame(payload, schema)


def _upsert_bronze(spark: SparkSession, settings: Settings, frame: DataFrame, table: str) -> None:
    if not delta_exists(spark, settings, "bronze", table):
        write_delta(frame, settings, "bronze", table, mode="overwrite", overwrite_schema=True)
        return
    existing = read_delta(spark, settings, "bronze", table)
    combined = existing.unionByName(frame, allowMissingColumns=True)
    window = Window.partitionBy("ingest_record_id").orderBy(F.col("ingest_ts").desc())
    deduped = (
        combined.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    write_delta(deduped, settings, "bronze", table, mode="overwrite", overwrite_schema=True)


def _write_quarantine(
    spark: SparkSession, settings: Settings, frame: DataFrame, batch_date: str, entity: str
) -> None:
    predicate = f"batch_date = '{batch_date}' AND entity = '{entity}'"
    has_rows = len(frame.take(1)) > 0
    if not delta_exists(spark, settings, "silver", "quarantine"):
        if has_rows:
            write_delta(
                frame, settings, "silver", "quarantine", mode="overwrite", overwrite_schema=True
            )
        return
    if not has_rows:
        delta_table(spark, settings, "silver", "quarantine").delete(predicate)
        return
    write_delta(
        frame,
        settings,
        "silver",
        "quarantine",
        mode="overwrite",
        replace_where=predicate,
        merge_schema=True,
    )


def _lookup(spark: SparkSession, settings: Settings, table: str) -> DataFrame | None:
    if not delta_exists(spark, settings, "silver", table):
        return None
    return read_delta(spark, settings, "silver", table)


def _emit_alert(
    spark: SparkSession,
    settings: Settings,
    logger,
    run_id: str,
    severity: str,
    code: str,
    message: str,
) -> None:
    created_at = _now()
    row = {
        "alert_id": uuid.uuid4().hex,
        "run_id": run_id,
        "severity": severity,
        "alert_code": code,
        "message": message,
        "created_at": created_at,
    }
    append_rows(spark, settings, "alert", [row], ALERT_COLUMNS)
    log_event(logger, message, alert_code=code, severity=severity, run_id=run_id)
    try:
        post_webhook(
            settings.alert_webhook_url,
            {"text": f"[{severity}] {code}: {message}", "run_id": run_id},
        )
    except Exception as exc:
        logger.warning("alert webhook failed: %s", exc)


def _finish(
    spark: SparkSession,
    settings: Settings,
    run_row: dict[str, str],
    *,
    status: str,
    error_message: str,
    reconciliation_status: str,
    bronze_rows: int,
    quarantine_rows: int,
    silver_rows: int,
    gold_rows: int,
) -> None:
    run_row.update(
        {
            "end_ts": _now(),
            "status": status,
            "bronze_rows": str(bronze_rows),
            "silver_rows": str(silver_rows),
            "quarantine_rows": str(quarantine_rows),
            "gold_rows": str(gold_rows),
            "reconciliation_status": reconciliation_status,
            "error_message": error_message[:2000],
        }
    )
    upsert_run(spark, settings, run_row)


def run_pipeline(
    settings: Settings,
    *,
    spark: SparkSession | None = None,
    fail_after: str | None = None,
    force: bool = False,
    stop_spark: bool = False,
) -> dict[str, object]:
    """Run bronze, silver, gold, and reconciliation. Return the run summary."""

    if settings.storage_mode == "local":
        Path(settings.lake_root).mkdir(parents=True, exist_ok=True)
        Path(settings.checkpoint_dir).mkdir(parents=True, exist_ok=True)
        Path(settings.schema_dir).mkdir(parents=True, exist_ok=True)
        Path(settings.log_dir).mkdir(parents=True, exist_ok=True)

    owns_spark = spark is None
    spark = spark or build_spark(settings)
    run_id = _run_id()
    logger = configure_logging(settings.log_dir, run_id)
    landing = Path(settings.landing_dir)
    batches = discover_batches(settings.landing_dir)
    checkpoint = load_checkpoint(settings.checkpoint_dir, settings.storage_mode)
    completed = [] if force else list(checkpoint["completed_batches"])
    processed: list[str] = []
    skipped: list[str] = []
    bronze_rows = 0
    quarantine_rows = 0
    schema_events: list[dict[str, str]] = []
    dq_rows: list[dict[str, str]] = []
    file_rows: list[dict[str, str]] = []
    run_row = {
        "run_id": run_id,
        "pipeline_name": settings.pipeline_name,
        "environment": settings.environment,
        "start_ts": _now(),
        "end_ts": "",
        "status": "RUNNING",
        "batches_processed": "",
        "batches_skipped": "",
        "bronze_rows": "0",
        "silver_rows": "0",
        "quarantine_rows": "0",
        "gold_rows": "0",
        "reconciliation_status": "",
        "error_message": "",
    }
    upsert_run(spark, settings, run_row)
    log_event(
        logger,
        "pipeline started",
        run_id=run_id,
        environment=settings.environment,
        storage_mode=settings.storage_mode,
        catalog_mode=settings.catalog_mode,
        landing=settings.landing_dir,
    )
    summary: dict[str, object] = {"run_id": run_id, "environment": settings.environment}
    try:
        for batch_date, files in batches:
            if batch_date in completed:
                skipped.append(batch_date)
                log_event(logger, "skipping completed batch", batch_date=batch_date, run_id=run_id)
                continue
            log_event(logger, "batch started", batch_date=batch_date, run_id=run_id)
            bronze_by_entity: dict[str, DataFrame] = {}
            for entity, file_path in files.items():
                known = load_schema(settings.schema_dir, entity, settings.storage_mode)
                relative = file_path.resolve().relative_to(landing.resolve()).as_posix()
                parsed = parse_landing_file(
                    spark,
                    str(file_path),
                    source_file=relative,
                    storage_mode=settings.storage_mode,
                    known_columns=known,
                )
                ingest_ts = _now()
                frame = _bronze_df(
                    spark,
                    parsed.rows,
                    parsed.header,
                    batch_date=batch_date,
                    entity=entity,
                    source_file=relative,
                    ingest_ts=ingest_ts,
                )
                _upsert_bronze(spark, settings, frame, BRONZE_TABLES[entity])
                save_schema(
                    settings.schema_dir, entity, parsed.header, settings.storage_mode, ingest_ts
                )
                bronze_rows += len(parsed.rows)
                file_rows.append(
                    {
                        "run_id": run_id,
                        "source_file": relative,
                        "entity": entity,
                        "batch_date": batch_date,
                        "physical_rows": str(parsed.physical_data_lines),
                        "ingested_at": ingest_ts,
                    }
                )
                if parsed.new_columns:
                    schema_events.append(
                        {
                            "run_id": run_id,
                            "entity": entity,
                            "batch_date": batch_date,
                            "new_columns": ",".join(parsed.new_columns),
                            "created_at": ingest_ts,
                        }
                    )
                    _emit_alert(
                        spark,
                        settings,
                        logger,
                        run_id,
                        "INFO",
                        "SCHEMA_EVOLVED",
                        f"{entity} gained columns {parsed.new_columns} on {batch_date}",
                    )
                bronze_by_entity[entity] = read_delta(
                    spark, settings, "bronze", BRONZE_TABLES[entity]
                ).filter((F.col("batch_date") == batch_date) & (F.col("source_file") == relative))
                log_event(
                    logger,
                    "bronze written",
                    entity=entity,
                    batch_date=batch_date,
                    rows=len(parsed.rows),
                    new_columns=parsed.new_columns,
                )

            _apply_dimensions(
                spark, settings, bronze_by_entity, batch_date, run_id, dq_rows, logger
            )
            _apply_claims(spark, settings, bronze_by_entity, batch_date, run_id, dq_rows, logger)
            completed.append(batch_date)
            processed.append(batch_date)
            save_checkpoint(settings.checkpoint_dir, settings.storage_mode, completed)
            log_event(logger, "batch committed", batch_date=batch_date, run_id=run_id)

        quarantine_rows = _quarantine_count(spark, settings)
        if fail_after == "silver":
            raise SimulatedFailure("simulated failure after silver and before gold")

        gold_counts = build_gold(spark, settings)
        if fail_after == "gold":
            raise SimulatedFailure("simulated failure after gold and before reconciliation")
        checks = reconcile(spark, settings)
        for check in checks:
            check["run_id"] = run_id
        append_rows(spark, settings, "reconciliation", checks, ["run_id", *CHECK_COLUMNS[1:]])
        failed = failed_checks(checks)
        append_rows(spark, settings, "dq_metric", dq_rows, DQ_COLUMNS)
        append_rows(spark, settings, "processed_file", file_rows, FILE_COLUMNS)
        append_rows(spark, settings, "schema_change", schema_events, SCHEMA_COLUMNS)
        silver_rows = _silver_claim_count(spark, settings)
        gold_rows = int(
            gold_counts.get("fact_medical_claim", 0) + gold_counts.get("fact_pharmacy_claim", 0)
        )
        run_row["batches_processed"] = ",".join(processed)
        run_row["batches_skipped"] = ",".join(skipped)
        if failed:
            message = "; ".join(
                f"{item['check_name']}/{item['entity']} source={item['source_value']} target={item['target_value']}"
                for item in failed[:8]
            )
            _emit_alert(
                spark, settings, logger, run_id, "CRITICAL", "RECONCILIATION_FAILED", message
            )
            _finish(
                spark,
                settings,
                run_row,
                status="FAILED",
                error_message=message,
                reconciliation_status="FAIL",
                bronze_rows=bronze_rows,
                quarantine_rows=quarantine_rows,
                silver_rows=silver_rows,
                gold_rows=gold_rows,
            )
            raise ReconciliationError(failed)

        pmpm_total = (
            read_delta(spark, settings, "gold", "fact_pmpm")
            .agg(F.sum("total_paid"))
            .collect()[0][0]
        )
        summary.update(
            {
                "status": "SUCCESS",
                "batches_processed": processed,
                "batches_skipped": skipped,
                "bronze_rows": bronze_rows,
                "silver_claim_rows": silver_rows,
                "quarantine_rows": quarantine_rows,
                "gold_counts": gold_counts,
                "reconciliation_status": "PASS",
                "reconciliation_checks": len(checks),
                "reconciliation_failed": 0,
                "total_paid": str(pmpm_total),
            }
        )
        _finish(
            spark,
            settings,
            run_row,
            status="SUCCESS",
            error_message="",
            reconciliation_status="PASS",
            bronze_rows=bronze_rows,
            quarantine_rows=quarantine_rows,
            silver_rows=silver_rows,
            gold_rows=gold_rows,
        )
        _write_summary(settings, summary)
        _print_summary(summary)
        log_event(
            logger, "pipeline succeeded", **{k: v for k, v in summary.items() if k != "gold_counts"}
        )
        return summary
    except Exception as exc:
        if not isinstance(exc, ReconciliationError):
            _emit_alert(
                spark,
                settings,
                logger,
                run_id,
                "CRITICAL",
                "PIPELINE_FAILED",
                str(exc),
            )
            run_row["batches_processed"] = ",".join(processed)
            run_row["batches_skipped"] = ",".join(skipped)
            try:
                append_rows(spark, settings, "dq_metric", dq_rows, DQ_COLUMNS)
                append_rows(spark, settings, "processed_file", file_rows, FILE_COLUMNS)
                append_rows(spark, settings, "schema_change", schema_events, SCHEMA_COLUMNS)
            except Exception:
                logger.exception("failed to flush partial audit rows")
            _finish(
                spark,
                settings,
                run_row,
                status="FAILED",
                error_message=str(exc),
                reconciliation_status="",
                bronze_rows=bronze_rows,
                quarantine_rows=quarantine_rows,
                silver_rows=0,
                gold_rows=0,
            )
        logger.exception("pipeline failed")
        raise
    finally:
        if owns_spark and stop_spark:
            spark.stop()


def _write_summary(settings: Settings, summary: dict[str, object]) -> None:
    path = Path(settings.log_dir) / "last_run_summary.json"
    path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")


def _print_summary(summary: dict[str, object]) -> None:
    print("")
    print("Healthcare claims medallion run complete")
    print(f"run_id={summary['run_id']} env={summary['environment']} status={summary['status']}")
    print(f"batches_processed={','.join(summary['batches_processed'])}")
    if summary["batches_skipped"]:
        print(f"batches_skipped={','.join(summary['batches_skipped'])}")
    print(
        f"bronze_rows={summary['bronze_rows']} silver_current_claims={summary['silver_claim_rows']} "
        f"quarantine_rows={summary['quarantine_rows']}"
    )
    print(
        f"reconciliation={summary['reconciliation_status']} "
        f"checks={summary['reconciliation_checks']} failed={summary['reconciliation_failed']}"
    )
    print(f"gold_total_paid={summary['total_paid']}")


def _quarantine_count(spark: SparkSession, settings: Settings) -> int:
    if not delta_exists(spark, settings, "silver", "quarantine"):
        return 0
    return (
        read_delta(spark, settings, "silver", "quarantine")
        .select("ingest_record_id")
        .distinct()
        .count()
    )


def _silver_claim_count(spark: SparkSession, settings: Settings) -> int:
    total = 0
    for table in ("medical_claim", "pharmacy_claim"):
        if delta_exists(spark, settings, "silver", table):
            total += read_delta(spark, settings, "silver", table).count()
    return total


def _record_dq(
    dq_rows: list[dict[str, str]],
    *,
    run_id: str,
    entity: str,
    batch_date: str,
    rows_in: int,
    quarantine: DataFrame,
    settings: Settings,
    spark: SparkSession,
    logger,
) -> None:
    distinct_failed = (
        quarantine.select("ingest_record_id").distinct().count() if len(quarantine.take(1)) else 0
    )
    rows_pass = rows_in - distinct_failed
    recorded_at = _now()
    dq_rows.append(
        {
            "run_id": run_id,
            "entity": entity,
            "batch_date": batch_date,
            "rule_id": "ALL",
            "rows_in": str(rows_in),
            "rows_pass": str(rows_pass),
            "rows_fail": str(distinct_failed),
            "recorded_at": recorded_at,
        }
    )
    if distinct_failed:
        for row in quarantine.groupBy("rule_id").count().collect():
            dq_rows.append(
                {
                    "run_id": run_id,
                    "entity": entity,
                    "batch_date": batch_date,
                    "rule_id": row["rule_id"],
                    "rows_in": str(rows_in),
                    "rows_pass": str(rows_pass),
                    "rows_fail": str(row["count"]),
                    "recorded_at": recorded_at,
                }
            )
    if rows_in and (distinct_failed / rows_in) > settings.dq_fail_rate_alert_threshold:
        _emit_alert(
            spark,
            settings,
            logger,
            run_id,
            "WARNING",
            "DQ_FAIL_RATE",
            f"{entity} {batch_date} quarantined {distinct_failed} of {rows_in} rows",
        )
    log_event(
        logger,
        "dq complete",
        entity=entity,
        batch_date=batch_date,
        rows_in=rows_in,
        rows_pass=rows_pass,
        rows_fail=distinct_failed,
    )


def _apply_dimensions(
    spark, settings, bronze_by_entity, batch_date, run_id, dq_rows, logger
) -> None:
    plan = [
        ("members", "member", parse_members, ["member_id"], None),
        ("providers", "provider", parse_providers, None, "provider"),
        ("pharmacies", "pharmacy", parse_pharmacies, None, "pharmacy"),
        ("drugs", "drug", parse_drugs, ["ndc"], None),
        ("drug_pricing", "drug_pricing", parse_drug_pricing, None, "drug_pricing"),
        ("prescriptions", "prescription", parse_prescriptions, ["rx_id"], None),
    ]
    for entity, table, parser, type1_keys, scd2_table in plan:
        frame = bronze_by_entity[entity]
        rows_in = frame.count()
        referential = _dimension_referential(spark, settings, entity, frame)
        working = referential if referential is not None else frame
        rules = _dimension_rules(entity)
        good, quarantine = apply_dq(
            working,
            entity,
            run_id=run_id,
            quarantined_at=_now(),
            referential=rules,
        )
        _write_quarantine(spark, settings, quarantine, batch_date, entity)
        _record_dq(
            dq_rows,
            run_id=run_id,
            entity=entity,
            batch_date=batch_date,
            rows_in=rows_in,
            quarantine=quarantine,
            settings=settings,
            spark=spark,
            logger=logger,
        )
        if len(good.take(1)) == 0:
            continue
        typed = parser(good)
        if scd2_table:
            stats = apply_scd2(spark, settings, typed, scd2_table, batch_date)
            log_event(logger, "scd2 applied", entity=entity, batch_date=batch_date, **stats)
        else:
            written = merge_type1(spark, settings, typed, table, type1_keys or [])
            log_event(logger, "type1 merged", entity=entity, batch_date=batch_date, rows=written)


def _dimension_referential(spark, settings, entity: str, frame: DataFrame) -> DataFrame | None:
    if entity != "prescriptions":
        return None
    members = _lookup(spark, settings, "member")
    providers = _lookup(spark, settings, "provider")
    drugs = _lookup(spark, settings, "drug")
    if members is None or providers is None or drugs is None:
        return frame
    flagged = with_reference_flag(frame, members, "member_id", "member_id", "_ref_member")
    flagged = with_reference_flag(
        flagged, providers, "prescriber_npi", "provider_npi", "_ref_prescriber"
    )
    flagged = with_reference_flag(flagged, drugs, "ndc", "ndc", "_ref_ndc")
    return flagged


def _dimension_rules(entity: str):
    if entity != "prescriptions":
        return []
    return [
        referential_rule(
            "_ref_member", "RX_REF_MEMBER", "member_id", "member_id is not on silver member"
        ),
        referential_rule(
            "_ref_prescriber",
            "RX_REF_PRESCRIBER",
            "prescriber_npi",
            "prescriber NPI is not on silver provider",
        ),
        referential_rule("_ref_ndc", "RX_REF_NDC", "ndc", "NDC is not on silver drug"),
    ]


def _apply_claims(spark, settings, bronze_by_entity, batch_date, run_id, dq_rows, logger) -> None:
    members = _lookup(spark, settings, "member")
    providers = _lookup(spark, settings, "provider")
    pharmacies = _lookup(spark, settings, "pharmacy")
    drugs = _lookup(spark, settings, "drug")
    prescriptions = _lookup(spark, settings, "prescription")

    medical = bronze_by_entity["medical_claims"]
    if members is not None and providers is not None:
        medical = with_reference_flag(medical, members, "member_id", "member_id", "_ref_member")
        medical = with_reference_flag(
            medical, providers, "billing_provider_npi", "provider_npi", "_ref_provider"
        )
    med_rules = [
        referential_rule(
            "_ref_member", "MED_REF_MEMBER", "member_id", "member_id is not on silver member"
        ),
        referential_rule(
            "_ref_provider",
            "MED_REF_PROVIDER",
            "billing_provider_npi",
            "billing NPI is not on silver provider",
        ),
    ]
    _merge_claim_entity(
        spark,
        settings,
        medical,
        entity="medical_claims",
        batch_date=batch_date,
        run_id=run_id,
        parser=parse_medical_claims,
        current_table="medical_claim",
        history_table="medical_claim_version",
        referential=med_rules,
        dq_rows=dq_rows,
        logger=logger,
    )

    pharmacy = bronze_by_entity["pharmacy_claims"]
    if None not in (members, providers, pharmacies, drugs, prescriptions):
        pharmacy = with_reference_flag(pharmacy, members, "member_id", "member_id", "_ref_member")
        pharmacy = with_reference_flag(
            pharmacy, pharmacies, "pharmacy_npi", "pharmacy_npi", "_ref_pharmacy"
        )
        pharmacy = with_reference_flag(
            pharmacy, providers, "prescriber_npi", "provider_npi", "_ref_prescriber"
        )
        pharmacy = with_reference_flag(pharmacy, drugs, "ndc", "ndc", "_ref_ndc")
        pharmacy = with_reference_flag(pharmacy, prescriptions, "rx_id", "rx_id", "_ref_rx")
    pharm_rules = [
        referential_rule(
            "_ref_member", "PHARM_REF_MEMBER", "member_id", "member_id is not on silver member"
        ),
        referential_rule(
            "_ref_pharmacy",
            "PHARM_REF_PHARMACY",
            "pharmacy_npi",
            "pharmacy NPI is not on silver pharmacy",
        ),
        referential_rule(
            "_ref_prescriber",
            "PHARM_REF_PRESCRIBER",
            "prescriber_npi",
            "prescriber NPI is not on silver provider",
        ),
        referential_rule("_ref_ndc", "PHARM_REF_NDC", "ndc", "NDC is not on silver drug"),
        referential_rule("_ref_rx", "PHARM_REF_RX", "rx_id", "rx_id is not on silver prescription"),
    ]
    _merge_claim_entity(
        spark,
        settings,
        pharmacy,
        entity="pharmacy_claims",
        batch_date=batch_date,
        run_id=run_id,
        parser=parse_pharmacy_claims,
        current_table="pharmacy_claim",
        history_table="pharmacy_claim_version",
        referential=pharm_rules,
        dq_rows=dq_rows,
        logger=logger,
    )

    remittances = bronze_by_entity["remittances"]
    medical_claims = _lookup(spark, settings, "medical_claim")
    pharmacy_claims = _lookup(spark, settings, "pharmacy_claim")
    if medical_claims is not None and pharmacy_claims is not None:
        known = (
            medical_claims.select("claim_id", F.lit("medical").alias("claim_domain"))
            .unionByName(
                pharmacy_claims.select("claim_id", F.lit("pharmacy").alias("claim_domain"))
            )
            .distinct()
            .withColumn("_ref_claim", F.lit(True))
        )
        remittances = remittances.join(known, ["claim_id", "claim_domain"], "left")
    rows_in = bronze_by_entity["remittances"].count()
    good, quarantine = apply_dq(
        remittances,
        "remittances",
        run_id=run_id,
        quarantined_at=_now(),
        referential=[
            referential_rule(
                "_ref_claim",
                "RMT_REF_CLAIM",
                "claim_id",
                "claim_id is not on silver claims for that domain",
            )
        ],
    )
    _write_quarantine(spark, settings, quarantine, batch_date, "remittances")
    _record_dq(
        dq_rows,
        run_id=run_id,
        entity="remittances",
        batch_date=batch_date,
        rows_in=rows_in,
        quarantine=quarantine,
        settings=settings,
        spark=spark,
        logger=logger,
    )
    if len(good.take(1)) > 0:
        merge_type1(spark, settings, parse_remittances(good), "remittance", ["remittance_id"])


def _merge_claim_entity(
    spark,
    settings,
    frame: DataFrame,
    *,
    entity: str,
    batch_date: str,
    run_id: str,
    parser,
    current_table: str,
    history_table: str,
    referential,
    dq_rows,
    logger,
) -> None:
    rows_in = frame.count()
    good, quarantine = apply_dq(
        frame,
        entity,
        run_id=run_id,
        quarantined_at=_now(),
        referential=referential,
    )
    _write_quarantine(spark, settings, quarantine, batch_date, entity)
    _record_dq(
        dq_rows,
        run_id=run_id,
        entity=entity,
        batch_date=batch_date,
        rows_in=rows_in,
        quarantine=quarantine,
        settings=settings,
        spark=spark,
        logger=logger,
    )
    if len(good.take(1)) == 0:
        return
    typed = parser(good)
    merge_claim_history(spark, settings, typed, history_table)
    merge_claim_current(spark, settings, typed, current_table)

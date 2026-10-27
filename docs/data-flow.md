# Data flow

## Batch calendar

The generator always emits four batches, seed 42:

| Batch | What it demonstrates |
| --- | --- |
| 2024-06-03 | Baseline snapshot. SCD2 rows open at 2020-01-01 so May service dates resolve. |
| 2024-06-04 | Provider specialty change, network status change, pharmacy tier change, MAC price change. |
| 2024-06-05 | Late-arriving claims, duplicate pharmacy claims, adjustments, bad rows, `##CORRUPT##` lines. |
| 2024-06-06 | `prior_auth_number` column, a reversal, and a stale version-1 replay that must not win. |

## Incremental processing

`_checkpoints/ingestion.json` lists committed batch dates. The checkpoint is written after silver for that batch commits. A crash before the checkpoint causes the batch to run again. Bronze dedupes on `ingest_record_id`. Claim MERGE and SCD2 hashes make the replay idempotent.

`hc-platform run --force` ignores the checkpoint and replays every batch. Bronze dedupes on `ingest_record_id`. Claim MERGE ignores a version older than the current row. SCD2 ignores a snapshot whose batch date is earlier than the open row's `effective_start`, so a historical replay does not close a later version with an inverted date range. The integration test replays with `--force` and asserts the claim grain is unchanged.

## Data quality

Rules live in `hc_platform.silver.dq`. They cover nulls, formats, ranges, NPI check digits, NDC shape, member id shape, and denial-code presence. Referential checks run after the dimensions for that batch are loaded: member, billing NPI, pharmacy NPI, prescriber NPI, NDC, and remittance claim id.

Quarantine is replaced for `(batch_date, entity)` so a rerun does not stack violations. DQ metrics are appended to `audit.dq_metric`. If the fail rate exceeds `dq_fail_rate_alert_threshold` the run emits `DQ_FAIL_RATE` and continues. Reconciliation, not the threshold, fails the run.

## Reconciliation

`audit.reconciliation` compares:

- landing physical lines to bronze rows per file
- bronze billed amounts to accepted plus quarantined amounts
- distinct accepted versions to history rows and amounts
- current claim grain uniqueness
- gold fact rows and paid amounts to silver current
- SCD2: one current row per key, no overlapping ranges, current flag matches the open end date, current key count matches the latest accepted snapshot
- type-1 member, prescription, and remittance key counts and remittance paid totals

Amount tolerance is $0.01. Row tolerance is zero. A miss raises `ReconciliationError` after the audit rows are written.

## Monitoring

Each run writes `audit.pipeline_run` with status `RUNNING`, then `SUCCESS` or `FAILED`, plus start, end, and row counts. Structured JSONL logs go to `data/logs/pipeline_<run_id>.jsonl`. Alerts use codes `SCHEMA_EVOLVED`, `DQ_FAIL_RATE`, `RECONCILIATION_FAILED`, and `PIPELINE_FAILED`.

Delta `ConcurrentAppend` / `ConcurrentWrite` exceptions are retried. `SimulatedFailure` and `ReconciliationError` are not.

## Schema evolution

The first time a file is seen, its header is stored and is not treated as a change. A later header with `prior_auth_number` appends the column, alerts `SCHEMA_EVOLVED`, and leaves earlier bronze rows null. Silver always projects `prior_auth_number`, so MERGE hashes stay stable before and after the column exists.

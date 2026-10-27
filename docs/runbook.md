# Runbook

## Local commands

```bash
make setup
make generate
make pipeline
make test
make dashboard
```

`make generate` rewrites `data/sample` at scale `small`, seed 42. `make pipeline` reads `conf/dev.yml`, which points at `data/sample` and `data/lake`.

Environment overrides:

```bash
HC_LANDING=data/sample HC_LOCAL_ROOT=data/lake HC_LOG_DIR=data/logs \
  .venv/bin/python -m hc_platform.cli run --env dev
```

Larger volume:

```bash
.venv/bin/python -m hc_platform.cli generate --scale medium --output /tmp/hc-medium --seed 42
```

`medium` is 5,000 members. It is not committed.

## Daily Azure operation

1. Confirm the source landing container has `batch_date=<yesterday>` and all nine files.
2. ADF trigger `TR_Daily_Batch` copies them and starts the Databricks job.
3. Check `audit.pipeline_run` for `SUCCESS`.
4. If `audit.reconciliation` has `FAIL` rows, the job is already failed. Do not publish Gold to analysts until the check is understood.
5. `audit.alert` with `DQ_FAIL_RATE` is a warning. Quarantine is expected on the 2024-06-05 batch because the generator plants bad rows. Investigate a jump above the usual fail rate.
6. `SCHEMA_EVOLVED` means a new column arrived. Confirm Silver still projects it (the code always projects `prior_auth_number`) and tell the Power BI author if a new attribute should be on a fact.

## Failure and recovery

| Symptom | What to do |
| --- | --- |
| Task dies after silver, before gold | Rerun the job. Completed batches are skipped. Gold rebuilds. |
| Task dies mid-batch, checkpoint not saved | Rerun. Bronze dedupes. Claim MERGE and SCD2 are idempotent. |
| Need to replay history | `hc-platform run --force`, or delete `_checkpoints/ingestion.json`. |
| Gold looks wrong, silver looks right | Delete the gold schema directory and rerun. |
| Reconciliation failed | Read `audit.reconciliation` where `status = FAIL`. Fix the rule or the source file. Do not raise the tolerance to hide a grain bug. |
| Duplicate current claims | Should not happen. If it does, the MERGE key was bypassed. Stop the job and inspect `silver.medical_claim` grain before the next gold build. |

A simulated failure is available for drills:

```bash
.venv/bin/python -m hc_platform.cli run --env dev --fail-after silver
.venv/bin/python -m hc_platform.cli run --env dev
```

The second command must not increase the current claim grain.

## Logs

- Console text and `data/logs/pipeline_<run_id>.jsonl`
- `data/logs/pipeline_latest.jsonl`
- `data/logs/last_run_summary.json`

Webhook alerts are best-effort. A timeout on `ALERT_WEBHOOK_URL` is logged and the pipeline status stays whatever the data checks decided.

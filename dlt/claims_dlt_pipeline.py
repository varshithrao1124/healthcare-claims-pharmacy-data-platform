"""Lakeflow Spark Declarative Pipelines / Delta Live Tables alternative.

This is a second Silver/Gold implementation for Databricks. It is not executed
by the local runner or by CI. Expectations below match the rule intent in
``hc_platform.silver.dq`` (format, range, and rescued-data quarantine).

The notebook path (``notebooks/02_silver_gold.py``) remains the implementation
that shares modules with the tested local pipeline. Use this pipeline when the
platform team wants declarative expectations and streaming tables instead of
the batch MERGE job.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Bronze streaming tables are produced by Auto Loader outside this file, or by
# a cloudFiles streaming table declared in the pipeline settings. The names
# below are Unity Catalog bronze tables.

CATALOG = spark.conf.get("hc.catalog", "healthcare_dev")  # noqa: F821


@dp.table(name="silver_medical_claim_clean", comment="Medical claim lines that pass expectations")
@dp.expect_or_drop("claim_id", "claim_id RLIKE '^MC[0-9]+$'")
@dp.expect_or_drop("member_id", "member_id RLIKE '^M[0-9]{8}$'")
@dp.expect_or_drop("billing_npi", "billing_provider_npi RLIKE '^[0-9]{10}$'")
@dp.expect_or_drop("billed_non_negative", "try_cast(billed_amount AS DECIMAL(18,2)) >= 0")
@dp.expect_or_drop("paid_non_negative", "try_cast(paid_amount AS DECIMAL(18,2)) >= 0")
@dp.expect_or_drop("not_rescued", "_rescued_data IS NULL")
def silver_medical_claim_clean():
    return spark.readStream.table(f"{CATALOG}.bronze.medical_claim")  # noqa: F821


@dp.table(name="silver_medical_quarantine", comment="Medical rows dropped by expectations")
def silver_medical_quarantine():
    source = spark.readStream.table(f"{CATALOG}.bronze.medical_claim")  # noqa: F821
    return source.filter(
        F.col("_rescued_data").isNotNull()
        | ~F.col("claim_id").rlike(r"^MC[0-9]+$")
        | ~F.col("member_id").rlike(r"^M[0-9]{8}$")
        | (F.try_cast(F.col("paid_amount"), "decimal(18,2)") < F.lit(0))
    )


@dp.materialized_view(name="gold_denial_rate_dlt", comment="Denial rate from the declarative silver table")
def gold_denial_rate_dlt():
    claims = spark.read.table("silver_medical_claim_clean")  # noqa: F821
    return claims.groupBy("batch_date").agg(
        F.countDistinct("claim_id").alias("claims"),
        F.countDistinct(F.when(F.col("claim_status") == "Denied", F.col("claim_id"))).alias("denied_claims"),
    )

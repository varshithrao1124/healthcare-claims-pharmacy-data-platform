"""Build the gold star schema from current silver tables.

Gold is a deterministic rebuild. Rebuilding from silver current state avoids
partial aggregate drift and makes a rerun after a failed gold step safe.
Claim facts stay at claim-line grain so paid amounts reconcile to silver
before they are rolled into PMPM and other marts.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from hc_platform.config import Settings
from hc_platform.io import read_delta, write_delta
from hc_platform.schemas import DENIAL_REASONS

MONEY = "decimal(18,2)"


def _publish(df: DataFrame, settings: Settings, table: str, counts: dict[str, int]) -> None:
    write_delta(df, settings, "gold", table, mode="overwrite", overwrite_schema=True)
    counts[table] = df.count()


def _year_month(column: str) -> F.Column:
    return F.date_format(F.col(column), "yyyy-MM")


def _register_views(spark: SparkSession, settings: Settings, tables: list[str]) -> None:
    if settings.use_unity_catalog or settings.storage_mode != "local":
        return
    for table in tables:
        path = settings.table_path("gold", table)
        spark.sql(f"CREATE OR REPLACE VIEW gold_{table} AS SELECT * FROM delta.`{path}`")


def build_gold(spark: SparkSession, settings: Settings) -> dict[str, int]:
    medical = read_delta(spark, settings, "silver", "medical_claim")
    pharmacy = read_delta(spark, settings, "silver", "pharmacy_claim")
    members = read_delta(spark, settings, "silver", "member")
    drugs = read_delta(spark, settings, "silver", "drug")
    pricing = read_delta(spark, settings, "silver", "drug_pricing")
    provider_hist = read_delta(spark, settings, "silver", "provider")
    pharmacy_hist = read_delta(spark, settings, "silver", "pharmacy")
    counts: dict[str, int] = {}

    member_dim = members.select(
        "member_id",
        "subscriber_id",
        "first_name",
        "last_name",
        "date_of_birth",
        "gender",
        "plan_id",
        "line_of_business",
        "group_id",
        "coverage_start",
        "coverage_end",
        "state",
        "postal_code",
        "pcp_npi",
    )
    _publish(member_dim, settings, "dim_member", counts)
    _publish(
        provider_hist.filter(F.col("is_current")).drop("attr_hash"),
        settings,
        "dim_provider",
        counts,
    )
    _publish(provider_hist, settings, "dim_provider_history", counts)
    _publish(
        pharmacy_hist.filter(F.col("is_current")).drop("attr_hash"),
        settings,
        "dim_pharmacy",
        counts,
    )
    _publish(pharmacy_hist, settings, "dim_pharmacy_history", counts)
    _publish(drugs, settings, "dim_drug", counts)
    _publish(
        pricing.filter(F.col("is_current")).select(
            "ndc", "price_type", "unit_price", "contract_id", "effective_start", "effective_end"
        ),
        settings,
        "dim_drug_pricing",
        counts,
    )

    dates = spark.sql(
        "SELECT explode(sequence(date'2024-01-01', date'2024-12-31', interval 1 day)) AS full_date"
    )
    dim_date = dates.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("full_date"),
        F.year("full_date").alias("calendar_year"),
        F.month("full_date").alias("calendar_month"),
        F.date_format("full_date", "yyyy-MM").alias("year_month"),
        F.quarter("full_date").alias("calendar_quarter"),
        F.date_format("full_date", "E").alias("day_name"),
    )
    _publish(dim_date, settings, "dim_date", counts)
    denial_dim = spark.createDataFrame(
        DENIAL_REASONS, ["denial_reason_code", "denial_reason_description"]
    )
    _publish(denial_dim, settings, "dim_denial_reason", counts)

    fact_medical = (
        medical.alias("c")
        .join(member_dim.alias("m"), "member_id", "left")
        .select(
            "c.claim_id",
            "c.claim_line_number",
            "c.claim_version",
            "c.adjustment_type",
            "c.member_id",
            "c.billing_provider_npi",
            "c.rendering_provider_npi",
            "c.service_from_date",
            "c.service_to_date",
            _year_month("c.service_from_date").alias("year_month"),
            "c.claim_type",
            "c.place_of_service",
            "c.procedure_code",
            "c.diagnosis_code",
            "c.billed_amount",
            "c.allowed_amount",
            "c.paid_amount",
            "c.member_responsibility",
            "c.claim_status",
            "c.denial_reason_code",
            "c.is_reversed",
            "c.prior_auth_number",
            "c.received_date",
            "c.paid_date",
            "m.line_of_business",
            "m.plan_id",
            "m.state",
        )
    )
    _publish(fact_medical, settings, "fact_medical_claim", counts)

    discount = F.lit(str(settings.brand_awp_discount)).cast("decimal(8,4)")
    awp = pricing.filter(F.col("price_type") == "AWP").select(
        F.col("ndc").alias("awp_ndc"),
        F.col("unit_price").alias("awp_unit_price"),
        F.col("effective_start").alias("awp_start"),
        F.col("effective_end").alias("awp_end"),
    )
    wac = pricing.filter(F.col("price_type") == "WAC").select(
        F.col("ndc").alias("wac_ndc"),
        F.col("unit_price").alias("wac_unit_price"),
        F.col("effective_start").alias("wac_start"),
        F.col("effective_end").alias("wac_end"),
    )
    mac = pricing.filter(F.col("price_type") == "MAC").select(
        F.col("ndc").alias("mac_ndc"),
        F.col("unit_price").alias("mac_unit_price"),
        F.col("effective_start").alias("mac_start"),
        F.col("effective_end").alias("mac_end"),
    )
    pharm_dim = pharmacy_hist.select(
        F.col("pharmacy_npi").alias("ph_npi"),
        F.col("pharmacy_name"),
        F.col("chain_name"),
        F.col("network_tier"),
        F.col("pharmacy_type"),
        F.col("effective_start").alias("ph_start"),
        F.col("effective_end").alias("ph_end"),
    )
    provider_dim = provider_hist.select(
        F.col("provider_npi").alias("pr_npi"),
        F.col("provider_name"),
        F.col("specialty"),
        F.col("effective_start").alias("pr_start"),
        F.col("effective_end").alias("pr_end"),
    )
    priced = (
        pharmacy.alias("p")
        .join(drugs.alias("d"), "ndc", "left")
        .join(member_dim.alias("m"), "member_id", "left")
        .join(
            awp,
            (F.col("p.ndc") == F.col("awp_ndc"))
            & (F.col("p.fill_date") >= F.col("awp_start"))
            & (F.col("p.fill_date") <= F.col("awp_end")),
            "left",
        )
        .join(
            wac,
            (F.col("p.ndc") == F.col("wac_ndc"))
            & (F.col("p.fill_date") >= F.col("wac_start"))
            & (F.col("p.fill_date") <= F.col("wac_end")),
            "left",
        )
        .join(
            mac,
            (F.col("p.ndc") == F.col("mac_ndc"))
            & (F.col("p.fill_date") >= F.col("mac_start"))
            & (F.col("p.fill_date") <= F.col("mac_end")),
            "left",
        )
        .join(
            pharm_dim,
            (F.col("p.pharmacy_npi") == F.col("ph_npi"))
            & (F.col("p.fill_date") >= F.col("ph_start"))
            & (F.col("p.fill_date") <= F.col("ph_end")),
            "left",
        )
        .join(
            provider_dim,
            (F.col("p.prescriber_npi") == F.col("pr_npi"))
            & (F.col("p.fill_date") >= F.col("pr_start"))
            & (F.col("p.fill_date") <= F.col("pr_end")),
            "left",
        )
    )
    contracted_unit = F.when(
        F.col("d.brand_generic_flag") == "G",
        F.coalesce(F.col("mac_unit_price"), F.col("wac_unit_price"), F.col("awp_unit_price")),
    ).otherwise(F.col("awp_unit_price") * (F.lit(1).cast("decimal(8,4)") - discount))
    fee_schedule = (
        F.when(F.col("network_tier") == "Preferred", F.lit(1.00).cast(MONEY))
        .when(F.col("network_tier") == "Standard", F.lit(0.50).cast(MONEY))
        .otherwise(F.lit(0).cast(MONEY))
    )
    fact_pharmacy = priced.select(
        "p.claim_id",
        "p.claim_line_number",
        "p.claim_version",
        "p.member_id",
        "p.pharmacy_npi",
        "p.prescriber_npi",
        "p.ndc",
        "p.rx_id",
        "p.fill_date",
        _year_month("p.fill_date").alias("year_month"),
        "p.days_supply",
        "p.quantity",
        "p.generic_indicator",
        "p.ingredient_cost",
        "p.dispensing_fee",
        "p.billed_amount",
        "p.paid_amount",
        "p.member_copay",
        "p.claim_status",
        "p.denial_reason_code",
        "p.is_reversed",
        "p.prior_auth_number",
        "d.drug_name",
        "d.therapeutic_class",
        "d.brand_generic_flag",
        "m.line_of_business",
        "m.plan_id",
        "m.state",
        F.col("pharmacy_name"),
        F.col("chain_name"),
        F.col("network_tier"),
        F.col("provider_name").alias("prescriber_name"),
        F.col("specialty").alias("prescriber_specialty"),
        F.col("awp_unit_price"),
        F.col("wac_unit_price"),
        F.col("mac_unit_price"),
        contracted_unit.cast(MONEY).alias("contracted_unit_price"),
        (contracted_unit * F.col("p.quantity") + fee_schedule)
        .cast(MONEY)
        .alias("contracted_amount"),
        (F.col("p.paid_amount") + F.col("p.member_copay")).cast(MONEY).alias("total_reimbursement"),
        (
            (F.col("p.paid_amount") + F.col("p.member_copay"))
            - (contracted_unit * F.col("p.quantity") + fee_schedule)
        )
        .cast(MONEY)
        .alias("variance_amount"),
    )
    _publish(fact_pharmacy, settings, "fact_pharmacy_claim", counts)

    med_month = fact_medical.select(
        "year_month",
        "line_of_business",
        "paid_amount",
        "claim_id",
    ).withColumn("domain", F.lit("medical"))
    ph_month = fact_pharmacy.select(
        "year_month",
        "line_of_business",
        "paid_amount",
        "claim_id",
    ).withColumn("domain", F.lit("pharmacy"))
    months = (
        med_month.select("year_month")
        .union(ph_month.select("year_month"))
        .where(F.col("year_month").isNotNull())
        .distinct()
        .withColumn("month_start", F.to_date(F.concat(F.col("year_month"), F.lit("-01"))))
    )
    eligible = member_dim.crossJoin(months).where(
        (F.col("coverage_start") <= F.last_day(F.col("month_start")))
        & (F.col("coverage_end") >= F.col("month_start"))
    )
    member_months = eligible.groupBy("year_month", "line_of_business").agg(
        F.countDistinct("member_id").alias("member_months")
    )
    med_paid = fact_medical.groupBy("year_month", "line_of_business").agg(
        F.sum("paid_amount").cast(MONEY).alias("medical_paid"),
        F.countDistinct("claim_id").alias("medical_claims"),
    )
    ph_paid = fact_pharmacy.groupBy("year_month", "line_of_business").agg(
        F.sum("paid_amount").cast(MONEY).alias("pharmacy_paid"),
        F.countDistinct("claim_id").alias("pharmacy_claims"),
    )
    pmpm = (
        member_months.join(med_paid, ["year_month", "line_of_business"], "left")
        .join(ph_paid, ["year_month", "line_of_business"], "left")
        .fillna(0, ["medical_paid", "pharmacy_paid", "medical_claims", "pharmacy_claims"])
        .withColumn("total_paid", (F.col("medical_paid") + F.col("pharmacy_paid")).cast(MONEY))
        .withColumn(
            "pmpm_total",
            F.when(F.col("member_months") > 0, F.col("total_paid") / F.col("member_months")),
        )
        .withColumn(
            "pmpm_medical",
            F.when(F.col("member_months") > 0, F.col("medical_paid") / F.col("member_months")),
        )
        .withColumn(
            "pmpm_pharmacy",
            F.when(F.col("member_months") > 0, F.col("pharmacy_paid") / F.col("member_months")),
        )
    )
    _publish(pmpm, settings, "fact_pmpm", counts)

    def _claim_flags(df: DataFrame, domain: str) -> DataFrame:
        return (
            df.groupBy("claim_id", "year_month")
            .agg(
                F.max(F.when(F.col("claim_status") == "Denied", 1).otherwise(0)).alias("is_denied"),
                F.max(F.when(F.col("claim_status") == "Paid", 1).otherwise(0)).alias("is_paid"),
                F.max(F.when(F.col("claim_status") == "Reversed", 1).otherwise(0)).alias(
                    "is_reversed"
                ),
                F.max(F.when(F.col("claim_status") == "Pending", 1).otherwise(0)).alias(
                    "is_pending"
                ),
                F.max(
                    F.when(F.col("claim_status").isin("Paid", "Denied", "Adjusted"), 1).otherwise(0)
                ).alias("is_adjudicated"),
            )
            .withColumn("claim_domain", F.lit(domain))
        )

    flags = _claim_flags(fact_medical, "medical").unionByName(
        _claim_flags(fact_pharmacy, "pharmacy")
    )
    denial_summary = (
        flags.groupBy("year_month", "claim_domain")
        .agg(
            F.sum("is_adjudicated").alias("adjudicated_claims"),
            F.sum("is_denied").alias("denied_claims"),
            F.sum("is_paid").alias("paid_claims"),
            F.sum("is_reversed").alias("reversed_claims"),
            F.sum("is_pending").alias("pending_claims"),
        )
        .withColumn(
            "denial_rate",
            F.when(
                F.col("adjudicated_claims") > 0,
                F.col("denied_claims") / F.col("adjudicated_claims"),
            ),
        )
    )
    _publish(denial_summary, settings, "fact_denial_summary", counts)

    denied_lines = (
        fact_medical.filter(F.col("claim_status") == "Denied")
        .select("claim_id", "year_month", "denial_reason_code", "billed_amount")
        .withColumn("claim_domain", F.lit("medical"))
        .unionByName(
            fact_pharmacy.filter(F.col("claim_status") == "Denied")
            .select("claim_id", "year_month", "denial_reason_code", "billed_amount")
            .withColumn("claim_domain", F.lit("pharmacy"))
        )
        .dropDuplicates(["claim_id", "denial_reason_code"])
    )
    denial_reason = denied_lines.groupBy("year_month", "claim_domain", "denial_reason_code").agg(
        F.countDistinct("claim_id").alias("denied_claims"),
        F.sum("billed_amount").cast(MONEY).alias("denied_billed_amount"),
    )
    _publish(denial_reason, settings, "fact_denial_reason", counts)

    fills = fact_pharmacy.filter((F.col("claim_status") == "Paid") & (~F.col("is_reversed")))
    gdr = (
        fills.groupBy("year_month")
        .agg(
            F.countDistinct("claim_id").alias("total_fills"),
            F.countDistinct(F.when(F.col("generic_indicator") == "Y", F.col("claim_id"))).alias(
                "generic_fills"
            ),
            F.sum("days_supply").alias("total_days"),
            F.sum(
                F.when(F.col("generic_indicator") == "Y", F.col("days_supply")).otherwise(0)
            ).alias("generic_days"),
            F.sum("paid_amount").cast(MONEY).alias("paid_amount"),
        )
        .withColumn(
            "generic_dispensing_rate",
            F.when(F.col("total_fills") > 0, F.col("generic_fills") / F.col("total_fills")),
        )
        .withColumn(
            "gdr_days",
            F.when(F.col("total_days") > 0, F.col("generic_days") / F.col("total_days")),
        )
    )
    _publish(gdr, settings, "fact_generic_dispensing", counts)

    drug_spend = fills.groupBy(
        "year_month", "ndc", "drug_name", "therapeutic_class", "brand_generic_flag"
    ).agg(
        F.sum("paid_amount").cast(MONEY).alias("paid_amount"),
        F.sum("quantity").cast(MONEY).alias("quantity"),
        F.countDistinct("claim_id").alias("fill_count"),
    )
    _publish(drug_spend, settings, "fact_drug_spend", counts)
    overall = drug_spend.groupBy("ndc", "drug_name", "therapeutic_class", "brand_generic_flag").agg(
        F.sum("paid_amount").cast(MONEY).alias("paid_amount"),
        F.sum("quantity").cast(MONEY).alias("quantity"),
        F.sum("fill_count").alias("fill_count"),
    )
    ranked = overall.withColumn(
        "spend_rank",
        F.row_number().over(Window.orderBy(F.col("paid_amount").desc(), F.col("ndc"))),
    )
    _publish(ranked.filter(F.col("spend_rank") <= 15), settings, "fact_top_drugs", counts)

    variance = (
        fills.groupBy("pharmacy_npi", "pharmacy_name", "network_tier", "year_month")
        .agg(
            F.countDistinct("claim_id").alias("fill_count"),
            F.sum("total_reimbursement").cast(MONEY).alias("total_reimbursement"),
            F.sum("contracted_amount").cast(MONEY).alias("contracted_amount"),
            F.sum("variance_amount").cast(MONEY).alias("variance_amount"),
        )
        .withColumn(
            "variance_pct",
            F.when(
                F.col("contracted_amount") != 0,
                F.col("variance_amount") / F.col("contracted_amount"),
            ),
        )
    )
    _publish(variance, settings, "fact_reimbursement_variance", counts)

    prescribing = (
        fills.groupBy(
            "prescriber_npi", "prescriber_name", "prescriber_specialty", "therapeutic_class"
        )
        .agg(
            F.countDistinct("claim_id").alias("fill_count"),
            F.countDistinct(F.when(F.col("generic_indicator") == "Y", F.col("claim_id"))).alias(
                "generic_fills"
            ),
            F.sum("paid_amount").cast(MONEY).alias("paid_amount"),
        )
        .withColumn(
            "generic_rate",
            F.when(F.col("fill_count") > 0, F.col("generic_fills") / F.col("fill_count")),
        )
    )
    _publish(prescribing, settings, "fact_provider_prescribing", counts)

    med_util = fact_medical.groupBy("member_id", "year_month").agg(
        F.countDistinct("claim_id").alias("medical_claims"),
        F.countDistinct(F.when(F.col("place_of_service") == "23", F.col("claim_id"))).alias(
            "er_visits"
        ),
        F.countDistinct(F.when(F.col("place_of_service") == "21", F.col("claim_id"))).alias(
            "inpatient_admits"
        ),
        F.sum("paid_amount").cast(MONEY).alias("medical_paid"),
    )
    ph_util = fact_pharmacy.groupBy("member_id", "year_month").agg(
        F.countDistinct("claim_id").alias("pharmacy_claims"),
        F.countDistinct("prescriber_npi").alias("distinct_prescribers"),
        F.sum("paid_amount").cast(MONEY).alias("pharmacy_paid"),
    )
    utilization = (
        med_util.join(ph_util, ["member_id", "year_month"], "fullouter")
        .join(
            member_dim.select("member_id", "line_of_business", "state", "plan_id"),
            "member_id",
            "left",
        )
        .fillna(
            0,
            [
                "medical_claims",
                "pharmacy_claims",
                "er_visits",
                "inpatient_admits",
                "medical_paid",
                "pharmacy_paid",
                "distinct_prescribers",
            ],
        )
        .withColumn("total_paid", (F.col("medical_paid") + F.col("pharmacy_paid")).cast(MONEY))
    )
    _publish(utilization, settings, "fact_member_utilization", counts)

    _register_views(spark, settings, list(counts))
    return counts

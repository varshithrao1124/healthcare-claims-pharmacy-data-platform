"""SparkSession factory with Delta Lake enabled.

``configure_spark_with_delta_pip`` pulls the Delta jar that matches the
installed ``delta-spark`` package, so local runs and CI do not need a
pre-provisioned cluster.
"""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from hc_platform.config import Settings


def build_spark(settings: Settings, warehouse_dir: str | None = None) -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    warehouse = warehouse_dir or os.path.join(settings.lake_root, "_warehouse")
    if settings.storage_mode == "local":
        os.makedirs(warehouse, exist_ok=True)
        os.makedirs(settings.log_dir, exist_ok=True)

    builder = (
        SparkSession.builder.appName(settings.spark_app_name)
        .master(settings.spark_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.driver.memory", settings.driver_memory)
        .config("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
        .config("spark.default.parallelism", str(settings.shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

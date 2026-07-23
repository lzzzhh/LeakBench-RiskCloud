"""P1.0 — Spark/Iceberg Local Execution Skeleton.

Provides a SparkSession builder configured for local Iceberg development.
Usage:
    from case_studies.home_credit.pipelines.spark_env import get_spark

    spark = get_spark()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS gold")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS audit")
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession

# Default warehouse location — can be overridden via env var
_WAREHOUSE = os.environ.get(
    "RISKCLOUD_ICEBERG_WAREHOUSE",
    str(Path(__file__).resolve().parents[3] / "data" / "iceberg_warehouse"),
)

# Iceberg catalog name
CATALOG = "riskcloud"


def get_spark(
    app_name: str = "riskcloud-home-credit",
    warehouse: str | None = None,
) -> SparkSession:
    """Create a local Spark session with Iceberg support.

    Requires:
        pip install pyspark pyiceberg

    The session uses:
    - A local Derby metastore (or Hadoop catalog for simple setups)
    - Iceberg Spark extensions
    - Timezone = UTC (essential for temporal contracts)
    """
    warehouse_path = warehouse or _WAREHOUSE

    # Ensure warehouse directory exists
    Path(warehouse_path).mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # Iceberg catalog — using Hadoop catalog for local simplicity
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse_path)
        # Session defaults for temporal integrity
        .config("spark.sql.session.timeZone", "UTC")
        # Disable adaptive query execution for deterministic re-runs in Phase 1
        .config("spark.sql.adaptive.enabled", "false")
        # Limit broadcast to avoid OOM on bureau tables
        .config("spark.sql.autoBroadcastJoinThreshold", "50MB")
        .getOrCreate()
    )

    # Set log level to WARN to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    return spark


def setup_namespaces(spark: SparkSession) -> None:
    """Create the standard Iceberg namespaces if they don't exist."""
    for ns in ("bronze", "silver", "gold", "audit"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")

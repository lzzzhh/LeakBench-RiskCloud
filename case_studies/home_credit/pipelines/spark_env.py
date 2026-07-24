"""P1.0 — Spark/Iceberg Local Execution Environment.

Pinned compatibility matrix:
  - PySpark 3.5.x
  - Iceberg 1.6.x
  - Scala 2.12
  - Java 11 or 17

The session automatically configures the Iceberg Spark runtime JAR via
spark.jars.packages. In air-gapped environments, download the JAR manually
and set SPARK_ICEBERG_JAR_PATH instead.

Usage:
    from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces, smoke_test

    spark = get_spark()
    setup_namespaces(spark)
    smoke_test(spark)  # creates → writes → reads → verifies snapshot → cleans up
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# -----------------------------------------------------------------
# Pinned version matrix
# -----------------------------------------------------------------

ICEBERG_VERSION = "1.6.1"
SCALA_BINARY = "2.12"
SPARK_MAJOR = "3.5"

ICEBERG_RUNTIME = (
    f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MAJOR}_{SCALA_BINARY}:{ICEBERG_VERSION}"
)

# Warehouse location
_WAREHOUSE = os.environ.get(
    "RISKCLOUD_ICEBERG_WAREHOUSE",
    str(Path(__file__).resolve().parents[3] / "data" / "iceberg_warehouse"),
)

CATALOG = "riskcloud"


# -----------------------------------------------------------------
# Spark session
# -----------------------------------------------------------------

def get_spark(
    app_name: str = "riskcloud-home-credit",
    warehouse: str | None = None,
) -> SparkSession:
    """Create a local Spark session with Iceberg support.

    Java 11 or 17 required. The Iceberg runtime JAR is pulled via Maven
    coordinates. Set SPARK_ICEBERG_JAR_PATH to use a pre-downloaded JAR.
    """
    from pyspark.sql import SparkSession

    warehouse_path = warehouse or _WAREHOUSE
    Path(warehouse_path).mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # Iceberg catalog
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse_path)
        # Deterministic execution (reduces plan variation)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "50MB")
    )

    # Runtime JAR — use env var override for air-gapped envs
    jar_path = os.environ.get("SPARK_ICEBERG_JAR_PATH")
    if jar_path:
        builder = builder.config("spark.jars", jar_path)
    else:
        builder = builder.config("spark.jars.packages", ICEBERG_RUNTIME)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# -----------------------------------------------------------------
# Namespaces
# -----------------------------------------------------------------

def setup_namespaces(spark: SparkSession) -> None:
    """Create the standard Iceberg namespaces."""
    for ns in ("bronze", "silver", "gold", "audit"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")


# -----------------------------------------------------------------
# Smoke test (creates → writes → reads → snapshot → cleans up)
# -----------------------------------------------------------------

def smoke_test(spark: SparkSession) -> bool:
    """Run a create-insert-read-snapshot smoke test. Returns True on pass."""
    table = f"{CATALOG}.audit.p10_smoke"

    try:
        # Ensure namespace
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.audit")

        # Create
        spark.sql(f"""
            CREATE OR REPLACE TABLE {table} (
                id BIGINT,
                value STRING
            ) USING iceberg
        """)

        # Insert
        spark.sql(f"INSERT INTO {table} VALUES (1, 'ok'), (2, 'phase1')")

        # Read
        rows = spark.sql(f"SELECT * FROM {table} ORDER BY id").collect()
        assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
        assert rows[0].id == 1 and rows[0].value == "ok"

        # Verify snapshot exists
        snapshots = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").collect()
        assert len(snapshots) >= 1, "No snapshots found"

        return True
    except Exception as exc:
        print(f"Smoke test FAILED: {exc}")
        return False
    finally:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        except Exception:
            pass


# -----------------------------------------------------------------
# CLI
# -----------------------------------------------------------------

def main() -> int:
    """Run the full smoke test with a temporary warehouse."""
    import tempfile

    with tempfile.TemporaryDirectory() as warehouse:
        spark = get_spark(app_name="riskcloud-p10-smoke", warehouse=warehouse)
        try:
            setup_namespaces(spark)
            ok = smoke_test(spark)
            print(f"Spark/Iceberg smoke test: {'PASS' if ok else 'FAIL'}")
            return 0 if ok else 1
        finally:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

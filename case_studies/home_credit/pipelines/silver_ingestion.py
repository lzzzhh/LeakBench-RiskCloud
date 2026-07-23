"""P1.3 — Silver Ingestion Pipeline with full governance contracts."""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

from case_studies.home_credit.pipelines.shared.governance import (
    create_iceberg_table,
    get_current_snapshot_metadata,
    publish_artifacts,
    table_exists,
    validate_table_contract,
    write_failure_artifact,
)

UTC = timezone.utc
SILVER_SCHEMA_VERSION = 1


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _row_hash(row_values: list, columns: list[str]) -> str:
    obj = {col: str(v) if v is not None else None for col, v in zip(columns, row_values)}
    return _sha256(json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode())


# -----------------------------------------------------------------
# Config
# -----------------------------------------------------------------


class SilverConfig:
    def __init__(self, path: Path):
        with open(path) as f:
            data = yaml.safe_load(f)
        s = data["silver"]
        for field, expected in [("version", "hc-silver-v1"), ("catalog", "riskcloud"), ("namespace", "silver")]:
            if s.get(field) != expected:
                raise ValueError(f"silver.{field} must be {expected!r}")
        self.version = s["version"]
        self.catalog = s["catalog"]
        self.namespace = s["namespace"]
        self.bronze_tables = data["bronze"]["tables"]
        self.tables = data["tables"]

    @classmethod
    def from_yaml(cls, path: Path) -> SilverConfig:
        return cls(path)


# -----------------------------------------------------------------
# Ingestion
# -----------------------------------------------------------------


def ingest_silver(
    config: SilverConfig,
    bronze_receipt_path: Path,
    receipt_dir: Path,
    run_id: str,
    git_commit: str = "",
    warehouse: str | None = None,
    spark=None,
) -> dict:

    from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces

    started_at = datetime.now(UTC)
    own_spark = spark is None

    br = yaml.safe_load(bronze_receipt_path.read_text())
    if br["receipt"]["status"] != "COMPLETE":
        raise RuntimeError("Bronze receipt not COMPLETE")
    manifest_sha = br["input"]["manifest_sha256"]

    if receipt_dir.exists():
        raise RuntimeError(f"run directory exists: {receipt_dir}")

    sess = get_spark(app_name=f"riskcloud-silver-{run_id}", warehouse=warehouse) if own_spark else spark
    completed: list[str] = []
    current: str | None = None
    table_results: dict = {}
    try:
        setup_namespaces(sess)
        for tbl_key, tbl_def in config.tables.items():
            current = tbl_key
            tr = _ingest_one_table(sess, config, tbl_key, tbl_def, config.bronze_tables[tbl_key], manifest_sha)
            table_results[tbl_key] = tr
            completed.append(tbl_key)
        current = None

        # Build receipt
        receipt = {
            "receipt": {
                "receipt_version": 1,
                "run_id": run_id,
                "status": "COMPLETE",
                "created_at": started_at.isoformat(),
            },
            "input": {"manifest_sha256": manifest_sha},
            "code": {"git_commit": git_commit, "silver_version": "hc-silver-v1"},
            "runtime": {"python_version": sys.version.split()[0]},
            "tables": table_results,
            "quality": {"row_count_closure": "PASS"},
        }
        sm = {
            "manifest": {"manifest_id": run_id, "status": "COMPLETE", "created_at": started_at.isoformat()},
            "input": {"manifest_sha256": manifest_sha},
            "code": {"git_commit": git_commit, "silver_version": "hc-silver-v1"},
            "tables": {
                "silver": {
                    tn: {
                        "iceberg_table": tr["table_name"],
                        "iceberg_snapshot_id": tr["iceberg_snapshot_id"],
                        "row_count": tr["silver_row_count"],
                        "schema_sha256": tr["schema_sha256"],
                    }
                    for tn, tr in table_results.items()
                }
            },
            "quality": {"status": "NOT_RUN"},
        }
        publish_artifacts(receipt_dir, sm, receipt)
        return receipt
    except BaseException as exc:
        try:
            write_failure_artifact(
                receipt_dir,
                run_id,
                exc,
                completed,
                current,
                {"manifest_sha256": manifest_sha},
                {"git_commit": git_commit, "silver_version": "hc-silver-v1"},
            )
        except BaseException:
            pass
        raise
    finally:
        if own_spark:
            sess.stop()


def _ingest_one_table(spark, config, tbl_key, tbl_def, bronze_table, manifest_sha):
    from pyspark.sql.functions import col, lit

    target = tbl_def["table"]
    type_map = tbl_def.get("type_mapping", {})
    tbl_def.get("primary_key")
    enrichment = tbl_def.get("enrichment")

    # Read bronze
    df = spark.table(bronze_table).filter(f"_source_manifest_sha256 = '{manifest_sha}'")
    source_count = df.count()
    bronze_meta = [
        "_source_file_name",
        "_source_file_sha256",
        "_source_header_sha256",
        "_source_manifest_sha256",
        "_source_snapshot_id",
        "_bronze_schema_version",
        "_raw_row_sha256",
    ]
    biz_cols = [c for c in df.columns if c not in bronze_meta]
    df_biz = df.select(*biz_cols)

    # Type casting
    for cname, ttype in type_map.items():
        if cname in biz_cols:
            df_biz = df_biz.withColumn(cname, col(cname).cast("int") if ttype == "INT" else col(cname).cast("double"))

    # Enrichment
    if enrichment:
        enr_src = config.bronze_tables.get(enrichment["source"])
        enr_df = spark.table(enr_src).filter(f"_source_manifest_sha256 = '{manifest_sha}'")
        jk = enrichment["join_key"]
        adds = enrichment["add_columns"]
        enr_sel = enr_df.select(jk, *adds).distinct()
        # Assert one-to-one
        dupes = enr_sel.groupBy(jk).count().filter("count > 1").count()
        if dupes > 0:
            raise RuntimeError(f"{tbl_key}: {jk} has {dupes} duplicate mappings")
        df_biz = df_biz.join(enr_sel, jk, "left")
        # Verify no nulls in added columns
        for ac in adds:
            nulls = df_biz.filter(col(ac).isNull()).count()
            if nulls > 0:
                raise RuntimeError(f"{tbl_key}: {nulls} nulls in enriched column {ac}")

    # Add lineage
    df_out = df_biz.withColumn("_source_manifest_sha256", lit(manifest_sha))
    silver_cols = df_out.columns

    # Create or write
    if not table_exists(spark, target):
        props = {
            "format-version": "2",
            "write.format.default": "parquet",
            "riskcloud.dataset_id": "home_credit",
            "riskcloud.layer": "silver",
            "riskcloud.silver_version": "hc-silver-v1",
            "riskcloud.source_table": bronze_table,
        }
        col_defs = [
            f"`{c}` {'INT' if type_map.get(c) == 'INT' else 'DOUBLE' if type_map.get(c) == 'DOUBLE' else 'STRING'}"
            for c in silver_cols
        ]
        create_iceberg_table(spark, target, col_defs, props)
    else:
        expected_names = silver_cols
        expected_types = {c: type_map.get(c, "STRING") for c in silver_cols}
        validate_table_contract(
            spark,
            target,
            expected_names,
            expected_types,
            {"riskcloud.dataset_id": "home_credit", "riskcloud.layer": "silver"},
        )
    df_out.writeTo(target).overwritePartitions()

    # Verify
    after = spark.table(target).filter(f"_source_manifest_sha256 = '{manifest_sha}'").count()
    if after != source_count:
        raise RuntimeError(f"{tbl_key}: {after} vs source {source_count}")

    meta = get_current_snapshot_metadata(spark, target)
    return {
        "table_name": target,
        "iceberg_snapshot_id": meta["snapshot_id"],
        "metadata_location": meta["metadata_location"],
        "bronze_row_count": source_count,
        "silver_row_count": after,
        "schema_sha256": _sha256(json.dumps(silver_cols, sort_keys=True).encode()),
    }

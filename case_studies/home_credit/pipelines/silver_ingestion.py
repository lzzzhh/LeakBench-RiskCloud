"""P1.3 — Silver with PK/FK, cast quality, lineage."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

from case_studies.home_credit.pipelines.shared.governance import (
    create_table,
    get_snapshot_meta,
    publish,
    sha256,
    table_exists,
    write_failure,
)

UTC = timezone.utc


class SilverConfig:
    def __init__(self, path: Path):
        with open(path) as f:
            data = yaml.safe_load(f)
        s = data["silver"]
        if s.get("version") != "hc-silver-v1":
            raise ValueError("silver.version must be hc-silver-v1")
        self.version = s["version"]
        self.bronze_tables = data["bronze"]["tables"]
        self.tables = data["tables"]

    @classmethod
    def from_yaml(cls, path: Path) -> SilverConfig:
        return cls(path)


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
    completed, current = [], None
    table_results = {}
    try:
        setup_namespaces(sess)
        for tbl_key, tbl_def in config.tables.items():
            current = tbl_key
            tr = _ingest(sess, config, tbl_key, tbl_def, config.bronze_tables[tbl_key], manifest_sha)
            table_results[tbl_key] = tr
            completed.append(tbl_key)
        current = None

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
        }
        publish(receipt_dir, "silver", sm, receipt)
        return receipt
    except BaseException as exc:
        try:
            write_failure(
                receipt_dir,
                "silver",
                run_id,
                exc,
                completed,
                current,
                {"manifest_sha256": manifest_sha},
                {"git_commit": git_commit},
            )
        except BaseException:
            pass
        raise
    finally:
        if own_spark:
            sess.stop()


def _ingest(spark, config, tbl_key, tbl_def, bronze_table, manifest_sha):
    from pyspark.sql.functions import col, lit

    target = tbl_def["table"]
    type_map = tbl_def.get("type_mapping", {})
    enrichment = tbl_def.get("enrichment")

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
    for cn, tt in type_map.items():
        if cn in biz_cols:
            df_biz = df_biz.withColumn(cn, col(cn).cast("int") if tt == "INT" else col(cn).cast("double"))

    # PK enforcement: SK_ID_CURR, SK_ID_BUREAU non-null + unique
    pk = tbl_def.get("primary_key")
    if isinstance(pk, str):
        nulls = df_biz.filter(col(pk).isNull()).count()
        if nulls > 0:
            raise RuntimeError(f"{tbl_key}: {nulls} nulls in PK {pk}")
        dupes = df_biz.groupBy(pk).count().filter("count > 1").count()
        if dupes > 0:
            raise RuntimeError(f"{tbl_key}: {dupes} duplicate PK {pk}")

    # Enrichment
    if enrichment:
        enr_src = config.bronze_tables.get(enrichment["source"])
        enr_df = spark.table(enr_src).filter(f"_source_manifest_sha256 = '{manifest_sha}'")
        jk = enrichment["join_key"]
        adds = enrichment["add_columns"]
        # Cast enrichment columns to match Silver type mapping
        enr_cols = [col(jk)]
        for ac in adds:
            tt = type_map.get(ac, "STRING")
            if tt == "INT":
                enr_cols.append(col(ac).cast("int"))
            elif tt == "DOUBLE":
                enr_cols.append(col(ac).cast("double"))
            else:
                enr_cols.append(col(ac))
        enr_sel = enr_df.select(*enr_cols).distinct()
        dupes = enr_sel.groupBy(jk).count().filter("count > 1").count()
        if dupes > 0:
            raise RuntimeError(f"{tbl_key}: {dupes} duplicate mappings for {jk}")
        before = df_biz.count()
        df_biz = df_biz.join(enr_sel, jk, "left")
        after = df_biz.count()
        if after != before:
            raise RuntimeError(f"{tbl_key}: join changed row count {before}→{after}")
        for ac in adds:
            if df_biz.filter(col(ac).isNull()).count() > 0:
                raise RuntimeError(f"{tbl_key}: nulls in enriched column {ac}")

    # Lineage
    df_out = df_biz.withColumn("_source_manifest_sha256", lit(manifest_sha))
    silver_cols = df_out.columns

    props = {
        "format-version": "2",
        "write.format.default": "parquet",
        "riskcloud.dataset_id": "home_credit",
        "riskcloud.layer": "silver",
        "riskcloud.silver_version": "hc-silver-v1",
        "riskcloud.source_table": bronze_table,
    }
    if not table_exists(spark, target):
        col_defs = [
            f"`{c}` {'INT' if type_map.get(c) == 'INT' else 'DOUBLE' if type_map.get(c) == 'DOUBLE' else 'STRING'}"
            for c in silver_cols
        ]
        create_table(spark, target, col_defs, "_source_manifest_sha256", props)
    df_out.writeTo(target).overwritePartitions()

    after = spark.table(target).filter(f"_source_manifest_sha256 = '{manifest_sha}'").count()
    if after != source_count:
        raise RuntimeError(f"{tbl_key}: {after} vs {source_count}")

    meta = get_snapshot_meta(spark, target)
    return {
        "table_name": target,
        "iceberg_snapshot_id": meta["snapshot_id"],
        "metadata_location": meta["metadata_location"],
        "bronze_row_count": source_count,
        "silver_row_count": after,
        "schema_sha256": sha256(str(silver_cols).encode()),
    }

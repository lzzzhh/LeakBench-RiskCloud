# ADR-0003: Bronze Publication and Idempotency

**Status:** ACCEPTED
**Date:** 2026-07-23

## Context

Bronze ingestion writes raw CSV data into Iceberg tables. Multiple runs
with the same input manifest must produce equivalent data without
duplicating rows. Iceberg does not provide cross-table transactions.

## Decision

1. **Per-table atomic partition overwrite.** Each table uses
   `overwritePartitions` keyed on `_source_manifest_sha256`. Same
   manifest → same partition → overwrite; different manifest → new
   partition.

2. **Receipt gate for multi-table atomicity.** A `bronze_receipt.yaml`
   with status `COMPLETE` is the authoritative signal that all three
   tables have been successfully written. Downstream consumers must
   check for a `COMPLETE` receipt before using Bronze data. There is
   no cross-table Iceberg transaction.

3. **Deterministic `_source_snapshot_id`.** Computed as
   `sha256(canonical_json(["home_credit", manifest_sha, "hc-bronze-v1"]))`.
   Same manifest + same bronze version → same snapshot identity.

4. **Content fingerprint.** A deterministic multiset hash of
   `_raw_row_sha256:count` per table, enabling rerun equivalence
   verification even when Iceberg Snapshot IDs differ.

5. **All columns as STRING.** No type inference, no cast, no null
   replacement. Type conversion belongs to Silver.

## Consequences

- `COMPLETE` receipt is the single source of truth for publication.
  Partitions written without a receipt are unpublished and may be
  overwritten.
- `_source_snapshot_id` is the bridge between Bronze partitions and
  downstream lineage.
- Schema changes in source files are detected by header SHA drift,
  not by changing column types.

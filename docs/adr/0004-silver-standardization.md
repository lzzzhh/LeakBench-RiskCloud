# ADR-0004: Silver Standardization and Entity Enrichment

**Status:** ACCEPTED
**Date:** 2026-07-23

## Context

Bronze preserves raw CSV data as STRING. Silver must:
1. Convert numeric columns to appropriate types (INT, DOUBLE)
2. Normalize entity IDs
3. Enrich bureau_balance with SK_ID_CURR from bureau
4. Establish primary/foreign key relationships
5. Preserve deterministic lineage from Bronze

## Decision

1. **Type conversion**: numeric columns are cast to INT or DOUBLE based
   on known schema. Invalid values become NULL (not errors).
2. **SK_ID_CURR enrichment**: bureau_balance is joined with bureau on
   SK_ID_BUREAU to add SK_ID_CURR.
3. **Key naming**: primary keys use original column names. Foreign keys
   reference the parent table's primary key.
4. **Partition**: `_source_manifest_sha256` (same as Bronze for lineage).
5. **Idempotency**: `overwritePartitions` by manifest SHA.

## Consequences

- Silver tables are NOT 1:1 with CSV files. Bureau_balance gains columns.
- Type conversion errors are silently converted to NULL.
- Silver depends on Bronze being published with a COMPLETE receipt.

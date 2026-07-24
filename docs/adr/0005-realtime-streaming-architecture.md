# ADR-0005: Realtime Streaming Architecture

**Status:** ACCEPTED
**Date:** 2026-07-24

## Context

The existing offline Spark + Iceberg pipeline computes 20 features from static CSV data.
To demonstrate batch-stream unification, we add Kafka + Flink for real-time feature
computation using deterministic event replay from the same fixture data.

## Decision

1. **Kafka as event log**: 5 topics (3 source + 1 feature update + 1 DLQ). KRaft mode, single broker for local dev.

2. **Topic configuration**:
   - Partitions: 3
   - Replication factor: 1
   - Source topics: cleanup.policy = delete
   - Feature Updates: cleanup.policy = compact,delete
   - DLQ: cleanup.policy = delete

3. **Flink 1.19 + Iceberg 1.6.1**: Chosen for compatibility with existing Spark Iceberg runtime.

4. **Redis as online store**: Latest feature values only. Iceberg latest as audit fallback.

5. **Deterministic event replay**: Same fixture CSV data produces identical events each run.

6. **Unified entity ID**: `SK_ID_CURR:<digits>` across batch and stream.

7. **Feature ID reuse**: Same 20 catalog feature IDs in both batch and stream.

8. **Canonical entity_id**: Enforced as `^SK_ID_CURR:[0-9]+$` in event envelope validation.

9. **Watermark**: Default out-of-orderness = 5 minutes.

10. **Duplicate identity**: `event_id` deduplication with exact match.

11. **Delivery semantics**:
    - Kafka → Flink → Iceberg: exactly-once via checkpoint
    - Redis: eventually consistent, at-least-once, idempotent conditional upsert
    - Exactly-once does NOT extend to Redis

## Consequences

- Real-time features are computed from synthetic historical replay, not live production traffic.
- Redis is eventually consistent, not transactionally synchronized with Iceberg.
- Checkpoint-based exactly-once applies to Kafka→Flink→Iceberg, not Redis.

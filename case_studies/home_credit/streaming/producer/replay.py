#!/usr/bin/env python3
"""P1.9 — Deterministic Event Replay Producer.

Reads Home Credit fixture CSV and publishes events to Kafka topics.
Deterministic: same seed + same fixture → same event_id, same order.
"""

import argparse
import hashlib
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[3]
FIXTURES = REPO / "tests" / "fixtures" / "home_credit_phase1"
UTC = timezone.utc
ANCHOR = datetime(2000, 1, 1, 12, 0, 0, tzinfo=UTC)

TOPICS = {
    "application_train": "riskcloud.home_credit.application.v1",
    "bureau": "riskcloud.home_credit.bureau.v1",
    "bureau_balance": "riskcloud.home_credit.bureau_balance.v1",
}

SCHEMA_VERSION = 1


def compute_event_id(source_table: str, source_pk: str, payload: dict, event_time: datetime) -> str:
    parts = [source_table, source_pk, event_time.isoformat(), "UPSERT",
             json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)]
    return "sha256:" + hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()


def read_csv_events(data_dir: Path, seed: int, speed: float) -> list[dict[str, Any]]:
    """Read fixture CSVs and generate ordered event list."""
    import csv
    events = []
    for table in ["application_train", "bureau", "bureau_balance"]:
        fpath = data_dir / f"{table}.csv"
        if not fpath.exists():
            continue
        with open(fpath, newline="") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                source_pk = row.get("SK_ID_CURR", row.get("SK_ID_BUREAU", str(i)))
                sk_curr = row.get("SK_ID_CURR", "0")
                entity_id = f"SK_ID_CURR:{sk_curr}"
                event_time = ANCHOR
                event_id = compute_event_id(table, source_pk, dict(row), event_time)
                events.append({
                    "event_id": event_id, "schema_version": SCHEMA_VERSION,
                    "event_type": f"{table}_upsert",
                    "source_table": TABLE_TO_SOURCE.get(table, table),
                    "source_pk": source_pk, "entity_id": entity_id,
                    "op": "UPSERT", "event_time": event_time.isoformat(),
                    "produced_at": datetime.now(UTC).isoformat(),
                    "payload": dict(row),
                })

    # Deterministic shuffle
    rng = random.Random(seed)
    rng.shuffle(events)
    return events


TABLE_TO_SOURCE = {
    "application_train": "application_train",
    "bureau": "bureau",
    "bureau_balance": "bureau_balance",
}


def main():
    parser = argparse.ArgumentParser(description="Deterministic event replay producer")
    parser.add_argument("--data-dir", required=True, help="Fixture CSV directory")
    parser.add_argument("--speed", type=float, default=1.0)
    parser.add_argument("--seed", type=int, default=20260724)
    parser.add_argument("--dry-run", action="store_true", help="Print events without Kafka")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    args = parser.parse_args()

    events = read_csv_events(Path(args.data_dir), args.seed, args.speed)

    if args.dry_run:
        for e in events:
            print(json.dumps(e, ensure_ascii=False))
        print(f"\nTotal events: {len(events)}")
        return

    # Kafka produce
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
    except ImportError:
        print("kafka-python not installed. Install with: pip install kafka-python")
        print("Running in dry-run mode...")
        for e in events:
            print(json.dumps(e, ensure_ascii=False))
        print(f"\nTotal events: {len(events)}")
        return

    count = 0
    for i, event in enumerate(events):
        src = event.get("source_table", "")
        topic = TOPICS.get(src, "riskcloud.home_credit.dlq.v1")
        try:
            producer.send(topic, key=event["entity_id"], value=event)
            count += 1
            delay = (1.0 / args.speed) if args.speed > 0 else 0
            if delay > 0:
                time.sleep(delay)
        except Exception as exc:
            print(f"ERROR sending event {i}: {exc}", file=sys.stderr)

    producer.flush()
    producer.close()
    print(f"Produced {count}/{len(events)} events")


if __name__ == "__main__":
    main()

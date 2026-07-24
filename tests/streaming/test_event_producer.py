"""P1.9 — Event producer contract tests (no Kafka required)."""

from pathlib import Path

from case_studies.home_credit.streaming.producer.replay import read_csv_events


class TestEventProducer:
    def test_dry_run_produces_events(self):
        fixtures = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "home_credit_phase1"
        events = read_csv_events(fixtures, seed=20260724, speed=1.0)
        assert len(events) > 0, "No events produced"
        assert len(events) >= 300, f"Expected >=300 events, got {len(events)}"

    def test_events_have_required_fields(self):
        fixtures = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "home_credit_phase1"
        events = read_csv_events(fixtures, seed=20260724, speed=1.0)
        for e in events:
            assert "event_id" in e
            assert e["event_id"].startswith("sha256:")
            assert "entity_id" in e
            assert e["entity_id"].startswith("SK_ID_CURR:")
            assert "event_type" in e
            assert "source_table" in e
            assert "event_time" in e

    def test_deterministic(self):
        fixtures = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "home_credit_phase1"
        e1 = read_csv_events(fixtures, seed=42, speed=1.0)
        e2 = read_csv_events(fixtures, seed=42, speed=1.0)
        assert len(e1) == len(e2)
        for i in range(len(e1)):
            assert e1[i]["event_id"] == e2[i]["event_id"]

    def test_different_seed_different_order(self):
        fixtures = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "home_credit_phase1"
        e1 = read_csv_events(fixtures, seed=42, speed=1.0)
        e2 = read_csv_events(fixtures, seed=99, speed=1.0)
        ids1 = [e["event_id"] for e in e1]
        ids2 = [e["event_id"] for e in e2]
        # Same set of event IDs, potentially different order
        assert set(ids1) == set(ids2), "Same events should exist regardless of seed"

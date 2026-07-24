"""PR #8 — Kafka topic specs and configuration tests (no Docker required)."""

from case_studies.home_credit.streaming.contracts.event_envelope import (
    ALL_SOURCE_TOPICS,
    TOPIC_DLQ,
    TOPIC_FEATURE_UPDATES,
    TOPIC_SPECS,
)


class TestKafkaTopicSpecs:
    def test_five_topics(self):
        assert len(TOPIC_SPECS) == 5

    def test_all_partitions_3(self):
        for spec in TOPIC_SPECS.values():
            assert spec.partitions == 3

    def test_replication_factor_1(self):
        for spec in TOPIC_SPECS.values():
            assert spec.replication_factor == 1

    def test_source_topics_delete_only(self):
        for t in ALL_SOURCE_TOPICS:
            assert TOPIC_SPECS[t].cleanup_policy == ("delete",)

    def test_feature_updates_compact_delete(self):
        assert TOPIC_SPECS[TOPIC_FEATURE_UPDATES].cleanup_policy == ("compact", "delete")

    def test_dlq_delete_only(self):
        assert TOPIC_SPECS[TOPIC_DLQ].cleanup_policy == ("delete",)

    def test_init_script_exists(self):
        from pathlib import Path
        script = Path(__file__).resolve().parents[3] / "deploy" / "local" / "kafka" / "init-topics.sh"
        assert script.is_file()
        content = script.read_text()
        for t in TOPIC_SPECS:
            assert t in content, f"Topic {t} not in init-topics.sh"

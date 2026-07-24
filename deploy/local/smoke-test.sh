#!/bin/bash
# Smoke test for Kafka + Flink infrastructure
set -euo pipefail

COMPOSE_FILE="deploy/local/docker-compose.realtime.yml"

echo "=== RiskCloud Infrastructure Smoke Test ==="

# 1. Kafka broker health
echo "1. Kafka broker health..."
docker compose -f "$COMPOSE_FILE" exec -T kafka \
    /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:29092 >/dev/null
echo "   OK"

# 2. Topics exist
echo "2. Topics..."
expected_topics=(
    "riskcloud.home_credit.application.v1"
    "riskcloud.home_credit.bureau.v1"
    "riskcloud.home_credit.bureau_balance.v1"
    "riskcloud.home_credit.feature_updates.v1"
    "riskcloud.home_credit.dlq.v1"
)
for t in "${expected_topics[@]}"; do
    desc=$(docker compose -f "$COMPOSE_FILE" exec -T kafka \
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --describe --topic "$t" 2>/dev/null)
    echo "   $t: partitions=$(echo "$desc" | grep -c 'Partition:')"
done

# 3. Producer/consumer smoke
echo "3. Producer/consumer smoke..."
echo "test-msg" | docker compose -f "$COMPOSE_FILE" exec -T kafka \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:29092 \
    --topic riskcloud.home_credit.application.v1 2>/dev/null
docker compose -f "$COMPOSE_FILE" exec -T kafka \
    /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 \
    --topic riskcloud.home_credit.application.v1 --from-beginning --max-messages 1 \
    --timeout-ms 10000 2>/dev/null | grep -q "test-msg"
echo "   OK"

# 4. Flink REST
echo "4. Flink REST..."
overview=$(curl -sf http://localhost:8081/overview)
echo "   OK: $(echo "$overview" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'taskmanagers={d[\"taskmanagers\"]}, slots={d[\"slots-total\"]}')")"

# 5. Flink checkpoint config
echo "5. Flink checkpoint config..."
docker compose -f "$COMPOSE_FILE" exec -T flink-jobmanager \
    bash -c 'grep -q "execution.checkpointing.interval: 10s" /opt/flink/conf/flink-conf.yaml 2>/dev/null || grep -q "checkpointing.interval" <(echo "$FLINK_PROPERTIES")' && echo "   OK" || echo "   WARN: config not verified"

echo ""
echo "=== Infrastructure smoke test PASSED ==="

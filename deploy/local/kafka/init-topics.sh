#!/bin/bash
# Initialize Kafka topics for RiskCloud realtime pipeline
set -euo pipefail

BROKER="kafka:9092"
RETRIES=30

echo "Waiting for Kafka broker at $BROKER..."
for i in $(seq 1 $RETRIES); do
    if /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BROKER" &>/dev/null; then
        echo "Kafka is ready."
        break
    fi
    sleep 2
done

declare -A TOPICS=(
    ["riskcloud.home_credit.application.v1"]="delete"
    ["riskcloud.home_credit.bureau.v1"]="delete"
    ["riskcloud.home_credit.bureau_balance.v1"]="delete"
    ["riskcloud.home_credit.feature_updates.v1"]="compact,delete"
    ["riskcloud.home_credit.dlq.v1"]="delete"
)

for topic in "${!TOPICS[@]}"; do
    policy="${TOPICS[$topic]}"
    echo "Creating topic: $topic (cleanup.policy=$policy)"
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions 3 \
        --replication-factor 1 \
        --config "cleanup.policy=$policy"
done

echo "Topics created:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --list

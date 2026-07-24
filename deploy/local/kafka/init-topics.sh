#!/bin/bash
# Initialize Kafka topics for RiskCloud realtime pipeline
set -euo pipefail

BROKER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
RETRIES=30

echo "Waiting for Kafka broker at $BROKER..."
ready=false
for _ in $(seq 1 "$RETRIES"); do
    if /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BROKER" >/dev/null 2>&1; then
        ready=true
        break
    fi
    sleep 2
done

if [[ "$ready" != "true" ]]; then
    echo "Kafka broker unavailable: $BROKER" >&2
    exit 1
fi
echo "Kafka is ready."

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

    # Ensure config converges on re-runs
    if [[ "$policy" == *,* ]]; then
        config_arg="cleanup.policy=[$policy]"
    else
        config_arg="cleanup.policy=$policy"
    fi
    /opt/kafka/bin/kafka-configs.sh         --bootstrap-server "$BROKER"         --alter         --entity-type topics         --entity-name "$topic"         --add-config "$config_arg"

    /opt/kafka/bin/kafka-configs.sh         --bootstrap-server "$BROKER"         --describe         --entity-type topics         --entity-name "$topic"
done

echo ""
echo "=== Topic details ==="
for topic in "${!TOPICS[@]}"; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --describe --topic "$topic"
done

echo ""
echo "All topics initialized."

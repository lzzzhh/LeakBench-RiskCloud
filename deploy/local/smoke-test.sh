#!/bin/bash
# Smoke test for Kafka + Flink infrastructure — all assertions fail-closed
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE=(docker compose --project-directory "$REPO_ROOT" -f "$REPO_ROOT/deploy/local/docker-compose.realtime.yml")

echo "=== RiskCloud Infrastructure Smoke Test ==="

# 1. Kafka broker health
echo "1. Kafka broker..."
"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:29092 >/dev/null
echo "   OK"

# 2. Topics with partition and policy verification
echo "2. Topics..."
expected_topics=(
    "riskcloud.home_credit.application.v1"
    "riskcloud.home_credit.bureau.v1"
    "riskcloud.home_credit.bureau_balance.v1"
    "riskcloud.home_credit.feature_updates.v1"
    "riskcloud.home_credit.dlq.v1"
)
for t in "${expected_topics[@]}"; do
    desc=$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --describe --topic "$t" 2>/dev/null)
    pc=$(echo "$desc" | grep -c 'Partition:' || true)
    if [ "$pc" -ne 3 ]; then
        echo "   FAIL: $t partitions=$pc (expected 3)" >&2
        exit 1
    fi
    config_out=$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:29092 --describe --entity-type topics --entity-name "$t" 2>/dev/null)
    policy=$(printf '%s\n' "$config_out" | grep -oE 'cleanup\.policy=(compact,delete|delete,compact|delete)' | head -n 1 | cut -d= -f2-)
    if [[ -z "$policy" ]]; then
        echo "FAIL: cleanup.policy not found for $t" >&2
        echo "$config_out" >&2
        exit 1
    fi
    case "$t" in
        *feature_updates*) case "$policy" in compact,delete|delete,compact) ;; *) echo "   FAIL: $t policy=$policy (expected compact,delete)" >&2; exit 1 ;; esac ;;
        *) [[ "$policy" != "delete" ]] && { echo "   FAIL: $t policy=$policy (expected delete)" >&2; exit 1; } || true ;;
    esac
    echo "   $t: partitions=$pc policy=$policy OK"
done

# 3. Producer/consumer smoke
echo "3. Producer/consumer..."
msg="riskcloud-smoke-$(date +%s%N)"
echo "$msg" | "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:29092 --topic riskcloud.home_credit.application.v1 2>/dev/null
consumed=$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic riskcloud.home_credit.application.v1 --from-beginning --timeout-ms 30000 2>/dev/null || true)
if ! grep -Fqx "$msg" <<<"$consumed"; then
    echo "   FAIL: message not consumed: $msg" >&2
    exit 1
fi
echo "   OK"

# 4. Flink REST
echo "4. Flink REST..."
overview=$(curl -sf http://localhost:8081/overview)
echo "$overview" | python3 -c "
import sys, json
d = json.load(sys.stdin)
assert d['taskmanagers'] >= 1, f'taskmanagers={d[\"taskmanagers\"]}'
assert d['slots-total'] >= 2, f'slots={d[\"slots-total\"]}'
print(f'   taskmanagers={d[\"taskmanagers\"]}, slots={d[\"slots-total\"]} OK')
"

# 5. Flink checkpoint config via REST
echo "5. Flink config..."
config=$(curl -sf http://localhost:8081/jobmanager/config)
echo "$config" | python3 -c "
import sys, json
d = json.load(sys.stdin)
assert any(x['key'] == 'execution.checkpointing.interval' and x['value'] == '10 s' for x in d), 'checkpoint interval missing'
assert any(x['key'] == 'execution.checkpointing.mode' and x['value'] == 'EXACTLY_ONCE' for x in d), 'checkpoint mode missing'
assert any(x['key'] == 'state.checkpoints.dir' and x['value'] == 'file:///data/flink/checkpoints' for x in d), 'checkpoint dir missing'
assert any(x['key'] == 'state.savepoints.dir' and x['value'] == 'file:///data/flink/savepoints' for x in d), 'savepoint dir missing'
print('   checkpoint config OK')
"

echo ""
echo "=== Infrastructure smoke test PASSED ==="

#!/usr/bin/env bash
# Reset a consumer group's offsets to earliest so re-running the Flink job replays
# everything already sitting in the topic - the whole point of keeping raw and ecs
# as two separate topics instead of transforming in-place.
#
# Usage: replay-topic.sh <topic> <consumer-group>
set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Usage: $0 <topic> <consumer-group>" >&2
    echo "  e.g. $0 k8s-audit-raw k8s-audit-ecs-transform" >&2
    exit 1
fi

TOPIC="$1"
GROUP="$2"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="docker compose -f $REPO_ROOT/docker/docker-compose.yml --env-file $REPO_ROOT/docker/.env"

# The Flink job must NOT be running when you do this - a live consumer will just
# re-commit its current offset on the next checkpoint and undo the reset.
echo "==> Resetting group '$GROUP' on topic '$TOPIC' to earliest"
echo "    (make sure the consuming Flink job is stopped first)"
$COMPOSE exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group "$GROUP" \
    --topic "$TOPIC" \
    --reset-offsets --to-earliest --execute

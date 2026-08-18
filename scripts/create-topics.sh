#!/usr/bin/env bash
# Create k8s-audit-raw and k8s-audit-ecs if they don't already exist. Safe to
# re-run - --if-not-exists makes this idempotent.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="docker compose -f $REPO_ROOT/docker/docker-compose.yml --env-file $REPO_ROOT/docker/.env"

# 3 partitions is a lab default, not a sized decision - one k3s node produces low
# enough volume that partition count won't matter until you're testing Flink
# parallelism deliberately. Replication factor 1 because there's only one broker.
for topic in k8s-audit-raw k8s-audit-ecs; do
    echo "==> Creating topic: $topic"
    $COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions 3 \
        --replication-factor 1
done

echo "==> Creating topic: k8s-allowlist (compacted)"
$COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "k8s-allowlist" \
    --partitions 1 \
    --replication-factor 1 \
    --config cleanup.policy=compact \
    --config min.cleanable.dirty.ratio=0.01 \
    --config segment.ms=60000

echo "==> Current topics:"
$COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

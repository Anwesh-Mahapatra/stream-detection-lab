#!/usr/bin/env bash
# One-time (but safe to re-run) setup: generate docker/.env with a fresh Kafka
# cluster ID, create any host directories the stack expects. Idempotent - running
# it twice does not regenerate .env or clobber existing state.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$REPO_ROOT/docker/.env"
ENV_EXAMPLE="$REPO_ROOT/docker/.env.example"

echo "==> Checking prerequisites"
command -v docker >/dev/null || { echo "docker not found on PATH" >&2; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "docker compose plugin not found" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 not found on PATH" >&2; exit 1; }

if [ -r /var/log/k3s-audit/audit.log ]; then
    echo "==> k3s audit log found and readable: /var/log/k3s-audit/audit.log"
else
    echo "==> WARNING: /var/log/k3s-audit/audit.log not readable by $(whoami)." >&2
    echo "    Fluent Bit will need read access to this path - see README." >&2
fi

if [ -f "$ENV_FILE" ]; then
    echo "==> docker/.env already exists, leaving it alone"
else
    echo "==> Generating docker/.env from .env.example"
    cp "$ENV_EXAMPLE" "$ENV_FILE"

    CLUSTER_ID=$(python3 -c "import base64, uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('='))")
    # BSD sed (macOS) needs -i '', GNU sed (Linux) needs -i with no arg following -
    # this form works on both without a portability branch.
    sed -i.bak "s/^KAFKA_CLUSTER_ID=.*/KAFKA_CLUSTER_ID=${CLUSTER_ID}/" "$ENV_FILE"
    rm -f "$ENV_FILE.bak"
    echo "    Generated Kafka cluster ID: ${CLUSTER_ID}"
fi

if [ -d /var/lib/fluent-bit ]; then
    echo "==> /var/lib/fluent-bit already exists (Fluent Bit position DB lives here)"
else
    echo "==> /var/lib/fluent-bit missing - fluent-bit/fluent-bit.conf writes its"
    echo "    position DB there. Since audit.log is root-owned, Fluent Bit likely"
    echo "    runs as root too, so create it with: sudo mkdir -p /var/lib/fluent-bit"
fi

echo "==> Done. Next: make up"

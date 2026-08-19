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

    # Kibana mints a throwaway Encrypted Saved Objects key at every boot unless one
    # is pinned, and then Alerting/Actions fail with "missing encryption key". Shipping
    # the .env.example placeholder would work but would give every clone the same
    # well-known key, so generate real ones here. Never regenerate for an existing
    # .env - that would orphan anything already encrypted with the old key.
    for KEY_NAME in KIBANA_ESO_ENCRYPTION_KEY \
                    KIBANA_REPORTING_ENCRYPTION_KEY \
                    KIBANA_SECURITY_ENCRYPTION_KEY; do
        KEY_VALUE=$(python3 -c "import secrets; print(secrets.token_hex(16))")
        sed -i.bak "s/^${KEY_NAME}=.*/${KEY_NAME}=${KEY_VALUE}/" "$ENV_FILE"
        rm -f "$ENV_FILE.bak"
    done
    echo "    Generated 3 Kibana encryption keys"

    # Elasticsearch security is on, so these must be real values, not placeholders.
    # kibana_system's password is only half-set here: this writes it into .env, and
    # the "post-start" step below pushes it into Elasticsearch itself.
    for PW_NAME in ELASTIC_PASSWORD KIBANA_SYSTEM_PASSWORD FLUENTBIT_ES_PASSWORD; do
        PW_VALUE=$(python3 -c "import secrets, string; a=string.ascii_letters+string.digits; print(''.join(secrets.choice(a) for _ in range(24)))")
        sed -i.bak "s/^${PW_NAME}=.*/${PW_NAME}=${PW_VALUE}/" "$ENV_FILE"
        rm -f "$ENV_FILE.bak"
    done
    echo "    Generated Elasticsearch passwords (elastic, kibana_system, fluentbit_writer)"
    echo
    echo "    NOTE: after 'make up', finish security setup by running:"
    echo "      scripts/setup-es-security.sh"
    echo "    That sets the kibana_system password inside Elasticsearch and creates the"
    echo "    least-privilege fluentbit_writer account. Kibana will not start until the"
    echo "    kibana_system password matches what is in docker/.env."

    # .env now holds secrets, not just tuning knobs.
    chmod 600 "$ENV_FILE"
fi

if [ -d /var/lib/fluent-bit ]; then
    echo "==> /var/lib/fluent-bit already exists (Fluent Bit position DB lives here)"
else
    echo "==> /var/lib/fluent-bit missing - fluent-bit/fluent-bit.conf writes its"
    echo "    position DB there. Since audit.log is root-owned, Fluent Bit likely"
    echo "    runs as root too, so create it with: sudo mkdir -p /var/lib/fluent-bit"
fi

echo "==> Done. Next: make up"

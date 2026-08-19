#!/usr/bin/env bash
# Finishes Elasticsearch security setup after 'make up'. Idempotent - safe to re-run.
#
# ELASTIC_PASSWORD is bootstrapped by Elasticsearch itself on first start, but two
# things cannot be: the kibana_system password (a built-in user with no password
# until one is pushed via the API) and the least-privilege Fluent Bit ingest account.
# This script does both, reading the values from docker/.env.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$REPO_ROOT/docker/.env"
ES="${ES_URL:-http://127.0.0.1:9200}"

[ -f "$ENV_FILE" ] || { echo "docker/.env not found - run scripts/bootstrap.sh first" >&2; exit 1; }

get() { grep -oP "(?<=^$1=).*" "$ENV_FILE" || true; }
ELASTIC_PASSWORD=$(get ELASTIC_PASSWORD)
KIBANA_SYSTEM_PASSWORD=$(get KIBANA_SYSTEM_PASSWORD)
FLUENTBIT_ES_USER=$(get FLUENTBIT_ES_USER)
FLUENTBIT_ES_PASSWORD=$(get FLUENTBIT_ES_PASSWORD)

for v in ELASTIC_PASSWORD KIBANA_SYSTEM_PASSWORD FLUENTBIT_ES_USER FLUENTBIT_ES_PASSWORD; do
    val="${!v}"
    if [ -z "$val" ] || [[ "$val" == CHANGE_ME* ]]; then
        echo "$v is unset or still a placeholder in docker/.env - run scripts/bootstrap.sh" >&2
        exit 1
    fi
done

echo "==> Waiting for Elasticsearch at $ES"
for i in $(seq 1 60); do
    if curl -s -f -u "elastic:$ELASTIC_PASSWORD" "$ES/_cluster/health" >/dev/null 2>&1; then
        echo "    reachable and authenticating"
        break
    fi
    [ "$i" = 60 ] && { echo "timed out. Is security enabled and ELASTIC_PASSWORD correct?" >&2; exit 1; }
    sleep 2
done

echo "==> Setting kibana_system password"
curl -s -f -X POST -u "elastic:$ELASTIC_PASSWORD" \
    "$ES/_security/user/kibana_system/_password" \
    -H 'Content-Type: application/json' \
    -d "{\"password\":\"$KIBANA_SYSTEM_PASSWORD\"}" >/dev/null
echo "    done"

echo "==> Creating role k8s_audit_writer"
# Deliberately narrow: the sink only ever writes k8s-audit*. auto_configure is needed
# so Fluent Bit can create the index and let mappings grow on first write.
curl -s -f -X PUT -u "elastic:$ELASTIC_PASSWORD" "$ES/_security/role/k8s_audit_writer" \
    -H 'Content-Type: application/json' -d '{
      "cluster": ["monitor"],
      "indices": [{
        "names": ["k8s-audit*"],
        "privileges": ["create_index","create_doc","index","write","view_index_metadata","auto_configure"]
      }]
    }' >/dev/null
echo "    done"

echo "==> Creating user $FLUENTBIT_ES_USER"
curl -s -f -X PUT -u "elastic:$ELASTIC_PASSWORD" "$ES/_security/user/$FLUENTBIT_ES_USER" \
    -H 'Content-Type: application/json' \
    -d "{\"password\":\"$FLUENTBIT_ES_PASSWORD\",\"roles\":[\"k8s_audit_writer\"],\"full_name\":\"Fluent Bit kafka-to-es sink\"}" >/dev/null
echo "    done"

echo
echo "==> Security setup complete."
echo "    Kibana login:  elastic / (see ELASTIC_PASSWORD in docker/.env)"
echo "    Restart Kibana so it picks up the kibana_system password:"
echo "      docker compose -f docker/docker-compose.yml --env-file docker/.env up -d kibana"
echo "    Before starting Fluent Bit's kafka-to-es sink:"
echo "      export ES_USER=$FLUENTBIT_ES_USER"
echo "      export ES_PASSWORD=\$(grep -oP '(?<=^FLUENTBIT_ES_PASSWORD=).*' docker/.env)"

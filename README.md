# stream-detection-lab

k3s audit events, streamed through Kafka and transformed in PyFlink instead of in
Fluent Bit or an Elasticsearch ingest pipeline. Built to learn Kafka + Flink on top
of detection engineering fundamentals already proven out elsewhere.

## Architecture

```
k3s audit.log
   -> Fluent Bit          (ship only, zero transform)
   -> Kafka: k8s-audit-raw
   -> Flink (PyFlink)     (raw JSON -> ECS)
   -> Kafka: k8s-audit-ecs
   -> Flink Elasticsearch sink
   -> Elasticsearch
```

Two Kafka topics, on purpose:
- **k8s-audit-raw** and **k8s-audit-ecs** side by side means the raw event and its
  parsed ECS output can be diffed directly - no guessing what the parser did to it.
- Raw events sit in Kafka with 7-day retention. Change the parser, reset the
  consumer group offset (`scripts/replay-topic.sh`), and every event gets
  re-transformed with the new logic - no need to wait for new audit events.

Fluent Bit runs on the **host**, not in Docker - it needs to read
`/var/log/k3s-audit/audit.log`, which is root-owned and lives outside any
container's filesystem. Everything else runs in Docker Compose.

## The Kafka dual-listener setup

Kafka is configured with two listeners for the same broker:

| Listener | Address | Used by |
|---|---|---|
| `BROKER` | `kafka:9092` | Flink, running inside the compose network |
| `HOST`   | `localhost:29092` | Fluent Bit, running on the host |

Containers on the compose network resolve the hostname `kafka`; your host machine
cannot (that name only exists inside Docker's internal DNS). Conversely, a process
running in a container that tried to connect to `localhost:29092` would hit itself,
not the Kafka container - `localhost` inside a container is that container.

Kafka's own advertised-listener negotiation makes this necessary: a client's
first connection goes to whatever address it was given, but the broker then tells
it the *advertised* address to use for actual produce/consume traffic. If both
listeners advertised the same address, one side of this split would get an address
it can't route to. Hence `KAFKA_ADVERTISED_LISTENERS` maps `BROKER` and `HOST` to
two different strings for what is physically the same port.

Only `HOST` (29092) is published to `127.0.0.1` in `docker-compose.yml`. `BROKER`
(9092) never needs to leave the compose network.

## RAM budget

Configured heap / process-size caps (see `docker/.env.example`):

| Service | Setting | Configured cap |
|---|---|---|
| Kafka | `KAFKA_HEAP_OPTS` | 1.0 GB heap |
| Flink JobManager | `FLINK_JM_PROCESS_SIZE` | 1.0 GB total |
| Flink TaskManager | `FLINK_TM_PROCESS_SIZE` | 1.5 GB total |
| Elasticsearch | `ES_JAVA_OPTS` | 1.0 GB heap |
| Kibana | `KIBANA_MAX_OLD_SPACE_MB` | 1.0 GB V8 heap |

Configured caps add up to ~5.5 GB. Real usage runs higher than configured heap
because of JVM/Node overhead outside the heap (metaspace, off-heap buffers, native
allocations) - **budget 7-8 GB actually resident while the full stack is running**,
next to whatever k3s itself is using. All values are in `docker/.env` (generated
from `.env.example` by `scripts/bootstrap.sh`) - lower them if this box gets tight.

## Prerequisites

- Docker + Docker Compose plugin
- `vm.max_map_count >= 262144` (Elasticsearch bootstrap check). Check with
  `sysctl vm.max_map_count`; set with
  `sudo sysctl -w vm.max_map_count=262144` if it's lower.
- Read access to `/var/log/k3s-audit/audit.log` for whatever user runs Fluent Bit
  (it's root-owned by default, so Fluent Bit will likely need to run as root)

## Bringing it up

```bash
scripts/bootstrap.sh   # generates docker/.env, checks prerequisites
make up                # builds the Flink image, starts everything
make topics             # creates k8s-audit-raw and k8s-audit-ecs
```

Then start Fluent Bit separately (outside Docker) pointed at
`fluent-bit/fluent-bit.conf` - e.g. `fluent-bit -c fluent-bit/fluent-bit.conf` run
as root, or wired into a systemd unit.

## Verifying each hop

```bash
# k3s is actually writing audit events
sudo tail -f /var/log/k3s-audit/audit.log

# every container reports healthy
docker compose -f docker/docker-compose.yml --env-file docker/.env ps

# Kafka broker responds
docker compose -f docker/docker-compose.yml --env-file docker/.env exec kafka \
    /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# raw events are landing in Kafka (Ctrl+C to stop)
docker compose -f docker/docker-compose.yml --env-file docker/.env exec kafka \
    /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic k8s-audit-raw --max-messages 1

# Flink cluster is up
curl -s http://127.0.0.1:8081/overview

# Elasticsearch is up (note the quotes - see "known trade-offs" on why that matters)
curl -s 'http://127.0.0.1:9200/_cluster/health?pretty'

# Kibana is up
curl -s http://127.0.0.1:5601/api/status
```

## Known trade-offs

- **Elasticsearch security is disabled** (`xpack.security.enabled: false`). This
  was already proven end-to-end (TLS, private CA, least-privilege ingest user) in
  a different setup - redoing it here would slow down the part of this repo that's
  actually about learning Kafka/Flink. **TODO: re-enable before this touches
  anything beyond localhost.**
- **Single Kafka broker, replication factor 1.** No HA. Fine for a lab; a broker
  restart means a brief outage, not just a failover.
- **Single Flink TaskManager, 2 slots.** Enough to prove the pipeline works, not
  enough to learn real parallelism/backpressure behavior - bump
  `FLINK_TM_NUM_SLOTS` (and add TaskManager replicas) when that's the goal.
- **No Elasticsearch connector jar is installed for Flink.** Flink's official
  Elasticsearch connector only ships `elasticsearch7`/`elasticsearch8` builds, and
  the latest `elasticsearch8` connector build targets Flink 2.0, not the 1.18.1
  pinned here - there isn't a clean, verified jar for "Flink 1.18 -> ES 9.x" yet.
  This needs a real decision (find/verify a compatible connector build, or write a
  custom sink) before `pipeline/jobs/k8s_audit_job.py`'s ES-sink half can work.
  Not solved here on purpose - it's an open question, not a hidden one.
- **No auth anywhere in this stack** - Kafka listeners are PLAINTEXT, ES has no
  users. Acceptable because everything binds to `127.0.0.1` only.
- **PyFlink is pinned to 1.18.1**, one release behind the newest available
  (`apache-flink` has gone past 2.0 upstream). Chosen deliberately for a
  well-documented, stable Flink-image/PyFlink-version pairing rather than chasing
  the newest release and hitting undocumented breakage.

## Replaying events after a parser change

```bash
# stop the running job first, then:
scripts/replay-topic.sh k8s-audit-raw <your-consumer-group-id>
make submit
```

## Allowlist enrichment (in progress)

```
k3s audit.log
   -> Fluent Bit          (ship only, zero transform)
   -> Kafka: k8s-audit-raw
   -> Flink (PyFlink)     (raw JSON -> ECS, broadcast-joined against
                            Kafka: k8s-allowlist, compacted, read from earliest())
   -> Kafka: k8s-audit-ecs
   -> Flink Elasticsearch sink
   -> Elasticsearch
```

`k8s-allowlist` is a compacted topic keyed by `<namespace>:<username>`, whose
value is either a JSON grant or a tombstone (`null`, meaning revoked). The
Flink job folds the whole topic into broadcast state on startup so every
parallel instance ends up with the same allowlist, then stamps
`k8s.audit.approved_exec` onto exec-subresource events by checking the
requesting namespace+user against that state - everything else is passed
through unstamped. The pure logic lives in `pipeline/enrich/allowlist.py`
(`apply_allowlist_record`, `stamp_approval`); the Flink wiring is sketched but
commented out in `pipeline/jobs/k8s_audit_job.py` until both functions are
implemented and the open questions in `decisions/allowlist_enrichment.txt`
(where in the chain to connect it, how the allowlist topic's Kafka key gets
read at all) are settled.

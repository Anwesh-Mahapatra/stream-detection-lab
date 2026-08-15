# STUB - I write this. The Flink job body (execution env, Kafka source/sink wiring,
# checkpointing, how parse_audit_event gets called per-record) is the thing I need
# to be able to defend line by line, so Claude is not allowed to fill this in.
#
# Per the architecture in the README, this covers the raw-topic -> ECS-topic hop.
# Whether the ECS-topic -> Elasticsearch hop is a second job in this same file, a
# separate file, or Flink SQL instead of the DataStream API is an open design
# decision - see README "known trade-offs" re: the Elasticsearch connector version
# question, which needs answering before that hop can be built either way.


def main() -> None:
    """
    STUB - implement this.

    Responsibilities this job needs to cover:
      - StreamExecutionEnvironment setup (parallelism, checkpointing interval/mode)
      - a KafkaSource reading k8s-audit-raw, starting from committed offsets
      - a map/process step calling pipeline.parsers.k8s_audit.parse_audit_event
        per record, plus a decision on what happens to records that fail to parse
      - a KafkaSink writing the result to k8s-audit-ecs
    """
    raise NotImplementedError("k8s_audit_job.main: build the Flink pipeline")


if __name__ == "__main__":
    main()

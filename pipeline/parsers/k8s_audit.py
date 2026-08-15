# STUB - I write this. The k8s audit JSON -> ECS mapping is the thing I need to be
# able to defend line by line, so Claude is not allowed to fill this in.


def parse_audit_event(raw: dict) -> dict:
    """
    STUB - implement this.

    Input: one decoded JSON object from the k8s-audit-raw Kafka topic - a single
    Kubernetes audit Event exactly as the API server wrote it to audit.log.

    Output: a dict shaped like ECS, ready to be re-serialized onto k8s-audit-ecs.

    This is where the exec/response-code/auditID/URL-decoding rules already worked
    out by hand belong. Keep it pure - no I/O, no Kafka or Flink imports - so it can
    be unit tested from tests/test_k8s_audit_parser.py without a running cluster.
    """
    raise NotImplementedError("parse_audit_event: fill in the ECS mapping")

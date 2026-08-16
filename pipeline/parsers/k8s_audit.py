from urllib.parse import urlparse, parse_qs

DATASET = "k8s.audit"

def _to_millis(ts: str | None) -> str | None:
    if not ts or "." not in ts:
        return ts

    head, frac = ts.split(".")
    return f"{head}.{frac[:3]}Z"

#101 is a websocket upgrade, not a failure.
def _outcome(code: int | None) -> str:
    if code is None:
        return "unknown"
    return "success" if code < 400 else "failure"

def parse_audit_event(raw: dict) -> dict | None:
    if not isinstance(raw, dict) or raw.get("kind") != "Event":
        return None

    stage = raw.get("stage")
    obj = raw.get("objectRef", {})
    code = raw.get("responseStatus", {}).get("code")
    subresource = obj.get("subresource")
    action = subresource if subresource else raw.get("verb")

    return {
        "@timestamp": _to_millis(raw.get("stageTimestamp")),
        "event": {
            "id": raw.get("auditID"),
            "start": raw.get("requestReceivedTimestamp"),
            "end": raw.get("stageTimestamp"),
            "type": ["start"] if stage == "ResponseStarted" else ["end"],
            "kind": "event",
            "action": action,
            "outcome": _outcome(code),
            "dataset": DATASET,
        },
        "user": {
            "name": raw.get("user", {}).get("username"),
            "id": raw.get("user", {}).get("uid"),
            "roles": raw.get("user", {}).get("groups", []),
        },
        "source": {"ip": (raw.get("sourceIPs") or [None])[0],},
        "k8s": {
            "audit": {
                "stage": stage,
                "level": raw.get("level"),
                "verb": raw.get("verb"),
                "subresource": obj.get("subresource"),
                "request_uri": raw.get("requestURI"),
                "stage_timestamp_raw": raw.get("stageTimestamp"),
                "row_id": f"{raw.get('auditID')}:{stage}",
                "exec_command": parse_qs(
                    urlparse(raw.get("requestURI") or "").query,
                    keep_blank_values=True,
                ).get("command", []),
                "authz_decision": raw.get("annotations", {}).get(
                    "authorization.k8s.io/decision"
                ),
            }
        },
        "orchestrator": {
            "type": "kubernetes",
            "cluster": {"name": raw.get("cluster")},
            "namespace": obj.get("namespace"),
            "resource": {
                "type": obj.get("resource"),
                "name": obj.get("name"),
            },
        },
        "http": {
            "request": {"method": raw.get("verb")},
            "response": {"status_code": raw.get("responseStatus", {}).get("code")},
        },
        "user_agent": {"original": raw.get("userAgent")},
    }

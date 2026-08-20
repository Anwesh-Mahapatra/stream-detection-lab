import json


def apply_allowlist_record(state: dict, key: str, value: str | None) -> None:
    if value is None:
        state.pop(key,None)
        return

    parsed = json.loads(value)
    state[key] = parsed

def stamp_approval(doc: dict, state: dict) -> dict:
    # exec-only gate
    if doc["k8s"]["audit"]["subresource"] != "exec":
        return doc

    cluster = doc["orchestrator"]["cluster"]["name"]
    groups = doc["user"].get("roles", []) or []

    # approved if ANY of the user's groups is allowlisted on this cluster
    approved = any(f"{cluster}:{group}" in state for group in groups)

    doc["k8s"]["audit"]["approved_exec"] = approved
    return doc
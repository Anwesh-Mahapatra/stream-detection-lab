import json


def apply_allowlist_record(state: dict, key: str, value: str | None) -> None:
    if value is None:
        state.pop(key,None)
        return

    parsed = json.loads(value)
    state[key] = parsed

def stamp_approval(doc: dict, state: dict) -> dict:
    if doc["k8s"]["audit"]["subresource"] != "exec":
        return doc

    #Building the lookup-key
    namespace = doc["orchestrator"]["namespace"]
    username = doc["user"]["name"]
    key = f"{namespace}:{username}"

    approved = key in state
    doc["k8s"]["audit"]["approved_exec"] = approved
    return doc
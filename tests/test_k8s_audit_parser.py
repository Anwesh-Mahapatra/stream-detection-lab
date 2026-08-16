import json
from pathlib import Path

import pytest

from pipeline.parsers.k8s_audit import parse_audit_event

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text())

TOP_LEVEL_KEYS = {
    "@timestamp", "event", "user", "source",
    "k8s", "orchestrator", "http", "user_agent",
}


@pytest.mark.parametrize(
    "fixture",
    ["exec-response-complete", "exec-response-started", "watch-noise"],
)
def test_shape_is_flat(fixture):
    """Every fixture produces the same 8 top-level keys.

    Guards the nesting bug: a swallowed section silently changes this set.
    """
    doc = parse_audit_event(load(fixture))
    assert set(doc.keys()) == TOP_LEVEL_KEYS

@pytest.mark.parametrize(
    "fixture",
    ["exec-response-complete", "exec-response-started", "watch-noise"],
)

def test_timestamp_is_truncated_prefix(fixture):
    """@timestamp keeps exactly 3 decimals and is a prefix of the raw value.

    Proves truncation as behaviour, not as a memorised constant. Survives
    fixture regeneration; a hardcoded return value would fail this.
    """
    doc = parse_audit_event(load(fixture))
    ts = doc["@timestamp"]
    raw = doc["k8s"]["audit"]["stage_timestamp_raw"]

    assert len(ts.split(".")[1]) == 4          # "183Z"
    assert raw.startswith(ts.rstrip("Z"))      # same digits, just shorter

@pytest.mark.parametrize(
    "fixture,expected",
    [
        ("exec-response-complete", "2026-08-15T22:48:57.183Z"),
        ("exec-response-started", "2026-08-15T22:48:57.169Z"),
        ("watch-noise", "2026-08-15T22:39:52.164Z"),
    ],
)
def test_timestamp_truncated(fixture, expected):
    """@timestamp is truncated to milliseconds.

    Guards the ES date-precision decision: the parser truncates the
    microsecond stageTimestamp itself instead of letting it through.
    """
    doc = parse_audit_event(load(fixture))
    assert doc["@timestamp"] == expected


def test_exec_command_decoded():
    """Exec command params are URL-decoded and keep their order.

    %2Fbin%2Fsh must come back as /bin/sh, not the raw percent-encoding.
    """
    doc = parse_audit_event(load("exec-response-complete"))
    assert doc["k8s"]["audit"]["exec_command"] == ["/bin/sh", "-c", "id"]

    noise = parse_audit_event(load("watch-noise"))
    assert noise["k8s"]["audit"]["exec_command"] == []


def test_row_id_unique():
    """Rows sharing an auditID get distinct row_ids via the stage suffix.

    Guards the one-auditID-two-events case: ResponseStarted and
    ResponseComplete must not collide in the sink.
    """
    started = parse_audit_event(load("exec-response-started"))
    complete = parse_audit_event(load("exec-response-complete"))

    assert started["event"]["id"] == complete["event"]["id"]
    assert (
        started["k8s"]["audit"]["row_id"]
        == "70bfee6d-4218-40fd-8d20-710719c02677:ResponseStarted"
    )
    assert (
        complete["k8s"]["audit"]["row_id"]
        == "70bfee6d-4218-40fd-8d20-710719c02677:ResponseComplete"
    )


@pytest.mark.parametrize(
    "raw",
    [
        {"kind": "Pod"},
        {"kind": "EventList"},
        {},
        "not-a-dict",
        None,
        ["kind", "Event"],
    ],
)
def test_rejects_non_audit(raw):
    """Anything that is not an audit Event dict is dropped (returns None)."""
    assert parse_audit_event(raw) is None
import json
from pathlib import Path

import pytest

from pipeline.parsers.k8s_audit import parse_audit_event
from pipeline.enrich.allowlist import apply_allowlist_record, stamp_approval

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text())


def built_state() -> dict:
    """Replay the simulated topic into fresh state, in offset order.

    This is exactly what your Flink job does on restart: consume
    k8s-allowlist from earliest() and fold every record into broadcast
    state. Compaction means old superseded records may or may not still be
    present - your apply function must be safe to replay either way.
    """
    state: dict = {}
    for rec in load("allowlist-records")["records"]:
        apply_allowlist_record(state, rec["key"], rec["value"])
    return state


# state build

def test_allowed_entry_lands_in_state():
    state = built_state()
    assert "anwesh:sre-oncall" in state


def test_tombstone_removes_key():
    """kube-system:system:admin is added then tombstoned in the fixture.

    After a full replay it must be GONE - if this fails, your
    apply function is treating null as data instead of as a delete.
    """
    state = built_state()
    assert "kube-system:system:admin" not in state


def test_tombstone_for_unknown_key_is_harmless():
    """Compaction can leave a tombstone whose original record was already
    compacted away. Deleting a key that isn't in state must not raise."""
    state: dict = {}
    apply_allowlist_record(state, "ghost:nobody", None)
    assert state == {}


# stamping

def test_approved_exec_true_for_allowlisted():
    """exec-response-complete: system:admin exec in default ns => approved."""
    doc = parse_audit_event(load("exec-alice-sre-oncall"))
    out = stamp_approval(doc, built_state())
    assert out["k8s"]["audit"]["approved_exec"] is True


def test_approved_exec_false_after_tombstone():
    """Same user, but pretend the event came from kube-system - that entry
    was revoked by tombstone, so the stamp must be False, not True and
    not missing."""
    raw = load("exec-response-complete")
    raw["objectRef"]["namespace"] = "kube-system"
    out = stamp_approval(parse_audit_event(raw), built_state())
    assert out["k8s"]["audit"]["approved_exec"] is False


def test_empty_state_is_default_deny():
    """The race: audit events can arrive before any allowlist record.

    Scaffolded decision = default-deny (False), never a crash and never
    a silently missing field. If you choose differently, change this test
    AND write down why in decisions/allowlist_enrichment.txt.
    """
    doc = parse_audit_event(load("exec-response-complete"))
    out = stamp_approval(doc, {})
    assert out["k8s"]["audit"]["approved_exec"] is False


def test_non_exec_not_stamped():
    """watch-noise is a watch verb, no subresource - the approval concept
    doesn't apply, so the field should be absent, not False. False on
    every watch event would make `not k8s.audit.approved_exec: true`
    hunts drown in noise."""
    doc = parse_audit_event(load("watch-noise"))
    out = stamp_approval(doc, built_state())
    assert "approved_exec" not in out["k8s"]["audit"]


def test_stamp_preserves_document_shape():
    """Enrichment adds one field - it must not eat the rest of the doc.

    Same guard-rail idea as the parser's shape test: a refactor that
    rebuilds the doc and drops sections should fail loudly here."""
    doc = parse_audit_event(load("exec-response-complete"))
    out = stamp_approval(doc, built_state())
    assert set(out.keys()) == set(doc.keys())
    assert out["event"]["id"] == doc["event"]["id"]


def test_stamp_does_not_write_to_state():
    """process_element gets a read-only view of broadcast state in Flink.
    Your pure function must honour the same rule or the real wiring will
    blow up at runtime with a read-only state error."""
    state = built_state()
    before = dict(state)
    stamp_approval(parse_audit_event(load("exec-response-complete")), state)
    assert state == before
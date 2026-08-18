import json


def apply_allowlist_record(state: dict, key: str, value: str | None) -> None:
    """Fold one record from the k8s-allowlist compacted topic into broadcast state.

    This is the replay step: called once per record, in offset order, both
    on startup (replaying the whole topic from earliest()) and live as new
    grants/revokes land. `key` is already "<namespace>:<username>" - the
    same shape produced on the lookup side in stamp_approval, and the same
    shape used in tests/fixtures/allowlist-records.json. `value` is either
    a JSON string (a grant) or None (a tombstone - Kafka's compaction
    convention for "delete this key"). Mutates `state` in place; returns
    nothing, same as dict.pop()/dict.__setitem__ would.

    See tests/test_allowlist_enrich.py, "state build" section, for the
    full contract this must satisfy.
    """
    # 1. Tombstone check: value is None means DELETE this key.
    #    Why: compacted topics signal deletion with a null value, and
    #    test_tombstone_for_unknown_key_is_harmless means the delete must
    #    not raise even when the key was never present (e.g. its original
    #    grant record was itself already compacted away before this replay
    #    started). test_tombstone_removes_key checks the positive case -
    #    a key that WAS present must actually be gone afterward, not just
    #    marked or nulled out in place.

    # 2. Parse the value as JSON.
    #    Why: the allowlist producer writes structured records (see the
    #    "value" strings in tests/fixtures/allowlist-records.json) so this
    #    function has to turn the wire string back into something usable
    #    before it goes in state - test_allowed_entry_lands_in_state only
    #    checks the key ends up in state, but stamp_approval later needs
    #    to trust whatever you store here is decoded, not still JSON text.

    # 3. Store the parsed value under `key` in `state`.
    #    Why: this is the only write path into broadcast state - decide
    #    here whether last-write-wins overwrite is enough (it is, as long
    #    as records are applied in offset order, which built_state() in
    #    the test file guarantees by construction).

    raise NotImplementedError


def stamp_approval(doc: dict, state: dict) -> dict:
    """Stamp an ECS audit doc with whether its exec call is allowlisted.

    Called per-event, read-only against `state` - this is the lookup side
    that mirrors apply_allowlist_record's write side. Only touches docs
    representing an exec subresource call; every other verb/subresource is
    passed through with no new field at all, since "approved_exec" doesn't
    mean anything for a watch or a get. Must not mutate `state` - in the
    real Flink job this runs in process_element, which only ever gets a
    read-only view of broadcast state.

    See tests/test_allowlist_enrich.py, "stamping" section, for the full
    contract this must satisfy.
    """
    # 1. Build the lookup key from the doc: "<namespace>:<username>".
    #    Why: this has to exactly match the key format written on the
    #    other side by apply_allowlist_record / the allowlist producer.
    #    The parser (pipeline/parsers/k8s_audit.py) puts these at:
    #      - namespace -> doc["orchestrator"]["namespace"]   (from objectRef.namespace)
    #      - username  -> doc["user"]["name"]                (from user.username)
    #    test_approved_exec_true_for_allowlisted's fixture has namespace
    #    "default" and username "system:admin", which is exactly the
    #    "default:system:admin" key seeded in
    #    tests/fixtures/allowlist-records.json - if the key you build here
    #    doesn't match that string byte-for-byte, the lookup silently misses.

    # 2. Exec-only gate: is this doc even an exec call?
    #    Why: doc["k8s"]["audit"]["subresource"] is the parser's direct
    #    copy of objectRef.subresource, set to "exec" only for exec calls -
    #    test_non_exec_not_stamped (watch-noise fixture, subresource is
    #    None) requires "approved_exec" to be ABSENT, not False, for
    #    anything that isn't exec. Stamping every event with False would
    #    make the field useless for `NOT approved_exec: true` hunting -
    #    every watch/get/list would falsely look like a denied exec.

    # 3. If it is exec: look up the key in `state` and stamp the result.
    #    Why: `key in state` alone gives you default-deny for free -
    #    test_empty_state_is_default_deny (state == {}) and
    #    test_approved_exec_false_after_tombstone (key was revoked, so
    #    apply_allowlist_record already removed it) both just need "not
    #    present -> False". You should not need a separate branch for
    #    "state is empty" vs "key was tombstoned" - they're the same case.

    # 4. Return a doc with exactly one field added, nothing else touched.
    #    Why: test_stamp_preserves_document_shape checks the top-level key
    #    set is unchanged and that unrelated fields (event.id) survive
    #    untouched - decide here whether to mutate `doc` in place and
    #    return it, or copy it first. Mutating is cheaper; copying protects
    #    whoever else is holding a reference to the original doc. Your call.

    # 5. Never write to `state` - read-only, full stop.
    #    Why: test_stamp_does_not_write_to_state asserts `state` is
    #    byte-for-byte unchanged after this call. More importantly, this
    #    is what process_element's broadcast state actually enforces at
    #    runtime in Flink (see the wiring skeleton in
    #    pipeline/jobs/k8s_audit_job.py) - a real write attempt there
    #    raises, it doesn't just fail a test.

    raise NotImplementedError

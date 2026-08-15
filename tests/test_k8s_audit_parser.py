# Harness only - Claude wrote the fixture-loading plumbing, but every real
# assertion about what the parser should output is mine to write. That's the part
# that encodes "I understand k8s audit semantics," so it doesn't get outsourced.

import json
from pathlib import Path

import pytest

from pipeline.parsers.k8s_audit import parse_audit_event

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixtures():
    """Yield (path, parsed_json) for every captured audit event fixture."""
    for path in sorted(FIXTURES_DIR.glob("*.json")):
        yield path, json.loads(path.read_text())


@pytest.mark.xfail(reason="parse_audit_event is not implemented yet", strict=False)
def test_parser_runs_on_first_fixture():
    """
    Placeholder: proves the harness wiring (import, fixture loading, function call)
    works end to end. Replace/extend with real assertions once fixtures exist and
    parse_audit_event has a body - e.g. asserting the exec/response-code/auditID
    rules from the README actually land in the right ECS fields.
    """
    fixtures = list(load_fixtures())
    if not fixtures:
        pytest.skip("no fixtures captured yet in tests/fixtures/")
    _, raw_event = fixtures[0]
    parse_audit_event(raw_event)

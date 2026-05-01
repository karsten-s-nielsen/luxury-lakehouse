"""Parser-level contract test for fct_action_values post-PR 4b Kimball migration.

Asserts the dbt YAML contract declares the expected columns + types +
tests. Complementary to dbt's own ``contract: enforced: true`` — catches
YAML drift without requiring a live dbt build.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

_MARTS_YAML = Path(__file__).resolve().parent.parent.parent / "dbt_project" / "models" / "marts" / "_marts__models.yml"


def _load_fct_action_values_entry() -> dict:
    if not _MARTS_YAML.exists():
        pytest.skip(f"{_MARTS_YAML} not found at expected path")
    doc = yaml.safe_load(_MARTS_YAML.read_text(encoding="utf-8"))
    for model in doc.get("models", []):
        if model.get("name") == "fct_action_values":
            return model
    pytest.fail("fct_action_values not found in _marts__models.yml")
    raise AssertionError  # pragma: no cover — satisfies return type for pyright


def _has_test(data_tests: list | None, test_name: str) -> bool:
    """Return True if a generic dbt test ``test_name`` appears in
    ``data_tests``, recognising BOTH bare-string and configured-dict forms.

    dbt allows either::

        data_tests:
          - not_null                                 # bare string
          - not_null:                                # dict with config
              config:
                where: "..."
                enabled: "{{ var(...) }}"

    Session 69 introduced var-gated `not_null` tests on `match_key` and
    legacy `match_id` (chicken-and-egg post-deploy gating per ADR-018);
    those land as the dict form, so a plain ``"not_null" in tests``
    membership check no longer matches. This helper centralises the
    both-form recognition.
    """
    for t in data_tests or []:
        if t == test_name:
            return True
        if isinstance(t, dict) and test_name in t:
            return True
    return False


class TestFctActionValuesContract:
    def test_contract_enforced(self) -> None:
        entry = _load_fct_action_values_entry()
        assert entry.get("config", {}).get("contract", {}).get("enforced") is True

    def test_match_key_not_null(self) -> None:
        entry = _load_fct_action_values_entry()
        cols = {c["name"]: c for c in entry.get("columns", [])}
        assert "match_key" in cols, "match_key must be in contract post-PR-4b"
        mk = cols["match_key"]
        assert mk.get("data_type", "").lower() == "bigint"
        assert _has_test(mk.get("data_tests"), "not_null"), (
            "match_key must declare a not_null data test (var-gated under "
            "include_post_deploy_tests is acceptable; bare not_null also OK)"
        )

    def test_competition_key_present_nullable(self) -> None:
        entry = _load_fct_action_values_entry()
        cols = {c["name"]: c for c in entry.get("columns", [])}
        assert "competition_key" in cols, "competition_key must be in contract post-PR-4b"
        ck = cols["competition_key"]
        assert ck.get("data_type", "").lower() == "bigint"
        assert not _has_test(ck.get("data_tests"), "not_null"), "competition_key is intentionally nullable post-PR-4b"

    def test_legacy_match_id_retained(self) -> None:
        entry = _load_fct_action_values_entry()
        cols = {c["name"]: c for c in entry.get("columns", [])}
        assert "match_id" in cols, "Legacy match_id must be retained for 90-day window"
        mid = cols["match_id"]
        assert mid.get("data_type", "").lower() == "bigint"
        desc = (mid.get("description") or "").lower()
        assert any(tok in desc for tok in ("legacy", "sunset", "deprecat")), (
            "match_id description should note legacy / sunset status"
        )

    def test_legacy_competition_id_retained(self) -> None:
        entry = _load_fct_action_values_entry()
        cols = {c["name"]: c for c in entry.get("columns", [])}
        assert "competition_id" in cols, "Legacy competition_id must be retained for 90-day window"

    def test_action_value_id_unique_and_not_null(self) -> None:
        entry = _load_fct_action_values_entry()
        cols = {c["name"]: c for c in entry.get("columns", [])}
        avid = cols["action_value_id"]
        tests = avid.get("data_tests")
        assert _has_test(tests, "unique")
        assert _has_test(tests, "not_null")

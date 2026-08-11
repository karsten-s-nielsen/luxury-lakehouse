"""The bronze ``sources.yml`` generator: append-only, description-preserving, fixer == checker.

Modelled on ``scripts/_tf_env_pins.py`` + ``scripts/sync_tf_env_pins.py``, whose pure core makes
the fixer and the ``--check`` mode the same code — the property that stops a sync tool drifting
from the gate that guards it (``test_terraform_env_dep_parity.py`` is the reference).

The assertions here encode two failures this repo has already paid for:

* **Append-only is not "appended at EOF".** dbt columns splice INSIDE each table's ``columns:``
  block, so an ``after.startswith(before)`` check fails on the correct implementation and passes
  on the dump-everything-at-EOF bug. Assert subsequence.
* **Placement must be verified structurally.** PR-2a's first splice put IDSSE's columns under
  ``elastic_sync_results`` while reporting success, because indentation auto-detection matched
  the SOURCE entry (indent 2) rather than table entries (indent 6). Both files stayed valid YAML
  throughout, so only a per-table membership check catches it.
"""

from __future__ import annotations

import yaml

from scripts._bronze_sources_sync import (
    apply_missing_columns,
    needs_yaml_quoting,
    plan_missing_columns,
    render_column_block,
)

_FIXTURE_YML = """version: 2

sources:
  - name: demo
    description: Demo source.
    database: soccer_analytics
    schema: bronze

    tables:
      - name: table_a
        description: >
          First table.
        columns:
          - name: match_id
            description: Native match ID (numeric string, e.g. '10502')
          - name: _ingested_at
            description: UTC timestamp when the row was ingested
"""

_FIXTURE_SNAPSHOT = {
    "tables": {
        "table_a": [
            {"name": "match_id", "type": "string"},
            {"name": "_ingested_at", "type": "timestamp"},
            {"name": "new_col_a", "type": "double"},
            {"name": "new_col_b", "type": "boolean"},
        ]
    }
}

_TWO_TABLE_YML = (
    _FIXTURE_YML
    + """      - name: table_b
        description: >
          Second table.
        columns:
          - name: match_id
            description: Native match ID
"""
)

_TWO_TABLE_SNAPSHOT = {
    "tables": {
        "table_a": [{"name": "match_id", "type": "string"}, {"name": "new_col_a", "type": "double"}],
        "table_b": [{"name": "match_id", "type": "string"}, {"name": "new_col_b", "type": "double"}],
    }
}


def _col_count(text: str, col_indent: int = 10) -> int:
    """Count COLUMN entries, not table entries.

    ``text.count("- name:")`` conflates tables (indent 6) with columns (indent 10). That happens
    to work on a single-table fixture and is wrong on every real file.
    """
    return sum(1 for line in text.splitlines() if line.startswith(" " * col_indent + "- name:"))


def test_plan_lists_only_missing_columns_in_snapshot_order() -> None:
    missing = plan_missing_columns(_FIXTURE_SNAPSHOT, _FIXTURE_YML, "table_a")
    assert missing == [("new_col_a", "double"), ("new_col_b", "boolean")]


def test_plan_is_empty_when_nothing_is_missing() -> None:
    applied = apply_missing_columns(_FIXTURE_YML, _FIXTURE_SNAPSHOT)
    assert plan_missing_columns(_FIXTURE_SNAPSHOT, applied, "table_a") == []


def test_apply_only_inserts_never_modifies() -> None:
    """The real invariant is 'no existing line changed', NOT 'appended at EOF'.

    ``after.startswith(before)`` holds only if every insertion lands at end of file. Columns must
    splice inside each table's block, so that assertion would fail on the CORRECT implementation
    and pass on the bug. Subsequence is the same property ``git diff | grep -c "^-[^-]"`` measures.
    """
    after = apply_missing_columns(_FIXTURE_YML, _FIXTURE_SNAPSHOT)
    it = iter(after.splitlines())
    assert all(any(a == b for a in it) for b in _FIXTURE_YML.splitlines()), (
        "an existing line was modified or removed; this must be insert-only"
    )
    assert len(after.splitlines()) > len(_FIXTURE_YML.splitlines())


def test_existing_descriptions_are_never_overwritten() -> None:
    """The generator owns column INVENTORY, never prose (spec D1)."""
    after = apply_missing_columns(_FIXTURE_YML, _FIXTURE_SNAPSHOT)
    assert "Native match ID (numeric string, e.g. '10502')" in after
    assert "UTC timestamp when the row was ingested" in after


def test_fixer_and_checker_agree() -> None:
    """fixer == checker; divergence is how sync tools rot."""
    missing = plan_missing_columns(_FIXTURE_SNAPSHOT, _FIXTURE_YML, "table_a")
    applied = apply_missing_columns(_FIXTURE_YML, _FIXTURE_SNAPSHOT)
    assert len(missing) == _col_count(applied) - _col_count(_FIXTURE_YML)


def test_multi_table_fixture_places_columns_in_the_right_table() -> None:
    """PR-2a's first splice put one table's columns under another while reporting success.

    Both files stayed valid YAML, so the only check that catches it is per-table membership.
    """
    out = yaml.safe_load(apply_missing_columns(_TWO_TABLE_YML, _TWO_TABLE_SNAPSHOT))
    by_name = {t["name"]: [c["name"] for c in t["columns"]] for t in out["sources"][0]["tables"]}
    assert "new_col_a" in by_name["table_a"] and "new_col_a" not in by_name["table_b"]
    assert "new_col_b" in by_name["table_b"] and "new_col_b" not in by_name["table_a"]


def test_result_is_valid_yaml_with_no_duplicate_columns() -> None:
    out = yaml.safe_load(apply_missing_columns(_TWO_TABLE_YML, _TWO_TABLE_SNAPSHOT))
    for table in out["sources"][0]["tables"]:
        names = [c["name"] for c in table["columns"]]
        assert len(names) == len(set(names)), f"{table['name']}: duplicate columns {names}"


def test_apply_is_idempotent() -> None:
    once = apply_missing_columns(_FIXTURE_YML, _FIXTURE_SNAPSHOT)
    twice = apply_missing_columns(once, _FIXTURE_SNAPSHOT)
    assert once == twice, "a second run changed the file; --check would never converge"


def test_render_uses_the_house_folded_block_style() -> None:
    """Match the five sibling sources.yml files, which use `description: >` throughout."""
    block = render_column_block("some_col", "bigint")
    lines = block.splitlines()
    assert lines[0] == " " * 10 + "- name: some_col"
    assert lines[1] == " " * 12 + "description: >"
    assert lines[2].startswith(" " * 14)
    assert "bigint" in lines[2] and "auto-documented" in lines[2]


def test_unknown_table_in_snapshot_raises() -> None:
    """A snapshot table absent from sources.yml is a classification gap, not a silent skip.

    scripts/_bronze_table_inventory.py exists to make that decision explicit; reaching this
    generator with an unclassified table means the partition was bypassed.
    """
    import pytest

    with pytest.raises(KeyError, match="table_zzz"):
        apply_missing_columns(_FIXTURE_YML, {"tables": {"table_zzz": [{"name": "x", "type": "int"}]}})


def test_yaml_quoted_column_names_round_trip() -> None:
    """`- name: "50_50"` must compare equal to the snapshot's `50_50`.

    Found 2026-08-10 by running --check across the whole repo: reading the raw text without
    unquoting reported statsbomb_events' `50_50` perpetually undocumented, and the generator
    would have appended a DUPLICATE. The column is quoted in sources.yml because unquoted it
    parses as the integer 5050 — the file says so in its own comment.

    This is the price of splicing text instead of round-tripping YAML, and it cuts both ways:
    such a name must also be EMITTED quoted, or the key lands with the wrong type.
    """
    yml = (
        "version: 2\n\nsources:\n  - name: demo\n\n    tables:\n      - name: t\n"
        '        columns:\n          - name: "50_50"\n            description: Quoted.\n'
    )
    snapshot = {"tables": {"t": [{"name": "50_50", "type": "string"}, {"name": "plain", "type": "string"}]}}

    assert plan_missing_columns(snapshot, yml, "t") == [("plain", "string")], (
        "the quoted column was reported missing — unquoting is broken"
    )
    out = apply_missing_columns(yml, snapshot)
    parsed = yaml.safe_load(out)["sources"][0]["tables"][0]["columns"]
    names = [c["name"] for c in parsed]
    assert names.count("50_50") == 1, f"duplicated the quoted column: {names}"


def test_a_name_that_yaml_would_mistype_is_emitted_quoted() -> None:
    """Emitting `- name: 50_50` unquoted yields the integer key 5050, not the string."""
    assert needs_yaml_quoting("50_50")
    assert needs_yaml_quoting("yes")
    assert not needs_yaml_quoting("match_id")

    block = render_column_block("50_50", "string")
    assert '- name: "50_50"' in block, f"emitted unquoted: {block!r}"

    doc = ("version: 2\n\nsources:\n  - name: d\n\n    tables:\n      - name: t\n        columns:\n") + block
    parsed = yaml.safe_load(doc)["sources"][0]["tables"][0]["columns"][0]["name"]
    assert parsed == "50_50", f"round-tripped to {parsed!r} ({type(parsed).__name__})"

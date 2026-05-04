"""ADR-002 §4 schema-drift guard — DDL <-> Python constant parity.

The migration scripts/migrations/2026-05-03-create-bronze-sk3-mig-b-runs.sql
defines the canonical column list for bronze.sk3_mig_b_runs. The writer-side
constant src/ingestion/sk3_mig_b_telemetry.py::_SK3_MIG_B_RUNS_COLUMNS must
match exactly.

Failure mode this test catches: someone edits the DDL without updating the
constant (or vice versa). The orchestrator's first MERGE then fails with
DELTA_MERGE_UNRESOLVED_EXPRESSION at runtime — too late to catch in CI.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_FILE = REPO_ROOT / "scripts" / "migrations" / "2026-05-03-create-bronze-sk3-mig-b-runs.sql"


def _parse_ddl_columns(ddl_text: str) -> list[tuple[str, str]]:
    """Extract (column_name, column_type) pairs from a CREATE TABLE DDL.

    Handles types with embedded commas (MAP<STRING, DOUBLE>) by tracking
    angle-bracket depth.
    """
    match = re.search(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+[^(]+\((.*?)\)\s*USING\s+DELTA",
        ddl_text,
        re.IGNORECASE | re.DOTALL,
    )
    assert match is not None, "Could not locate CREATE TABLE column list in migration DDL"
    body = match.group(1)

    pairs: list[tuple[str, str]] = []
    depth = 0
    buf: list[str] = []
    for ch in body:
        if ch == "<":
            depth += 1
            buf.append(ch)
        elif ch == ">":
            depth -= 1
            buf.append(ch)
        elif ch == "," and depth == 0:
            pairs.append(_parse_column_line("".join(buf)))
            buf = []
        else:
            buf.append(ch)
    if buf and "".join(buf).strip():
        pairs.append(_parse_column_line("".join(buf)))
    return pairs


def _parse_column_line(line: str) -> tuple[str, str]:
    """`  cycle_id STRING,\n` -> ('cycle_id', 'STRING')."""
    line = line.strip().rstrip(",").strip()
    parts = line.split(None, 1)
    assert len(parts) == 2, f"Could not parse column line: {line!r}"
    name = parts[0].strip()
    col_type = parts[1].strip()
    col_type = re.sub(r"\s+", " ", col_type)
    col_type = col_type.replace("< ", "<").replace(" >", ">").replace(" ,", ",")
    return name, col_type


def _normalize(cols: list[tuple[str, str]]) -> list[tuple[str, str]]:
    return [(n, re.sub(r"\s+", " ", t).replace("< ", "<").replace(" >", ">")) for n, t in cols]


def test_migration_ddl_matches_python_constant() -> None:
    from ingestion.sk3_mig_b_telemetry import _SK3_MIG_B_RUNS_COLUMNS

    assert MIGRATION_FILE.exists(), f"Migration file missing: {MIGRATION_FILE}"
    ddl_columns = _parse_ddl_columns(MIGRATION_FILE.read_text(encoding="utf-8"))

    py_normalized = _normalize(_SK3_MIG_B_RUNS_COLUMNS)
    ddl_normalized = _normalize(ddl_columns)

    assert py_normalized == ddl_normalized, (
        "DDL <-> Python constant drift detected.\n"
        f"DDL columns: {ddl_normalized}\n"
        f"Python constant: {py_normalized}\n"
        "Update one to match the other; both must agree (ADR-002 §4)."
    )


def test_struct_type_factory_produces_one_field_per_constant_entry() -> None:
    pytest.importorskip("pyspark.sql.types")
    from ingestion.sk3_mig_b_telemetry import (
        _SK3_MIG_B_RUNS_COLUMNS,
        get_sk3_mig_b_runs_struct_type,
    )

    struct = get_sk3_mig_b_runs_struct_type()
    assert len(struct.fields) == len(_SK3_MIG_B_RUNS_COLUMNS), (
        f"StructType has {len(struct.fields)} fields but constant has {len(_SK3_MIG_B_RUNS_COLUMNS)} entries."
    )
    for sf, (name, _) in zip(struct.fields, _SK3_MIG_B_RUNS_COLUMNS, strict=True):
        assert sf.name == name, f"Field name mismatch at position: {sf.name} vs {name}"


@pytest.mark.parametrize(
    ("item", "expected_kind"),
    [
        ("vaep", "trained_model"),
        ("xg_v2", "trained_model"),
        ("ext_v2_p0", "trained_model"),
        ("ext_v2_p1", "trained_model"),
        ("f2v_v1", "trained_model"),
        ("f2v_v2", "trained_model"),
        ("f2v_360", "trained_model"),
        ("scoutgpt", "trained_model"),
        ("defcon_lite", "compute_only"),
        ("obso", "compute_only"),
        ("pausa", "compute_only"),
        ("spadl_vaep_publish", "publish"),
        ("xg_shots_publish", "publish"),
        ("obso_pausa_values_publish", "publish"),
        ("f2v_embeddings_publish", "publish"),
        ("pre_state", "meta_event"),
        ("baseline_rebase", "meta_event"),
        ("xg1_retire_runtime", "meta_event"),
        ("scoutgpt_export", "meta_event"),
        ("heartbeat", "meta_event"),
    ],
)
def test_classify_cycle_item(item: str, expected_kind: str) -> None:
    from ingestion.sk3_mig_b_telemetry import classify_cycle_item

    assert classify_cycle_item(item) == expected_kind


def test_classify_cycle_item_rejects_unknown() -> None:
    from ingestion.sk3_mig_b_telemetry import classify_cycle_item

    with pytest.raises(ValueError, match="Unknown cycle_item"):
        classify_cycle_item("nonexistent_item")

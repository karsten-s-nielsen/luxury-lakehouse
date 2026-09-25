"""P2 first-materialization write→DDL dtype parity — the ``format_version``-class detector.

Every table P2 materializes for the FIRST time is guarded: the writer's declared WRITE dtypes must match
its table DDL by column NAME and type. A payload dtype that mismatches the column type — e.g. a bare
Python ``1`` inferring to int64/LONG written into a ``format_version INT`` column, which Delta
``replaceWhere`` rejects with ``DELTA_FAILED_TO_MERGE_FIELDS`` — is caught at unit time, not at the live
write. Closes the Phase-G ExT-grid gap (write path was "exercised by the live drain, not here") and the
sk4118 P1 mart writers whose first materialization is the P2 recompute.

PYSPARK-FREE (pyspark is serverless-only, absent locally): the explicit-schema writers build their
``StructType`` from ``_OUTPUT_TYPES`` via a fixed ``{long: LongType, double: DoubleType, string:
StringType}`` map, so ``_OUTPUT_TYPES`` IS the write-dtype source of truth — comparing it (mapped to SQL
types) to the DDL is exactly the StructType↔DDL check without importing pyspark. Were this test to
``importorskip("pyspark")`` it would SKIP everywhere and guard nothing.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from tests._ddl import ddl_columns  # shared CREATE TABLE parser

_MIGRATIONS = Path(__file__).resolve().parents[2] / "scripts" / "migrations"
_AUDIT_COLS = {"_ingested_at", "_loaded_at"}

# The writers' _OUTPUT_TYPES type-name -> the SQL type its StructField maps to (LongType.simpleString()
# == "bigint", etc.). Mirrors each writer's local type_map; a new type-name here fails loud below.
_TYPENAME_TO_SQL = {"long": "bigint", "double": "double", "string": "string", "int": "int", "boolean": "boolean"}

# pandas dtype -> SQL type spark.createDataFrame infers for a pandas payload (the INFERRED writers).
_PD_TO_SQL = {
    "int32": "int",
    "int64": "bigint",
    "float64": "double",
    "float32": "float",
    "bool": "boolean",
    "object": "string",
    "string": "string",
}


def _ddl_pairs(text: str) -> dict[str, str]:
    """{column: lowercase_sql_type} from a CREATE TABLE file OR a comma-list schema constant, audit cols
    dropped."""
    if "create table" in text.lower():
        pairs = ddl_columns(text)
    else:
        pairs = []
        for part in text.split(","):
            part = part.strip()
            if part:
                name, sql_type = part.split(None, 1)
                pairs.append((name, sql_type.lower()))
    return {n: t for n, t in pairs if n not in _AUDIT_COLS}


def _output_type_pairs(mod) -> dict[str, str]:
    return {c: _TYPENAME_TO_SQL[mod._OUTPUT_TYPES[c]] for c in mod.OUTPUT_COLUMNS}


# ── explicit-StructType P1 mart writers: _OUTPUT_TYPES ↔ migration CREATE TABLE (name + type) ─────────
_STRUCT_WRITERS = {
    "team_metrics": ("ingestion.team_metrics_writer", "2026-09-17-add-team-metrics.sql"),
    "match_outcome": ("ingestion.match_outcome_writer", "2026-09-17-add-match-outcome.sql"),
    "shot_stopping": ("ingestion.shot_stopping_writer", "2026-09-17-add-shot-stopping.sql"),
    "gk_decision": ("ingestion.gk_decision_writer", "2026-09-17-add-gk-decision.sql"),
    "territory": ("ingestion.territory_writer", "2026-09-17-add-territory.sql"),
    "restdefense": ("ingestion.restdefense_writer", "2026-09-17-add-restdefense.sql"),
}


@pytest.mark.parametrize("name", sorted(_STRUCT_WRITERS))
def test_struct_writer_dtypes_match_migration_ddl(name: str) -> None:
    module_path, migration = _STRUCT_WRITERS[name]
    mod = importlib.import_module(module_path)
    write = _output_type_pairs(mod)
    ddl = _ddl_pairs((_MIGRATIONS / migration).read_text(encoding="utf-8"))
    mismatched = {c: (write[c], ddl.get(c)) for c in write if ddl.get(c) != write[c]}
    assert not mismatched, f"{name}: (write, ddl) dtype mismatch: {mismatched}"
    assert not (set(write) - set(ddl)), f"{name}: writer emits columns absent from DDL: {sorted(set(write) - set(ddl))}"
    # Non-vacuity: the DDL must have actually parsed some columns.
    assert len(ddl) >= len(write) > 0, f"{name}: DDL parse looks empty ({len(ddl)} cols) — parser/migration drift"


# ── schema-INFERRED writers (spark.createDataFrame(pandas)) — the format_version class ────────────────
def test_ext_grids_payload_dtypes_match_schema() -> None:
    """ExT grid + zones payloads (inferred writes) must match _RESULTS_SCHEMA / _ZONES_SCHEMA — the
    format_version int64→LONG vs INT bug that failed the live write."""
    pytest.importorskip("silly_kicks")
    import numpy as np
    from silly_kicks.xthreat import ExpectedThreat

    from ingestion.expected_threat import (
        _RESULTS_SCHEMA,
        _XT_L,
        _XT_W,
        _ZONES_SCHEMA,
        _grids_payload,
        _zones_payload,
    )

    # A real fit (fit_from_counts) so to_dict() succeeds — a bare .xT set leaves the prob matrices None.
    n = _XT_L * _XT_W
    sc = np.zeros((_XT_W, _XT_L), dtype=int)
    gc = np.zeros((_XT_W, _XT_L), dtype=int)
    mc = np.zeros((_XT_W, _XT_L), dtype=int)
    tsc = np.zeros((_XT_W, _XT_L), dtype=int)
    tc = np.zeros((n, n), dtype=int)
    sc[6, 8] = 2
    gc[6, 8] = 1
    mc[6, 4] = 3
    tsc[6, 4] = 3
    tc[6 * _XT_L + 4, 6 * _XT_L + 8] = 2
    model = ExpectedThreat(l=_XT_L, w=_XT_W).fit_from_counts(
        shot_counts=sc, goal_counts=gc, move_counts=mc, transition_start_counts=tsc, transition_counts=tc
    )

    grids = {c: _PD_TO_SQL[str(dt)] for c, dt in _grids_payload(model, "global").dtypes.items()}
    grids_ddl = _ddl_pairs(_RESULTS_SCHEMA)
    assert grids == grids_ddl, f"grids payload {grids} != _RESULTS_SCHEMA {grids_ddl}"

    zones = {c: _PD_TO_SQL[str(dt)] for c, dt in _zones_payload(model, "global").dtypes.items()}
    zones_ddl = _ddl_pairs(_ZONES_SCHEMA)
    assert zones == zones_ddl, f"zones payload {zones} != _ZONES_SCHEMA {zones_ddl}"


def test_duels_has_no_int32_inferred_columns() -> None:
    """duels writes via spark.createDataFrame(out) (inferred). An inferred pandas int becomes int64/LONG,
    matching BIGINT but NOT INT — an INT column would silently fail the write. Assert duels declares no
    INT column and its self-DDL matches the migration."""
    from ingestion import duels_writer

    int32_cols = [c for c, t in duels_writer._OUTPUT_TYPES.items() if t == "int"]
    assert not int32_cols, f"duels _OUTPUT_TYPES has INT(int32) columns unsafe for an inferred write: {int32_cols}"
    self_ddl = _ddl_pairs(duels_writer._ddl())
    mig_ddl = _ddl_pairs((_MIGRATIONS / "2026-09-17-add-duels.sql").read_text(encoding="utf-8"))
    assert self_ddl == mig_ddl, f"duels _ddl() vs migration DDL drift: self={self_ddl} mig={mig_ddl}"

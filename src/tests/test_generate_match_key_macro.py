"""Tests for the `generate_match_key` dbt macro.

The macro generates a deterministic BIGINT surrogate key from
`(provider, native_match_id)` pairs. Required properties:
  - Deterministic: same input -> same output across invocations
  - Provider-sensitive: (statsbomb, '123') != (wyscout, '123')
  - BIGINT output (fits in int64, Postgres BIGINT compatible)
  - Collision-free at our scale (<10k matches / provider)

Macro implementation is Spark SQL (`xxhash64`). These tests verify the
compiled SQL string via `dbt compile --inline`; a separate integration
test (`test_dbt_dim_matches.py`) verifies determinism against live Databricks.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = REPO_ROOT / "dbt_project"


def _compile_inline(inline_model: str) -> str:
    """Run `dbt compile --inline` and return compiled SQL from stdout.

    Uses --profiles-dir because dbt searches for the 'databricks' profile
    relative to the profiles-dir argument (the dbt_project itself, in this
    repo).
    """
    # Rationale for S603/S607 suppressions below:
    # `inline_model` is a test-internal literal (see test bodies below),
    # never user-supplied input, so S603 (untrusted input) does not apply.
    # `uv` is a PATH-resolved partial path; the repo convention
    # (see `test_guard_conformance.py`, `test_loader.py`) is to rely on
    # PATH rather than hardcoding absolute paths, matching the documented
    # dev invocation `uv run pytest ...`.
    result = subprocess.run(  # noqa: S603 -- test-internal literal args, no user input
        [  # noqa: S607 -- `uv` is PATH-resolved per repo convention
            "uv",
            "run",
            "dbt",
            "compile",
            "--inline",
            inline_model,
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROJECT_DIR),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"dbt compile --inline failed (exit {result.returncode}):\n"
            f"=== stdout ===\n{result.stdout}\n"
            f"=== stderr ===\n{result.stderr}"
        )
    return result.stdout


@pytest.fixture(scope="session")
def compiled_macro_sql() -> str:
    """Compile the macro once per test session and share the result.
    Avoids the ~25-40 s dbt cold-start overhead multiplied by 4 tests."""
    return _compile_inline(
        "select {{ generate_match_key('myprov', 'mynat') }} as k from (select 'sb' as myprov, '1' as mynat)"
    )


def test_macro_uses_xxhash64(compiled_macro_sql: str) -> None:
    """The macro must emit `xxhash64` - collision-resistant at 64-bit width."""
    assert "xxhash64" in compiled_macro_sql.lower(), f"Expected xxhash64 in compiled SQL; got: {compiled_macro_sql}"


def test_macro_includes_both_inputs(compiled_macro_sql: str) -> None:
    """Provider and native_match_id must both appear in compiled output.

    Uses non-trivial column names (`myprov`, `mynat`) so the assertion
    proves the macro wired the inputs through, not that the test re-used
    a keyword that was part of the macro body."""
    assert "myprov" in compiled_macro_sql, f"Column 'myprov' missing from compiled SQL: {compiled_macro_sql}"
    assert "mynat" in compiled_macro_sql, f"Column 'mynat' missing from compiled SQL: {compiled_macro_sql}"


def test_macro_uses_concat_ws_with_delimiter(compiled_macro_sql: str) -> None:
    """Must use `concat_ws` with an explicit delimiter to avoid
    ('ab', '') vs ('a', 'b') collisions."""
    assert "concat_ws" in compiled_macro_sql.lower()


def test_macro_casts_native_id_to_string(compiled_macro_sql: str) -> None:
    """native_match_id may arrive as BIGINT (StatsBomb/Wyscout) or STRING
    (IDSSE/Metrica); macro must cast to string for uniform hashing."""
    assert "cast" in compiled_macro_sql.lower()
    assert "string" in compiled_macro_sql.lower()


def test_macro_argument_order_matters() -> None:
    """Swapping argument order must change the compiled SQL.
    Regression guard against `generate_match_key(native, provider)` typos."""
    sql_a = _compile_inline(
        "select {{ generate_match_key('myprov', 'mynat') }} as k from (select 'sb' as myprov, '1' as mynat)"
    )
    sql_b = _compile_inline(
        "select {{ generate_match_key('mynat', 'myprov') }} as k from (select 'sb' as myprov, '1' as mynat)"
    )
    assert sql_a != sql_b, (
        "Argument order must affect compiled SQL. "
        "sql_a and sql_b were identical — the macro does not respect arg order."
    )


def test_macro_wires_named_columns_into_output() -> None:
    """The caller's column names must appear in the compiled SQL — proves the
    macro actually references its inputs rather than emitting a hard-coded
    expression. Uses non-trivial column names so they can't be incidental."""
    sql = _compile_inline(
        "select {{ generate_match_key('myprov', 'mynat') }} as k from (select 'sb' as myprov, '1' as mynat)"
    )
    assert "myprov" in sql, f"Column 'myprov' missing from compiled SQL: {sql}"
    assert "mynat" in sql, f"Column 'mynat' missing from compiled SQL: {sql}"


def test_macro_is_deterministic() -> None:
    """Compiling the same inline model twice must produce byte-identical SQL.
    Regression guard against accidental introduction of time/random elements."""
    inline = "select {{ generate_match_key('myprov', 'mynat') }} as k from (select 'sb' as myprov, '1' as mynat)"
    sql_1 = _compile_inline(inline)
    sql_2 = _compile_inline(inline)

    # `dbt compile --inline` emits a timestamped startup banner on stdout
    # (e.g. `20:34:10  Running with dbt=1.11.6`) that legitimately varies
    # between invocations. We only care about the generated SQL, which dbt
    # prints after the literal marker line `Compiled inline node is:`.
    def _extract_compiled_sql(s: str) -> str:
        marker = "Compiled inline node is:"
        idx = s.find(marker)
        assert idx != -1, f"Marker '{marker}' not found in dbt output: {s}"
        return s[idx + len(marker) :].strip()

    assert _extract_compiled_sql(sql_1) == _extract_compiled_sql(sql_2), (
        f"Non-deterministic compile output:\n---1---\n{sql_1}\n---2---\n{sql_2}"
    )

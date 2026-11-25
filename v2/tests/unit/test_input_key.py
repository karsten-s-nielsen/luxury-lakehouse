"""The exact tracking-compute input key (perf spec §7.4 lever K; plan Task 3).

The key must change for ANY change of an input row or column, a code change on the shaping/scoring path, or a library
version change, and must stay the same for identical inputs. The read hash runs inside DuckDB (MD5 of each row's
`to_json`, two independent 64-bit halves summed as HUGEINT, plus the row count).
"""

from __future__ import annotations

import ast
import importlib.metadata
import importlib.util
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest
from lakekit import LakeKit

from lakehouse.catalog.working import WorkingCatalog
from lakehouse.compute import input_key as ik
from lakehouse.compute.frames import FRAME_INPUT_READS, InputRead
from lakehouse.config.model import ResourceCaps, load_defaults
from lakehouse.ids.keys import match_key
from lakehouse.resolve.links import run_resolution
from lakehouse.runs.dbt import run_dbt

_T4 = InputRead(name="t4", sql="SELECT * FROM lake.main.t4 WHERE canonical_match_key = ?", with_provider=False)
_MK = 7


@pytest.fixture
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS lake")
    c.execute("CREATE TABLE lake.main.t4 (canonical_match_key BIGINT, team_key BIGINT, x DOUBLE, label VARCHAR)")
    c.execute("INSERT INTO lake.main.t4 VALUES (7, 100, 1.5, 'RCB'), (7, 200, 2.5, 'LCB'), (8, 100, 9.0, 'ST')")
    yield c
    c.close()


def _key(c: duckdb.DuckDBPyConnection, reads: tuple[InputRead, ...] = (_T4,)) -> str:
    return ik.input_key(c, match_key=_MK, provider="idsse", reads=reads, code_modules=())


def test_unchanged_reads_give_the_same_key(conn: duckdb.DuckDBPyConnection) -> None:
    assert _key(conn) == _key(conn)


@pytest.mark.parametrize(
    "update",
    [
        "UPDATE lake.main.t4 SET canonical_match_key = 9 WHERE label = 'RCB'",
        "UPDATE lake.main.t4 SET team_key = 300 WHERE label = 'RCB'",
        "UPDATE lake.main.t4 SET x = 1.6 WHERE label = 'RCB'",
        "UPDATE lake.main.t4 SET label = 'RB' WHERE label = 'RCB'",
    ],
    ids=["canonical_match_key", "team_key", "x", "label"],
)
def test_a_changed_value_in_any_column_changes_the_key(conn: duckdb.DuckDBPyConnection, update: str) -> None:
    before = _key(conn)
    conn.execute(update)
    assert _key(conn) != before


def test_a_rows_outside_the_match_do_not_change_the_key(conn: duckdb.DuckDBPyConnection) -> None:
    before = _key(conn)
    conn.execute("UPDATE lake.main.t4 SET x = 99.0 WHERE canonical_match_key = 8")
    assert _key(conn) == before


def test_a_duplicated_row_changes_the_key(conn: duckdb.DuckDBPyConnection) -> None:
    before = _key(conn)
    conn.execute("INSERT INTO lake.main.t4 VALUES (7, 100, 1.5, 'RCB')")
    assert _key(conn) != before


def test_null_to_value_changes_the_key_and_null_is_stable(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("INSERT INTO lake.main.t4 VALUES (7, NULL, 'nan'::DOUBLE, NULL), (7, 300, -0.0, 'GK')")
    with_null = _key(conn)
    assert _key(conn) == with_null  # NULL, NaN and -0.0 hash the same way on every call
    conn.execute("UPDATE lake.main.t4 SET team_key = 400 WHERE team_key IS NULL")
    assert _key(conn) != with_null


def test_row_digest_halves_are_independent(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        "CREATE TABLE lake.main.big AS SELECT 7 AS canonical_match_key, range AS i, range * 0.5 AS v FROM range(1000)"
    )
    big = InputRead(name="big", sql="SELECT * FROM lake.main.big WHERE canonical_match_key = ?", with_provider=False)
    count, upper, lower = ik.read_sums(conn, big, _MK, "idsse")
    conn.execute("UPDATE lake.main.big SET v = v + 1 WHERE i = 500")
    count2, upper2, lower2 = ik.read_sums(conn, big, _MK, "idsse")
    assert count == count2 == 1000
    assert upper != upper2 and lower != lower2  # one row change moves BOTH independent halves


def test_an_embedded_separator_does_not_collide() -> None:
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS lake")
    c.execute("CREATE TABLE lake.main.t (canonical_match_key BIGINT, c VARCHAR, d INTEGER)")
    read = InputRead(name="t", sql="SELECT * FROM lake.main.t WHERE canonical_match_key = ?", with_provider=False)
    c.execute("INSERT INTO lake.main.t VALUES (7, 'a'', ''d'': 1', NULL)")
    imitation = ik.read_content_hash(c, read, _MK, "idsse")
    c.execute("DELETE FROM lake.main.t")
    c.execute("INSERT INTO lake.main.t VALUES (7, 'a', 1)")
    assert ik.read_content_hash(c, read, _MK, "idsse") != imitation


def test_float_change_in_the_last_bit_changes_the_key(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("UPDATE lake.main.t4 SET x = 0.3 WHERE label = 'RCB'")
    exact = _key(conn)
    conn.execute("UPDATE lake.main.t4 SET x = 0.1::DOUBLE + 0.2::DOUBLE WHERE label = 'RCB'")
    assert _key(conn) != exact  # 0.30000000000000004 is not 0.3


def test_library_version_change_changes_the_key(
    conn: duckdb.DuckDBPyConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = _key(conn)
    real = importlib.metadata.version

    def bumped(name: str) -> str:
        return "999.0" if name == "silly-kicks" else real(name)

    monkeypatch.setattr(ik.metadata, "version", bumped)
    assert _key(conn) != before


def _write_module(folder: Path, name: str, text: str) -> None:
    (folder / f"{name}.py").write_text(text, encoding="utf-8")
    importlib.invalidate_caches()


def test_docstring_only_change_keeps_the_code_digest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "path", [str(tmp_path), *sys.path])
    _write_module(tmp_path, "_ik_mod_a", '"""One."""\n\ndef f(x):\n    """Doc."""\n    return x + 1  # note\n')
    before = ik.code_digest(["_ik_mod_a"])
    _write_module(tmp_path, "_ik_mod_a", '"""Two, longer."""\n\n\ndef f(x):\n    """Other doc."""\n    return x + 1\n')
    assert ik.code_digest(["_ik_mod_a"]) == before


def test_code_change_changes_the_code_digest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "path", [str(tmp_path), *sys.path])
    _write_module(tmp_path, "_ik_mod_b", "def f(x):\n    return x + 1\n")
    before = ik.code_digest(["_ik_mod_b"])
    _write_module(tmp_path, "_ik_mod_b", "def f(x):\n    return x + 2\n")
    assert ik.code_digest(["_ik_mod_b"]) != before


def _runtime_imports(module: str) -> set[str]:
    """Every module a module imports at runtime (`if TYPE_CHECKING:` blocks skipped), as dotted names."""
    spec = importlib.util.find_spec(module)
    assert spec is not None and spec.origin is not None, f"{module} not found"
    tree = ast.parse(Path(spec.origin).read_text(encoding="utf-8"))
    skipped: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and "TYPE_CHECKING" in ast.unparse(node.test):
            skipped.update(id(child) for body in node.body for child in ast.walk(body))
    found: set[str] = set()
    for node in ast.walk(tree):
        if id(node) in skipped:
            continue
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            for alias in node.names:
                full = f"{node.module}.{alias.name}"
                found.add(full if importlib.util.find_spec(node.module) and _is_module(full) else node.module)
    return found


def _is_module(name: str) -> bool:
    try:
        return importlib.util.find_spec(name) is not None
    except ModuleNotFoundError:
        return False


_CODE_PATHS = pytest.mark.parametrize(
    "code_modules", [ik.TRACKING_CODE_MODULES, ik.SPADL_CODE_MODULES], ids=["tracking", "spadl"]
)


@_CODE_PATHS
def test_code_modules_cover_the_import_closure(code_modules: tuple[str, ...]) -> None:
    pending, seen, missing = list(code_modules), set[str](), set[str]()
    while pending:
        module = pending.pop()
        if module in seen:
            continue
        seen.add(module)
        for target in _runtime_imports(module):
            if not target.startswith("lakehouse"):
                continue
            if target in code_modules:
                pending.append(target)
            elif not ik.is_infra(target):
                missing.add(f"{module} -> {target}")
    assert not missing, f"lakehouse imports on the shaping/scoring path are neither digested nor infra: {missing}"


def _normalise(name: str) -> str:
    return name.lower().replace("_", "-").replace(".", "-")


@_CODE_PATHS
def test_libraries_cover_third_party_imports(code_modules: tuple[str, ...]) -> None:
    dists = importlib.metadata.packages_distributions()
    listed = {_normalise(d) for d in ik.TRACKING_LIBRARIES}
    missing: set[str] = set()
    for module in code_modules:
        for target in _runtime_imports(module):
            top = target.split(".")[0]
            if top in sys.stdlib_module_names or top == "lakehouse" or top == "__future__":
                continue
            names = {_normalise(d) for d in dists.get(top, [top])}
            if not names & listed:
                missing.add(f"{module} -> {top} ({sorted(names)})")
    assert not missing, f"third-party imports whose distribution is not in TRACKING_LIBRARIES: {missing}"


def test_frame_input_reads_are_the_four_builder_reads() -> None:
    assert [r.name for r in FRAME_INPUT_READS] == [
        "fct_tracking_frames",
        "fct_tracking_ball",
        "fct_match_params",
        "fct_roster_gk",
    ]
    assert [r.with_provider for r in FRAME_INPUT_READS] == [True, False, False, False]
    # the frames and ball reads push the match's record keys down (perf spec §7.4a)
    assert [r.with_record_keys for r in FRAME_INPUT_READS] == [True, True, False, False]


# --- the record keys of a match (perf spec §7.4a item 4) ------------------------------------------------------------


def test_a_read_with_record_keys_refuses_an_empty_list() -> None:
    """An empty record-key list would read no rows and look like an EMPTY match; it is refused instead."""
    read = InputRead(
        name="r",
        sql="SELECT 1 WHERE ? = 1 AND list_contains(?::BIGINT[], 1)",
        with_provider=False,
        with_record_keys=True,
    )
    with pytest.raises(ValueError, match="record keys"):
        read.params(_MK, "idsse")
    assert read.params(_MK, "idsse", (_MK, 70)) == [_MK, [_MK, 70]]
    with_provider = InputRead(name="p", sql="", with_provider=True, with_record_keys=True)
    assert with_provider.params(_MK, "idsse", (70, _MK)) == [_MK, "idsse", [70, _MK]]


def test_match_record_keys_include_the_match_and_its_pointed_records() -> None:
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS lake")
    c.execute("CREATE TABLE lake.main.ops_canonical_pointers (entity VARCHAR, record_key BIGINT, canonical_key BIGINT)")
    c.execute(
        "INSERT INTO lake.main.ops_canonical_pointers VALUES "
        "('match', 202, 202), ('match', 303, 202), ('match', 404, 505), ('player', 606, 202)"
    )
    assert ik.match_record_keys(c, 202) == (202, 303)  # the self-pointer and the record that points to it
    assert ik.match_record_keys(c, 505) == (404, 505)  # the canonical key itself is always in the set
    assert ik.match_record_keys(c, 101) == (101,)  # no pointer: the match is its own record
    assert ik.MATCH_RECORD_KEYS_SQL in ik.CONTROL_READS


# --- the lake_table_present branch flip, on real dbt views (spec §7.4) ------------------------------------------------

CAPS = ResourceCaps(threads=1, memory_limit="1GB", arrow_cpus=1)


@pytest.fixture
def working(kit: LakeKit) -> Iterator[WorkingCatalog]:
    cat = kit.catalog(run_id="run-ik")
    yield cat
    cat.close()


def _seed_idsse_match(cat: WorkingCatalog) -> int:
    from lakehouse.ids.keys import team_key
    from lakehouse.ids.native import idsse_native_team_id
    from lakehouse.runs.history import MatchRow, replace_match

    conn = cat.conn
    mk = match_key("idsse", "J03WMX")
    home = team_key("idsse", idsse_native_team_id("DFL-CLU-00000G"))
    guest = team_key("idsse", idsse_native_team_id("DFL-CLU-000017"))
    conn.execute(
        "CREATE TABLE lake.main.idsse_match_information (match_key BIGINT, home_team_key BIGINT, "
        "guest_team_key BIGINT, native_match_id VARCHAR, general_kickoff_time TIMESTAMPTZ, "
        "general_home_team_id VARCHAR, general_home_team_name VARCHAR, general_guest_team_id VARCHAR, "
        "general_guest_team_name VARCHAR, general_competition_id VARCHAR, general_competition_name VARCHAR, "
        "general_season_id VARCHAR, general_season VARCHAR, environment_country VARCHAR)"
    )
    conn.execute(
        # test fixture; the interpolated values are integer surrogate keys, not user input
        f"INSERT INTO lake.main.idsse_match_information VALUES ({mk}, {home}, {guest}, 'J03WMX', "  # noqa: S608 - test constants, no outside input
        "TIMESTAMPTZ '2022-08-05 18:30:00+00', 'DFL-CLU-00000G', 'FC Bayern', 'DFL-CLU-000017', 'Frankfurt', "
        "'DFL-COM-000001', 'Bundesliga', 'DFL-SEA-0001K6', '2022/2023', 'Germany')"
    )
    replace_match(
        conn,
        MatchRow(
            match_key=mk,
            provider="idsse",
            native_match_id="J03WMX",
            visibility="public",
            kept_channel="public",
            kept_reason="only copy",
            competition_id="DFL-COM-000001",
            competition_name=None,
            season_id="DFL-SEA-0001K6",
            season_name=None,
            has_360_frames=False,
            run_id=cat.run_id,
            updated_at=datetime(2026, 11, 6, tzinfo=UTC),
        ),
        [],
        [],
    )
    return mk


def test_lake_table_present_branch_flip_changes_the_key(kit: LakeKit, working: WorkingCatalog) -> None:
    mk = _seed_idsse_match(working)
    conn = working.conn
    run_resolution(conn, storage=kit.storage, settings=load_defaults("laptop"), run_id="run-ik")
    assert run_dbt(working, run_id="run-ik", caps=CAPS).success
    row = conn.execute(
        "SELECT canonical_key FROM lake.main.ops_canonical_pointers WHERE entity = 'match' AND record_key = ?", [mk]
    ).fetchone()
    canonical = int(row[0]) if row is not None else mk
    record_keys = ik.match_record_keys(conn, canonical)
    before = ik.input_key(
        conn, match_key=canonical, provider="idsse", reads=FRAME_INPUT_READS, code_modules=(), record_keys=record_keys
    )

    # the provider's tracking table appears: fct_tracking_ball's lake_table_present branch flips on rebuild
    conn.execute(
        "CREATE TABLE lake.main.idsse_tracking_frames (match_key BIGINT, period BIGINT, frame BIGINT, "
        "ball_x_mm BIGINT, ball_y_mm BIGINT, ball_detected BOOLEAN, ball_possession BIGINT, timestamp_us BIGINT)"
    )
    conn.execute(
        # test fixture; mk is an integer surrogate key, not user input
        f"INSERT INTO lake.main.idsse_tracking_frames VALUES ({mk}, 1, 100, 500, -300, true, 1, 1000000)"  # noqa: S608 - test constants, no outside input
    )
    assert run_dbt(working, run_id="run-ik", caps=CAPS).success
    after = ik.input_key(
        conn, match_key=canonical, provider="idsse", reads=FRAME_INPUT_READS, code_modules=(), record_keys=record_keys
    )
    assert after != before

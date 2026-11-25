"""The exact input key of a tracking compute (perf spec §7.4, lever K; plan Task 3), and of a SPADL step.

The SPADL steps (`compute/spadl_actions.py`) use the same key over their declared per-match reads, with
`SPADL_CODE_MODULES` as the code part (owner rule 2026-11-06: every per-match step has a skip guard).

A tracking step decides "already computed" BEFORE it builds a match frame, so a nightly run with no new or changed
matches builds no frame. The key is as exact as hashing the built output, and more exact than the earlier 5-column
frame hash. It has three parts:

1. **Input content.** For each declared input read (the SAME SQL the step executes), DuckDB hashes every row and
   column without moving rows into Python: the row count plus the sums of the upper and lower 64-bit halves of each
   row's MD5 over `to_json(row)`. The sums are independent of row order, sensitive to duplicates, and 128 bits wide
   per row. (`hash(x, k)` is NOT used for a second hash: DuckDB combines `hash(x)` with `hash(k)`, so it adds no
   bits.) `to_json` quotes strings, keeps NaN distinct, prints floats round-trip, and writes `-0.0` as `0.0` — the one
   known exception: a sign-of-zero-only change does not re-compute (equal values; no scorer branches on it).
2. **Code digest.** A SHA-256 over the normalised source (the `ast` dump with docstrings removed) of every module on
   the step's shaping and scoring path (`TRACKING_CODE_MODULES`). Derived, never hand-kept: any code change
   re-computes, a docstring/comment/format change does not.
3. **Libraries.** The installed versions of `TRACKING_LIBRARIES` and the backend flag (`numba_active()`).

`INFRA_MODULES` are the modules a step may import that read, write, or log but never shape output values; a change to
them does not re-compute. This module is itself infra: a change to the key logic changes every stored key, so it
re-computes conservatively by itself. MD5 here is a content fingerprint, not a security control.
"""

from __future__ import annotations

import ast
import hashlib
import importlib.util
from collections.abc import Mapping, Sequence
from importlib import metadata
from pathlib import Path
from typing import Final

import duckdb

from lakehouse.compute.backend import numba_active
from lakehouse.compute.base import MATCH_RECORD_KEYS_SQL, MATCH_VISIBILITY_SQL
from lakehouse.compute.reads import InputRead

__all__ = [
    "ACTIONS_INPUT_READ",
    "CONTROL_READS",
    "HOME_TEAM_READ",
    "INFRA_MODULES",
    "LINE_BREAKING_DISCOVERY_SQL",
    "MATCH_RECORD_KEYS_SQL",
    "SPADL_CODE_MODULES",
    "SPADL_EVENT_TABLES",
    "SPADL_SKIP_SQL",
    "TABLE_PRESENT_SQL",
    "TRACKING_CODE_MODULES",
    "TRACKING_DISCOVERY_SQL",
    "TRACKING_LIBRARIES",
    "code_digest",
    "combine_key",
    "input_key",
    "is_infra",
    "library_versions",
    "match_record_keys",
    "read_content_hash",
    "read_sums",
    "skip_lookup_sql",
    "spadl_discovery_sql",
    "static_part",
    "sums_sql",
]

#: line_breaking's `fct_actions` read (an INPUT read: the actions are scored).
ACTIONS_INPUT_READ: Final = InputRead(
    "fct_actions", "SELECT * FROM lake.main.fct_actions WHERE canonical_match_key = ?", with_provider=False
)

#: The home team of a match (an INPUT read of pitch_control: it decides which team gets the home fraction).
HOME_TEAM_READ: Final = InputRead(
    "home_team_key",
    "SELECT home_team_key FROM lake.main.fct_match_params WHERE canonical_match_key = ?",
    with_provider=False,
)

#: Match discovery of the layer-1 tracking steps (CONTROL: decides which matches to visit).
TRACKING_DISCOVERY_SQL: Final = "SELECT DISTINCT canonical_match_key, provider FROM lake.main.fct_tracking_frames"
#: Match discovery of line_breaking: matches with both actions and tracking params (CONTROL).
LINE_BREAKING_DISCOVERY_SQL: Final = (
    "SELECT DISTINCT a.canonical_match_key, p.provider "
    "FROM lake.main.fct_actions a JOIN lake.main.fct_match_params p "
    "ON p.canonical_match_key = a.canonical_match_key"
)
#: Whether an output bronze table exists yet (CONTROL: part of the skip lookup).
TABLE_PRESENT_SQL: Final = (
    "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main' AND table_name = ?"
)

#: The tracking output tables whose skip lookup is a declared control read.
_SKIP_TABLES: Final = ("pitch_control_values", "shape_graph_labels", "line_breaking_results")


def skip_lookup_sql(table: str) -> str:
    """The skip lookup of one output table: does the match already hold rows written from this key (CONTROL)."""
    if table not in _SKIP_TABLES:
        raise ValueError(f"no declared skip lookup for table {table!r}")
    return f"SELECT 1 FROM lake.main.{table} WHERE canonical_match_key = ? AND _source_hash = ? LIMIT 1"  # noqa: S608 - declared constant table names only


#: The event bronze each SPADL step iterates (one step per provider, `compute/spadl_actions.py`).
SPADL_EVENT_TABLES: Final = (
    "statsbomb_events",
    "wyscout_events",
    "idsse_events",
    "skillcorner_dynamic_events",
    "gradientsports_events",
)


def spadl_discovery_sql(table: str) -> str:
    """Match discovery of one SPADL step: the provider surrogate keys of its event bronze (CONTROL)."""
    if table not in SPADL_EVENT_TABLES:
        raise ValueError(f"no declared SPADL discovery for table {table!r}")
    return f"SELECT DISTINCT match_key FROM lake.main.{table}"  # noqa: S608 - declared constant table names only


#: The SPADL skip lookup (CONTROL). `spadl_actions` holds one file per provider SURROGATE `match_key` (two providers of
#: one canonical match each write their own), so the lookup keys on `match_key`, not `canonical_match_key`.
SPADL_SKIP_SQL: Final = "SELECT 1 FROM lake.main.spadl_actions WHERE match_key = ? AND _source_hash = ? LIMIT 1"

#: Every control read, by EXACT text: they decide what runs, never what the output is, so they are not in the key.
CONTROL_READS: Final[frozenset[str]] = frozenset(
    {
        TRACKING_DISCOVERY_SQL,
        LINE_BREAKING_DISCOVERY_SQL,
        TABLE_PRESENT_SQL,
        MATCH_VISIBILITY_SQL,
        MATCH_RECORD_KEYS_SQL,
        SPADL_SKIP_SQL,
        *(skip_lookup_sql(table) for table in _SKIP_TABLES),
        *(spadl_discovery_sql(table) for table in SPADL_EVENT_TABLES),
    }
)

#: The modules whose code shapes or scores the tracking outputs; their normalised source is digested into the key.
#: The step driver module is digested too, so any driver change re-computes (conservative).
TRACKING_CODE_MODULES: Final[tuple[str, ...]] = (
    "lakehouse.compute.frames",
    "lakehouse.compute.pitch_control",
    "lakehouse.compute.shape_graphs",
    "lakehouse.compute.line_breaking",
    "lakehouse.compute.control",
    "lakehouse.compute.tracking_steps",
)

#: The modules whose code shapes the SPADL outputs: the steps, their declared reads and converter adapters, and the
#: surrogate/native id functions that resolve the canonical keys. `control` is on the path through the shared loop.
SPADL_CODE_MODULES: Final[tuple[str, ...]] = (
    "lakehouse.compute.spadl_actions",
    "lakehouse.compute.control",
    "lakehouse.ids.keys",
    "lakehouse.ids.native",
)

#: Modules (an entry covers its submodules) a compute step may import that never shape output values.
_INFRA_REASONS: Final[Mapping[str, str]] = {
    "lakehouse.compute.base": "the per-match write loop, skip/visibility plumbing and outcome counts",
    "lakehouse.compute.input_key": "the key itself; a change re-keys every match by construction",
    "lakehouse.compute.reads": "the declared-read type; the SQL texts live in the digested modules",
    "lakehouse.compute.backend": "reads the backend flag; the flag is in the key's library part",
    "lakehouse.bronze": "bronze writers and schemas: persist rows, never compute them",
    "lakehouse.catalog": "catalog connections and publishes",
    "lakehouse.logging": "structured logs",
    "lakehouse.hashing": "content hashes for audit columns",
    "lakehouse.runs._common": "the error sink and control-error types",
    "lakehouse.config.model": "run configuration (caps, compute budget)",
}
INFRA_MODULES: Final[frozenset[str]] = frozenset(_INFRA_REASONS)

#: The libraries on the shaping/scoring path; their installed versions are in the key.
TRACKING_LIBRARIES: Final[tuple[str, ...]] = (
    "silly-kicks",
    "numba",
    "llvmlite",
    "numpy",
    "scipy",
    "pandas",
    "pyarrow",
    "duckdb",
    "xxhash",  # the surrogate keys (`lakehouse.ids.keys`) the SPADL steps resolve canonical keys from
)

_SUMS_SQL: Final = (
    "SELECT count(*), sum(md5_number_upper(to_json(s))::HUGEINT), sum(md5_number_lower(to_json(s))::HUGEINT) "
    "FROM (SELECT r AS s FROM ({read}) r)"
)


def is_infra(module: str) -> bool:
    """True when `module` is an infra module or inside an infra package."""
    return any(module == entry or module.startswith(f"{entry}.") for entry in INFRA_MODULES)


def sums_sql(read: InputRead) -> str:
    """The exact SQL text that hashes one input read inside DuckDB (declared: the read guard accepts it)."""
    return _SUMS_SQL.format(read=read.sql)


def match_record_keys(conn: duckdb.DuckDBPyConnection, match_key: int) -> tuple[int, ...]:
    """The provider record keys of a canonical match (perf spec §7.4a item 4): `match_key` itself plus every record
    whose match pointer names it, sorted. The frames and ball reads filter on them so the filter reaches the
    per-provider views; the canonical filter still decides the row set, so a superset is exact."""
    found = conn.execute(MATCH_RECORD_KEYS_SQL, [match_key]).fetchall()
    return tuple(sorted({match_key, *(int(row[0]) for row in found)}))


def read_sums(
    conn: duckdb.DuckDBPyConnection,
    read: InputRead,
    match_key: int,
    provider: str,
    *,
    record_keys: Sequence[int] = (),
) -> tuple[int, int | None, int | None]:
    """The row count and the two MD5-half sums of one input read for one match, computed inside DuckDB."""
    row = conn.execute(sums_sql(read), read.params(match_key, provider, record_keys)).fetchone()
    if row is None:
        return 0, None, None
    count, upper, lower = row
    return int(count), None if upper is None else int(upper), None if lower is None else int(lower)


def read_content_hash(
    conn: duckdb.DuckDBPyConnection,
    read: InputRead,
    match_key: int,
    provider: str,
    *,
    record_keys: Sequence[int] = (),
) -> str:
    """SHA-256 of `"{count}:{sum_upper}:{sum_lower}"` for one input read of one match."""
    count, upper, lower = read_sums(conn, read, match_key, provider, record_keys=record_keys)
    return hashlib.sha256(f"{count}:{upper}:{lower}".encode()).hexdigest()


def _strip_docstrings(tree: ast.Module) -> ast.Module:
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        body = node.body
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            if isinstance(body[0].value.value, str):
                node.body = body[1:] or [ast.Pass()]
    return tree


def code_digest(modules: Sequence[str]) -> str:
    """SHA-256 over each module's name and its docstring-free `ast` dump, in list order (read from the source file,
    without importing the module)."""
    digest = hashlib.sha256()
    for name in modules:
        spec = importlib.util.find_spec(name)
        if spec is None or spec.origin is None:
            raise ModuleNotFoundError(f"code module {name!r} not found for the compute key")
        tree = _strip_docstrings(ast.parse(Path(spec.origin).read_text(encoding="utf-8")))
        digest.update(f"{name}\n{ast.dump(tree, include_attributes=False)}\n".encode())
    return digest.hexdigest()


def library_versions(dists: Sequence[str]) -> str:
    """`name==version` for each distribution, in list order (`absent` when it is not installed)."""
    parts: list[str] = []
    for name in dists:
        try:
            version = metadata.version(name)
        except metadata.PackageNotFoundError:
            version = "absent"
        parts.append(f"{name}=={version}")
    return ";".join(parts)


def static_part(code_modules: Sequence[str]) -> str:
    """The part of the key that does not depend on the match: code digest, libraries, backend. A step computes it
    once per run and passes it to `combine_key` for every match."""
    return "\n".join(
        (
            f"code={code_digest(code_modules)}",
            f"libs={library_versions(TRACKING_LIBRARIES)}",
            f"numba={numba_active()}",
        )
    )


def combine_key(read_hashes: Sequence[tuple[str, str]], static: str) -> str:
    """The input key from `(read name, read hash)` pairs (in declaration order) and the static part."""
    lines = [f"{name}={value}" for name, value in read_hashes]
    return hashlib.sha256("\n".join([*lines, static]).encode()).hexdigest()


def input_key(
    conn: duckdb.DuckDBPyConnection,
    *,
    match_key: int,
    provider: str,
    reads: Sequence[InputRead],
    code_modules: Sequence[str],
    record_keys: Sequence[int] = (),
) -> str:
    """The exact input key of one match for one step (see the module docstring)."""
    hashes = [
        (read.name, read_content_hash(conn, read, match_key, provider, record_keys=record_keys)) for read in reads
    ]
    return combine_key(hashes, static_part(code_modules))

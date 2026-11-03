"""Run history, the match metadata table and the coordinate-system table (spec §9, §11.3, §11.4, §17.4; plan Task 21).

The tables live in catalog `lake` and are written by DuckLake into the lake root; they hold keys, names and metadata,
never row-level data (§9, §10.5). Every run, module run, match step and error has a stable id (§17.4): the run id;
`<run id>.<module id>`; `<module run id>.<native match id>`; `<run id>.e<n>` in order of writing. An error of a private
match keeps only its error class (and, for an unmapped field, the field paths), because a message can quote a value.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Final, Literal, cast

import duckdb

from lakehouse.catalog.duck import insert_rows
from lakehouse.errors import UnmappedFieldError
from lakehouse.parsers.base import CoordinateSystem

__all__ = [
    "TABLES",
    "ErrorRow",
    "MatchFile",
    "MatchRow",
    "MatchSource",
    "MatchStep",
    "ModuleRun",
    "RunRow",
    "VisibilityChange",
    "error_message",
    "read_table",
    "read_visibility_changes",
    "record_visibility_change",
    "replace_listing",
    "replace_match",
    "set_match_visibility",
    "upsert_source",
    "write_coordinate_systems",
    "write_errors",
    "write_module_run",
    "write_run",
    "write_steps",
    "write_visibility_changes",
]

Outcome = Literal["added", "changed", "unchanged", "failed", "excluded", "moved", "quarantined"]
Severity = Literal["error", "warning"]
_COUNTS: Final = (("added", "BIGINT"), ("changed", "BIGINT"), ("unchanged", "BIGINT"), ("failed", "BIGINT"))
TABLES: Final[dict[str, tuple[tuple[str, str], ...]]] = {
    "ops_runs": (
        ("run_id", "VARCHAR NOT NULL"),
        ("trigger", "VARCHAR"),  # null for a skipped run folded from its skip record
        ("status", "VARCHAR NOT NULL"),
        ("reason", "VARCHAR"),
        ("started_at", "TIMESTAMPTZ NOT NULL"),
        ("ended_at", "TIMESTAMPTZ"),
        *_COUNTS,
        ("skipped", "BIGINT"),
        ("excluded", "BIGINT"),
    ),
    "ops_module_runs": (
        ("module_run_id", "VARCHAR NOT NULL"),
        ("run_id", "VARCHAR NOT NULL"),
        ("module_id", "VARCHAR NOT NULL"),
        ("status", "VARCHAR NOT NULL"),
        ("started_at", "TIMESTAMPTZ NOT NULL"),
        ("ended_at", "TIMESTAMPTZ"),
        *_COUNTS,
        ("excluded", "BIGINT"),
    ),
    "ops_match_steps": (
        ("step_id", "VARCHAR NOT NULL"),
        ("run_id", "VARCHAR NOT NULL"),
        ("module_run_id", "VARCHAR NOT NULL"),
        ("module_id", "VARCHAR NOT NULL"),
        ("match_key", "BIGINT NOT NULL"),
        ("native_match_id", "VARCHAR NOT NULL"),
        ("outcome", "VARCHAR NOT NULL"),
        ("visibility", "VARCHAR NOT NULL"),
        ("recorded_at", "TIMESTAMPTZ NOT NULL"),
    ),
    "ops_errors": (
        ("error_id", "VARCHAR NOT NULL"),
        ("run_id", "VARCHAR NOT NULL"),
        ("module_run_id", "VARCHAR"),
        ("module_id", "VARCHAR"),
        ("native_match_id", "VARCHAR"),
        ("kind", "VARCHAR NOT NULL"),
        ("severity", "VARCHAR NOT NULL"),
        ("message", "VARCHAR NOT NULL"),
        ("recorded_at", "TIMESTAMPTZ NOT NULL"),
    ),
    "ops_matches": (
        ("match_key", "BIGINT NOT NULL"),
        ("provider", "VARCHAR NOT NULL"),
        ("native_match_id", "VARCHAR NOT NULL"),
        ("visibility", "VARCHAR NOT NULL"),
        ("kept_channel", "VARCHAR NOT NULL"),
        ("kept_reason", "VARCHAR NOT NULL"),
        ("competition_id", "VARCHAR"),
        ("competition_name", "VARCHAR"),
        ("season_id", "VARCHAR"),
        ("season_name", "VARCHAR"),
        ("has_360_frames", "BOOLEAN"),
        ("run_id", "VARCHAR NOT NULL"),
        ("updated_at", "TIMESTAMPTZ NOT NULL"),
        ("parser_version", "VARCHAR"),  # the version of the parser that wrote the bronze (§13.2)
    ),
    "ops_match_sources": (
        ("match_key", "BIGINT NOT NULL"),
        ("module_id", "VARCHAR NOT NULL"),
        ("channel", "VARCHAR NOT NULL"),
        ("channel_id", "VARCHAR NOT NULL"),
        ("last_listed", "TIMESTAMPTZ NOT NULL"),
    ),
    "ops_match_files": (
        ("match_key", "BIGINT NOT NULL"),
        ("provider", "VARCHAR NOT NULL"),
        ("bronze_table", "VARCHAR NOT NULL"),
        ("root", "VARCHAR NOT NULL"),
        ("path", "VARCHAR NOT NULL"),
        ("rows", "BIGINT NOT NULL"),
        ("ingested_at", "TIMESTAMPTZ NOT NULL"),
        ("run_id", "VARCHAR NOT NULL"),
        ("source_hash", "VARCHAR"),
    ),
    "ops_listings": (
        ("listing_key", "BIGINT NOT NULL"),
        ("module_id", "VARCHAR NOT NULL"),
        ("kind", "VARCHAR NOT NULL"),
        ("bronze_table", "VARCHAR NOT NULL"),
        ("content_sha256", "VARCHAR NOT NULL"),
        ("run_id", "VARCHAR NOT NULL"),
        ("updated_at", "TIMESTAMPTZ NOT NULL"),
    ),
    "ops_visibility_changes": (  # append-only ledger of encrypted-local visibility flips (plan T9; read by T12)
        ("match_key", "BIGINT NOT NULL"),
        ("new_visibility", "VARCHAR NOT NULL"),
        ("changed_at", "TIMESTAMPTZ NOT NULL"),  # not `at`: `AT` is a DuckDB reserved word
        ("run_id", "VARCHAR NOT NULL"),
    ),
    "ops_coordinate_systems": (
        ("match_key", "BIGINT NOT NULL"),
        ("provider", "VARCHAR NOT NULL"),
        ("artifact", "VARCHAR NOT NULL"),
        ("origin", "VARCHAR NOT NULL"),
        ("units", "VARCHAR NOT NULL"),
        ("pitch_length", "DOUBLE"),
        ("pitch_width", "DOUBLE"),
        ("x_direction", "VARCHAR NOT NULL"),
        ("y_direction", "VARCHAR NOT NULL"),
        ("play_direction", "VARCHAR"),
        ("source_ref", "VARCHAR NOT NULL"),
        ("run_id", "VARCHAR NOT NULL"),
    ),
}


@dataclass(frozen=True)
class _Row:
    def as_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RunRow(_Row):
    run_id: str
    trigger: str | None
    status: str
    reason: str | None
    started_at: datetime
    ended_at: datetime | None
    added: int
    changed: int
    unchanged: int
    failed: int
    skipped: int
    excluded: int


@dataclass(frozen=True)
class ModuleRun(_Row):
    module_run_id: str
    run_id: str
    module_id: str
    status: str
    started_at: datetime
    ended_at: datetime | None
    added: int
    changed: int
    unchanged: int
    failed: int
    excluded: int


@dataclass(frozen=True)
class MatchStep(_Row):
    step_id: str
    run_id: str
    module_run_id: str
    module_id: str
    match_key: int
    native_match_id: str
    outcome: Outcome
    visibility: str
    recorded_at: datetime


@dataclass(frozen=True)
class ErrorRow:
    module_run_id: str | None
    module_id: str | None
    native_match_id: str | None
    kind: str
    severity: Severity
    message: str
    recorded_at: datetime


@dataclass(frozen=True)
class MatchRow(_Row):
    match_key: int
    provider: str
    native_match_id: str
    visibility: Literal["public", "private"]
    kept_channel: str
    kept_reason: str
    competition_id: str | None
    competition_name: str | None
    season_id: str | None
    season_name: str | None
    has_360_frames: bool | None
    run_id: str
    updated_at: datetime
    parser_version: str | None = None


@dataclass(frozen=True)
class VisibilityChange(_Row):
    match_key: int
    new_visibility: Literal["public", "private"]
    changed_at: datetime
    run_id: str


@dataclass(frozen=True)
class MatchSource(_Row):
    match_key: int
    module_id: str
    channel: str
    channel_id: str
    last_listed: datetime


@dataclass(frozen=True)
class MatchFile(_Row):
    match_key: int
    provider: str
    bronze_table: str
    root: str
    path: str
    rows: int
    ingested_at: datetime
    run_id: str
    source_hash: str | None


def _create(conn: duckdb.DuckDBPyConnection, table: str) -> None:
    columns = ", ".join(f"{name} {kind}" for name, kind in TABLES[table])
    conn.execute(f"CREATE TABLE IF NOT EXISTS lake.main.{table} ({columns})")


def _insert(conn: duckdb.DuckDBPyConnection, table: str, rows: Sequence[Mapping[str, Any]]) -> None:
    _create(conn, table)
    if not rows:
        return
    names = [name for name, _ in TABLES[table]]
    # one statement, so one file per write on the encrypted-local path, not one per row (T3 spec D6)
    insert_rows(conn, table, names, [[row[name] for name in names] for row in rows])


def read_table(conn: duckdb.DuckDBPyConnection, table: str) -> list[dict[str, Any]]:
    """Every row of one history table in insertion order of its first column; `[]` before the first write."""
    if table not in TABLES:
        raise ValueError(f"unknown history table {table!r}")
    exists = conn.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main' AND table_name = ?",
        [table],
    ).fetchone()
    if exists is None or not exists[0]:
        return []
    names = [name for name, _ in TABLES[table]]
    result = conn.execute(f"SELECT {', '.join(names)} FROM lake.main.{table} ORDER BY {names[0]}").fetchall()  # noqa: S608 - constant names
    return [dict(zip(names, values, strict=True)) for values in result]


def write_run(conn: duckdb.DuckDBPyConnection, run: RunRow) -> None:
    _insert(conn, "ops_runs", [run.as_row()])


def write_module_run(conn: duckdb.DuckDBPyConnection, module_run: ModuleRun) -> None:
    _insert(conn, "ops_module_runs", [module_run.as_row()])


def write_steps(conn: duckdb.DuckDBPyConnection, steps: Sequence[MatchStep]) -> None:
    _insert(conn, "ops_match_steps", [step.as_row() for step in steps])


def write_errors(conn: duckdb.DuckDBPyConnection, run_id: str, errors: Sequence[ErrorRow]) -> None:
    """Insert errors with ids `<run id>.e<n>`, numbered on from the run's errors already written."""
    _create(conn, "ops_errors")
    found = conn.execute("SELECT count(*) FROM lake.main.ops_errors WHERE run_id = ?", [run_id]).fetchone()
    first = (found[0] if found is not None else 0) + 1
    rows = [
        {"error_id": f"{run_id}.e{first + index}", "run_id": run_id, **asdict(error)}
        for index, error in enumerate(errors)
    ]
    _insert(conn, "ops_errors", rows)


def replace_match(
    conn: duckdb.DuckDBPyConnection, match: MatchRow, sources: Sequence[MatchSource], files: Sequence[MatchFile]
) -> None:
    """The match's metadata row and file registry, replaced; its source rows replaced for the given modules only
    (another channel's source row stays)."""
    for table in ("ops_matches", "ops_match_files"):
        _create(conn, table)
        conn.execute(f"DELETE FROM lake.main.{table} WHERE match_key = ?", [match.match_key])  # noqa: S608 - constant names
    _create(conn, "ops_match_sources")
    for source in sources:
        conn.execute(
            "DELETE FROM lake.main.ops_match_sources WHERE match_key = ? AND module_id = ?",
            [source.match_key, source.module_id],
        )
    _insert(conn, "ops_matches", [match.as_row()])
    _insert(conn, "ops_match_sources", [source.as_row() for source in sources])
    _insert(conn, "ops_match_files", [file.as_row() for file in files])


def set_match_visibility(
    conn: duckdb.DuckDBPyConnection,
    match_key: int,
    visibility: Literal["public", "private"],
    *,
    run_id: str,
    updated_at: datetime,
) -> bool:
    """The match's visibility flag in `ops_matches`, set; `True` when it changed (no row for the match: `False`)."""
    _create(conn, "ops_matches")
    found = conn.execute("SELECT visibility FROM lake.main.ops_matches WHERE match_key = ?", [match_key]).fetchone()
    if found is None or found[0] == visibility:
        return False
    conn.execute(
        "UPDATE lake.main.ops_matches SET visibility = ?, run_id = ?, updated_at = ? WHERE match_key = ?",
        [visibility, run_id, updated_at, match_key],
    )
    return True


def record_visibility_change(conn: duckdb.DuckDBPyConnection, change: VisibilityChange) -> None:
    """Append one visibility-change record to the ledger (plan T9). The ledger is append-only; T12 restore reads
    the latest `new_visibility` per match from the pre-restore `CURRENT` and re-applies it after a data rollback."""
    _insert(conn, "ops_visibility_changes", [change.as_row()])


def read_visibility_changes(conn: duckdb.DuckDBPyConnection) -> list[VisibilityChange]:
    """Every visibility-change record, oldest first by `changed_at` (so the last per match is its latest); `[]`
    before the first flip."""
    exists = conn.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main' AND table_name = ?",
        ["ops_visibility_changes"],
    ).fetchone()
    if exists is None or not exists[0]:
        return []
    rows = conn.execute(
        "SELECT match_key, new_visibility, changed_at, run_id FROM lake.main.ops_visibility_changes ORDER BY changed_at"
    ).fetchall()
    return [
        VisibilityChange(
            match_key=int(match_key),
            new_visibility=cast('Literal["public", "private"]', str(new_visibility)),
            changed_at=changed_at,
            run_id=str(run_id),
        )
        for match_key, new_visibility, changed_at, run_id in rows
    ]


def write_visibility_changes(
    conn: duckdb.DuckDBPyConnection, changes: Sequence[VisibilityChange], *, replace: bool = False
) -> None:
    """Insert visibility-change records (plan T9/T12). `replace=True` first clears the ledger, so `restore` can
    carry the pre-restore `CURRENT`'s full flip history into the new version (as it carries move records)."""
    _create(conn, "ops_visibility_changes")
    if replace:
        conn.execute("DELETE FROM lake.main.ops_visibility_changes")
    _insert(conn, "ops_visibility_changes", [change.as_row() for change in changes])


def upsert_source(conn: duckdb.DuckDBPyConnection, source: MatchSource) -> None:
    """The match's source row for one module, replaced (its `last_listed`, §9)."""
    _create(conn, "ops_match_sources")
    conn.execute(
        "DELETE FROM lake.main.ops_match_sources WHERE match_key = ? AND module_id = ?",
        [source.match_key, source.module_id],
    )
    _insert(conn, "ops_match_sources", [source.as_row()])


def upsert_sources(conn: duckdb.DuckDBPyConnection, sources: Sequence[MatchSource]) -> None:
    """Many matches' source rows, replaced with one delete and one insert (the unchanged-match batch, T3 spec O-5)."""
    _create(conn, "ops_match_sources")
    if not sources:
        return
    pairs = ", ".join("(?, ?)" for _ in sources)
    conn.execute(
        f"DELETE FROM lake.main.ops_match_sources AS s USING (VALUES {pairs}) AS v(match_key, module_id) "  # noqa: S608 - placeholders only
        "WHERE s.match_key = v.match_key AND s.module_id = v.module_id",
        [value for source in sources for value in (source.match_key, source.module_id)],
    )
    _insert(conn, "ops_match_sources", [source.as_row() for source in sources])


def replace_listing(conn: duckdb.DuckDBPyConnection, row: Mapping[str, Any]) -> None:
    """The listing's row in `ops_listings`, replaced: its content hash decides the next re-write."""
    _create(conn, "ops_listings")
    conn.execute("DELETE FROM lake.main.ops_listings WHERE listing_key = ?", [row["listing_key"]])
    _insert(conn, "ops_listings", [row])


def write_coordinate_systems(
    conn: duckdb.DuckDBPyConnection,
    *,
    match_key: int,
    provider: str,
    run_id: str,
    systems: Mapping[str, CoordinateSystem],
) -> None:
    """One row per artifact of the match (§11.4); a re-parse replaces the match's rows."""
    _create(conn, "ops_coordinate_systems")
    conn.execute("DELETE FROM lake.main.ops_coordinate_systems WHERE match_key = ?", [match_key])
    rows = [
        {"match_key": match_key, "provider": provider, "artifact": artifact, "run_id": run_id, **asdict(system)}
        for artifact, system in sorted(systems.items())
    ]
    _insert(conn, "ops_coordinate_systems", rows)


def error_message(error: BaseException, *, visibility: Literal["public", "private"]) -> str:
    """The text stored for an error: in full for a public match; for a private one, the error class and, for an
    unmapped field, the artifact and field paths (a message can quote a value, §9)."""
    if visibility == "public":
        return str(error)
    if isinstance(error, UnmappedFieldError):
        return f"UnmappedFieldError: {error.artifact}: {', '.join(error.field_paths)}"
    return type(error).__name__

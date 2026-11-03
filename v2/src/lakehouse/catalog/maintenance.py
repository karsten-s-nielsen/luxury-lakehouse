"""Run-end maintenance of the DuckLake-managed small files (T12 spec D3).

Every insert into an ops table writes one data file, and partial deletes add delete files; v2 never runs DuckLake's
compaction (`CHECKPOINT` is forbidden on the attach, ADR-001). So at the run end, for each non-bronze table:

1. **Rewrite** each file that carries a delete file (`ducklake_rewrite_data_files`, delete threshold 0.0, owner
   decision 2026-11-01): the merge skips such files.
2. **Merge** the small files left (`ducklake_merge_adjacent_files`), in windows of creation time that hold at most
   `MERGE_SLICE_FILES` files each (one call over 100,000 files took 203 s; 5,000 per window take 0.5 s), then once
   more over the whole table when the windows left more than one file.

A **guard** runs first and fails closed: every bronze table (the `lakehouse:bronze` mark) must carry `auto_compact =
false`, because register-path bronze files are one file per match by design and DuckLake's rewrite makes tracking
files 2.7 to 3.5 times larger (§10.2). A table with at most `MERGE_MIN_FILES` live files and no delete file is left
alone. A **budget** (seconds) bounds the work: no new table or window starts after it is spent, and the next run
continues. Neither step deletes a file: the replaced files leave through the existing guarded cleanup once the
snapshots that name them expire (spike, plain and encrypted). The working copy carries the changes; the run-end
publish (T3 D1) publishes them.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from lakehouse.bronze.writer import BRONZE_COMMENT
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.errors import GuardStopError
from lakehouse.logging import get_logger

__all__ = [
    "MERGE_MIN_FILES",
    "MERGE_SLICE_FILES",
    "REWRITE_DELETE_THRESHOLD",
    "MaintenanceReport",
    "TableMaintenance",
    "maintain_tables",
]

REWRITE_DELETE_THRESHOLD: Final = 0.0
MERGE_MIN_FILES: Final = 16
MERGE_SLICE_FILES: Final = 20_000
_PAST_THE_LAST: Final = timedelta(microseconds=1)  # `older_than` excludes its own instant

_log = get_logger(__name__)


@dataclass(frozen=True)
class TableMaintenance:
    table: str
    files_before: int
    deletes_before: int
    rewritten: bool
    files_after: int
    slices: int
    ms: int


@dataclass(frozen=True)
class MaintenanceReport:
    tables: tuple[TableMaintenance, ...]
    budget_spent: bool
    left: tuple[str, ...]
    ms: int


@dataclass(frozen=True)
class _Window:
    newer_than: datetime
    older_than: datetime


def _tables(cat: WorkingCatalog) -> tuple[list[str], list[str]]:
    """(bronze tables, every other table) of `lake.main`, by name."""
    rows = cat.conn.execute(
        "SELECT table_name, comment FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main' "
        "ORDER BY table_name"
    ).fetchall()
    bronze = [str(name) for name, comment in rows if comment == BRONZE_COMMENT]
    return bronze, [str(name) for name, comment in rows if comment != BRONZE_COMMENT]


def _guard(cat: WorkingCatalog, bronze: list[str]) -> None:
    off = {
        str(entry).split(".", 1)[-1]
        for entry, value in cat.conn.execute(
            "SELECT scope_entry, value FROM ducklake_options('lake') "
            "WHERE option_name = 'auto_compact' AND scope = 'TABLE'"
        ).fetchall()
        if str(value).lower() == "false"
    }
    missing = [table for table in bronze if table not in off]
    if missing:
        raise GuardStopError(
            f"bronze tables without auto_compact = false: {', '.join(missing)}; no table is rewritten or merged "
            "(register-path bronze files are one file per match, spec §10.2)"
        )


def _counts(cat: WorkingCatalog, table: str) -> tuple[int, int]:
    row = cat.conn.execute(
        "SELECT count(*), count(delete_file) FROM ducklake_list_files('lake', ?)", [table]
    ).fetchone()
    return (int(row[0]), int(row[1])) if row is not None else (0, 0)


def _windows(cat: WorkingCatalog, table: str) -> list[_Window]:
    """Creation-time windows of at most `MERGE_SLICE_FILES` live files each (a snapshot is never split)."""
    times = [
        row[0]
        for row in cat.conn.execute(
            "SELECT s.snapshot_time FROM __ducklake_metadata_lake.ducklake_data_file f "
            "JOIN __ducklake_metadata_lake.ducklake_snapshot s ON s.snapshot_id = f.begin_snapshot "
            "JOIN __ducklake_metadata_lake.ducklake_table t ON t.table_id = f.table_id AND t.end_snapshot IS NULL "
            "WHERE t.table_name = ? AND f.end_snapshot IS NULL ORDER BY 1",
            [table],
        ).fetchall()
    ]
    windows: list[_Window] = []
    for start in range(0, len(times), MERGE_SLICE_FILES):
        end = start + MERGE_SLICE_FILES
        upper = times[end] if end < len(times) else times[-1] + _PAST_THE_LAST
        if upper > times[start]:
            windows.append(_Window(newer_than=times[start], older_than=upper))
    return windows


def _merge(cat: WorkingCatalog, table: str, window: _Window | None) -> int:
    """One merge call (over `window`, or the whole table); returns the number of files it merged."""
    if window is None:
        rows = cat.conn.execute("CALL ducklake_merge_adjacent_files('lake', ?)", [table]).fetchall()
    else:
        rows = cat.conn.execute(
            "CALL ducklake_merge_adjacent_files('lake', ?, newer_than => ?, older_than => ?)",
            [table, window.newer_than, window.older_than],
        ).fetchall()
    return sum(int(row[2]) for row in rows)


def maintain_tables(
    cat: WorkingCatalog, *, budget_s: float | None, monotonic: Callable[[], float] = time.monotonic
) -> MaintenanceReport:
    """Rewrite, then merge, every non-bronze table that needs it (see the module docstring). `budget_s` `None` is no
    budget (`sweep_now`). Raises `GuardStopError` before any change when a bronze table lacks `auto_compact = false`."""
    started_all = time.perf_counter()
    started = monotonic()
    bronze, tables = _tables(cat)
    _guard(cat, bronze)

    def spent() -> bool:
        return budget_s is not None and monotonic() - started >= budget_s

    done: list[TableMaintenance] = []
    left: list[str] = []
    for table in tables:
        files_before, deletes_before = _counts(cat, table)
        if files_before <= MERGE_MIN_FILES and deletes_before == 0:
            continue
        if spent():
            left.append(table)
            continue
        table_started = time.perf_counter()
        rewritten = False
        if deletes_before:
            cat.conn.execute(
                "CALL ducklake_rewrite_data_files('lake', ?, delete_threshold => ?)",
                [table, REWRITE_DELETE_THRESHOLD],
            ).fetchall()
            rewritten = True
        slices, stopped = 0, False
        for window in _windows(cat, table):
            if spent():
                stopped = True
                break
            _merge(cat, table, window)
            slices += 1
        files, _ = _counts(cat, table)
        if not stopped and 1 < files <= MERGE_SLICE_FILES and not spent():
            _merge(cat, table, None)
            slices += 1
        elif not stopped and files > 1 and spent():
            stopped = True
        files_after, _ = _counts(cat, table)
        if stopped:
            left.append(table)
        entry = TableMaintenance(
            table=table,
            files_before=files_before,
            deletes_before=deletes_before,
            rewritten=rewritten,
            files_after=files_after,
            slices=slices,
            ms=int((time.perf_counter() - table_started) * 1000),
        )
        done.append(entry)
        _log.info(
            "data files merged",
            extra={
                "table": table,
                "files_before": files_before,
                "deletes_before": deletes_before,
                "rewritten": rewritten,
                "files_after": files_after,
                "slices": slices,
                "ms": entry.ms,
            },
        )
    if left:
        _log.info("merge budget spent", extra={"tables_left": left, "budget_s": budget_s})
    return MaintenanceReport(
        tables=tuple(done),
        budget_spent=bool(left),
        left=tuple(left),
        ms=int((time.perf_counter() - started_all) * 1000),
    )

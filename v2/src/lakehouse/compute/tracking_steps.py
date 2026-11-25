"""The per-match drivers of the tracking computes (perf spec §7.3 lever S, §7.4 lever K; plan Task 5).

`TRACKING_FRAME_METRICS` is ONE layer-1 step group for pitch_control and shape_graphs: per match it computes each
scorer's exact input key BEFORE any frame build (`input_key`), skips a scorer whose bronze already holds rows written
from that key, builds the frame ONCE when at least one scorer is stale, and runs each stale scorer independently (one
scorer's failure records only that scorer's `compute_failed`; each table is written and swapped on its own). The run
still records one outcome per scorer name. `LINE_BREAKING` is the layer-2 step (actions x frames) with the same exact
key over its `fct_actions` read and the four frame reads.

Output logic lives only in the digested modules (`frames`, the scorer modules, `control`); this module is digested
too (`TRACKING_CODE_MODULES`), so any driver change re-computes. Every SQL text the drivers execute is a declared input
read (`FRAME_INPUT_READS`, `HOME_TEAM_READ`, `ACTIONS_INPUT_READ`) or a declared control read (`CONTROL_READS`).
"""

# pandas is stub-less; relax the strict "unknown"-family rules for this boundary module (see frames.py).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

import duckdb
import pandas as pd  # pyright: ignore[reportMissingTypeStubs]
import pyarrow as pa

from lakehouse.compute import input_key as ik
from lakehouse.compute.base import (
    ComputeOutcome,
    GroupMember,
    GroupResult,
    MatchBronze,
    SkipReason,
    run_per_match_bronze,
    run_per_match_group,
)
from lakehouse.compute.control import UNBOUNDED, ComputeAbandoned, ComputeControl
from lakehouse.compute.frames import FRAME_INPUT_READS, build_match_frame
from lakehouse.compute.line_breaking import LINE_BREAKING_SCHEMA, actions_for_scorer, line_breaking_values
from lakehouse.compute.pitch_control import PITCH_CONTROL_SCHEMA, home_and_away, pitch_control_values
from lakehouse.compute.shape_graphs import SHAPE_GRAPH_SCHEMA, shape_graph_values
from lakehouse.runs._common import FATAL

if TYPE_CHECKING:
    from datetime import datetime

    from lakehouse.bronze.writers import BronzeWriter
    from lakehouse.catalog.working import WorkingCatalog
    from lakehouse.compute.reads import InputRead
    from lakehouse.config.model import ResourceCaps
    from lakehouse.runs._common import ErrorSink

__all__ = [
    "LINE_BREAKING",
    "LINE_BREAKING_CODE_MODULES",
    "PITCH_CODE_MODULES",
    "SHAPE_CODE_MODULES",
    "TRACKING_FRAME_METRICS",
    "LineBreaking",
    "TrackingFrameMetrics",
    "match_keys",
]

_DRIVER: Final = "lakehouse.compute.tracking_steps"
_SHARED: Final = ("lakehouse.compute.frames", "lakehouse.compute.control", _DRIVER)

#: The code each output's key digests (perf spec §7.4 part 2): the shared frame path, the scorer, and this driver.
PITCH_CODE_MODULES: Final = (*_SHARED, "lakehouse.compute.pitch_control")
SHAPE_CODE_MODULES: Final = (*_SHARED, "lakehouse.compute.shape_graphs")
LINE_BREAKING_CODE_MODULES: Final = (*_SHARED, "lakehouse.compute.line_breaking")

_PITCH: Final = GroupMember("pitch_control", "pitch_control_values", PITCH_CONTROL_SCHEMA)
_SHAPE: Final = GroupMember("shape_graphs", "shape_graph_labels", SHAPE_GRAPH_SCHEMA)
_PITCH_READS: Final = (*FRAME_INPUT_READS, ik.HOME_TEAM_READ)
_LINE_BREAKING_READS: Final = (*FRAME_INPUT_READS, ik.ACTIONS_INPUT_READ)


def _read_hashes(
    conn: duckdb.DuckDBPyConnection,
    reads: Sequence[InputRead],
    match_key: int,
    provider: str,
    record_keys: tuple[int, ...],
) -> dict[str, str]:
    return {read.name: ik.read_content_hash(conn, read, match_key, provider, record_keys=record_keys) for read in reads}


def _key(hashes: Mapping[str, str], reads: Sequence[InputRead], static: str) -> str:
    return ik.combine_key([(read.name, hashes[read.name]) for read in reads], static)


def match_keys(
    conn: duckdb.DuckDBPyConnection, match_key: int, provider: str, record_keys: tuple[int, ...]
) -> dict[str, str]:
    """The exact input keys of one match for pitch_control and shape_graphs (each read hashed once). `record_keys`
    are the match's provider record keys (`input_key.match_record_keys`)."""
    hashes = _read_hashes(conn, _PITCH_READS, match_key, provider, record_keys)
    return _frame_metric_keys(hashes, ik.static_part(PITCH_CODE_MODULES), ik.static_part(SHAPE_CODE_MODULES))


def _frame_metric_keys(hashes: Mapping[str, str], pitch_static: str, shape_static: str) -> dict[str, str]:
    """The two group members' keys from one set of read hashes (pitch_control also hashes the home-team read)."""
    return {
        _PITCH.step: _key(hashes, _PITCH_READS, pitch_static),
        _SHAPE.step: _key(hashes, FRAME_INPUT_READS, shape_static),
    }


def _already_computed(conn: duckdb.DuckDBPyConnection, table: str, match_key: int, key: str) -> bool:
    """The skip lookup (control reads): the match's bronze already holds rows written from this exact input key."""
    present = conn.execute(ik.TABLE_PRESENT_SQL, [table]).fetchone()
    if present is None or not present[0]:
        return False
    return conn.execute(ik.skip_lookup_sql(table), [match_key, key]).fetchone() is not None


def _home_team(conn: duckdb.DuckDBPyConnection, match_key: int, provider: str) -> int:
    row = conn.execute(ik.HOME_TEAM_READ.sql, ik.HOME_TEAM_READ.params(match_key, provider)).fetchone()
    if row is None or row[0] is None:
        raise ValueError(f"no fct_match_params.home_team_key for match {match_key}")
    return int(row[0])


@dataclass(frozen=True)
class TrackingFrameMetrics:
    """The layer-1 step group for pitch_control + shape_graphs (one frame build per match). The callables are the
    real builder and scorers; tests inject fakes."""

    names: tuple[str, ...] = (_PITCH.step, _SHAPE.step)
    output_tables: tuple[str, ...] = (_PITCH.output_table, _SHAPE.output_table)
    layer: int = 1
    input_select: str = "+fct_tracking_frames"
    frame_builder: Callable[..., pd.DataFrame] = field(default=build_match_frame)
    pitch_scorer: Callable[..., pa.Table] = field(default=pitch_control_values)
    shape_scorer: Callable[..., pa.Table] = field(default=shape_graph_values)
    writer: BronzeWriter | None = None

    def __call__(
        self,
        cat: WorkingCatalog,
        *,
        run_id: str,
        caps: ResourceCaps,
        now: Callable[[], datetime],
        error: ErrorSink,
        control: ComputeControl = UNBOUNDED,
    ) -> tuple[ComputeOutcome, ...]:
        conn = cat.conn
        matches = conn.execute(ik.TRACKING_DISCOVERY_SQL).fetchall()
        pitch_static = ik.static_part(PITCH_CODE_MODULES)
        shape_static = ik.static_part(SHAPE_CODE_MODULES)

        def produce(row: Any, match_key: int) -> dict[str, GroupResult]:
            provider = str(row[1])
            # one record-key lookup per match feeds the key reads AND the frame build (perf spec §7.4a item 6)
            record_keys = ik.match_record_keys(conn, match_key)
            hashes = _read_hashes(conn, _PITCH_READS, match_key, provider, record_keys)
            keys = _frame_metric_keys(hashes, pitch_static, shape_static)
            results: dict[str, GroupResult] = {}
            for member in (_PITCH, _SHAPE):
                if _already_computed(conn, member.output_table, match_key, keys[member.step]):
                    results[member.step] = SkipReason.DONE
            stale = [m for m in (_PITCH, _SHAPE) if m.step not in results]
            if not stale:
                return results
            frame = self.frame_builder(cat, match_key, provider=provider, record_keys=record_keys)
            for member in stale:
                if len(frame) == 0:
                    results[member.step] = SkipReason.EMPTY
                    continue
                try:
                    arrow = self._score(member, frame, conn, match_key, provider, control)
                except (*FATAL, ComputeAbandoned):
                    raise  # a control error, or the hard deadline/stop: the whole match is deferred, nothing written
                except Exception as exc:  # noqa: BLE001 - this scorer fails alone; the other stands (perf spec §7.3)
                    results[member.step] = exc
                    continue
                results[member.step] = (
                    SkipReason.EMPTY if arrow.num_rows == 0 else MatchBronze(data=arrow, source_hash=keys[member.step])
                )
            return results

        return run_per_match_group(
            cat,
            members=(_PITCH, _SHAPE),
            matches=matches,
            match_key_of=lambda r: int(r[0]),
            produce=produce,
            run_id=run_id,
            now=now,
            error=error,
            writer=self.writer,
            control=control,
        )

    def _score(
        self,
        member: GroupMember,
        frame: pd.DataFrame,
        conn: duckdb.DuckDBPyConnection,
        match_key: int,
        provider: str,
        control: ComputeControl,
    ) -> pa.Table:
        if member is _PITCH:
            home, away = home_and_away(frame, _home_team(conn, match_key, provider))
            return self.pitch_scorer(
                frame,
                home_team_id=home,
                away_team_id=away,
                should_stop=control.should_stop,
                check_every=control.check_every,
            )
        return self.shape_scorer(frame, should_stop=control.should_stop, check_every=control.check_every)


@dataclass(frozen=True)
class LineBreaking:
    """The layer-2 `line_breaking` step (actions x frames -> one bronze table), skipping on its exact input key."""

    name: str = "line_breaking"
    output_table: str = "line_breaking_results"
    layer: int = 2
    input_select: str = "+fct_actions"
    frame_builder: Callable[..., pd.DataFrame] = field(default=build_match_frame)
    scorer: Callable[..., pa.Table] = field(default=line_breaking_values)
    writer: BronzeWriter | None = None

    def __call__(
        self,
        cat: WorkingCatalog,
        *,
        run_id: str,
        caps: ResourceCaps,
        now: Callable[[], datetime],
        error: ErrorSink,
        control: ComputeControl = UNBOUNDED,
    ) -> ComputeOutcome:
        conn = cat.conn
        matches = conn.execute(ik.LINE_BREAKING_DISCOVERY_SQL).fetchall()
        static = ik.static_part(LINE_BREAKING_CODE_MODULES)

        def produce(row: Any, match_key: int) -> MatchBronze | SkipReason:
            provider = str(row[1])
            # one record-key lookup per match feeds the key reads AND the frame build (perf spec §7.4a item 6)
            record_keys = ik.match_record_keys(conn, match_key)
            hashes = _read_hashes(conn, _LINE_BREAKING_READS, match_key, provider, record_keys)
            key = _key(hashes, _LINE_BREAKING_READS, static)
            if _already_computed(conn, self.output_table, match_key, key):
                return SkipReason.DONE
            read = ik.ACTIONS_INPUT_READ
            fct_actions: Any = conn.execute(read.sql, read.params(match_key, provider)).df()
            frame = self.frame_builder(cat, match_key, provider=provider, record_keys=record_keys)
            if len(fct_actions) == 0 or len(frame) == 0:
                return SkipReason.EMPTY
            arrow = self.scorer(actions_for_scorer(fct_actions), frame)
            if arrow.num_rows == 0:
                return SkipReason.EMPTY
            return MatchBronze(data=arrow, source_hash=key)

        return run_per_match_bronze(
            cat,
            step=self.name,
            output_table=self.output_table,
            schema=LINE_BREAKING_SCHEMA,
            matches=matches,
            match_key_of=lambda r: int(r[0]),
            produce=produce,
            run_id=run_id,
            now=now,
            error=error,
            writer=self.writer,
            control=control,
        )


#: The registered layer-1 group (pitch_control + shape_graphs) and the layer-2 line_breaking step.
TRACKING_FRAME_METRICS: Final = TrackingFrameMetrics()
LINE_BREAKING: Final = LineBreaking()

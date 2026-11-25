"""The compute phase: the `ComputeStep` contract and the `run_compute` driver (spec D2/D6; plan Task 1).

`run_compute(layer=N)` runs the registered steps of layer N in order (spec D9). The run's `finish` calls it once per
layer with a dbt build between: full build -> layer 1 (the five spadl_actions steps; pitch_control and shape_graphs
are registered but out of the default run, calc-layer spec D10) -> `+fct_actions` build -> layer 2 (line_breaking,
reads `fct_actions`) -> `+fct_tracking_metrics` build. A step reads its dbt input
view(s) per match, calls a scorer, and writes one bronze table; it catches **per match**, records a failing match
through the run's `ErrorSink`
(`kind="compute_failed"`, the match key in the message, `native=None` — the canonical key is not the provider's
native id), and keeps going. A step that raises as a whole is recorded the same way and the remaining steps still
run (the §12.3 "a unit fails, the run goes on" rule). Both a failing match and a failing step set the run failed
through the `ErrorSink` (its `severity="error"`), exactly like every other run error. A `FATAL` control error
(lost lease, catalog conflict, guard stop) propagates.

The mart build that follows the phase (`+fct_tracking_metrics`, `COMPUTE_MART_SELECT`) repopulates the staging and
mart views from the bronze the steps wrote.
"""

from __future__ import annotations

import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Protocol, runtime_checkable

import pyarrow as pa

from lakehouse.bronze.schemas import bronze_table
from lakehouse.bronze.writer import Visibility
from lakehouse.bronze.writers import BronzeWriter, Put, select_writer
from lakehouse.compute.control import UNBOUNDED, ComputeAbandoned, ComputeControl
from lakehouse.logging import get_logger
from lakehouse.runs._common import FATAL, ErrorSink

if TYPE_CHECKING:
    from lakehouse.catalog.working import WorkingCatalog
    from lakehouse.config.model import ResourceCaps

__all__ = [
    "COMPUTE_L1_SELECT",
    "COMPUTE_MART_SELECT",
    "MATCH_RECORD_KEYS_SQL",
    "MATCH_VISIBILITY_SQL",
    "ComputeOutcome",
    "ComputeResult",
    "ComputeStep",
    "ComputeStepGroup",
    "GroupMember",
    "GroupResult",
    "MatchBronze",
    "SkipReason",
    "run_compute",
    "run_per_match_bronze",
    "run_per_match_group",
]

#: The middle build's selector (spec D9). After the layer-1 compute writes `spadl_actions` bronze, this build
#: materialises `fct_actions` (its `+` upstream prefix also rebuilds `stg__spadl_actions` off the populated bronze,
#: not its empty `lake_table_present` branch) so the layer-2 step (line_breaking) can read it. It names ONLY
#: `fct_actions`; the pitch_control/shape_graphs staging views have no layer-2 consumer and build in the final
#: `COMPUTE_MART_SELECT` pass, so no model is built by two of the three builds (CALC-A2-01/02).
COMPUTE_L1_SELECT: Final = "+fct_actions"

#: The final build's selector (spec D2 / CALC-SPEC-11): the `+` upstream prefix rebuilds the pitch_control +
#: shape_graphs + line_breaking staging views as well as the mart, so they pick up the bronze the compute wrote.
#: The FIRST build is the run's full build (no selector), so dimensions and every non-compute model still build
#: (plan ruling R1).
COMPUTE_MART_SELECT: Final = "+fct_tracking_metrics"

#: The provider record keys that point to a canonical match (a CONTROL read; perf spec §7.4a item 4).
MATCH_RECORD_KEYS_SQL: Final = (
    "SELECT record_key FROM lake.main.ops_canonical_pointers WHERE entity = 'match' AND canonical_key = ?"
)

#: The per-match visibility lookup (a CONTROL read: it routes the write, it never shapes output; perf spec §7.4).
MATCH_VISIBILITY_SQL: Final = (
    "SELECT bool_or(visibility = 'private') FROM lake.main.ops_matches WHERE match_key = ? OR match_key IN "  # noqa: S608 - a module constant, no outside input
    f"({MATCH_RECORD_KEYS_SQL})"
)

_log = get_logger("lakehouse.runs.run")  # one logger name for the whole run


@dataclass(frozen=True)
class ComputeOutcome:
    """One step's result. `failed_matches` counts matches the step recorded as `compute_failed` (it keeps going).
    `deferred_matches` counts matches NOT REACHED this run (the soft deadline passed, the run was asked to stop, or a
    scorer was abandoned at the hard deadline): their skip state is unchecked, and the next run continues (§7.5)."""

    step: str
    matches: int
    rows: int
    empty_matches: int
    failed_matches: int
    deferred_matches: int = 0


@dataclass(frozen=True)
class ComputeResult:
    outcomes: list[ComputeOutcome]


@dataclass(frozen=True)
class MatchBronze:
    """One match's computed bronze: the arrow table to persist + its deterministic source hash (idempotency +
    skip-guard + `content_version`). A step's `produce` callback returns this to write, or a `SkipReason` to skip."""

    data: pa.Table
    source_hash: str


class SkipReason(Enum):
    """Why `run_per_match_bronze` wrote nothing for a match (both skip the write; they differ only in the tally)."""

    EMPTY = "empty"  # the scorer produced no rows for this match -> counted in `empty_matches`
    DONE = "done"  # the skip-guard matched an identical prior write -> silent (an idempotent re-run)


def _match_visibility(cat: WorkingCatalog, match_key: int) -> Visibility:
    """The match's visibility, for the bronze write (the writer routes a `private` match to the restricted bucket).

    `match_key` is the step's iterated key — the provider surrogate for the SPADL steps, the CANONICAL key for the
    tracking steps — so resolve BOTH: the direct `ops_matches` row, OR, when the key is a canonical key, every
    contributing surrogate (via `ops_canonical_pointers`). Take the MOST RESTRICTIVE (`private` if any contributor is
    private) and FAIL CLOSED to `private` when no row is found — a missed lookup must never write private data to the
    public bucket (security: fail-open visibility leaks a restricted match)."""
    row = cat.conn.execute(MATCH_VISIBILITY_SQL, [match_key, match_key]).fetchone()
    any_private = row[0] if row is not None else None
    return "public" if any_private is False else "private"


def _write_match_bronze(
    cat: WorkingCatalog,
    writer: BronzeWriter,
    table: Any,
    schema: pa.Schema,
    bronze: MatchBronze,
    *,
    match_key: int,
    run_id: str,
    now: Callable[[], datetime],
) -> None:
    """Write one match's bronze through the writer and swap it in (the write+swap incantation shared by every step).

    The bronze writer keys one file per match on a `match_key` column (`writer._check_rows`: every row must equal the
    `match_key` arg). The pure scorers emit only canonical keys, so the per-match write key is injected here — equal to
    the step's iterated key (the canonical key for the tracking steps, the provider surrogate for the SPADL step). The
    staging views read `canonical_match_key`, not this column.
    """
    visibility = _match_visibility(cat, match_key)
    mk_field = pa.field("match_key", pa.int64(), nullable=False)
    data = bronze.data.append_column(mk_field, pa.array([match_key] * bronze.data.num_rows, pa.int64()))
    # pyarrow's stubs type Schema.append's field param as Field[Unknown]; the call is correct, the type is just noisy.
    schema = schema.append(mk_field)  # pyright: ignore[reportUnknownMemberType]
    with tempfile.TemporaryDirectory() as tmp:
        put = Put(table=table, data=data, is_tracking=False, schema=schema, source_hash=bronze.source_hash)
        written = writer.write_table(
            put,
            match_key=match_key,
            visibility=visibility,
            content_version=bronze.source_hash[:16],
            run_id=run_id,
            out_dir=Path(tmp),
            ingested_at=now(),
        )
        writer.swap(match_key, [written], {table}, also=lambda: None)


def _record_deferred(error: ErrorSink, step: str, deferred: int) -> None:
    """One warning per step with matches not reached: not a failure (the run stays ok), visible in ops (§7.5)."""
    if deferred:
        error(
            kind="compute_deferred",
            message=f"{step}: {deferred} matches not reached this run (skip state unchecked); the next run continues",
            severity="warning",
            native=None,
        )


def _log_match(step: str, match_key: int, started: float, rows: int, outcome: str) -> None:
    """The per-match timing line (perf spec §6: worst-match and steady-state calibration read it)."""
    _log.info(
        "compute match",
        extra={
            "step": step,
            "match_key": match_key,
            "seconds": round(time.perf_counter() - started, 3),
            "rows": rows,
            "outcome": outcome,
        },
    )


def run_per_match_bronze(
    cat: WorkingCatalog,
    *,
    step: str,
    output_table: str,
    schema: pa.Schema,
    matches: Sequence[Any],
    match_key_of: Callable[[Any], int],
    produce: Callable[[Any, int], MatchBronze | SkipReason],
    run_id: str,
    now: Callable[[], datetime],
    error: ErrorSink,
    writer: BronzeWriter | None = None,
    control: ComputeControl = UNBOUNDED,
) -> ComputeOutcome:
    """The per-match bronze write loop shared by every compute step (spec D6/D9; plan Task 4 DRY extraction).

    For each row in `matches`, `produce(row, match_key)` returns the match's `MatchBronze` (written + counted in `rows`)
    or a `SkipReason` (`EMPTY` -> `empty_matches`; `DONE` -> silent). A per-match exception is recorded through `error`
    (`kind="compute_failed"`, `severity="error"` -> the run fails) and the loop goes on (§12.3); a `FATAL` control
    error propagates. The only variation between steps — the match-discovery query (its rows become `matches`), how to
    read the match key from a row (`match_key_of`), and the per-match scorer (`produce`) — is injected by the caller.

    `control` (perf spec §7.5): no match starts once `may_start_match()` is False — that match and every later one are
    NOT REACHED (`deferred_matches`, one `compute_deferred` warning); a `ComputeAbandoned` from `produce` writes nothing
    and counts as not reached; each written match calls `on_written()` (the run's checkpoint policy). The writer is
    created on the first write, so a run whose matches all skip never opens one.

    A single step is a group of one member: the loop is `run_per_match_group`, so the two cannot drift apart.
    """

    def produce_one(row: Any, match_key: int) -> Mapping[str, GroupResult]:
        return {step: produce(row, match_key)}

    (outcome,) = run_per_match_group(
        cat,
        members=(GroupMember(step, output_table, schema),),
        matches=matches,
        match_key_of=match_key_of,
        produce=produce_one,
        run_id=run_id,
        now=now,
        error=error,
        writer=writer,
        control=control,
    )
    return outcome


def _failed(error: ErrorSink, step: str, match_key: int, exc: BaseException) -> None:
    error(
        kind="compute_failed",
        message=f"{step}: match_key={match_key} failed: {type(exc).__name__}: {exc}",
        severity="error",
        native=None,
    )


@dataclass(frozen=True)
class GroupMember:
    """One output of a step group (perf spec §7.3): its outcome name, its bronze table and that table's schema."""

    step: str
    output_table: str
    schema: pa.Schema


#: What a group's `produce` returns per member: rows to write, a skip, or the exception that member's scorer raised.
GroupResult = MatchBronze | SkipReason | Exception


def run_per_match_group(
    cat: WorkingCatalog,
    *,
    members: Sequence[GroupMember],
    matches: Sequence[Any],
    match_key_of: Callable[[Any], int],
    produce: Callable[[Any, int], Mapping[str, GroupResult]],
    run_id: str,
    now: Callable[[], datetime],
    error: ErrorSink,
    writer: BronzeWriter | None = None,
    control: ComputeControl = UNBOUNDED,
) -> tuple[ComputeOutcome, ...]:
    """The per-match loop of a step GROUP: several scorers share one per-match input (one frame build), and each
    member keeps the semantics of a separate step (perf spec §7.3).

    `produce(row, match_key)` returns one `GroupResult` per member name. A member's `MatchBronze` is written and
    swapped on its own; its `Exception` is recorded as that member's `compute_failed` (the other members are not
    affected); `SkipReason.EMPTY` counts as empty; `SkipReason.DONE` is silent. An exception raised by `produce`
    itself (before any scorer, e.g. the shared frame build) fails the match for every member. `FATAL` propagates.
    `control` works as in `run_per_match_bronze`; a `ComputeAbandoned` (from either scorer) writes NOTHING for the match
    and counts it as not reached for every member. The returned outcomes are in `members` order, one per member name
    (the run records and logs them per name)."""
    tally = {m.step: {"matches": 0, "rows": 0, "empty": 0, "failed": 0, "deferred": 0} for m in members}
    for index, row in enumerate(matches):
        if not control.may_start_match():
            for member in members:
                tally[member.step]["deferred"] += len(matches) - index
            break
        match_key = match_key_of(row)
        started = time.perf_counter()
        try:
            results = produce(row, match_key)
        except FATAL:
            raise
        except ComputeAbandoned:
            for member in members:
                tally[member.step]["deferred"] += 1
                _log_match(member.step, match_key, started, 0, "abandoned")
            continue
        except Exception as exc:  # noqa: BLE001 - a shared-phase failure fails the match for every member (§12.3)
            for member in members:
                tally[member.step]["matches"] += 1
                tally[member.step]["failed"] += 1
                _failed(error, member.step, match_key, exc)
                _log_match(member.step, match_key, started, 0, "failed")
            continue
        for member in members:
            tally[member.step]["matches"] += 1
            result = results[member.step]
            if result is SkipReason.DONE:
                _log_match(member.step, match_key, started, 0, "done")
                continue
            if result is SkipReason.EMPTY:
                tally[member.step]["empty"] += 1
                _log_match(member.step, match_key, started, 0, "empty")
                continue
            if isinstance(result, Exception):
                tally[member.step]["failed"] += 1
                _failed(error, member.step, match_key, result)
                _log_match(member.step, match_key, started, 0, "failed")
                continue
            try:
                writer = writer if writer is not None else select_writer(cat, cat.storage)
                _write_match_bronze(
                    cat,
                    writer,
                    bronze_table(member.output_table),
                    member.schema,
                    result,
                    match_key=match_key,
                    run_id=run_id,
                    now=now,
                )
            except FATAL:
                raise
            except Exception as exc:  # noqa: BLE001 - this member's write failed; the others stand (§12.3)
                tally[member.step]["failed"] += 1
                _failed(error, member.step, match_key, exc)
                _log_match(member.step, match_key, started, 0, "failed")
                continue
            tally[member.step]["rows"] += result.data.num_rows
            control.on_written()
            _log_match(member.step, match_key, started, result.data.num_rows, "written")
    for member in members:
        _record_deferred(error, member.step, tally[member.step]["deferred"])
    return tuple(
        ComputeOutcome(
            step=m.step,
            matches=tally[m.step]["matches"],
            rows=tally[m.step]["rows"],
            empty_matches=tally[m.step]["empty"],
            failed_matches=tally[m.step]["failed"],
            deferred_matches=tally[m.step]["deferred"],
        )
        for m in members
    )


@runtime_checkable
class ComputeStepGroup(Protocol):
    """Several per-match computes that share one per-match input (perf spec §7.3): one frame build per match.

    The run sees one outcome per member name (`names`, in order), exactly as if they were separate steps."""

    @property
    def names(self) -> tuple[str, ...]: ...
    @property
    def output_tables(self) -> tuple[str, ...]: ...
    @property
    def layer(self) -> int: ...
    @property
    def input_select(self) -> str: ...

    def __call__(
        self,
        cat: WorkingCatalog,
        *,
        run_id: str,
        caps: ResourceCaps,
        now: Callable[[], datetime],
        error: ErrorSink,
        control: ComputeControl = UNBOUNDED,
    ) -> tuple[ComputeOutcome, ...]: ...


class ComputeStep(Protocol):
    """A per-match compute: reads `input_select`'s view(s), writes `output_table` bronze, returns its outcome.

    `layer` places the step in the run's build/compute alternation (spec D9): `layer=1` reads only layer-0 input
    views (`run_compute` runs it before the `+fct_actions` build); `layer=2` reads a layer-1 output view
    (`fct_actions`) and runs after that build. Only line_breaking is `layer=2` this cycle.

    The four attributes are read-only (properties): a step is immutable config, so the concrete steps are frozen
    dataclasses. A writable-attribute Protocol would reject a frozen dataclass (pyright reportAssignmentType), so
    they are declared as read-only here; a class attribute on a test fake satisfies a read-only property too.
    """

    @property
    def name(self) -> str: ...
    @property
    def output_table(self) -> str: ...
    @property
    def layer(self) -> int: ...
    @property
    def input_select(self) -> str: ...

    def __call__(
        self,
        cat: WorkingCatalog,
        *,
        run_id: str,
        caps: ResourceCaps,
        now: Callable[[], datetime],
        error: ErrorSink,
        control: ComputeControl = UNBOUNDED,
    ) -> ComputeOutcome: ...


def run_compute(
    cat: WorkingCatalog,
    *,
    run_id: str,
    caps: ResourceCaps,
    now: Callable[[], datetime],
    steps: Sequence[ComputeStep | ComputeStepGroup],
    layer: int,
    error: ErrorSink,
    control_for: Callable[[int], ComputeControl] = lambda _layer: UNBOUNDED,
) -> ComputeResult:
    """Run the steps whose `step.layer == layer`; a step that raises is recorded via `error` (and the run fails) and
    the rest of the layer still run (spec D9 layered seam). A step group reports one outcome per member name, and a
    group that raises records one `compute_failed` per member name (perf spec §7.3)."""
    outcomes: list[ComputeOutcome] = []
    for step in steps:
        if step.layer != layer:
            continue
        names = step.names if isinstance(step, ComputeStepGroup) else (step.name,)
        try:
            control = control_for(layer)
            if isinstance(step, ComputeStepGroup):
                produced = step(cat, run_id=run_id, caps=caps, now=now, error=error, control=control)
            else:
                produced = (step(cat, run_id=run_id, caps=caps, now=now, error=error, control=control),)
        except FATAL:
            raise
        except Exception as exc:  # noqa: BLE001 - a failing step is recorded (ops_errors) and the run goes on (§12.3)
            for name in names:
                error(
                    kind="compute_failed",
                    message=f"{name}: {type(exc).__name__}: {exc}",
                    severity="error",
                    native=None,
                )
                _log.warning("compute step failed", extra={"step": name, "kind": type(exc).__name__})
            continue
        for outcome in produced:
            outcomes.append(outcome)
            _log.info(
                "compute step",
                extra={
                    "step": outcome.step,
                    "matches": outcome.matches,
                    "rows": outcome.rows,
                    "empty_matches": outcome.empty_matches,
                    "failed_matches": outcome.failed_matches,
                    "deferred_matches": outcome.deferred_matches,
                },
            )
    return ComputeResult(outcomes=outcomes)

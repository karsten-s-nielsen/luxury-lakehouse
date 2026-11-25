"""Profile one match of a tracking compute step against the PUBLISHED catalog (perf plan Task 1; spec §6 step 1).

Usage (run from `v2/`, with the lake roots resolved as for any `lakehouse` command):

    uv run python scripts/profile_tracking_compute.py --provider gradientsports --out <dir> \
        [--match-key <int>] [--step pitch_control|shape_graphs|line_breaking] [--lake <root>]

The script is READ-ONLY: it opens the version that `CURRENT` names through `CatalogReader`, computes the step's exact
input key (the skip check), builds the match frame and scores it under `cProfile`, and writes nothing to the lake.
It writes one JSON per step to `<dir>`:
`{provider, match_key, step, frames, backend, phases, total_s}`. `phases` maps each phase of the fixed phase map
(`PHASES`) to seconds. Without `--match-key` it takes the provider's largest match by `fct_tracking_frames` rows.
Without `--step` it profiles `pitch_control` and `shape_graphs`. The write + swap time is not here: it comes from
the per-match `"compute match"` log line of a real run.
"""

# pandas/silly_kicks are stub-less; relax the strict "unknown"-family rules (see src/lakehouse/compute/frames.py).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

import argparse
import cProfile
import json
import os
import pstats
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast

import duckdb

from lakehouse.catalog.reader import CatalogReader
from lakehouse.compute import input_key as ik
from lakehouse.compute import line_breaking, pitch_control, shape_graphs, tracking_steps
from lakehouse.compute.backend import numba_active
from lakehouse.compute.frames import FRAME_INPUT_READS, build_match_frame
from lakehouse.storage.roots import default_bootstrap_file, open_storage, resolve_roots

if TYPE_CHECKING:
    from lakehouse.catalog.working import WorkingCatalog

STEPS: Final = ("pitch_control", "shape_graphs", "line_breaking")
DEFAULT_STEPS: Final = ("pitch_control", "shape_graphs")

#: The fixed phase map (perf plan Task 1). A plain name adds that function's cumulative time; `-child<parent`
#: subtracts the child's cumulative time spent in calls made from `parent` (so `combine` is net of its influence).
PHASES: Final[Mapping[str, tuple[str, ...]]] = {
    "read_convert": ("build_match_frame",),
    "skip_key": ("match_record_keys", "match_keys", "input_key"),
    "velocity": ("smooth_frames", "derive_velocities"),
    "tti": ("compute_tti",),
    "influence": ("_compute_influence",),
    "combine": ("_spearman_combine", "-_compute_influence<_spearman_combine", "-influence_numba<_spearman_combine"),
    "extract": ("_extract_frame_players",),
    "ball": ("_resolve_ball_position",),
    # `infer_positions` re-enters `compute_shape_graph` via `_decompose_middle`: count those nested calls once.
    "shape": ("compute_shape_graph", "infer_positions", "-compute_shape_graph<_decompose_middle"),
    "line_breaking": ("detect_line_breaking",),
}

# pstats' per-function row: (primitive calls, total calls, own time, cumulative time, callers).
_StatsRow = tuple[int, int, float, float, dict[tuple[str, int, str], tuple[int, int, float, float]]]


def summarise_profile(stats: pstats.Stats, phases: Mapping[str, tuple[str, ...]]) -> dict[str, float]:
    """Seconds per phase: the summed cumulative time of the phase's functions (matched by function name).

    An entry `-child<parent` subtracts the cumulative time of `child` calls made directly from `parent`, read from
    pstats' per-caller records, so a phase can be reported net of a sub-phase it calls."""
    table = cast("dict[tuple[str, int, str], _StatsRow]", stats.stats)  # pyright: ignore[reportAttributeAccessIssue]
    result: dict[str, float] = {}
    for phase, entries in phases.items():
        total = 0.0
        for entry in entries:
            if entry.startswith("-"):
                child, parent = entry[1:].split("<", 1)
                for key, row in table.items():
                    if key[2] != child:
                        continue
                    total -= sum(times[3] for caller, times in row[4].items() if caller[2] == parent)
            else:
                total += sum(row[3] for key, row in table.items() if key[2] == entry)
        result[phase] = total
    return result


@dataclass(frozen=True)
class _ReadOnlyCatalog:
    """The one attribute the frame builder and the step helpers read (`cat.conn`), on a published-version session."""

    conn: duckdb.DuckDBPyConnection


def _largest_match(conn: duckdb.DuckDBPyConnection, provider: str) -> int:
    row = conn.execute(
        "SELECT canonical_match_key FROM lake.main.fct_tracking_frames WHERE provider = ? "
        "GROUP BY canonical_match_key ORDER BY count(*) DESC LIMIT 1",
        [provider],
    ).fetchone()
    if row is None:
        raise SystemExit(f"no fct_tracking_frames rows for provider {provider!r}")
    return int(row[0])


def _work(step: str, cat: WorkingCatalog, match_key: int, provider: str) -> Callable[[], int]:
    """The step's per-match work as the step does it today (read, hash, skip check, score), minus the bronze write.
    Returns a callable that runs it and gives the number of distinct frames scored."""

    def frames_of(frame: Any) -> int:
        return len(frame[["period_id", "frame_id"]].drop_duplicates())

    def run_pitch_control() -> int:
        record_keys = ik.match_record_keys(cat.conn, match_key)
        tracking_steps.match_keys(cat.conn, match_key, provider, record_keys)
        frame = build_match_frame(cat, match_key, provider=provider, record_keys=record_keys)
        read = ik.HOME_TEAM_READ
        row = cat.conn.execute(read.sql, read.params(match_key, provider)).fetchone()
        if row is None or row[0] is None:
            raise SystemExit(f"no fct_match_params.home_team_key for match {match_key}")
        home, away = pitch_control.home_and_away(frame, int(row[0]))
        pitch_control.pitch_control_values(frame, home_team_id=home, away_team_id=away)
        return frames_of(frame)

    def run_shape_graphs() -> int:
        record_keys = ik.match_record_keys(cat.conn, match_key)
        tracking_steps.match_keys(cat.conn, match_key, provider, record_keys)
        frame = build_match_frame(cat, match_key, provider=provider, record_keys=record_keys)
        shape_graphs.shape_graph_values(frame)
        return frames_of(frame)

    def run_line_breaking() -> int:
        record_keys = ik.match_record_keys(cat.conn, match_key)
        ik.input_key(
            cat.conn,
            match_key=match_key,
            provider=provider,
            reads=(*FRAME_INPUT_READS, ik.ACTIONS_INPUT_READ),
            code_modules=tracking_steps.LINE_BREAKING_CODE_MODULES,
            record_keys=record_keys,
        )
        read = ik.ACTIONS_INPUT_READ
        actions: Any = cat.conn.execute(read.sql, read.params(match_key, provider)).df()
        frame = build_match_frame(cat, match_key, provider=provider, record_keys=record_keys)
        line_breaking.line_breaking_values(line_breaking.actions_for_scorer(actions), frame)
        return frames_of(frame)

    work = {"pitch_control": run_pitch_control, "shape_graphs": run_shape_graphs, "line_breaking": run_line_breaking}
    return work[step]


def profile_step(step: str, cat: WorkingCatalog, match_key: int, provider: str) -> dict[str, Any]:
    """Run one step's per-match work under cProfile and summarise it by the fixed phase map."""
    profiler = cProfile.Profile()
    started = time.perf_counter()
    profiler.enable()
    frames = _work(step, cat, match_key, provider)()
    profiler.disable()
    total = time.perf_counter() - started
    return {
        "provider": provider,
        "match_key": match_key,
        "step": step,
        "frames": frames,
        "backend": "numba" if numba_active() else "numpy",
        "phases": summarise_profile(pstats.Stats(profiler), PHASES),
        "total_s": total,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--provider", required=True, choices=("gradientsports", "idsse", "skillcorner"))
    parser.add_argument("--match-key", type=int, default=None)
    parser.add_argument("--step", choices=STEPS, default=None)
    parser.add_argument("--out", type=Path, required=True, help="a directory; one JSON per step is written there")
    parser.add_argument("--lake", default=None)
    parser.add_argument("--restricted", default=None)
    args = parser.parse_args(argv)

    env = os.environ
    resolved = resolve_roots(
        cli_lake=args.lake, cli_restricted=args.restricted, env=env, bootstrap_file=default_bootstrap_file(env)
    )
    reader = CatalogReader(open_storage(resolved, env))
    steps: tuple[str, ...] = (args.step,) if args.step else DEFAULT_STEPS
    args.out.mkdir(parents=True, exist_ok=True)
    with reader.session() as conn:
        match_key = args.match_key if args.match_key is not None else _largest_match(conn, args.provider)
        cat = cast("WorkingCatalog", _ReadOnlyCatalog(conn))
        for step in steps:
            result = profile_step(step, cat, match_key, args.provider)
            target = args.out / f"{step}-{args.provider}-{match_key}.json"
            target.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print(json.dumps({"written": str(target), "total_s": round(result["total_s"], 2)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

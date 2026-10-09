"""In-preflight drain canary (ADR-087, two-probe bounded + scope-aware amendment 2026-10-09).

A drain preflight dry-runs a BOUNDED, scope-aware canary through the real processor BEFORE the fan-out
spins up, so a systemic compute-path defect (e.g. the ``gk_decision is_actor`` bug) fails preflight —
cheaply, and before any corpus write — instead of burning the worker retry budget.

Two probes, each at its cheapest sufficient input:

- **Probe C (correctness, BOTH drains):** full ``process(dry_run=True)`` on the SMALLEST unit of EACH
  provider present in the (already scope-filtered) discovered units. Both stages + all scorers + real
  schemas → catches systemic code/schema/wiring + per-provider data shape. Whole-unit (the global
  action→frame link is not window-decomposable).
- **Probe M (memory fit, tracking-marts ONLY):** Stage-1 windowed build+spill of the GLOBAL densest
  window across the in-scope units, asserting it fits the UDF-group cap (no OOM). The ADR-089 memory
  bound is per-window, so the global worst-case window proves the whole corpus; it can live in a
  non-densest unit. No scoring. Supplied only by the tracking-marts preflight (``memory_window``); the
  AC drain is not two-stage (direct ``mapInPandas``) so it passes ``window_counts=None`` → no Probe M.

Why ``units[:1]`` was wrong (the defect this replaces): discovery returns provider-sorted units, so the
first unit is always ``gradientsports`` (densest provider); one full-processor dry-run of a dense unit
costs ~one real drain unit and blew the 1200 s preflight budget under the ADR-088 two-stage path.

Scope-awareness lives in DISCOVERY (the preflight's ``--providers`` / ``provider_filter``), so the
``units``/``window_counts`` handed here are already in-scope — selection just picks from them.

Pure orchestration (no Spark/Delta): the ``processor`` is injected by the caller. ``analytics`` cannot
import ``ingestion`` (import-linter), so this lives here and each preflight supplies the concrete
processor. Probe M needs a ``BuildFitProbePort`` processor; Probe C needs only ``GameProcessorPort``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast

from analytics.action_context.drain import GameProcessorPort, unit_label
from analytics.action_context.work_unit import WorkUnit

if TYPE_CHECKING:
    from collections.abc import Sequence


class BuildFitProbePort(Protocol):
    """Probe-M seam: build+spill ONE window and assert it fits, persisting nothing (tracking-marts only).

    Separate from ``GameProcessorPort`` so adding it never forces the AC ``SparkGameProcessor`` (which
    has no two-stage build) to implement it."""

    def build_fit_probe(self, unit: WorkUnit, window_id: int) -> int: ...


@dataclass(frozen=True)
class WindowRef:
    """A single frame-window of one unit + its raw-tracking-row count (the UDF-group peak proxy)."""

    unit: WorkUnit
    window_id: int
    n_rows: int


@dataclass(frozen=True)
class CanaryProbes:
    """The selected probe inputs. ``memory_window is None`` ⇒ no Probe M (the AC drain)."""

    memory_window: WindowRef | None
    correctness_units: tuple[WorkUnit, ...]


def _unit_sort_key(u: WorkUnit) -> tuple[int, str, str, int]:
    # Smallest-first by frame count; an unsized unit (n_frames None) sorts LAST so a sized peer wins.
    # Deterministic tie-break on the identity triple.
    n = u.n_frames if u.n_frames is not None else (1 << 62)
    return (n, u.provider, str(u.match_id), -1 if u.period is None else int(u.period))


def _window_sort_key(w: WindowRef) -> tuple[int, str, str, int, int]:
    # Densest-first (negated n_rows), deterministic tie-break on identity.
    u = w.unit
    return (-w.n_rows, u.provider, str(u.match_id), -1 if u.period is None else int(u.period), w.window_id)


def select_canary_probes(
    units: Sequence[WorkUnit],
    window_counts: Sequence[WindowRef] | None = None,
) -> CanaryProbes:
    """Pick Probe C (smallest unit per provider present) + Probe M (global densest window).

    ``units`` / ``window_counts`` are already scope-filtered by discovery. ``window_counts=None`` (or
    empty) ⇒ ``memory_window=None`` ⇒ no Probe M (the AC drain). Empty ``units`` ⇒ no correctness units.
    """
    smallest_by_provider: dict[str, WorkUnit] = {}
    for u in units:
        cur = smallest_by_provider.get(u.provider)
        if cur is None or _unit_sort_key(u) < _unit_sort_key(cur):
            smallest_by_provider[u.provider] = u
    correctness = tuple(smallest_by_provider[p] for p in sorted(smallest_by_provider))

    memory_window: WindowRef | None = None
    if window_counts:
        memory_window = sorted(window_counts, key=_window_sort_key)[0]

    return CanaryProbes(memory_window=memory_window, correctness_units=correctness)


def run_canary(processor: GameProcessorPort, probes: CanaryProbes, logger: logging.Logger) -> None:
    """Run Probe C on every correctness unit, then Probe M if present; raise on the FIRST failure.

    A compute-path defect surfaces here (dry_run computes every writer, persists nothing; Probe M forces
    the windowed build), before the N-worker fan-out and before any write. The raise fails the preflight
    task, so the dependent ``compute_*`` task is skipped. A canary unit whose DATA (not code) is bad
    blocks only that provider; the runtime circuit-breaker is the backstop for defects a canary unit does
    not exercise.
    """
    for u in probes.correctness_units:
        label = unit_label(u)
        try:
            processor.process(u, dry_run=True)
        except Exception as exc:
            logger.error("drain_canary_failed probe=C unit=%s err=%s", label, exc, exc_info=True)
            raise RuntimeError(f"drain canary FAILED (correctness) on {label}: {exc}") from exc
        logger.info("drain_canary_ok probe=C unit=%s", label)

    w = probes.memory_window
    if w is None:
        return
    if not hasattr(processor, "build_fit_probe"):
        raise RuntimeError(
            "drain canary: a memory_window probe was selected but the processor has no build_fit_probe "
            f"(type={type(processor).__name__}); Probe M requires a BuildFitProbePort processor."
        )
    label = f"{unit_label(w.unit)}#w{w.window_id}(rows={w.n_rows})"
    try:
        cast("BuildFitProbePort", processor).build_fit_probe(w.unit, w.window_id)
    except Exception as exc:
        logger.error("drain_canary_failed probe=M window=%s err=%s", label, exc, exc_info=True)
        raise RuntimeError(f"drain canary FAILED (memory-fit) on {label}: {exc}") from exc
    logger.info("drain_canary_ok probe=M window=%s", label)

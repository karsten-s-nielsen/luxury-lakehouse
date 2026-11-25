"""The default compute-step registry (spec D9; plan Task 7).

`DEFAULT_COMPUTE_STEPS` is the list a calc run executes. The run's `finish` groups them by `step.layer`: it runs the
layer-1 steps (the per-provider SPADL actions — statsbomb, wyscout, idsse, skillcorner, gradientsports; pitch_control
and shape_graphs are layer 1 too, but excluded from the default, see below), builds `+fct_actions`, then runs the
layer-2 step (line_breaking), then builds `+fct_tracking_metrics`. SPADL runs for every provider v2 ingests (spec D9
amendment, 2026-11-05); the tracking steps (pitch_control, shape_graphs, line_breaking) apply only to the tracking
providers — statsbomb/wyscout are events-only, so they feed `fct_actions` but no frames. A step whose inputs are absent
(a provider that was not ingested, or no tracking frames) iterates zero matches and is a no-op, so this default is safe
on every run (spec D9).

`REGISTERED_COMPUTE_STEPS` lists every step v2 builds. `DEFAULT_COMPUTE_STEPS` is that list minus
`EXCLUDED_COMPUTE_STEPS`: since calc-layer spec D10 (owner 2026-11-23) the default run leaves out pitch_control and
shape_graphs until the silly-kicks batch upgrade. Their code, bronze tables, staging views and mart columns stay.
"""

from __future__ import annotations

from lakehouse.compute.base import ComputeStep, ComputeStepGroup
from lakehouse.compute.spadl_actions import (
    SPADL_ACTIONS_GRADIENTSPORTS,
    SPADL_ACTIONS_IDSSE,
    SPADL_ACTIONS_SKILLCORNER,
    SPADL_ACTIONS_STATSBOMB,
    SPADL_ACTIONS_WYSCOUT,
)
from lakehouse.compute.tracking_steps import LINE_BREAKING, TRACKING_FRAME_METRICS

__all__ = ["DEFAULT_COMPUTE_STEPS", "EXCLUDED_COMPUTE_STEPS", "REGISTERED_COMPUTE_STEPS", "STEP_OUTPUTS"]

#: Every compute step v2 builds, in run order.
REGISTERED_COMPUTE_STEPS: tuple[ComputeStep | ComputeStepGroup, ...] = (
    SPADL_ACTIONS_STATSBOMB,  # layer 1 (events-only; fct_actions, no frames)
    SPADL_ACTIONS_WYSCOUT,  # layer 1 (events-only; fct_actions, no frames)
    SPADL_ACTIONS_IDSSE,  # layer 1
    SPADL_ACTIONS_SKILLCORNER,  # layer 1
    SPADL_ACTIONS_GRADIENTSPORTS,  # layer 1
    TRACKING_FRAME_METRICS,  # layer 1: pitch_control + shape_graphs, one frame build per match (perf spec §7.3)
    LINE_BREAKING,  # layer 2 (reads fct_actions)
)

#: Registered steps that the default calc run leaves out (calc-layer spec D10, owner 2026-11-23): pitch_control and
#: shape_graphs wait for the silly-kicks batch upgrade, which replaces their scorers. Their cards say so (`excluded`).
EXCLUDED_COMPUTE_STEPS: tuple[ComputeStep | ComputeStepGroup, ...] = (TRACKING_FRAME_METRICS,)

DEFAULT_COMPUTE_STEPS: tuple[ComputeStep | ComputeStepGroup, ...] = tuple(
    step for step in REGISTERED_COMPUTE_STEPS if step not in EXCLUDED_COMPUTE_STEPS
)

#: Every registered compute output as `(step name, bronze table)`, a group expanded to its members: the names the
#: run records outcomes under and the cards are keyed by.
STEP_OUTPUTS: tuple[tuple[str, str], ...] = tuple(
    pair
    for step in REGISTERED_COMPUTE_STEPS
    for pair in (
        zip(step.names, step.output_tables, strict=True)
        if isinstance(step, ComputeStepGroup)
        else ((step.name, step.output_table),)
    )
)

"""The calc-layer slice cards (spec D9; plan Task 8): the DAG the AI/ML Workflows page renders.

Four bands: the layer-0 sources (the tracking input views + the five event bronzes), the layer-1/2 computes (the
five per-provider SPADL steps, pitch_control, shape_graphs — then the `fct_actions` view — then line_breaking), the
mart, and the planned publish sink. `depends_on` holds card ids, so the page draws the two compute layers. The card
ids of the computes are exactly the `ComputeStep.name`s, so `test_workflow_cards` can check every step has a card.

Calc-layer spec D10 (owner 2026-11-23): pitch_control and shape_graphs are `excluded` (out of the default run until the
silly-kicks batch upgrade); IDSSE SPADL is `incomplete`, and so is Gradient Sports SPADL (owner 2026-11-24: its
actions are timed about 1 s early), and so is every card below a marked card (fct_actions, line_breaking, the mart).
Each such card says why in its `note`.
"""

from __future__ import annotations

from lakehouse.cards.model import CardIO, WorkflowCard

__all__ = ["SLICE_CARDS"]

SLICE_CARDS: tuple[WorkflowCard, ...] = (
    # --- Layer 0: inputs (gold views + the per-provider event bronzes) ---
    WorkflowCard(
        id="fct_tracking_frames",
        name="Tracking frames",
        type="source",
        outputs=CardIO(views=("fct_tracking_frames",)),
    ),
    WorkflowCard(
        id="fct_tracking_ball",
        name="Tracking ball + possession",
        type="source",
        outputs=CardIO(views=("fct_tracking_ball",)),
    ),
    WorkflowCard(
        id="gradientsports_events",
        name="Gradient Sports events",
        type="source",
        outputs=CardIO(tables=("gradientsports_events",)),
    ),
    WorkflowCard(
        id="idsse_events",
        name="IDSSE (DFL) events",
        type="source",
        outputs=CardIO(tables=("idsse_events",)),
    ),
    WorkflowCard(
        id="skillcorner_dynamic_events",
        name="SkillCorner dynamic events",
        type="source",
        outputs=CardIO(tables=("skillcorner_dynamic_events",)),
    ),
    WorkflowCard(
        id="statsbomb_events",
        name="StatsBomb events",
        type="source",
        outputs=CardIO(tables=("statsbomb_events",)),
    ),
    WorkflowCard(
        id="wyscout_events",
        name="Wyscout events",
        type="source",
        outputs=CardIO(tables=("wyscout_events",)),
    ),
    # --- Layer 1: the per-provider SPADL computes + the two tracking computes ---
    WorkflowCard(
        id="spadl_actions:gradientsports",
        name="SPADL actions — Gradient Sports",
        type="compute",
        status="incomplete",
        note=(
            "Known timing gap for Gradient Sports: an action is timed at the start of its on-ball sequence, about "
            "1 s early (median 0.8 s on this lake, 1.3 s in silly-kicks' action-level measure), until the "
            "silly-kicks upgrade."
        ),
        depends_on=("gradientsports_events",),
        inputs=CardIO(tables=("gradientsports_events",)),
        outputs=CardIO(tables=("spadl_actions",)),
    ),
    WorkflowCard(
        id="spadl_actions:idsse",
        name="SPADL actions — IDSSE (DFL)",
        type="compute",
        status="incomplete",
        note=(
            "Known incomplete: v2 places IDSSE events by their tracking frame, which only shots carry, so most events "
            "give no action (about 20 of 1500 per match). The fix waits for the silly-kicks DFL event-period release. "
            "IDSSE line breaking has the same gap."
        ),
        depends_on=("idsse_events",),
        inputs=CardIO(tables=("idsse_events",)),
        outputs=CardIO(tables=("spadl_actions",)),
    ),
    WorkflowCard(
        id="spadl_actions:skillcorner",
        name="SPADL actions — SkillCorner",
        type="compute",
        depends_on=("skillcorner_dynamic_events",),
        inputs=CardIO(tables=("skillcorner_dynamic_events",)),
        outputs=CardIO(tables=("spadl_actions",)),
    ),
    WorkflowCard(
        id="spadl_actions:statsbomb",
        name="SPADL actions — StatsBomb",
        type="compute",
        depends_on=("statsbomb_events",),
        inputs=CardIO(tables=("statsbomb_events",)),
        outputs=CardIO(tables=("spadl_actions",)),
    ),
    WorkflowCard(
        id="spadl_actions:wyscout",
        name="SPADL actions — Wyscout",
        type="compute",
        depends_on=("wyscout_events",),
        inputs=CardIO(tables=("wyscout_events",)),
        outputs=CardIO(tables=("spadl_actions",)),
    ),
    WorkflowCard(
        id="pitch_control",
        name="Pitch control",
        type="compute",
        status="excluded",
        note=(
            "Not in the default run until the silly-kicks batch upgrade: the current scorer takes about 2.9 ms per "
            "frame on one core. The mart column mean_controlled_fraction is filled only for matches computed before "
            "2026-11-23; those rows stay and are not re-checked until the upgrade."
        ),
        depends_on=("fct_tracking_frames", "fct_tracking_ball"),
        inputs=CardIO(views=("fct_tracking_frames", "fct_tracking_ball")),
        outputs=CardIO(tables=("pitch_control_values",)),
    ),
    WorkflowCard(
        id="shape_graphs",
        name="Team shape",
        type="compute",
        status="excluded",
        note=(
            "Not in the default run until the silly-kicks shape-graph upgrade: the current scorer takes up to about "
            "80 minutes for one large match. The mart columns modal_position and time_in_position are filled only for "
            "matches computed before 2026-11-23; those rows stay and are not re-checked until the upgrade."
        ),
        depends_on=("fct_tracking_frames", "fct_tracking_ball"),
        inputs=CardIO(views=("fct_tracking_frames", "fct_tracking_ball")),
        outputs=CardIO(tables=("shape_graph_labels",)),
    ),
    # --- Layer 1 view over the SPADL bronze; read by line_breaking ---
    WorkflowCard(
        id="fct_actions",
        name="SPADL actions (gold)",
        type="view",
        status="incomplete",
        note=(
            "Known incomplete for IDSSE: it holds the few IDSSE actions the IDSSE SPADL step keeps (see that step). "
            "Gradient Sports action times are about 1 s early until the silly-kicks upgrade (see that step)."
        ),
        depends_on=(
            "spadl_actions:statsbomb",
            "spadl_actions:wyscout",
            "spadl_actions:idsse",
            "spadl_actions:skillcorner",
            "spadl_actions:gradientsports",
        ),
        inputs=CardIO(tables=("spadl_actions",)),
        outputs=CardIO(views=("fct_actions",)),
    ),
    # --- Layer 2: line_breaking reads fct_actions + the frames ---
    WorkflowCard(
        id="line_breaking",
        name="Line breaking",
        type="compute",
        status="incomplete",
        note=(
            "Known incomplete for IDSSE: it scores only the few IDSSE actions the IDSSE SPADL step keeps, so IDSSE "
            "counts are far too low until the silly-kicks DFL event-period release. Gradient Sports actions are timed "
            "about 1 s early (see that SPADL step), so their line breaks are judged on an early frame until the "
            "silly-kicks upgrade. SkillCorner is complete."
        ),
        depends_on=("fct_actions", "fct_tracking_frames", "fct_tracking_ball"),
        inputs=CardIO(views=("fct_actions", "fct_tracking_frames", "fct_tracking_ball")),
        outputs=CardIO(tables=("line_breaking_results",)),
    ),
    # --- The per-player tracking-metrics mart ---
    WorkflowCard(
        id="fct_tracking_metrics",
        name="Tracking metrics mart",
        type="mart",
        status="incomplete",
        note=(
            "Known incomplete: mean_controlled_fraction, modal_position and time_in_position are filled only for "
            "matches computed before 2026-11-23 (those rows stay and are not re-checked) while pitch control and team "
            "shape are excluded. line_breaking_passes is far too low for IDSSE matches, and for Gradient Sports "
            "matches it is judged on actions timed about 1 s early (see line breaking)."
        ),
        depends_on=("pitch_control", "shape_graphs", "line_breaking"),
        inputs=CardIO(tables=("pitch_control_values", "shape_graph_labels", "line_breaking_results")),
        outputs=CardIO(views=("fct_tracking_metrics",)),
    ),
    # --- The deferred HF publish sink (defined, built in a later cycle; spec D5 / Task 10) ---
    WorkflowCard(
        id="publish",
        name="HF publish (planned)",
        type="sink",
        status="planned",
        depends_on=("fct_tracking_metrics",),
        inputs=CardIO(views=("fct_tracking_metrics",)),
    ),
)

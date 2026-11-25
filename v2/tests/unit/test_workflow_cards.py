"""The typed workflow-card model + slice cards (spec D9; plan Task 8).

The cards describe the calc-layer DAG for the AI/ML Workflows page (Task 9). The invariants: every compute step has a
card, the `depends_on` edges form a well-formed DAG that shows the two compute layers (line_breaking -> fct_actions ->
the SPADL steps), nothing declares a HuggingFace input (HF publishing is optional — spec D5), and each compute card's
outputs name the step's actual bronze/mart table.
"""

from __future__ import annotations

from collections.abc import Iterable

import pytest
from pydantic import ValidationError

from lakehouse.cards import SLICE_CARDS, CardIO, WorkflowCard
from lakehouse.compute.base import ComputeStep, ComputeStepGroup
from lakehouse.compute.frames import FRAME_INPUT_READS
from lakehouse.compute.registry import (
    DEFAULT_COMPUTE_STEPS,
    EXCLUDED_COMPUTE_STEPS,
    REGISTERED_COMPUTE_STEPS,
    STEP_OUTPUTS,
)

_SPADL_STEPS = {
    "spadl_actions:statsbomb",
    "spadl_actions:wyscout",
    "spadl_actions:idsse",
    "spadl_actions:skillcorner",
    "spadl_actions:gradientsports",
}


def _by_id() -> dict[str, WorkflowCard]:
    return {card.id: card for card in SLICE_CARDS}


def _names(steps: Iterable[ComputeStep | ComputeStepGroup]) -> set[str]:
    return {name for step in steps for name in (step.names if isinstance(step, ComputeStepGroup) else (step.name,))}


def test_cards_form_a_dag_matching_the_compute_registry() -> None:
    by_id = _by_id()
    for name, _table in STEP_OUTPUTS:  # every compute output (a group expanded) has a card keyed by its name
        assert name in by_id, name
    for card in SLICE_CARDS:  # every edge points at a real card (a well-formed DAG)
        for dep in card.depends_on:
            assert dep in by_id, (card.id, dep)
    # the two compute layers: line_breaking (layer 2) -> fct_actions -> the layer-1 SPADL steps
    assert "fct_actions" in by_id["line_breaking"].depends_on
    assert any(dep.startswith("spadl_actions:") for dep in by_id["fct_actions"].depends_on)


@pytest.mark.parametrize("step", ["pitch_control", "shape_graphs", "line_breaking"])
def test_a_tracking_card_names_every_frame_view_its_step_reads(step: str) -> None:
    # Each tracking step builds its frame from FRAME_INPUT_READS; every one of those views that has a card is an edge.
    by_id = _by_id()
    frame_views = {read.name for read in FRAME_INPUT_READS} & set(by_id)
    assert frame_views == {"fct_tracking_frames", "fct_tracking_ball"}
    assert frame_views <= set(by_id[step].depends_on), step
    assert frame_views <= set(by_id[step].inputs.views), step


def test_no_card_declares_an_hf_input() -> None:
    assert not hasattr(CardIO(), "hf")  # the IO model has no HF field — nothing depends on a published HF artifact
    for card in SLICE_CARDS:  # defence in depth: no card names an hf table/view
        names = [*card.inputs.tables, *card.inputs.views, *card.outputs.tables, *card.outputs.views]
        assert not any("hf" in name.lower() for name in names), card.id


def test_card_outputs_match_bronze_and_mart_names() -> None:
    by_id = _by_id()
    for name, table in STEP_OUTPUTS:  # each compute card outputs the step's bronze table
        assert table in by_id[name].outputs.tables, name
    assert "fct_tracking_metrics" in by_id["fct_tracking_metrics"].outputs.views  # the mart card outputs the mart view


def test_publish_sink_is_planned_not_active() -> None:
    # the HF publish sink is in the DAG but deferred (build in a later cycle; spec D5 / Task 10)
    assert _by_id()["publish"].status == "planned"


def test_default_run_leaves_out_pitch_control_and_shape_graphs() -> None:
    # calc-layer spec D10 (owner 2026-11-23): the two unoptimised metrics wait for the silly-kicks batch upgrade
    default = _names(DEFAULT_COMPUTE_STEPS)
    excluded = _names(EXCLUDED_COMPUTE_STEPS)
    assert excluded == {"pitch_control", "shape_graphs"}
    assert not default & excluded
    assert default == _SPADL_STEPS | {"line_breaking"}
    assert _names(REGISTERED_COMPUTE_STEPS) == default | excluded  # the code stays registered
    assert {name for name, _table in STEP_OUTPUTS} == default | excluded  # cards + outcomes cover every registered step


def test_compute_card_is_excluded_exactly_when_its_step_is_not_in_the_default_run() -> None:
    by_id = _by_id()
    default = _names(DEFAULT_COMPUTE_STEPS)
    for name in _names(REGISTERED_COMPUTE_STEPS):
        assert (by_id[name].status == "excluded") == (name not in default), name


@pytest.mark.parametrize("status", ["excluded", "incomplete"])
@pytest.mark.parametrize("note", ["", "   "])
def test_excluded_or_incomplete_card_needs_a_note(status: str, note: str) -> None:
    with pytest.raises(ValidationError, match="note"):
        WorkflowCard(id="x", name="X", type="compute", status=status, note=note)  # pyright: ignore[reportArgumentType]


def test_every_excluded_or_incomplete_slice_card_says_why() -> None:
    marked = [card for card in SLICE_CARDS if card.status in ("excluded", "incomplete")]
    assert marked
    for card in marked:
        assert card.note.strip(), card.id


def test_idsse_spadl_and_the_mart_are_marked_incomplete() -> None:
    by_id = _by_id()
    idsse = by_id["spadl_actions:idsse"]
    assert idsse.status == "incomplete"
    assert "silly-kicks" in idsse.note  # names the release that fixes it
    mart = by_id["fct_tracking_metrics"]
    assert mart.status == "incomplete"
    for column in ("mean_controlled_fraction", "modal_position", "time_in_position"):  # names the empty columns
        assert column in mart.note, column
    assert "IDSSE" in mart.note and "line_breaking_passes" in mart.note  # the idsse gap reaches the mart column
    assert "is filled" not in mart.note
    # the idsse gap flows down: fct_actions (the view) and line_breaking (layer 2) are partial for IDSSE too
    for card_id in ("fct_actions", "line_breaking"):
        assert by_id[card_id].status == "incomplete", card_id
        assert "IDSSE" in by_id[card_id].note, card_id
    marked = {"spadl_actions:idsse", "spadl_actions:gradientsports", "line_breaking"}
    for name in _names(DEFAULT_COMPUTE_STEPS) - marked:  # the rest is plainly active
        assert by_id[name].status == "active", name


def test_the_gradientsports_clock_gap_is_marked_at_its_source_and_below() -> None:
    # Owner 2026-11-24 (impl review r3, A PERF-IMPL-17): GS actions are timed about 1 s early until the silly-kicks
    # upgrade, so the card where the time is made says so, and so does every card that shows it.
    by_id = _by_id()
    assert by_id["spadl_actions:gradientsports"].status == "incomplete"
    for card_id in ("spadl_actions:gradientsports", "fct_actions", "line_breaking", "fct_tracking_metrics"):
        note = by_id[card_id].note
        assert "Gradient Sports" in note and "about 1 s early" in note, card_id


def test_a_card_below_an_excluded_or_incomplete_card_is_not_active() -> None:
    # A partial or missing upstream output makes every output below it partial (spec D10 item 3, anti-goal D6). The
    # direct-edge check is enough: by induction it holds along every path, so the mart stays marked while any of its
    # inputs is excluded.
    by_id = _by_id()
    for card in SLICE_CARDS:
        marked = [dep for dep in card.depends_on if by_id[dep].status != "active"]
        if marked:
            assert card.status != "active", (card.id, marked)


def test_excluded_and_mart_notes_say_earlier_rows_stay() -> None:
    # spec D10 item 2: rows written before the exclusion stay visible and no skip key re-checks them
    by_id = _by_id()
    for card_id in ("pitch_control", "shape_graphs", "fct_tracking_metrics"):
        assert "before 2026-11-23" in by_id[card_id].note, card_id
        assert "not re-checked" in by_id[card_id].note, card_id

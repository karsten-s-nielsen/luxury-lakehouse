"""The truth set (spec §15.4, §15.5, §15.6; plan Task 22): ids only, labelled by the owner, grown by the owner's
hand-checked decisions, never by links accepted as recommended; precision and recall against the link tables."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from resolvekit import NOW, lake, settings, statsbomb_teams, team, wyscout_teams

from lakehouse.resolve.bands import Bands
from lakehouse.resolve.decisions import AcceptRequest, CannotLink, DecisionSet, MustLink
from lakehouse.resolve.links import resolve_all
from lakehouse.resolve.truthset import (
    TRUTHSET_COLUMNS,
    TruthRow,
    measure,
    read_truthset,
    truthset_file,
    truthset_rows_from_decisions,
    validate_truthset,
)

OWNER = "karsten"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "truthset"


def _row(a: str, b: str, label: str = "match", labelled_by: str = OWNER) -> TruthRow:
    return TruthRow(
        provider_a="statsbomb",
        native_id_a=a,
        provider_b="wyscout",
        native_id_b=b,
        label=label,  # type: ignore[arg-type]
        labelled_by=labelled_by,
        labelled_at=NOW,
        evidence_ref="note:hand-check",
    )


def test_truthset_purity() -> None:
    """§23.6: a recommended link never enters the truth set, and a row without an owner `labelled_by` fails."""
    validate_truthset([_row("1", "10")], owner=OWNER)
    with pytest.raises(ValueError, match="owner"):
        validate_truthset([_row("1", "10", labelled_by="demo-user")], owner=OWNER)
    with pytest.raises(ValueError, match="truthset_owner"):
        validate_truthset([_row("1", "10")], owner=None)
    with pytest.raises(ValueError, match="recommended"):
        validate_truthset([_row("1", "10").model_copy(update={"evidence_ref": "batch:r1.auto"})], owner=OWNER)
    with pytest.raises(ValueError, match="twice"):
        validate_truthset([_row("1", "10"), _row("1", "10", label="non_match")], owner=OWNER)
    with pytest.raises(ValueError, match="label"):
        TruthRow.model_validate({**_row("1", "10").model_dump(), "label": "maybe"})
    # a batch accepted as recommended gives no truth row: only decisions become rows
    accept = AcceptRequest(request_id="req-1", entity=None, batch_filter=None, requested_by=OWNER, requested_at=NOW)
    decisions = DecisionSet(must_links=[], cannot_links=[], accept_requests=[accept], undo_requests=[])
    assert truthset_rows_from_decisions(decisions, owner=OWNER, entity="team") == []


def test_hand_decisions_grow_truthset() -> None:
    """§15.5: an owner must-link and an owner cannot-link become two truth rows; a decision by another operator and a
    batch accepted as recommended become none."""
    must = MustLink(
        entity="team", a=team("statsbomb", "1"), b=team("wyscout", "10"), decided_by=OWNER, decided_at=NOW, reason="x"
    )
    cannot = CannotLink(
        entity="team", a=team("wyscout", "30"), b=team("statsbomb", "3"), decided_by=OWNER, decided_at=NOW, reason="y"
    )
    other = MustLink(
        entity="team", a=team("statsbomb", "2"), b=team("wyscout", "20"), decided_by="demo", decided_at=NOW, reason="z"
    )
    player = must.model_copy(update={"entity": "player"})
    accept = AcceptRequest(request_id="req-1", entity=None, batch_filter=None, requested_by=OWNER, requested_at=NOW)
    decisions = DecisionSet(
        must_links=[must, other, player], cannot_links=[cannot], accept_requests=[accept], undo_requests=[]
    )
    found = truthset_rows_from_decisions(decisions, owner=OWNER, entity="team")
    assert [(r.provider_a, r.native_id_a, r.provider_b, r.native_id_b, r.label) for r in found] == [
        ("statsbomb", "1", "wyscout", "10", "match"),
        ("statsbomb", "3", "wyscout", "30", "non_match"),
    ]
    assert all(r.labelled_by == OWNER and r.evidence_ref.startswith("decision:") for r in found)
    validate_truthset(found, owner=OWNER)


def test_measure_precision_recall_and_auto_band() -> None:
    conn = lake()
    statsbomb_teams(conn, [(1, "Germany"), (2, "Mexico"), (3, "Barcelona")])
    wyscout_teams(conn, [(10, "Germany"), (20, "Mexico"), (30, "Barcelona A"), (31, "Barcelona B")])
    hand = MustLink(
        entity="team", a=team("statsbomb", "3"), b=team("wyscout", "30"), decided_by=OWNER, decided_at=NOW, reason="x"
    )
    resolve_all(
        conn,
        decisions=DecisionSet(must_links=[hand], cannot_links=[], accept_requests=[], undo_requests=[]),
        bands={
            "team": Bands(auto_link_min=95.0, reject_max=10.0, margin=5.0),
            "player": None,
            "competition": None,
            "match": None,
        },
        settings=settings(auto_accept=False),
        run_id="r1",
    )
    truth = [_row("1", "10"), _row("2", "20", label="non_match"), _row("3", "31"), _row("3", "30", label="non_match")]
    report = measure(conn, truth, "team")
    # linked: 1-10 (auto, right), 2-20 (auto, wrong), 3-30 (hand, wrong); missed: 3-31
    assert report.precision == pytest.approx(1 / 3)
    assert report.recall == pytest.approx(1 / 2)
    assert report.auto_band_precision == pytest.approx(1 / 2)
    assert measure(conn, [], "team").precision is None  # nothing labelled is linked: no value, not a fake 1.0


def test_truthset_fixtures_hold_ids_only() -> None:
    """The repository holds ids only (§15.4): one CSV per entity with exactly the truth-set columns."""
    for entity in ("player", "team", "competition", "match"):
        path = FIXTURES / truthset_file(entity)  # type: ignore[arg-type]
        header = path.read_text(encoding="utf-8").splitlines()[0]
        assert header.split(",") == list(TRUTHSET_COLUMNS)
        rows = read_truthset(path)
        if rows:  # Task 34 adds the owner's labels
            validate_truthset(rows, owner=OWNER)


def test_read_truthset_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "teams.csv"
    path.write_text(
        ",".join(TRUTHSET_COLUMNS) + "\n" + f"statsbomb,1,wyscout,10,match,{OWNER},{NOW.isoformat()},note:x\n",
        encoding="utf-8",
    )
    [row] = read_truthset(path)
    assert row == _row("1", "10").model_copy(update={"evidence_ref": "note:x"})
    assert row.labelled_at - NOW == timedelta(0)

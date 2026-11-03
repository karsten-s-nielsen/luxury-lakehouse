"""`resolve_all`: bands, the review queue, accept recommended, undo and holds, hand decisions, the order of
application, the canonical pointers and their move log (spec §14.3, §15.3 to §15.6, §23.6; plan Task 22)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any

import duckdb
import pytest
from lakekit import LakeKit
from resolvekit import NOW, idsse_teams, key, lake, rows, settings, statsbomb_teams, team, wyscout_teams

from lakehouse.resolve.bands import Bands
from lakehouse.resolve.decisions import AcceptRequest, CannotLink, DecisionSet, DecisionStore, MustLink, UndoRequest
from lakehouse.resolve.links import ResolutionResult, resolve_all
from lakehouse.resolve.model import Entity

GERMANY, MEXICO, BARCA = team("statsbomb", "1"), team("statsbomb", "2"), team("statsbomb", "3")
WS_GERMANY, WS_MEXICO, WS_BARCA_A, WS_BARCA_B = (
    team("wyscout", "10"),
    team("wyscout", "20"),
    team("wyscout", "30"),
    team("wyscout", "31"),
)
QUEUE_ALL = Bands(auto_link_min=100.5, reject_max=10.0, margin=5.0)  # every candidate is in the middle band
EMPTY = DecisionSet(must_links=[], cannot_links=[], accept_requests=[], undo_requests=[])


def _world() -> duckdb.DuckDBPyConnection:
    conn = lake()
    statsbomb_teams(conn, [(1, "Germany"), (2, "Mexico"), (3, "Barcelona")])
    wyscout_teams(conn, [(10, "Germany"), (20, "Mexico"), (30, "Barcelona A"), (31, "Barcelona B")])
    return conn


def _resolve(
    conn: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    decisions: DecisionSet = EMPTY,
    bands: Bands | None = QUEUE_ALL,
    auto_accept: bool = True,
) -> ResolutionResult:
    per_entity: Mapping[Entity, Bands | None] = {"team": bands, "player": None, "competition": None, "match": None}
    return resolve_all(
        conn, decisions=decisions, bands=per_entity, settings=settings(auto_accept=auto_accept), run_id=run_id
    )


def _links(conn: duckdb.DuckDBPyConnection) -> dict[tuple[str, str], dict[str, Any]]:
    found = rows(conn, "SELECT * FROM lake.main.gold_team_links")
    return {(r["native_id_a"], r["native_id_b"]): r for r in found}


def _queue(conn: duckdb.DuckDBPyConnection) -> dict[tuple[str, str], dict[str, Any]]:
    found = rows(conn, "SELECT * FROM lake.main.ops_review_queue WHERE entity = 'team'")
    return {(r["native_id_a"], r["native_id_b"]): r for r in found}


def _decisions(*items: object) -> DecisionSet:
    return DecisionSet(
        must_links=[i for i in items if isinstance(i, MustLink)],
        cannot_links=[i for i in items if isinstance(i, CannotLink)],
        accept_requests=[i for i in items if isinstance(i, AcceptRequest)],
        undo_requests=[i for i in items if isinstance(i, UndoRequest)],
    )


def _pairs(found: Mapping[tuple[str, str], object]) -> set[tuple[str, str]]:
    return set(found)


def test_no_margin_no_auto_link() -> None:
    """§15.4: an entity type without measured bands gets no automatic link, even with the setting on."""
    conn = _world()
    result = _resolve(conn, run_id="r1", bands=None)
    assert result.links_written["team"] == 0
    assert result.queued["team"] == 3
    queue = _queue(conn)
    assert {item["recommended"] for item in queue.values()} == {False}
    assert {item["reason"] for item in queue.values()} == {"no measured bands for teams"}
    assert result.accepted_batches == []


def test_auto_band_links_and_reject_band_drops() -> None:
    conn = _world()
    result = _resolve(
        conn, run_id="r1", bands=Bands(auto_link_min=95.0, reject_max=89.0, margin=5.0), auto_accept=False
    )
    assert _pairs(_links(conn)) == {("1", "10"), ("2", "20")}
    assert {r["accepted_by"] for r in _links(conn).values()} == {"auto"}
    assert result.queued["team"] == 1  # Barcelona at 90 lies in the middle band
    dropped = _resolve(
        conn, run_id="r2", bands=Bands(auto_link_min=95.0, reject_max=90.0, margin=5.0), auto_accept=False
    )
    assert dropped.queued["team"] == 0  # 90 is in the reject band now


def test_tie_stays_in_queue() -> None:
    """§15.6: 'Barcelona' scores the same against 'Barcelona A' and 'Barcelona B': no clear margin, so the setting
    leaves it in the queue; the two clear top candidates are accepted as recommended in one batch."""
    conn = _world()
    result = _resolve(conn, run_id="r1")
    links = _links(conn)
    assert _pairs(links) == {("1", "10"), ("2", "20")}
    assert {(r["accepted_by"], r["batch_id"]) for r in links.values()} == {("recommended", "r1.auto")}
    assert result.accepted_batches == ["r1.auto"]
    [(pair, item)] = list(_queue(conn).items())
    assert pair[0] == "3"
    assert item["recommended"] is False
    assert item["score"] == item["runner_up"]


def test_cannot_link_blocks_accept() -> None:
    conn = _world()
    cannot = CannotLink(entity="team", a=GERMANY, b=WS_GERMANY, decided_by="karsten", decided_at=NOW, reason="x")
    accept = AcceptRequest(request_id="req-1", entity="team", batch_filter=None, requested_by="ops", requested_at=NOW)
    _resolve(conn, run_id="r1", decisions=_decisions(cannot, accept))
    assert _pairs(_links(conn)) == {("2", "20")}
    assert ("1", "10") not in _queue(conn)  # a cannot-link pair leaves the queue
    assert _links(conn)[("2", "20")]["batch_id"] == "req-1"  # the request comes before the setting


def test_hand_must_link_wins_and_records_the_operator() -> None:
    conn = _world()
    must = MustLink(entity="team", a=BARCA, b=WS_BARCA_B, decided_by="karsten", decided_at=NOW, reason="B team")
    _resolve(conn, run_id="r1", decisions=_decisions(must), auto_accept=False)
    link = _links(conn)[("3", "31")]
    assert (link["accepted_by"], link["decided_by"], link["batch_id"]) == ("hand", "karsten", None)
    assert not any(pair[0] == "3" for pair in _queue(conn))  # the decided record leaves the queue


def test_later_decision_on_a_pair_wins() -> None:
    conn = _world()
    must = MustLink(entity="team", a=GERMANY, b=WS_GERMANY, decided_by="karsten", decided_at=NOW, reason="x")
    cannot = CannotLink(
        entity="team", a=WS_GERMANY, b=GERMANY, decided_by="karsten", decided_at=NOW + timedelta(minutes=1), reason="y"
    )
    _resolve(conn, run_id="r1", decisions=_decisions(must, cannot), auto_accept=False)
    assert ("1", "10") not in _links(conn)


def test_undo_holds_pairs() -> None:
    """§23.6: undo of a batch, applied by the next run, restores the state before it, and the run after that does not
    accept the held pairs again with the setting on."""
    conn = _world()
    _resolve(conn, run_id="r1")
    assert _pairs(_links(conn)) == {("1", "10"), ("2", "20")}
    undo = UndoRequest(request_id="req-u", batch_id="r1.auto", requested_by="ops", requested_at=NOW)
    second = _resolve(conn, run_id="r2", decisions=_decisions(undo))
    assert _pairs(_links(conn)) == set()
    assert second.accepted_batches == []
    queue = _queue(conn)
    assert {pair for pair, item in queue.items() if item["held"]} == {("1", "10"), ("2", "20")}
    _resolve(conn, run_id="r3", decisions=_decisions(undo))  # the request is applied once; the holds stay
    assert _pairs(_links(conn)) == set()
    assert [r["request_id"] for r in rows(conn, "SELECT request_id FROM lake.main.ops_applied_requests")] == ["req-u"]


def test_earlier_batches_stay_until_undone() -> None:
    conn = _world()
    _resolve(conn, run_id="r1")
    _resolve(conn, run_id="r2")
    assert {r["batch_id"] for r in _links(conn).values()} == {"r1.auto"}  # not re-accepted under a new batch


def test_order_of_application() -> None:
    """§15.6: (1) hand decisions, (2) undo requests, (3) accept requests, (4) the setting."""
    conn = _world()
    _resolve(conn, run_id="r1")  # Germany and Mexico in batch r1.auto
    undo = UndoRequest(request_id="req-u", batch_id="r1.auto", requested_by="ops", requested_at=NOW)
    accept = AcceptRequest(request_id="req-a", entity=None, batch_filter=None, requested_by="ops", requested_at=NOW)
    must = MustLink(entity="team", a=MEXICO, b=WS_MEXICO, decided_by="karsten", decided_at=NOW, reason="checked")
    result = _resolve(conn, run_id="r2", decisions=_decisions(undo, accept, must))
    links = _links(conn)
    assert {pair: r["accepted_by"] for pair, r in links.items()} == {("2", "20"): "hand"}  # undo keeps hand links
    queue = _queue(conn)
    assert queue[("1", "10")]["held"] is True  # neither the accept request nor the setting takes a held pair
    assert result.accepted_batches == []
    applied = rows(conn, "SELECT request_id, kind FROM lake.main.ops_applied_requests ORDER BY request_id")
    assert [(r["request_id"], r["kind"]) for r in applied] == [("req-a", "accept"), ("req-u", "undo")]


def test_accept_request_filter() -> None:
    conn = _world()
    idsse_teams(conn, [("DFL-CLU-000017", "Germany")])
    accept = AcceptRequest(
        request_id="req-a", entity="team", batch_filter="statsbomb,wyscout", requested_by="ops", requested_at=NOW
    )
    _resolve(conn, run_id="r1", decisions=_decisions(accept), auto_accept=False)
    accepted = {(r["provider_a"], r["provider_b"]) for r in _links(conn).values()}
    assert accepted == {("statsbomb", "wyscout")}
    with pytest.raises(ValueError, match="batch filter"):  # a malformed request is refused before it is stored
        AcceptRequest(request_id="req-b", entity="team", batch_filter="statsbomb", requested_by="ops", requested_at=NOW)


def test_pointer_move_logged() -> None:
    """§23.6: a cluster change that moves a canonical pointer writes one log row per moved record with the old
    value, the new value, the run id and the reason."""
    conn = lake()
    wyscout_teams(conn, [(10, "Germany")])
    idsse_teams(conn, [("DFL-CLU-000017", "Germany")])
    auto = Bands(auto_link_min=95.0, reject_max=10.0, margin=5.0)
    first = _resolve(conn, run_id="r1", bands=auto)
    assert first.pointer_moves == 0  # a first pointer is not a move
    pointers = {
        r["native_id"]: r["canonical_key"] for r in rows(conn, "SELECT * FROM lake.main.ops_canonical_pointers")
    }
    assert pointers == {"10": key(WS_GERMANY), "DFL-CLU-000017": key(WS_GERMANY)}
    statsbomb_teams(conn, [(1, "Germany")])
    second = _resolve(conn, run_id="r2", bands=auto)
    assert second.pointer_moves == 2
    moves = rows(conn, "SELECT * FROM lake.main.ops_pointer_moves ORDER BY native_id")
    assert [(m["native_id"], m["old_canonical_key"], m["new_canonical_key"], m["run_id"]) for m in moves] == [
        ("10", key(WS_GERMANY), key(GERMANY), "r2"),
        ("DFL-CLU-000017", key(WS_GERMANY), key(GERMANY), "r2"),
    ]
    assert {m["reason"] for m in moves} == {"a higher-ranked record joined the cluster"}
    _resolve(conn, run_id="r3", bands=auto)
    assert len(rows(conn, "SELECT * FROM lake.main.ops_pointer_moves")) == 2  # no change, no new row


def test_records_are_kept_for_the_dimensions() -> None:
    """The pass keeps every record it resolved in `ops_entity_records`, with the key of the one generator; the dbt
    dimensions read them (plan Task 23), so dbt never hashes or builds an id."""
    conn = _world()
    _resolve(conn, run_id="r1")
    kept = rows(conn, "SELECT * FROM lake.main.ops_entity_records WHERE entity = 'team' ORDER BY provider, native_id")
    assert [(r["provider"], r["native_id"], r["name"]) for r in kept] == [
        ("statsbomb", "1", "Germany"),
        ("statsbomb", "2", "Mexico"),
        ("statsbomb", "3", "Barcelona"),
        ("wyscout", "10", "Germany"),
        ("wyscout", "20", "Mexico"),
        ("wyscout", "30", "Barcelona A"),
        ("wyscout", "31", "Barcelona B"),
    ]
    assert all(r["record_key"] == key(team(r["provider"], r["native_id"])) and r["run_id"] == "r1" for r in kept)
    statsbomb_teams(conn, [(4, "Spain")])
    _resolve(conn, run_id="r2")
    assert len(rows(conn, "SELECT * FROM lake.main.ops_entity_records WHERE entity = 'team'")) == 8  # replaced


def test_run_never_writes_request_objects(kit: LakeKit) -> None:
    store = DecisionStore(kit.storage)
    store.add(AcceptRequest(request_id="req-a", entity=None, batch_filter=None, requested_by="ops", requested_at=NOW))
    store.add(UndoRequest(request_id="req-u", batch_id="none", requested_by="ops", requested_at=NOW))
    before = _config_objects(kit)
    conn = _world()
    _resolve(conn, run_id="r1", decisions=store.load())
    assert _config_objects(kit) == before
    assert {r["request_id"] for r in rows(conn, "SELECT request_id FROM lake.main.ops_applied_requests")} == {
        "req-a",
        "req-u",
    }


def test_operator_save_during_batch(kit: LakeKit) -> None:
    """§23.6: an operator saves a review decision while a run applies an accept batch. Both survive, and the next
    run applies the new decision."""
    store = DecisionStore(kit.storage)
    store.add(AcceptRequest(request_id="req-a", entity=None, batch_filter=None, requested_by="ops", requested_at=NOW))
    snapshot = store.load()  # the run reads the decisions once, at its start
    cannot = CannotLink(entity="team", a=GERMANY, b=WS_GERMANY, decided_by="karsten", decided_at=NOW, reason="x")
    store.add(cannot)  # the operator saves during the run
    conn = _world()
    _resolve(conn, run_id="r1", decisions=snapshot, auto_accept=False)
    assert _links(conn)[("1", "10")]["batch_id"] == "req-a"
    later = store.load()
    assert later.cannot_links == [cannot] and len(later.accept_requests) == 1
    _resolve(conn, run_id="r2", decisions=later, auto_accept=False)
    assert ("1", "10") not in _links(conn)  # the next run applies the saved cannot-link


def _config_objects(kit: LakeKit) -> Sequence[tuple[str, bytes]]:
    return sorted((info.key, kit.storage.get("lake", info.key)) for info in kit.storage.list("lake", "config/"))

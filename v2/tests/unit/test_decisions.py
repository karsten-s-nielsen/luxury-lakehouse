"""Review decisions and requests as config-side objects (spec §8.3, §15.5, §15.6; plan Task 22)."""

from __future__ import annotations

import threading
from datetime import timedelta
from pathlib import Path

import pytest
from lakekit import LakeKit
from resolvekit import NOW, lake, team

from lakehouse.config.store import ConfigLock
from lakehouse.errors import ConflictError
from lakehouse.resolve.decisions import (
    AcceptRequest,
    CannotLink,
    DecisionStore,
    MustLink,
    UndoRequest,
    applied_requests,
    new_request_id,
)

SB, WS = team("statsbomb", "1"), team("wyscout", "10")


def _must(reason: str = "same club", **changes: object) -> MustLink:
    base = MustLink(entity="team", a=SB, b=WS, decided_by="karsten", decided_at=NOW, reason=reason)
    return base.model_copy(update=changes)


def test_add_and_load_every_kind(kit: LakeKit) -> None:
    store = DecisionStore(kit.storage)
    cannot = CannotLink(entity="player", a=SB, b=WS, decided_by="karsten", decided_at=NOW, reason="namesakes")
    accept = AcceptRequest(request_id="req-1", entity=None, batch_filter=None, requested_by="ops", requested_at=NOW)
    undo = UndoRequest(request_id="req-2", batch_id="req-1", requested_by="ops", requested_at=NOW)
    for item in (_must(), cannot, accept, undo):
        store.add(item)
    loaded = store.load()
    assert loaded.must_links == [_must()]
    assert loaded.cannot_links == [cannot]
    assert loaded.accept_requests == [accept]
    assert loaded.undo_requests == [undo]
    assert all(
        info.key.startswith(("config/decisions/", "config/requests/")) for info in kit.storage.list("lake", "config/")
    )


def test_add_is_idempotent_and_refuses_a_changed_request(kit: LakeKit) -> None:
    store = DecisionStore(kit.storage)
    store.add(_must())
    store.add(_must())
    assert len(store.load().must_links) == 1
    accept = AcceptRequest(request_id="req-1", entity=None, batch_filter=None, requested_by="ops", requested_at=NOW)
    store.add(accept)
    with pytest.raises(ConflictError, match="req-1"):
        store.add(accept.model_copy(update={"entity": "team"}))


def test_add_refuses_a_pair_of_one_provider(kit: LakeKit) -> None:
    store = DecisionStore(kit.storage)
    with pytest.raises(ValueError, match="two providers"):
        store.add(_must(b=team("statsbomb", "2")))
    with pytest.raises(ValueError, match="operator"):
        store.add(_must(decided_by=""))


def test_add_waits_for_the_config_lock(kit: LakeKit) -> None:
    store = DecisionStore(kit.storage)
    store.load()  # warm up, so that the timing below measures only the lock
    done = threading.Event()

    def add() -> None:
        store.add(_must())
        done.set()

    with ConfigLock(kit.storage):
        saver = threading.Thread(target=add)
        saver.start()
        assert not done.wait(0.5)  # the save waits while another writer holds the operator lock
        assert store.load().must_links == []
    assert done.wait(10)
    saver.join()
    assert store.load().must_links == [_must()]


def test_export_and_import(kit: LakeKit, tmp_path: Path) -> None:
    store = DecisionStore(kit.storage)
    store.add(_must())
    store.add(_must(reason="later look", decided_at=NOW + timedelta(hours=1)))
    store.add(
        CannotLink(entity="team", a=SB, b=team("wyscout", "11"), decided_by="karsten", decided_at=NOW, reason="x")
    )
    store.add(AcceptRequest(request_id="req-1", entity=None, batch_filter=None, requested_by="ops", requested_at=NOW))
    path = tmp_path / "decisions.json"
    assert store.export(path) == 3  # decisions only: requests are not exported
    assert store.import_(path) == 0  # every decision is already there
    for info in kit.storage.list("lake", "config/decisions/"):
        kit.storage.delete("lake", info.key)
    assert store.import_(path) == 3
    assert len(store.load().must_links) == 2
    assert len(store.load().accept_requests) == 1  # the request object was not touched


def test_request_ids_are_unique_and_safe() -> None:
    ids = {new_request_id(NOW) for _ in range(50)}
    assert len(ids) == 50
    assert all(i.startswith("req-20261027T120000-") and i.replace("-", "").isalnum() for i in ids)


def test_applied_requests_before_and_after_the_table() -> None:
    conn = lake()
    assert applied_requests(conn) == set()
    conn.execute(
        "CREATE TABLE lake.main.ops_applied_requests (request_id VARCHAR, kind VARCHAR, batch_id VARCHAR, "
        "links BIGINT, applied_at TIMESTAMPTZ, run_id VARCHAR)"
    )
    conn.execute("INSERT INTO lake.main.ops_applied_requests VALUES ('req-1', 'accept', 'req-1', 2, ?, 'r1')", [NOW])
    assert applied_requests(conn) == {"req-1"}

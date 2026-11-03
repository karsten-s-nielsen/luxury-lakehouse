"""The `VisibilityMover` collaborator (M1 extraction, plan Task 3): the full pining visibility listings, the
move on a kept-copy flag flip, and whether the run failed. Plus the `_Run.visibility_pass` delegator, which ORs
the returned `failed` flag into the run's `failed_run` (a defensive invariant the oracle does not reach)."""

from __future__ import annotations

from typing import Any

from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, checkpoints_for, entry, save_settings, with_entry

from lakehouse.cli import observe_run
from lakehouse.errors import HttpStatusError
from lakehouse.ids.keys import match_key
from lakehouse.observability import read_logs
from lakehouse.raw.archive import RawArchive
from lakehouse.runs.match_files import MatchFiles
from lakehouse.runs.run import _Run  # pyright: ignore[reportPrivateUsage] - a unit test of the run internals
from lakehouse.runs.visibility import VisibilityMover

KEY = match_key("gradientsports", "3812")


class _Errors:
    """A structural `ErrorSink` that records the (kind, severity) of each call."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def __call__(
        self,
        *,
        kind: str,
        message: str,
        severity: str,
        module_run_id: str | None = None,
        module_id: str | None = None,
        native: str | None = None,
    ) -> None:
        self.calls.append((kind, severity))


class _StubVis:
    """A `VisibilityMover` stand-in whose `run()` reports no failure (for the delegator test)."""

    def run(self) -> tuple[dict[Any, Any], bool]:
        return {}, False


def _pining(*natives: str, visibility: str = "public") -> FakeAdapter:
    entries = [entry("gradientsports_pining", n, visibility=visibility, competition=("38", "2022")) for n in natives]
    files = {(n, "events"): f"line {n}".encode() for n in natives}
    return FakeAdapter("gradientsports_pining", "gradientsports", "pining", entries, files)


def _ingest(kit: LakeKit, adapter: FakeAdapter) -> Any:
    settings = save_settings(kit.storage, [active("gradientsports_pining")], raw_archive=True)
    from lakehouse.runs.run import run_ingestion

    result = run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={adapter.module_id: adapter},  # type: ignore[dict-item]
        parsers={adapter.provider: FakeParser(adapter.provider)},  # type: ignore[dict-item]
        now=lambda: NOW,
        work_dir=kit.base / "work",
        dbt=None,
    )
    assert result.status == "ok", result.errors
    return settings


def _mover(kit: LakeKit, settings: Any, adapter: FakeAdapter, errors: _Errors) -> tuple[VisibilityMover, MatchFiles]:
    cat = kit.catalog()
    files = MatchFiles(
        cat,
        kit.storage,
        RawArchive(kit.storage, enabled=False),
        "run-vis",
        lambda: NOW,
        checkpoints=checkpoints_for(cat, kit.storage),
    )
    mover = VisibilityMover(
        cat,
        settings,
        {adapter.module_id: adapter},
        files,
        errors,
        confirm_lease=lambda: None,  # type: ignore[dict-item]
    )
    return mover, files


def test_kept_copy_flag_flip_moves(kit: LakeKit) -> None:
    adapter = _pining("3812")
    settings = _ingest(kit, adapter)
    with_entry(adapter, "3812", visibility="private")
    mover, files = _mover(kit, settings, adapter, _Errors())

    with observe_run(kit.storage, "vis-log", {}):
        listings, failed = mover.run()

    # the visibility_check span logs under the fixed run logger (guards a collaborator get_logger(__name__))
    recs = read_logs(kit.storage, "vis-log", limit=1000)
    assert {r.logger for r in recs if r.msg == "lakehouse.run.visibility_check"} == {"lakehouse.runs.run"}
    assert failed is False
    assert "gradientsports_pining" in listings
    assert {f.root for f in files.files_of(KEY)} == {"restricted"}  # the flip moved the match


def test_failed_full_listing_fails_the_run(kit: LakeKit) -> None:
    adapter = _pining("3812")
    settings = _ingest(kit, adapter)
    adapter.fail_listing = HttpStatusError(401, "https://pining.example.test/v1/gradientsports/matches")
    errors = _Errors()
    mover, _ = _mover(kit, settings, adapter, errors)

    listings, failed = mover.run()

    assert failed is True  # a failed visibility listing fails the run
    assert ("visibility_check_failed", "warning") in errors.calls
    assert listings == {}


def test_visibility_pass_preserves_prior_failed_run(kit: LakeKit) -> None:
    cat = kit.catalog()
    run = _Run(  # pyright: ignore[reportPrivateUsage]
        storage=kit.storage,
        trigger="cli",
        adapters={},
        parsers={},
        now=lambda: NOW,
        run_id="run-del",
        lease=kit.lease_of(cat),  # pyright: ignore[reportArgumentType] - a stub lease satisfies the run
        settings=kit.settings,
        cat=cat,
        work_dir=kit.base / "w",
        stop=lambda: False,
    )
    run.failed_run = True  # pyright: ignore[reportPrivateUsage]
    run._visibility = _StubVis()  # type: ignore[assignment]  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]

    assert run.visibility_pass() == {}  # pyright: ignore[reportPrivateUsage]
    assert run.failed_run is True  # pyright: ignore[reportPrivateUsage] - the `or` kept the prior failure

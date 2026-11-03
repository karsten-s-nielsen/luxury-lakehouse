"""The write-time lease check (T3 spec D5; plan round 3 Task 8): a writer that lost its lease stops at its next write
step, not at its next publish. A confirmation calls `assert_held()` only when the last passing one is older than one
`HEARTBEAT`."""

from __future__ import annotations

import inspect
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from lakekit import Clock, LakeKit
from runkit import FakeAdapter, FakeParser, active, entry, save_settings

from lakehouse.catalog.publish import listed_sequences
from lakehouse.config.model import CheckpointConfig
from lakehouse.errors import LeaseLostError
from lakehouse.runs.dbt import DbtResult
from lakehouse.runs.run import RunResult, run_ingestion
from lakehouse.storage.lease import LEASE_TTL, TAKEOVER_GRACE, Lease


def _adapter(*natives: str, module: str = "idsse_public", provider: str = "idsse", channel: str = "public") -> Any:
    files = {(n, "events"): f"{n}-a\n{n}-b".encode() for n in natives}
    return FakeAdapter(module, provider, channel, [entry(module, n) for n in natives], files)  # type: ignore[arg-type]


def _run(kit: LakeKit, adapter: Any, clock: Clock, **kwargs: Any) -> RunResult:
    kwargs.setdefault("dbt", None)
    return run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={adapter.module_id: adapter},
        parsers={adapter.provider: FakeParser(adapter.provider)},
        now=clock,
        work_dir=kit.base / "work",
        **kwargs,
    )


@pytest.fixture
def confirms(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """The calling function of every `Lease.confirm()`."""
    callers: list[str] = []
    real = Lease.confirm

    def spy(self: Lease) -> None:
        callers.append(inspect.stack()[1].function)
        real(self)

    monkeypatch.setattr(Lease, "confirm", spy)
    yield callers


def test_confirm_runs_at_each_module_and_match(kit: LakeKit, confirms: list[str]) -> None:
    save_settings(kit.storage, [active("idsse_public")])
    _run(kit, _adapter("J03WPY", "J03WMX"), Clock(datetime.now(UTC)))
    assert confirms.count("_check_stop") >= 3  # the module, then each of the two matches


def test_confirm_runs_before_each_visibility_move(kit: LakeKit, confirms: list[str]) -> None:
    save_settings(kit.storage, [active("statsbomb_pining")])
    pining = _adapter("3788741", module="statsbomb_pining", provider="statsbomb", channel="pining")
    clock = Clock(datetime.now(UTC))
    _run(kit, pining, clock)
    pining.entries = [entry("statsbomb_pining", "3788741", visibility="private")]
    confirms.clear()
    _run(kit, pining, clock)
    assert "_apply_flags" in confirms


def test_confirm_runs_before_resolution_and_dbt(kit: LakeKit, confirms: list[str]) -> None:
    """The run end confirms the lease before each of its write steps: resolution, dbt and the run-end maintenance
    merge (T3 spec D5; the merge is a real write step, T12 spec D3 / T12 plan Task 5 Step 1). Without a dbt step the
    run end confirms twice: resolution and the maintenance."""
    save_settings(kit.storage, [active("idsse_public")])

    def dbt(cat: Any, *, run_id: str, caps: Any) -> DbtResult:
        return DbtResult(success=True, results=[], run_results_key="runs/dbt/x.json")

    _run(kit, _adapter("J03WPY"), Clock(datetime.now(UTC)), dbt=dbt)
    assert confirms.count("finish") == 3  # resolution, dbt, maintenance
    confirms.clear()
    _run(kit, _adapter("J03WPY"), Clock(datetime.now(UTC)))  # no dbt step
    assert confirms.count("finish") == 2  # resolution, maintenance


def test_stalled_writer_stops_at_its_next_match(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("idsse_public")])
    adapter = _adapter("J03WPY", "J03WMX", "J03WN1")
    clock = Clock(datetime.now(UTC))
    checks = 0

    def stall_after_two_matches() -> bool:
        nonlocal checks
        checks += 1
        if checks == 4:  # the module check, then before matches 1, 2 and 3: the writer stalls before match 3
            clock.now += LEASE_TTL + TAKEOVER_GRACE + timedelta(minutes=3)
            assert Lease.acquire(kit.storage, holder_id="other", run_id="run-other", clock=clock) is not None
        return False

    with pytest.raises(LeaseLostError, match="generation 2"):
        _run(kit, adapter, clock, stop=stall_after_two_matches)
    assert [native for native, _ in adapter.fetched] == ["J03WPY", "J03WMX"]  # match 3 never started
    assert listed_sequences(kit.storage) == []  # M = 50: nothing was published, and a lost lease publishes nothing


@pytest.mark.parametrize(("matches", "expected"), [(1, 0), (1000, 1)])
def test_recent_publishes_need_no_extra_assert_held(
    kit: LakeKit, monkeypatch: pytest.MonkeyPatch, matches: int, expected: int
) -> None:
    """Each step takes 25 s. With M = 1 every match publishes, and each publish's own `assert_held()` resets the
    confirmation, so `confirm()` never needs one. With M = 1000 nothing publishes until the run end, so the gap passes
    one HEARTBEAT and `confirm()` calls `assert_held()` (the control case)."""
    save_settings(kit.storage, [active("idsse_public")], checkpoint=CheckpointConfig(matches=matches))
    clock = Clock(datetime.now(UTC))
    from_confirm: list[str] = []
    real = Lease.assert_held

    def spy(self: Lease) -> None:
        if inspect.stack()[1].function == "confirm":
            from_confirm.append("confirm")
        real(self)

    monkeypatch.setattr(Lease, "assert_held", spy)

    def twenty_five_seconds_per_step() -> bool:
        clock.now += timedelta(seconds=25)
        return False

    _run(kit, _adapter("J03WPY", "J03WMX", "J03WN1"), clock, stop=twenty_five_seconds_per_step)
    assert len(from_confirm) == expected

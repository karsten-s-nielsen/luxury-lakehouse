"""The interactive Ops actions (spec §6.2, §8; tests 6, 7, 8 + schedule/review/no-bypass; plan Task 8): every page
action routes through the guarded dispatch and the services; a toggle persists; Preview reads and never writes; no
page action bypasses dispatch. Actions are gated on the caller's permission (`WRITE`, or `VIEW_PRIVATE` for Preview)."""

from __future__ import annotations

from typing import Any, cast

import pytest
from lakekit import LakeKit

from lakehouse.config.model import CompetitionClause
from lakehouse.services.authz import Principal, Role, Unauthorized
from lakehouse.services.filters import FilterDiff
from lakehouse.services.settings import SettingsService
from lakehouse.storage.roots import ResolvedRoots
from lakehouse.ui.actions import ACTION_NAMES, ACTION_OUTCOMES, READ_ONLY_ACTIONS, build_action_registry
from lakehouse.ui.pages import modules, review, runs, settings
from lakehouse.ui.shell import Services

_OP = Principal("op", frozenset({Role.OPERATOR}))
_ANON = Principal(None, frozenset({Role.ANONYMOUS}))

_SERVICE_FIELDS = ("settings", "modules", "filters", "runs", "review", "schedule")


def _services(**overrides: Any) -> Services:
    base: dict[str, Any] = {k: cast("Any", object()) for k in _SERVICE_FIELDS}
    base.update(overrides)
    return Services(**base)


def test_module_toggle_persists(kit: LakeKit) -> None:
    settings = SettingsService(kit.storage, ResolvedRoots(roots=kit.roots, source="cli"))
    settings.save(kit.settings, expected_version=0, operator="setup")  # seed the install defaults (first save)
    registry = build_action_registry(_services(settings=settings))
    before = {m.module_id: m.active for m in settings.load()[0].modules}
    target = "statsbomb_public"

    registry.dispatch("on_module_active", _OP, module_id=target, active=not before[target])

    after = {m.module_id: m.active for m in settings.load()[0].modules}
    assert after[target] is (not before[target])  # the toggle persisted through the config lock
    assert {k: v for k, v in after.items() if k != target} == {k: v for k, v in before.items() if k != target}


class _FakeFilters:
    def __init__(self) -> None:
        self.diffs: list[tuple[str, list[Any], bool | None]] = []
        self.writes = 0

    def diff(self, module_id: str, draft: list[Any], *, statsbomb_360_only: bool | None = None) -> FilterDiff:
        self.diffs.append((module_id, draft, statsbomb_360_only))
        return FilterDiff(added=3, stop_updating=1, resume_updating=0)


def test_preview_diffs_the_draft_no_write_private_operator_only() -> None:
    """Preview shows what saving the DRAFT would add or stop (spec §19.5) — the diff of the draft clauses and the
    360 option against the saved filter — not a re-listing of the tournaments. It is an operator-only (VIEW_PRIVATE)
    read: it writes nothing, and an anonymous caller is refused."""
    fake = _FakeFilters()
    registry = build_action_registry(_services(filters=fake))
    clause = CompetitionClause(competition_id="43", seasons=["281"])

    result = registry.dispatch(
        "on_preview_filter", _OP, module_id="statsbomb_public", clauses=[clause], statsbomb_360_only=True
    )
    assert result == FilterDiff(added=3, stop_updating=1, resume_updating=0)
    assert fake.diffs == [("statsbomb_public", [clause], True)] and fake.writes == 0  # the draft, no bronze written
    with pytest.raises(Unauthorized):
        registry.dispatch("on_preview_filter", _ANON, module_id="statsbomb_public", clauses=[])


def test_every_action_has_a_plain_words_outcome() -> None:
    """Every action has an outcome message, so the notice never shows a raw action name (the former notice read
    "Done: on_save_filter."). The Preview outcome also places the diff beside its button."""
    assert set(ACTION_OUTCOMES) == ACTION_NAMES
    state: Any = type("S", (), {})()
    for name, outcome in ACTION_OUTCOMES.items():
        message = outcome(state, FilterDiff(added=0, stop_updating=0, resume_updating=0))
        assert message and "on_" not in message, name
    assert state.filter_diff == "**If you save this filter:** no change"
    assert READ_ONLY_ACTIONS == {"on_preview_filter"}  # a read: no post-action refresh wipes the previewed draft


class _FakeRuns:
    def __init__(self) -> None:
        self.n = 0

    def trigger(self) -> str:
        self.n += 1
        return "started"


def test_run_now_nonblocking() -> None:
    fake = _FakeRuns()
    registry = build_action_registry(_services(runs=fake))

    assert registry.dispatch("on_run_now", _OP) == "started"  # returns at once, never blocks
    assert fake.n == 1
    with pytest.raises(Unauthorized):  # a write needs WRITE; an anonymous caller is refused
        registry.dispatch("on_run_now", _ANON)


class _FakeSchedule:
    def __init__(self) -> None:
        self.calls: list[tuple[bool, str | None, int, str]] = []

    def set(self, cfg: Any, *, expected_version: int, operator: str) -> str:
        self.calls.append((cfg.enabled, cfg.cron, expected_version, operator))
        return "status"


def test_schedule_save_through_dispatch() -> None:
    fake = _FakeSchedule()
    registry = build_action_registry(_services(schedule=fake))

    registry.dispatch("on_save_schedule", _OP, cron="@daily", enabled=True, expected_version=3)
    registry.dispatch("on_pause_schedule", _OP, cron="@daily", expected_version=4)
    registry.dispatch("on_resume_schedule", _OP, cron="@daily", expected_version=5)

    assert fake.calls == [
        (True, "@daily", 3, "op"),
        (False, "@daily", 4, "op"),
        (True, "@daily", 5, "op"),
    ]


class _FakeReview:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    def accept_recommended(self, entity: Any, batch_filter: str | None, *, operator: str) -> str:
        self.calls.append(("accept", entity, batch_filter, operator))
        return "r1"

    def undo_batch(self, batch_id: str, *, operator: str) -> str:
        self.calls.append(("undo", batch_id, operator))
        return "r2"

    def decide(self, item_id: str, action: str, *, operator: str, reason: str) -> None:
        self.calls.append(("decide", item_id, action, operator, reason))


def test_review_decisions_through_dispatch() -> None:
    fake = _FakeReview()
    registry = build_action_registry(_services(review=fake))

    registry.dispatch("on_accept_recommended", _OP, entity=None, batch_filter=None)
    registry.dispatch("on_undo_batch", _OP, batch_id="b1")
    registry.dispatch("on_decide", _OP, item_id="i1", action="confirm", reason="same player")

    assert fake.calls == [
        ("accept", None, None, "op"),
        ("undo", "b1", "op"),
        ("decide", "i1", "confirm", "op", "same player"),
    ]


def test_no_page_action_bypasses_dispatch() -> None:
    from lakehouse.ui.template import action_names

    registry = build_action_registry(_services())
    used: set[str] = set()
    for page in (modules, runs, review, settings):  # filters folded into modules (F7); schedule into settings (F8)
        tokens = set(action_names(page.CONFIG))  # the actions the page declares (its Buttons), not markup grep
        assert tokens, f"{page.__name__} declares no action"
        for token in tokens:
            assert token in registry, f"{page.__name__} action {token!r} is not registered — it bypasses dispatch"
        used |= tokens
    assert used == ACTION_NAMES  # every registered action is used by a page, and every page action is registered

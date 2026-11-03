"""The Ops pages (plan Task 28): every page is behind the sign-in guard, shows names never raw ids, spells out the
score, surfaces every fallback with a marker, keeps "Run now" non-blocking, and shows the StatsBomb 360 option and
share only on the StatsBomb page. The pages are rendered from fake services so the content is deterministic."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from lakekit import LakeKit

from lakehouse.config.model import load_defaults
from lakehouse.config.store import ConfigVersionInfo
from lakehouse.schedule.base import ScheduleStatus
from lakehouse.services.filters import FilterDiff
from lakehouse.services.modules import ModuleStatus
from lakehouse.services.review import RANKING_LIMIT, QueueItem, RecordView, score_meaning
from lakehouse.services.runs import RunSummary, TriggerResult
from lakehouse.sources.base import PreviewResult, PreviewTournament
from lakehouse.storage.keys import RootMap, StorageRootInfo
from lakehouse.storage.roots import ResolvedRoots
from lakehouse.ui import shell
from lakehouse.ui.pages import filters, modules, review, runs
from lakehouse.ui.template import iter_controls
from lakehouse.web.app import create_app

NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)


class _FakeModules:
    def __init__(self, statuses: list[ModuleStatus]) -> None:
        self._statuses = statuses

    def list_modules(self) -> list[ModuleStatus]:
        return self._statuses


class _FakeFilters:
    def __init__(self, preview: PreviewResult) -> None:
        self._preview = preview

    def preview(self, module_id: str) -> PreviewResult:
        return self._preview


class _FakeRuns:
    def __init__(self, history: list[RunSummary], trigger_result: TriggerResult) -> None:
        self._history = history
        self._trigger_result = trigger_result
        self.triggered = 0

    def history(self, limit: int = 500) -> list[RunSummary]:
        return self._history

    def trigger(self) -> TriggerResult:
        self.triggered += 1
        return self._trigger_result


class _FakeReview:
    def __init__(self, queue: list[QueueItem], accepted: int, pending: list[str]) -> None:
        self._queue = queue
        self._accepted = accepted
        self._pending = pending

    def queue(self, limit: int = RANKING_LIMIT) -> list[QueueItem]:
        return self._queue

    def unreviewed_count(self) -> int:
        return self._accepted

    def pending_requests(self) -> list[str]:
        return self._pending

    def page(self, limit: int = RANKING_LIMIT) -> Any:
        from lakehouse.services.review import ReviewPage

        return ReviewPage(items=self._queue[:limit], unreviewed_count=self._accepted, pending=self._pending)

    def recent_batches(self, limit: int = 20) -> list[tuple[str, str]]:
        return []  # the undo selector options (audit F4); empty for these fixtures


class _FakeSchedule:
    def __init__(self, status: ScheduleStatus) -> None:
        self._status = status

    def get(self) -> ScheduleStatus:
        return self._status


class _FakeSettings:
    def __init__(self, resolved: ResolvedRoots) -> None:
        self._resolved = resolved

    def load(self) -> tuple[Any, int]:
        return load_defaults("laptop"), 3

    def storage_roots(self) -> ResolvedRoots:
        return self._resolved

    def history(self, limit: int = 500) -> list[ConfigVersionInfo]:
        return [ConfigVersionInfo(version=3, saved_by="ops", saved_at=NOW, changes=["raw_archive: false -> true"])]


def _status(module_id: str, display: str, **over: Any) -> ModuleStatus:
    base: dict[str, Any] = {
        "module_id": module_id,
        "display_name": display,
        "active": True,
        "filter_summary": "all tournaments",
        "match_count": 7,
        "last_run": None,
        "token_status": None,
        "warning": None,
    }
    base.update(over)
    return ModuleStatus(**base)


def _record(name: str, team: str) -> RecordView:
    return RecordView(name=name, birth_date=None, team=team, jersey=None, position=None, matches=5)


def _item(
    entity: str, left: RecordView, right: RecordView, score: float, *, recommended: bool, held: bool
) -> QueueItem:
    return QueueItem(
        item_id=f"{entity}-0000000000000000",
        entity=cast("Any", entity),
        left=left,
        right=right,
        score=score,
        score_meaning=score_meaning(score, recommended=recommended, held=held, measured=True),
        recommended=recommended,
        held=held,
    )


def _preview() -> PreviewResult:
    return PreviewResult(
        tournaments=[
            PreviewTournament(
                competition_id="9",
                competition_name="1. Bundesliga",
                season_id="281",
                season_name="2023/2024",
                match_count=306,
                count_note=None,
                match_count_360=306,
            ),
            PreviewTournament(
                competition_id="43",
                competition_name="FIFA World Cup",
                season_id="3",
                season_name="2018",
                match_count=64,
                count_note=None,
                match_count_360=0,
            ),
        ],
        fetched_at=NOW,
    )


_RESOLVED = ResolvedRoots(
    roots=RootMap(
        lake=StorageRootInfo(local_path=Path("/lake"), hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=Path("/lake-restricted"), hf_bucket=None, s3_bucket_path=None),
    ),
    source="bootstrap",
)


def _services(**over: Any) -> shell.Services:
    parts: dict[str, Any] = {
        "settings": _FakeSettings(_RESOLVED),
        "modules": _FakeModules([_status("statsbomb_public", "StatsBomb (incl. 360)")]),
        "filters": _FakeFilters(_preview()),
        "runs": _FakeRuns([], TriggerResult(True, None, "started")),
        "review": _FakeReview([], 0, []),
        "schedule": _FakeSchedule(ScheduleStatus("windows", False, False, None, None, None)),
    }
    parts.update(over)
    return shell.Services(**parts)


# --- the tests -------------------------------------------------------------------------------------------------------


def _cells(columns: dict[str, list[str]]) -> list[str]:
    """Every cell of a native table (the Taipy column format), for a content scan."""
    return [cell for column in columns.values() for cell in column]


def test_every_page_requires_login(kit: LakeKit) -> None:
    app = create_app(load_defaults("laptop"), mode="local", accounts=cast("Any", _NoAccount()), env={})
    app.testing = True
    routes = shell.page_routes()
    # the five operator pages (logs folded into Runs, F6; "What to download" into Data sources, F7; Schedule into
    # Settings, F8). The per-caller anon opening of monitoring is wired into the web guard in plan Task 10; every other
    # page still needs login.
    assert sorted(routes) == ["modules", "monitoring", "review", "runs", "settings"]
    for route in routes:  # a Taipy page carries no decorator of its own: only the global guard covers it
        app.add_url_rule(f"/{route}", f"page_{route}", lambda: "page")
    client = app.test_client()
    for route in routes:
        reply = client.get(f"/{route}", base_url="https://localhost")
        assert reply.status_code == 302 and "/signin" in reply.headers["Location"], route


def test_pages_show_names_not_ids(kit: LakeKit) -> None:
    left, right = _record("Lionel Messi", "Argentina"), _record("L. Messi", "Argentina")
    services = _services(review=_FakeReview([_item("player", left, right, 92, recommended=True, held=False)], 1, []))
    rstate = SimpleNamespace()
    review.refresh(rstate, services)
    rcells = _cells(rstate.review_rows)
    assert any("Lionel Messi" in c for c in rcells) and any("Argentina" in c for c in rcells)
    assert all("0000000000000000" not in c for c in rcells)  # the synthetic item id never reaches a display cell
    mstate = SimpleNamespace()
    modules.refresh(mstate, services)
    mcells = _cells(mstate.module_rows)
    assert any("StatsBomb (incl. 360)" in c for c in mcells)  # the display name
    assert all("statsbomb_public" not in c for c in mcells)  # not the raw module id


def test_resolution_score_has_help_text(kit: LakeKit) -> None:
    item = _item("team", _record("Germany", "Germany"), _record("Germany", "DE"), 95, recommended=True, held=False)
    state = SimpleNamespace()
    review.refresh(state, _services(review=_FakeReview([item], 0, [])))
    assert "0 to 100" in state.review_notes and "higher is more alike" in state.review_notes
    assert item.score_meaning in _cells(state.review_rows)  # each row spells out what its score means


def test_fallback_markers_visible(kit: LakeKit) -> None:
    mstate = SimpleNamespace()
    modules.refresh(mstate, _services(modules=_FakeModules([_status("idsse_public", "IDSSE", match_count=None)])))
    assert any(modules.COUNT_UNAVAILABLE in c for c in _cells(mstate.module_rows))
    full = [
        _item("player", _record(f"P{i}", "T"), _record(f"Q{i}", "T"), 50, recommended=False, held=False)
        for i in range(RANKING_LIMIT)
    ]
    rstate = SimpleNamespace()
    review.refresh(rstate, _services(review=_FakeReview(full, 12, [])))
    assert "12 links accepted without review" in rstate.review_notes
    assert review.NOT_LISTED in rstate.review_notes


def test_run_now_non_blocking(kit: LakeKit) -> None:
    started = TriggerResult(True, "run-9", "started: the run shows here when it takes the lease")
    fake = _FakeRuns([], started)
    services = _services(runs=fake)
    result = runs.run_now(services)
    assert result is started and fake.triggered == 1  # it returns the trigger result at once, runs nothing inline


def test_filter_diff_text() -> None:
    assert filters.diff_text(FilterDiff(added=34, stop_updating=0, resume_updating=0)) == (
        "+34 matches would be added · 0 would stop updating · 0 would resume updating"
    )
    assert "(their data stays)" in filters.diff_text(FilterDiff(added=0, stop_updating=2, resume_updating=0))
    assert filters.diff_text(FilterDiff(added=0, stop_updating=0, resume_updating=0)) == "no change"


def test_statsbomb_360_checkbox_and_share(kit: LakeKit) -> None:
    services = _services()
    sb_state = SimpleNamespace(filter_module="statsbomb_public")
    filters.refresh(sb_state, services)
    assert any("with 360" in c for c in _cells(sb_state.filter_preview_rows))  # the StatsBomb 360 share column
    ws_state = SimpleNamespace(filter_module="wyscout_public")
    filters.refresh(ws_state, services)
    assert all("with 360" not in c for c in _cells(ws_state.filter_preview_rows))  # no other module shows it
    # the "360 freeze frames" toggle sits in the editor beside the tournament picker; it is gated to the StatsBomb
    # modules (spec §29.13)
    toggle = next(w for w in iter_controls(modules.CONFIG) if w.var == "draft_360")
    assert toggle.label == filters.THREE_SIXTY_LABEL and toggle.condition == "draft_is_statsbomb"
    on_state = SimpleNamespace()
    modules.refresh(
        on_state,
        _services(
            modules=_FakeModules(
                [_status("statsbomb_public", "StatsBomb (incl. 360)", filter_summary="all tournaments, 360 data only")]
            )
        ),
    )
    assert any("360 only" in c for c in _cells(on_state.module_rows))


class _NoAccount:
    def exists(self) -> bool:
        return True

    def verify(self, username: str, password: str) -> bool:
        return False

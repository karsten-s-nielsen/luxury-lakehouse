"""Page latency on a fixture lake (spec §19.5; plan Task 28): the warm (second) server-side render of every Ops page
is under the §29.18 cached-interaction budget — the five pure lake pages ≤ 500 ms, `review` ≤ 1.0 s (local carve-out,
§29.19), the two external pages ≤ 3 s. The COLD first-load bound (§19.4, ≤ 3 s) and the client render are measured on
HF (tests/hf/test_space_hf_live.py, Task 33), where the object cache warms a re-render. The external source preview is
faked, because this test runs offline."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, cast

from lakekit import LakeKit
from runkit import FakeAdapter, FakeParser, active, entry, save_settings

from lakehouse.config.model import ModuleConfig
from lakehouse.runs.run import run_ingestion
from lakehouse.schedule.base import ScheduleStatus
from lakehouse.services.filters import FilterService
from lakehouse.services.modules import ModuleService
from lakehouse.services.review import ReviewService
from lakehouse.services.runs import LocalRunTrigger, RunService
from lakehouse.services.schedule import ScheduleService
from lakehouse.services.settings import SettingsService
from lakehouse.sources.base import PreviewResult
from lakehouse.storage.roots import ResolvedRoots
from lakehouse.ui import shell

NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
_LAKE_PAGES = frozenset({"monitoring", "runs", "review", "modules"})  # §19.5: ≤ 500 ms cached
# Data sources loads its what-to-download editor on demand (audit D1), so navigate is a lake read. Settings carries
# the schedule section, which reads the schedule backend / HF-jobs API (audit F8), so it keeps the §19.4 (≤ 3 s) bound.
_EXTERNAL_PAGES = frozenset({"settings"})  # network / HF-jobs API; §19.4 page-load bound: ≤ 3 s

# Local carve-out (owner decision, ingestion §29.19). The §19.5 cached-interaction budget is 500 ms for EVERY lake
# page, and that budget is AUTHORITATIVE on HuggingFace, where the Space's object cache warms a re-render; it is
# verified there by tests/hf/test_space_hf_live.py. This offline test has no such warm object cache: `render_for`
# re-attaches the DuckLake catalog on every call, so the Review page — which reads the queue, every entity record,
# and a link table per entity kind in ONE attach (review V2UI-IMPL-18) — renders in ~0.7 s locally. The one-attach
# fix (three attaches down to one) is the real optimization; a sub-500 ms LOCAL render would need either HF's warm
# object cache or a deeper query rewrite. The local budget for `review` is therefore 1.0 s; HF holds the 500 ms.
_LOCAL_BUDGETS: dict[str, float] = {"review": 1.0}  # route -> local-only budget; all other lake pages use 0.5 s


class _FakeAdapter:
    """A channel adapter whose source preview is faked: this test is offline."""

    def preview(self) -> PreviewResult:
        return PreviewResult(tournaments=[], fetched_at=NOW)


class _FakeSchedule:
    def status(self) -> ScheduleStatus:
        return ScheduleStatus("windows", False, False, None, None, None)


def _services(kit: LakeKit) -> shell.Services:
    resolved = ResolvedRoots(roots=kit.roots, source="bootstrap")
    settings = SettingsService(kit.storage, resolved)

    def factory(module: ModuleConfig) -> Any:
        return _FakeAdapter()

    return shell.Services(
        settings=settings,
        modules=ModuleService(kit.storage, env={}),
        filters=FilterService(kit.storage, factory),
        runs=RunService(kit.storage, trigger=LocalRunTrigger(popen=lambda *a, **k: None)),
        review=ReviewService(kit.storage),
        schedule=ScheduleService(settings, cast("Any", _FakeSchedule()), env={}),
    )


def test_page_render_within_budget(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("idsse_public")])
    adapter = FakeAdapter(
        "idsse_public",
        "idsse",
        "public",
        [entry("idsse_public", "J03WPY"), entry("idsse_public", "J03WMX")],
        {("J03WPY", "events"): b"a\nb", ("J03WMX", "events"): b"c"},
    )
    run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={"idsse_public": adapter},  # type: ignore[dict-item]
        parsers={"idsse": FakeParser("idsse")},  # type: ignore[dict-item]
        now=lambda: NOW,
        work_dir=kit.base / "work",
        dbt=None,
    )
    services = _services(kit)

    # The §29.18 budgets, measured PER PAGE on the warm (second) render — the same shape as the HF page test
    # (test_space_hf_live), not a summed-cold total (owner decision, ingestion §29.19, impl round-3 review). Five
    # lake-read pages meet the §19.5 cached-interaction budget (≤ 500 ms, second render); `review` uses the local
    # carve-out above (1.0 s offline; the 500 ms budget holds on HF, verified by test_space_hf_live). The two
    # external pages (`filters`, `schedule`) meet the §19.4 page-load bound (≤ 3 s). This times the server-side
    # render path (render_for → refresh); the client render is measured on HF (Task 33).
    for route in shell.page_routes():
        assert route in _LAKE_PAGES or route in _EXTERNAL_PAGES, f"unclassified page {route}"
        shell.render_for(route, services, "operator")  # first render warms the object cache for this route
        start = time.perf_counter()
        shell.render_for(route, services, "operator")
        elapsed = time.perf_counter() - start
        budget = _LOCAL_BUDGETS.get(route, 0.5 if route in _LAKE_PAGES else 3.0)
        assert elapsed <= budget, f"the {route} second render took {elapsed:.3f}s (budget {budget:.1f}s)"

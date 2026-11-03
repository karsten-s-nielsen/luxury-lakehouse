"""The services layer that the Ops pages call (spec §8, §15.5, §15.6, §18, §19.5, §20; plan Task 25).

The services read the published catalog through `CatalogReader` and write only config-side objects; they never
return a raw id or a token value, and every list has a limit.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Generator, Iterable, Iterator, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import duckdb
import pytest
from lakekit import LakeKit, lake_roots
from piningkit import FakePining
from piningkit import adapter as pining_adapter
from runkit import NOW, FakeAdapter, FakeParser, active, entry, save_settings

from lakehouse.catalog.reader import CatalogReader
from lakehouse.config.model import ModuleConfig, ScheduleConfig
from lakehouse.config.store import ConfigStore
from lakehouse.errors import HttpStatusError, StaleCatalogError
from lakehouse.resolve.decisions import CannotLink, DecisionStore, MustLink
from lakehouse.resolve.links import resolve_all
from lakehouse.resolve.model import ENTITIES
from lakehouse.runs.progress import Progress as RunProgress
from lakehouse.runs.progress import write_progress
from lakehouse.runs.run import run_ingestion
from lakehouse.schedule.base import ScheduleStatus
from lakehouse.services.account import AccountService
from lakehouse.services.filters import FilterDiff, FilterService
from lakehouse.services.modules import ModuleService
from lakehouse.services.review import ReviewService
from lakehouse.services.runs import HfRunTrigger, LocalRunTrigger, RunService
from lakehouse.services.schedule import ScheduleService
from lakehouse.services.settings import SettingsService
from lakehouse.sources.pining import PUBLIC_MARKER, PiningToken
from lakehouse.storage.keys import Root
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake

OWNER_SECRET = "owner-token-value-never-shown"  # noqa: S105 - a fake test value
NATIVE_IDS = ("J03WPY", "J03WMX", "7525", "7526", "7527", "10", "20", "30", "31")


def _strings(value: object) -> Iterator[str]:
    """Every string inside a service result (dataclasses, lists, dicts)."""
    if isinstance(value, str):
        yield value
    elif dataclasses.is_dataclass(value) and not isinstance(value, type):
        for field in dataclasses.fields(value):
            yield from _strings(getattr(value, field.name))
    elif isinstance(value, Mapping):
        for key, item in cast("Mapping[object, object]", value).items():
            yield from _strings(key)
            yield from _strings(item)
    elif isinstance(value, list | tuple | set | frozenset):
        for item in cast("Iterable[object]", value):
            yield from _strings(item)


def _no_raw_ids(*results: object) -> None:
    for text in (s for result in results for s in _strings(result)):
        for native in NATIVE_IDS:
            assert native not in text.split(), f"a raw id {native!r} reached the page: {text!r}"
        assert "DFL-" not in text, text


def _idsse(kit: LakeKit) -> FakeAdapter:
    save_settings(kit.storage, [active("idsse_public")])
    return FakeAdapter(
        "idsse_public",
        "idsse",
        "public",
        [entry("idsse_public", "J03WPY"), entry("idsse_public", "J03WMX")],
        {("J03WPY", "events"): b"a\nb", ("J03WMX", "events"): b"c"},
    )


def _run(kit: LakeKit, *adapters: FakeAdapter, trigger: str = "cli") -> Any:
    return run_ingestion(
        storage=kit.storage,
        trigger=trigger,  # type: ignore[arg-type]
        adapters={a.module_id: a for a in adapters},  # type: ignore[misc]
        parsers={a.provider: FakeParser(a.provider) for a in adapters},  # type: ignore[misc]
        now=lambda: NOW,
        work_dir=kit.base / "work",
        dbt=None,
    )


def _factory(adapters: Mapping[str, FakeAdapter]) -> Any:
    def build(module: ModuleConfig) -> FakeAdapter:
        return adapters[module.module_id]

    return build


# --- settings --------------------------------------------------------------------------------------------------------


def test_settings_load_save_history(kit: LakeKit) -> None:
    roots = ResolvedRoots(roots=kit.roots, source="env")
    service = SettingsService(kit.storage, roots)
    save_settings(kit.storage, [active("idsse_public")])
    settings, version = service.load()
    changed = settings.model_copy(update={"raw_archive": True})
    assert service.save(changed, expected_version=version, operator="ops") == version + 1
    assert [h.saved_by for h in service.history()][:1] == ["ops"]
    assert service.load()[0].raw_archive is True


def test_set_module_rejects_unknown_id(kit: LakeKit) -> None:
    """A config write for an unknown module id fails loudly, never a silent no-op reported as success (review
    V2UI-IMPL-24). Seen to fail: drop `_require_known` and the comprehension leaves every module unchanged, `save`
    bumps the version, and the action shows "Done"."""
    from typing import cast

    from lakehouse.config.model import ModuleId

    service = SettingsService(kit.storage, ResolvedRoots(roots=kit.roots, source="env"))
    save_settings(kit.storage, [active("idsse_public")])
    with pytest.raises(ValueError, match="unknown module"):
        service.set_module_active(cast("ModuleId", "does_not_exist_public"), active=True, operator="ops")
    with pytest.raises(ValueError, match="unknown module"):
        service.set_module_filter(cast("ModuleId", "does_not_exist_public"), clauses=[], operator="ops")


def test_display_names_cover_every_module() -> None:
    """Every MODULE_IDS has a DISPLAY_NAMES label, so the Source selector never falls back to a raw module id
    (review A T28-IMPL-06). Seen to fail: add a ModuleId without a DISPLAY_NAMES entry."""
    from lakehouse.config.model import MODULE_IDS
    from lakehouse.services.modules import DISPLAY_NAMES

    missing = set(MODULE_IDS) - set(DISPLAY_NAMES)
    assert not missing, f"MODULE_IDS without a DISPLAY_NAMES label: {missing}"


def test_storage_roots_shown_with_source(kit: LakeKit) -> None:
    roots = ResolvedRoots(roots=kit.roots, source="bootstrap")
    shown = SettingsService(kit.storage, roots).storage_roots()
    assert shown.source == "bootstrap" and shown.roots == kit.roots


# --- modules ---------------------------------------------------------------------------------------------------------


def test_list_modules(kit: LakeKit) -> None:
    adapter = _idsse(kit)
    _run(kit, adapter)
    statuses = {m.module_id: m for m in ModuleService(kit.storage, env={}).list_modules()}
    idsse = statuses["idsse_public"]
    assert (idsse.display_name, idsse.active, idsse.match_count) == ("IDSSE", True, 2)
    assert idsse.last_run is not None and idsse.last_run.status == "ok"
    assert idsse.token_status is None and idsse.warning is None
    pining = statuses["idsse_pining"]
    assert (pining.display_name, pining.active, pining.token_status) == ("IDSSE (pining)", False, "public-only")
    assert pining.warning == PUBLIC_MARKER  # §7.2: the visible marker while only the public token exists
    assert statuses["statsbomb_public"].filter_summary  # a plain-words summary of the clauses
    _no_raw_ids(list(statuses.values()))


def test_token_value_never_returned(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("idsse_public")])
    for env in ({"PINING_FOR_THE_DATA_TOKEN": OWNER_SECRET}, {"PINING_LIST_TOKEN": OWNER_SECRET}):
        statuses = ModuleService(kit.storage, env=env).list_modules()
        assert {m.token_status for m in statuses if m.module_id.endswith("_pining")} == {"present"}
        assert all(OWNER_SECRET not in text for text in _strings(statuses))


def test_empty_filter_is_a_warning(kit: LakeKit) -> None:
    save_settings(kit.storage, [ModuleConfig(module_id="idsse_public", active=True, filter=[])])
    [idsse] = [m for m in ModuleService(kit.storage, env={}).list_modules() if m.module_id == "idsse_public"]
    assert idsse.warning == "no tournament selected: the next run downloads nothing for this module"


# --- filters ---------------------------------------------------------------------------------------------------------


def _objects(kit: LakeKit) -> list[tuple[Root, str]]:
    return sorted((root, info.key) for root in ("lake", "restricted") for info in kit.storage.list(root, ""))


def test_preview_never_ingests(kit: LakeKit) -> None:
    adapter = _idsse(kit)
    before = _objects(kit)
    result = FilterService(kit.storage, _factory({"idsse_public": adapter})).preview("idsse_public")
    assert result.fetched_at == NOW
    assert _objects(kit) == before  # no storage write
    assert adapter.fetched == [] and adapter.listed == 0


def test_pining_preview_public_only_marker(kit: LakeKit) -> None:
    """§7.2 interim: without the list-only token, the pining preview says that private tournaments are missing."""
    save_settings(kit.storage, [active("idsse_pining")])
    http = FakePining("idsse", [], {})
    public = pining_adapter(http, token=PiningToken.of("test-token-pining-for-the-data", "public"))
    result = FilterService(kit.storage, lambda module: public).preview("idsse_pining")  # type: ignore[arg-type, return-value]
    assert PUBLIC_MARKER in result.notes


def _statsbomb(kit: LakeKit, *, only_360: bool) -> FakeAdapter:
    save_settings(kit.storage, [active("statsbomb_public", statsbomb_360_only=only_360)])
    entries = [
        entry("statsbomb_public", "7525", has_360=True),
        entry("statsbomb_public", "7526", has_360=False),
        entry("statsbomb_public", "7527", has_360=False),
    ]
    files = {(n, "events"): f"line {n}".encode() for n in ("7525", "7526", "7527")}
    adapter = FakeAdapter("statsbomb_public", "statsbomb", "public", entries[:2], files)
    adapter.later = entries[2]  # type: ignore[attr-defined]  # listed only after the first run
    return adapter


def test_diff_with_360_option(kit: LakeKit) -> None:
    """Turning `statsbomb_360_only` on stops updating the ingested matches without 360 data; nothing is added."""
    adapter = _statsbomb(kit, only_360=False)
    _run(kit, adapter)
    service = FilterService(kit.storage, _factory({"statsbomb_public": adapter}))
    settings, _ = ConfigStore(kit.storage).load()
    [module] = [m for m in settings.modules if m.module_id == "statsbomb_public"]
    diff = service.diff("statsbomb_public", module.filter, statsbomb_360_only=True)
    assert diff == FilterDiff(added=0, stop_updating=1, resume_updating=0)


def test_diff_widened_resumes(kit: LakeKit) -> None:
    """Turning the option off again resumes the kept matches without 360 data; `added` counts only matches that
    were never ingested."""
    adapter = _statsbomb(kit, only_360=False)
    _run(kit, adapter)  # 7525 and 7526 ingested
    save_settings(kit.storage, [active("statsbomb_public", statsbomb_360_only=True)])
    adapter.entries.append(adapter.later)  # type: ignore[attr-defined]  # a new match without 360 data appears
    _run(kit, adapter)  # 7526 is excluded now, 7527 is never fetched
    service = FilterService(kit.storage, _factory({"statsbomb_public": adapter}))
    settings, _ = ConfigStore(kit.storage).load()
    [module] = [m for m in settings.modules if m.module_id == "statsbomb_public"]
    diff = service.diff("statsbomb_public", module.filter, statsbomb_360_only=False)
    assert diff == FilterDiff(added=1, stop_updating=0, resume_updating=1)
    _no_raw_ids(diff)


# --- runs ------------------------------------------------------------------------------------------------------------


def test_history_and_progress(kit: LakeKit) -> None:
    adapter = _idsse(kit)
    first = _run(kit, adapter, trigger="button")
    started = datetime(2026, 10, 27, 13, tzinfo=UTC)
    running = RunProgress(
        run_id="run-live",
        trigger="schedule",
        status="running",
        step="fetch",
        module_id="idsse_public",
        done=1,
        total=4,
        started_at=started,
        updated_at=started,
        message=None,
    )
    write_progress(kit.storage, running)
    service = RunService(kit.storage, trigger=LocalRunTrigger(popen=lambda *a, **k: None))
    history = service.history()
    assert [(r.run_id, r.status) for r in history][:2] == [("run-live", "running"), (first.run_id, "ok")]
    assert history[1].trigger == "button" and history[1].counts.added == 2 and history[1].error_count == 0
    progress = service.progress("run-live")
    assert (progress.step, progress.module_id, progress.done, progress.total) == ("fetch", "idsse_public", 1, 4)
    assert progress.match_name is None
    _no_raw_ids(history, progress)


def test_trigger_local_and_refused_on_hf(kit: LakeKit) -> None:
    started: list[Sequence[str]] = []

    def popen(args: Sequence[str], **kwargs: object) -> None:
        del kwargs
        started.append(args)

    local = RunService(kit.storage, trigger=LocalRunTrigger(popen=popen))
    result = local.trigger()
    assert result.started and started and list(started[0])[-2:] == ["--trigger", "button"]

    class Refusing:
        def trigger(self) -> str:
            raise HttpStatusError(409, "https://huggingface.co/api/scheduled-jobs/sched-1/trigger")

    refused = RunService(kit.storage, trigger=HfRunTrigger(Refusing())).trigger()  # type: ignore[arg-type]
    assert (refused.started, refused.run_id, refused.message) == (False, None, "not started: a run is already active")

    class Accepting:
        def trigger(self) -> str:
            return "job-77"

    accepted = RunService(kit.storage, trigger=HfRunTrigger(Accepting())).trigger()  # type: ignore[arg-type]
    assert accepted.started and accepted.message.startswith("started")


def test_trigger_refused_shown(kit: LakeKit) -> None:
    """§19.5: a "Run now" that HF refuses is shown, never lost silently."""

    class Busy:
        def trigger(self) -> str:
            raise HttpStatusError(409, "https://huggingface.co/api/scheduled-jobs/sched-1/trigger")

    result = RunService(kit.storage, trigger=HfRunTrigger(Busy())).trigger()  # type: ignore[arg-type]
    assert result.started is False and result.message == "not started: a run is already active"


def test_limits_applied(kit: LakeKit) -> None:
    adapter = _idsse(kit)
    for _ in range(3):
        _run(kit, adapter)
    service = RunService(kit.storage, trigger=LocalRunTrigger(popen=lambda *a, **k: None))
    assert len(service.history(limit=2)) == 2
    with pytest.raises(ValueError, match="2000"):
        service.history(limit=2001)  # a timeline: at most 2000
    review = ReviewService(kit.storage)
    with pytest.raises(ValueError, match="500"):
        review.queue(limit=501)  # a ranking: at most 500


# --- review ----------------------------------------------------------------------------------------------------------


def _queued(kit: LakeKit) -> None:
    """Teams of two providers resolved into the review queue (no measured bands) and published."""
    cat = kit.catalog(run_id="run-resolve")
    conn = cat.conn
    conn.execute("CREATE TABLE lake.main.statsbomb_lineup_teams (team_id BIGINT, team_name VARCHAR, match_key BIGINT)")
    conn.execute("INSERT INTO lake.main.statsbomb_lineup_teams VALUES (1, 'Germany', 1), (3, 'Barcelona', 1)")
    conn.execute("CREATE TABLE lake.main.wyscout_teams (team_id BIGINT, name VARCHAR, area_name VARCHAR)")
    conn.execute("INSERT INTO lake.main.wyscout_teams VALUES (10, 'Germany', 'Germany'), (30, 'Barcelona A', 'Spain')")
    settings, _ = ConfigStore(kit.storage).load()
    decisions = DecisionStore(kit.storage).load()
    resolve_all(conn, decisions=decisions, bands=dict.fromkeys(ENTITIES), settings=settings, run_id="run-resolve")
    cat.commit("run_end")
    cat.close()


def test_review_queue_decide_and_requests(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("idsse_public")])
    _queued(kit)
    review = ReviewService(kit.storage)
    queue = review.queue()
    assert {(i.left.name, i.right.name) for i in queue} == {("Germany", "Germany"), ("Barcelona", "Barcelona A")}
    germany = next(i for i in queue if i.left.name == "Germany")
    assert germany.entity == "team" and 0 < germany.score <= 100 and germany.score_meaning
    assert not germany.recommended and not germany.held
    _no_raw_ids(queue)
    review.decide(germany.item_id, "confirm", operator="karsten", reason="same national team")
    barcelona = next(i for i in queue if i.left.name == "Barcelona")
    review.decide(barcelona.item_id, "reject", operator="karsten", reason="B team")
    review.decide(barcelona.item_id, "later", operator="karsten", reason="")
    stored = DecisionStore(kit.storage).load()
    assert [type(d) for d in [*stored.must_links, *stored.cannot_links]] == [MustLink, CannotLink]
    assert stored.must_links[0].decided_by == "karsten" and stored.must_links[0].reason == "same national team"
    accept = review.accept_recommended("team", None, operator="karsten")
    undo = review.undo_batch("run-resolve.auto", operator="karsten")
    assert set(review.pending_requests()) == {accept, undo}  # no run has applied them yet
    batch_ids = [batch_id for batch_id, _label in review.recent_batches()]
    assert accept in batch_ids  # recent_batches surfaces the accept batch for the undo selector (audit F4)
    assert review.unreviewed_count() == 0
    with pytest.raises(KeyError):
        review.decide("team-0000000000000000", "confirm", operator="karsten", reason="x")


def test_decisions_export_and_import(kit: LakeKit, tmp_path: Path) -> None:
    """`lakehouse decisions export|import` (spec §15.6): the decisions go to a file and into another lake; the requests
    stay behind, and a second import adds nothing."""
    save_settings(kit.storage, [active("idsse_public")])
    _queued(kit)
    review = ReviewService(kit.storage)
    queue = review.queue()
    germany = next(i for i in queue if i.left.name == "Germany")
    barcelona = next(i for i in queue if i.left.name == "Barcelona")
    review.decide(germany.item_id, "confirm", operator="karsten", reason="same national team")
    review.decide(barcelona.item_id, "reject", operator="karsten", reason="B team")
    review.accept_recommended("team", None, operator="karsten")
    file = tmp_path / "decisions.json"
    assert review.export(file) == 2
    roots = lake_roots(tmp_path / "other")
    init_lake(ResolvedRoots(roots=roots, source="cli"), bootstrap_file=tmp_path / "other-boot.json")
    other = ReviewService(LocalStorage(roots))
    assert other.import_(file) == 2
    assert other.import_(file) == 0
    copied = DecisionStore(LocalStorage(roots)).load()
    assert [(d.decided_by, d.reason) for d in copied.must_links] == [("karsten", "same national team")]
    assert [d.reason for d in copied.cannot_links] == ["B team"]
    assert copied.accept_requests == [] and copied.undo_requests == []


# --- schedule --------------------------------------------------------------------------------------------------------


class FakeBackend:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def install(self, cron: str, *, system: bool, env: Mapping[str, str], run_as: str | None = None) -> None:
        self.calls.append(("install", (cron, system, dict(env))))

    def uninstall(self) -> None:
        self.calls.append(("uninstall", None))

    def status(self) -> ScheduleStatus:
        return ScheduleStatus("systemd", True, True, "0 6 * * *", None, None)


class FakeHf:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def reschedule(self, cron: str) -> None:
        self.calls.append(("reschedule", cron))

    def suspend(self) -> None:
        self.calls.append(("suspend", None))

    def resume(self) -> None:
        self.calls.append(("resume", None))

    def status(self) -> ScheduleStatus:
        return ScheduleStatus("hf_jobs", True, False, "@daily", None, None)


def test_schedule_set_saves_then_installs(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("idsse_public")])
    settings_service = SettingsService(kit.storage, ResolvedRoots(roots=kit.roots, source="env"))
    backend = FakeBackend()
    service = ScheduleService(settings_service, backend, env={"LAKEHOUSE_LAKE_ROOT": "D:/Lake"})
    _, version = settings_service.load()
    service.set(ScheduleConfig(enabled=True, cron="0 6 * * *"), expected_version=version, operator="ops")
    assert settings_service.load()[0].schedule == ScheduleConfig(enabled=True, cron="0 6 * * *")
    assert backend.calls == [("install", ("0 6 * * *", False, {"LAKEHOUSE_LAKE_ROOT": "D:/Lake"}))]
    _, version = settings_service.load()
    service.set(ScheduleConfig(enabled=False, cron="0 6 * * *"), expected_version=version, operator="ops")
    assert backend.calls[-1] == ("uninstall", None)
    hf = FakeHf()
    hf_service = ScheduleService(settings_service, hf, env={})
    _, version = settings_service.load()
    hf_service.set(ScheduleConfig(enabled=True, cron="@daily"), expected_version=version, operator="ops")
    _, version = settings_service.load()
    hf_service.set(ScheduleConfig(enabled=False, cron="@daily"), expected_version=version, operator="ops")
    assert hf.calls == [("reschedule", "@daily"), ("resume", None), ("suspend", None)]
    assert hf_service.get().backend == "hf_jobs"


# --- restricted access, stale readers ---------------------------------------------------------------------------------


class NoRestricted:
    """A storage whose restricted root cannot be read, like the Space's token (§18, §20)."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._inner, name)
        if not callable(attribute):
            return attribute

        def guarded(*args: Any, **kwargs: Any) -> Any:
            if (args and args[0] == "restricted") or kwargs.get("root") == "restricted":
                raise PermissionError("the Space cannot read the restricted root")
            return attribute(*args, **kwargs)

        return guarded


def test_schedule_install_and_uninstall_the_saved_schedule(kit: LakeKit) -> None:
    """`lakehouse schedule install|uninstall` (spec §18): install applies the saved schedule without a new config
    version; an off schedule is refused before the backend is called; the HF job is never installed from here."""
    save_settings(kit.storage, [active("idsse_public")])
    settings = SettingsService(kit.storage, ResolvedRoots(roots=kit.roots, source="cli"))
    backend = FakeBackend()
    service = ScheduleService(settings, backend, env={"LAKEHOUSE_X": "1"}, system=True, run_as="ops")
    with pytest.raises(ValueError, match="off"):
        service.install()
    assert backend.calls == []
    saved, version = settings.load()
    on = saved.model_copy(update={"schedule": ScheduleConfig(enabled=True, cron="0 6 * * *")})
    settings.save(on, expected_version=version, operator="ops")
    assert service.install().installed
    service.uninstall()
    assert backend.calls == [("install", ("0 6 * * *", True, {"LAKEHOUSE_X": "1"})), ("uninstall", None)]
    assert settings.load()[1] == version + 1  # install saved nothing
    with pytest.raises(ValueError, match="HF"):
        ScheduleService(settings, FakeHf(), env={}).install()
    lakeless = ScheduleService(None, backend, env={})  # status and uninstall need no lake; install and set do
    assert lakeless.get().installed and lakeless.uninstall().installed
    with pytest.raises(ValueError, match="lives in the lake"):
        lakeless.install()
    with pytest.raises(ValueError, match="lives in the lake"):
        lakeless.set(ScheduleConfig(), expected_version=1, operator="ops")


def test_account_service(kit: LakeKit) -> None:
    """The local operator account through the services layer (the CLI and the web sign-in never use the store)."""
    accounts = AccountService(kit.storage)
    assert not accounts.exists() and accounts.username() is None
    accounts.create("ops", "correct horse battery staple")
    assert accounts.exists() and accounts.username() == "ops"
    assert accounts.verify("ops", "correct horse battery staple") and not accounts.verify("ops", "wrong password!")


def test_services_without_restricted_access(kit: LakeKit) -> None:
    """§23.6 "Space without restricted access", local half: every service works without the restricted root."""
    save_settings(kit.storage, [active("gradientsports_pining")])
    entries = [entry("gradientsports_pining", "3812", visibility="private", competition=("38", "2022"))]
    private = FakeAdapter("gradientsports_pining", "gradientsports", "pining", entries, {("3812", "events"): b"x"})
    assert _run(kit, private).status == "ok"
    _queued(kit)
    storage = NoRestricted(kit.storage)
    roots = ResolvedRoots(roots=kit.roots, source="env")
    ModuleService(storage, env={}).list_modules()  # type: ignore[arg-type]
    RunService(storage, trigger=LocalRunTrigger(popen=lambda *a, **k: None)).history()  # type: ignore[arg-type]
    review = ReviewService(storage)  # type: ignore[arg-type]
    review.queue()
    review.pending_requests()
    review.unreviewed_count()
    SettingsService(storage, roots).load()  # type: ignore[arg-type]
    with pytest.raises(PermissionError):
        storage.get("restricted", "data/x")  # the guard itself works


def test_stale_reader_after_move_shows_error(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    """§9: a read that fails with `IO Error: Cannot open file` after a move is an error to show, never missing data;
    the next call reads the new `CURRENT`."""
    adapter = _idsse(kit)
    _run(kit, adapter)
    service = RunService(kit.storage, trigger=LocalRunTrigger(popen=lambda *a, **k: None))
    original = CatalogReader.session

    class Broken:
        def execute(self, *args: Any, **kwargs: Any) -> Any:
            raise duckdb.IOException('IO Error: Cannot open file "data/bronze/x.parquet": No such file')

    from contextlib import contextmanager

    @contextmanager
    def stale(self: CatalogReader) -> Generator[Any, None, None]:
        yield Broken()

    monkeypatch.setattr(CatalogReader, "session", stale)
    with pytest.raises(StaleCatalogError, match="changed while"):
        service.history()
    monkeypatch.setattr(CatalogReader, "session", original)
    assert service.history()  # a fresh reader on the current version


def test_no_raw_ids_in_outputs(kit: LakeKit) -> None:
    """§19.5: no service output that a page shows carries a native id (the ids of this test's matches and teams)."""
    adapter = _idsse(kit)
    first = _run(kit, adapter)
    _queued(kit)
    runs = RunService(kit.storage, trigger=LocalRunTrigger(popen=lambda *a, **k: None))
    review = ReviewService(kit.storage)
    outputs: list[object] = [
        ModuleService(kit.storage, env={}).list_modules(),
        runs.history(),
        runs.progress(first.run_id),
        review.queue(),
        review.unreviewed_count(),
        FilterService(kit.storage, _factory({"idsse_public": adapter})).diff("idsse_public", []),
    ]
    _no_raw_ids(*outputs)
    assert any("Germany" in text for text in _strings(outputs))  # names, not ids, reach the page

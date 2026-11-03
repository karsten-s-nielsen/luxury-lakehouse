"""The catalog scale test (T3 spec §8 "Scale test", D1, D4, D7, D8; plan round 3 Task 11; closes ingestion spec §26
item 4). Marker `bench`: Mac Mini only.

The real parsers of the five providers ingest 1,000 matches into an encrypted local lake (the Mac Mini's production
form) with the production checkpoint settings. The matches are the repository's fixtures, re-keyed with distinct
native ids, so no real data is added. Three more runs follow: 100 replaces (re-ingest), then 100 public matches of a
pining module and their move to private, which is a burst of 100 barrier publishes (the privacy direction is never
batched, T3 spec D3). Every publish is recorded through a `CheckpointPolicy` subclass, and the test asserts the T3
bounds on each run:

- publishes <= ceil(run time / T) + ceil(U / M) + barriers + 1, where U is the count of deferrable units;
- right after each in-run publish and its pruning, the retained in-run versions <= 1 + the versions superseded within
  the last G;
- after the run-end pruning, no in-run version superseded more than G ago is left;
- the retained in-run versions stay within G / (shortest publish time) + 1;
- the bytes published per run <= the publish bound * the largest published copy;
- the run-end publish stays below the kill grace (D1).

It prints a JSON report, part by part for §26 item 4: the catalog size and attach time at 1,000 matches, the growth
after the replaces with and without compaction, the version-retention storage, the native data and the inlined rows,
the guard time per publish, the exempted count of each sweep, and the run-end publish time.
"""

from __future__ import annotations

import bz2
import itertools
import json
import logging
import math
import re
import secrets
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar, Final

import pytest
from lakekit import lake_roots
from runkit import FakeAdapter, FakeParser, ListingAdapter, active, entry, save_settings

from lakehouse.catalog.publish import listed_sequences, read_current
from lakehouse.catalog.reader import CatalogReader
from lakehouse.config.model import CheckpointConfig, ModuleId
from lakehouse.entries import SourceEntry
from lakehouse.ids.native import Provider
from lakehouse.parsers.gradientsports import GradientSportsParser
from lakehouse.parsers.idsse import IdsseParser
from lakehouse.parsers.skillcorner import SkillCornerParser
from lakehouse.parsers.statsbomb import StatsBombParser
from lakehouse.parsers.wyscout import WyscoutParser
from lakehouse.runs import run as run_module
from lakehouse.runs.checkpoint import CheckpointPolicy, PublishRecord
from lakehouse.runs.run import RunResult, run_ingestion
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake

pytestmark = [pytest.mark.bench]

FIXTURES: Final = Path(__file__).resolve().parents[1] / "fixtures"
PER_PROVIDER: Final = 200  # 5 providers: 1,000 matches (§26 item 4)
REPLACED: Final = 100
BURST: Final = 100
CONFIG: Final = CheckpointConfig()  # the production defaults: T 5 min, M 50, G 2 min, B 50
# The policy prunes against its own clock reading; the test reads the clock a moment after the publish returns. One
# second covers that gap (the delete calls and one log record) without loosening a 2-minute grace in any real way.
CLOCK_SLACK: Final = timedelta(seconds=1)
# Below launchd's ExitTimeOut default (20 s, launchd.plist(5)) and Kubernetes' terminationGracePeriodSeconds default
# (30 s). The HF Jobs grace is not documented; Task 12 measures it on a real Job.
KILL_GRACE_FLOOR_S: Final = 10.0
PINING_MODULE: Final[ModuleId] = "statsbomb_pining"
PARSERS: Final[dict[Provider, Any]] = {
    "statsbomb": StatsBombParser(),
    "idsse": IdsseParser(),
    "skillcorner": SkillCornerParser(),
    "wyscout": WyscoutParser(),
    "gradientsports": GradientSportsParser(),
}


@dataclass(frozen=True)
class _Source:
    """One provider's fixture match and how to re-key it: every bounded occurrence of `native` in its files becomes
    the new native id (the parsers check the in-file id against the listed one)."""

    module: ModuleId
    provider: Provider
    native: str
    artifacts: Mapping[str, str]
    competition: tuple[str, str]
    listings: Mapping[str, str]
    new_id: str  # a format string over the match index
    has_360: bool | None = None
    compressed: tuple[str, ...] = ()  # artifacts delivered as bz2, as the Drive folder delivers them


SOURCES: Final = (
    _Source(
        "statsbomb_public",
        "statsbomb",
        "9100001",
        {
            "events": "statsbomb/events_9100001.json",
            "lineups": "statsbomb/lineups_9100001.json",
            "three_sixty": "statsbomb/three_sixty_9100001.json",
        },
        ("43", "106"),
        {"competitions": "statsbomb/competitions.json", "matches/43/106": "statsbomb/matches_43_106.json"},
        "{:d}",
        has_360=True,
    ),
    _Source(
        "idsse_public",
        "idsse",
        "J03WPY",
        {
            "metadata": "idsse/matchinformation_J03WPY.xml",
            "events": "idsse/events_J03WPY_slice.xml",
            "positions": "idsse/positions_J03WPY_slice.xml",
        },
        ("DFL-COM-000002", "DFL-SEA-0001K6"),
        {},
        "JS{:04d}",
    ),
    _Source(
        "skillcorner_public",
        "skillcorner",
        "1899585",
        {
            "match": "skillcorner/match_1899585.json",
            "tracking": "skillcorner/tracking_1899585_slice.jsonl",
            "dynamic_events": "skillcorner/dynamic_events_1899585_slice.csv",
            "phases_of_play": "skillcorner/phases_of_play_1899585_slice.csv",
        },
        ("61", "95"),
        {"matches": "skillcorner/matches.json"},
        "{:d}",
    ),
    _Source(
        "wyscout_public",
        "wyscout",
        "2058017",
        {"matches": "wyscout/matches_World_Cup_slice.json", "events": "wyscout/events_World_Cup_slice.json"},
        ("28", "10078"),
        {
            "competitions": "wyscout/competitions.json",
            "teams": "wyscout/teams.json",
            "players": "wyscout/players_slice.json",
            "coaches": "wyscout/coaches_slice.json",
            "event_names": "wyscout/eventid2name.csv",
            "tag_names": "wyscout/tags2name.csv",
        },
        "{:d}",
    ),
    _Source(
        "gradientsports_public",
        "gradientsports",
        "9001",
        {
            "metadata": "gradientsports/metadata_9001.json",
            "roster": "gradientsports/roster_9001.json",
            "events": "gradientsports/events_9001.json",
            "tracking": "gradientsports/tracking_9001.jsonl",
        },
        ("77", "2030"),
        {"competitions": "gradientsports/competitions.csv", "players": "gradientsports/players.csv"},
        "{:d}",
        compressed=("tracking",),
    ),
)
# The first new id of each provider: far from the fixture ids, so a re-keyed file never names another fixture match.
FIRST_ID: Final[dict[Provider, int]] = {
    "statsbomb": 9_200_000,
    "idsse": 0,
    "skillcorner": 2_900_000,
    "wyscout": 3_900_000,
    "gradientsports": 590_000,
}
ID_PATTERNS: Final[dict[str, re.Pattern[bytes]]] = {
    source.native: re.compile(rb"(?<![0-9A-Za-z.])" + re.escape(source.native.encode()) + rb"(?![0-9A-Za-z])")
    for source in SOURCES
}
REPLACED_MARK: Final = b"\n<!-- replaced by the scale test -->\n"


# --- the recording policy ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class _Publish:
    record: PublishRecord
    retained: int  # every retained version right after the publish and its pruning
    seconds: float  # the publish call: flush hooks, guard, commit (compaction included) and pruning
    done_at: datetime  # the clock a moment after the publish returned


@dataclass
class _Trace:
    base: int  # versions retained before the run's first publish
    units: int = 0
    barriers: int = 0
    publishes: list[_Publish] = field(default_factory=list[_Publish])

    def add(self, record: PublishRecord, retained: int, seconds: float) -> None:
        self.publishes.append(_Publish(record, retained, seconds, datetime.now(UTC)))


class Recording(CheckpointPolicy):
    """The production policy, unchanged, with every publish recorded."""

    traces: ClassVar[list[_Trace]] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.trace = _Trace(base=self.retained_versions)
        Recording.traces.append(self.trace)

    def maybe(self, reason: str) -> PublishRecord | None:
        self.trace.units += 1
        started = time.perf_counter()
        record = super().maybe(reason)
        if record is not None:
            self.trace.add(record, self.retained_versions, time.perf_counter() - started)
        return record

    def now(self, reason: str) -> PublishRecord:
        self.trace.barriers += 1
        started = time.perf_counter()
        record = super().now(reason)
        self.trace.add(record, self.retained_versions, time.perf_counter() - started)
        return record

    def run_end(self) -> PublishRecord:
        started = time.perf_counter()
        record = super().run_end()
        self.trace.add(record, self.retained_versions, time.perf_counter() - started)
        return record


class _Records(logging.Handler):
    """The run's own structured log records that the report reads (`cleanup`, `catalog compacted`, `catalog
    publish`)."""

    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.found: list[tuple[str, dict[str, Any]]] = []

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if message in ("cleanup", "catalog compacted", "catalog publish"):
            self.found.append((message, dict(record.__dict__)))

    def take(self, message: str) -> list[dict[str, Any]]:
        taken = [extra for name, extra in self.found if name == message]
        self.found = [(name, extra) for name, extra in self.found if name != message]
        return taken


@pytest.fixture
def recording(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Records]:
    Recording.traces = []
    monkeypatch.setattr(run_module, "CheckpointPolicy", Recording)
    handler = _Records()
    logger = logging.getLogger("lakehouse")
    old = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        yield handler
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old)


# --- the re-keyed fixtures --------------------------------------------------------------------------------------------


def _read(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _natives(source: _Source) -> list[str]:
    first = FIRST_ID[source.provider]
    return [source.new_id.format(first + i) for i in range(PER_PROVIDER)]


def _rekeyed(source: _Source, native: str) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for artifact, name in source.artifacts.items():
        data = ID_PATTERNS[source.native].sub(native.encode(), _read(name))
        files[artifact] = bz2.compress(data) if artifact in source.compressed else data
    return files


def _public_adapters(replaced: frozenset[str] = frozenset()) -> dict[ModuleId, FakeAdapter]:
    """The five public modules over the re-keyed fixtures. A native id in `replaced` gets new bytes and a new
    change signal for one artifact, so the run re-ingests it (a replace)."""
    adapters: dict[ModuleId, FakeAdapter] = {}
    for source in SOURCES:
        entries: list[SourceEntry] = []
        raw: dict[tuple[str, str], bytes] = {}
        for native in _natives(source):
            files = _rekeyed(source, native)
            signals = dict.fromkeys(files, "s1")
            if native in replaced:
                files["events"] = files["events"] + REPLACED_MARK
                signals["events"] = "s2"
            entries.append(
                entry(source.module, native, signals=signals, competition=source.competition, has_360=source.has_360)
            )
            raw |= {(native, artifact): data for artifact, data in files.items()}
        listings = {kind: _read(name) for kind, name in source.listings.items()}
        adapters[source.module] = ListingAdapter(
            source.module, source.provider, "public", entries, raw, listings=listings
        )
    return adapters


def _pining_adapter(visibility: str) -> FakeAdapter:
    natives = [f"{9_600_000 + i}" for i in range(BURST)]
    files = {(n, "events"): f"{n}-a\n{n}-b".encode() for n in natives}
    entries = [entry(PINING_MODULE, n, visibility=visibility) for n in natives]
    return FakeAdapter(PINING_MODULE, "statsbomb", "pining", entries, files)


# --- the bounds -------------------------------------------------------------------------------------------------------


def _superseded_within(publishes: Sequence[_Publish], index: int, window: timedelta) -> int:
    """In-run versions before `index` whose successor was published less than `window` before `index` returned."""
    cutoff = publishes[index].done_at - window
    return sum(
        1
        for earlier, later in itertools.pairwise(publishes[: index + 1])
        if earlier.record.kind == "in_run" and later.record.at > cutoff
    )


def _check_bounds(label: str, trace: _Trace, run_s: float, storage: LocalStorage) -> dict[str, Any]:
    grace = timedelta(minutes=CONFIG.grace_minutes)
    publishes = trace.publishes
    assert publishes and publishes[-1].record.kind == "run_end", f"{label}: no run-end publish"
    bound = math.ceil(run_s / (CONFIG.minutes * 60)) + math.ceil(trace.units / CONFIG.matches) + trace.barriers + 1
    assert len(publishes) <= bound, f"{label}: {len(publishes)} publishes > bound {bound}"

    in_run = [i for i, p in enumerate(publishes) if p.record.kind == "in_run"]
    shortest = min(p.seconds for p in publishes)
    burst_bound = math.ceil(grace.total_seconds() / shortest) + 1
    retained_in_run = [publishes[i].retained - trace.base for i in in_run]
    for i, retained in zip(in_run, retained_in_run, strict=True):
        allowed = 1 + _superseded_within(publishes, i, grace + CLOCK_SLACK)
        assert retained <= allowed, f"{label}: publish {i} retains {retained} in-run versions > {allowed}"
        assert retained <= burst_bound, f"{label}: publish {i} retains {retained} > G / publish time + 1"

    end = publishes[-1]
    left = set(listed_sequences(storage))
    stale = [
        earlier.record.seq
        for earlier, later in itertools.pairwise(publishes)
        if earlier.record.kind == "in_run" and later.record.at <= end.done_at - grace - CLOCK_SLACK
    ]
    assert not left & set(stale), f"{label}: in-run versions superseded more than G ago are left: {left & set(stale)}"

    largest = max(p.record.size for p in publishes)
    published = sum(p.record.size for p in publishes)
    assert published <= bound * largest, f"{label}: {published} bytes published > {bound} * {largest}"
    assert end.seconds < KILL_GRACE_FLOOR_S, f"{label}: the run-end publish took {end.seconds:.1f} s"
    reasons: dict[str, int] = {}
    for p in publishes:
        reasons[p.record.reason] = reasons.get(p.record.reason, 0) + 1
    return {
        "run_s": round(run_s, 1),
        "units": trace.units,
        "barriers": trace.barriers,
        "publishes": len(publishes),
        "publish_bound": bound,
        "reasons": reasons,
        "max_retained_in_run": max(retained_in_run, default=0),
        "retained_bound_g_over_publish": burst_bound,
        "publish_s_min_median_max": _spread([p.seconds for p in publishes]),
        "run_end_publish_s": round(end.seconds, 3),
        "kill_grace_floor_s": KILL_GRACE_FLOOR_S,
        "bytes_published": published,
        "largest_copy_bytes": largest,
    }


def _spread(values: Sequence[float]) -> list[float]:
    ordered = sorted(values)
    return [round(ordered[0], 3), round(ordered[len(ordered) // 2], 3), round(ordered[-1], 3)]


# --- the measurements -------------------------------------------------------------------------------------------------


def _bytes_under(folder: Path) -> int:
    return sum(p.stat().st_size for p in folder.rglob("*") if p.is_file()) if folder.exists() else 0


def _lake_state(storage: LocalStorage, base: Path, env: Mapping[str, str]) -> dict[str, Any]:
    started = time.perf_counter()
    with CatalogReader(storage, env=env).session() as conn:
        attach_s = time.perf_counter() - started
        snapshots = conn.execute("SELECT count(*) FROM __ducklake_metadata_lake.ducklake_snapshot").fetchone()
        inlined = conn.execute("SELECT count(*) FROM __ducklake_metadata_lake.ducklake_inlined_data_tables").fetchone()
        matches = conn.execute("SELECT count(*) FROM lake.main.ops_matches").fetchone()
    versions = list(storage.list("lake", "catalog/v/"))
    return {
        "matches": matches[0] if matches else None,
        "catalog_bytes": read_current(storage).size,
        "attach_s": round(attach_s, 3),
        "snapshots": snapshots[0] if snapshots else None,
        "inlined_data_tables": inlined[0] if inlined else None,
        "retained_version_objects": len(versions),
        "version_store_bytes": sum(info.size for info in versions),
        "native_data_bytes": _bytes_under(base / "lake" / "data") + _bytes_under(base / "lake-restricted" / "data"),
    }


def _sweeps(records: _Records) -> list[dict[str, Any]]:
    return [
        {"swept": r.get("swept"), "exempted": r.get("exempted"), "pruned_versions": r.get("pruned_versions")}
        for r in records.take("cleanup")
    ]


def _publish_parts(records: _Records) -> dict[str, Any]:
    """The guard and compaction time of each publish, from the D8 `catalog publish` records (§26 item 4 report)."""
    found = records.take("catalog publish")
    return {
        "guard_ms_min_median_max": _spread([float(r.get("guard_ms", 0)) for r in found]) if found else None,
        "compaction_ms_min_median_max": _spread([float(r.get("compaction_ms", 0)) for r in found]) if found else None,
    }


def _compactions(records: _Records) -> dict[str, Any]:
    found = records.take("catalog compacted")
    return {
        "count": len(found),
        "last_bytes_before": found[-1].get("bytes_before") if found else None,
        "last_bytes_after": found[-1].get("bytes_after") if found else None,
        "ms_max": max((r.get("ms", 0) for r in found), default=0),
    }


# --- the test ---------------------------------------------------------------------------------------------------------


def test_catalog_scale(tmp_path: Path, recording: _Records, capsys: pytest.CaptureFixture[str]) -> None:
    base = tmp_path / "scale"
    env = {"LAKEHOUSE_ENCRYPTION_KEY": secrets.token_hex(32)}  # generated here, never printed
    roots = lake_roots(base)
    init_lake(ResolvedRoots(roots=roots, source="cli"), bootstrap_file=base / "bootstrap.json")
    storage = LocalStorage(roots)
    report: dict[str, Any] = {"config": CONFIG.model_dump(), "matches": PER_PROVIDER * len(SOURCES)}

    def run(label: str, adapters: Mapping[ModuleId, FakeAdapter], parsers: Mapping[Provider, Any]) -> RunResult:
        before = len(Recording.traces)
        started = time.perf_counter()
        result = run_ingestion(
            storage=storage,
            trigger="cli",
            adapters=adapters,  # type: ignore[arg-type]
            parsers=parsers,
            work_dir=base / f"work-{label}",
            env=env,
            dbt=None,
        )
        run_s = time.perf_counter() - started
        assert result.status == "ok", (label, result.errors)
        [trace] = Recording.traces[before:]
        report[label] = {
            "counts": {
                "added": result.counts.added,
                "changed": result.counts.changed,
                "unchanged": result.counts.unchanged,
                "failed": result.counts.failed,
            },
            "bounds": _check_bounds(label, trace, run_s, storage),
            "sweeps": _sweeps(recording),
            "compactions": _compactions(recording),
            "publish_parts": _publish_parts(recording),
        }
        return result

    public = [active(source.module) for source in SOURCES]
    save_settings(storage, public, checkpoint=CONFIG)
    ingest = run("ingest", _public_adapters(), PARSERS)
    assert ingest.counts.added == PER_PROVIDER * len(SOURCES)
    report["after_ingest"] = _lake_state(storage, base, env)

    idsse = next(source for source in SOURCES if source.provider == "idsse")
    replaced = frozenset(_natives(idsse)[:REPLACED])
    replace = run("replace", _public_adapters(replaced), PARSERS)
    assert replace.counts.changed == REPLACED
    report["after_replace"] = _lake_state(storage, base, env)
    report["catalog_growth_after_replace_bytes"] = (
        report["after_replace"]["catalog_bytes"] - report["after_ingest"]["catalog_bytes"]
    )

    save_settings(storage, [active(PINING_MODULE)], checkpoint=CONFIG)
    pining_parsers: dict[Provider, Any] = {"statsbomb": FakeParser("statsbomb")}
    burst_ingest = run("burst_ingest", {PINING_MODULE: _pining_adapter("public")}, pining_parsers)
    assert burst_ingest.counts.added == BURST
    burst = run("burst_private", {PINING_MODULE: _pining_adapter("private")}, pining_parsers)
    assert report["burst_private"]["bounds"]["reasons"].get("move", 0) == BURST, burst.counts
    report["after_burst"] = _lake_state(storage, base, env)

    with capsys.disabled():
        print("\nT3 SCALE REPORT " + json.dumps(report, sort_keys=True, default=str), flush=True)

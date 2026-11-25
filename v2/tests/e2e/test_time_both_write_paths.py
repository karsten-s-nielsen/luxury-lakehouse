"""Guard 3 (no-duration spec §4): time reads the same on BOTH bronze write paths.

The same fixture matches are ingested twice — once into a plain lake (`RegisterWriter`, parquet registered with
`ducklake_add_data_files`) and once into an encrypted lake (`LocalNativeWriter`, native inserts). The old duration
columns surfaced as BIGINT on the first and INTERVAL on the second, and each consumer broke on one of them. Now every
bronze time is an explicit-unit int64, so `fct_tracking_ball.time_seconds` and the SPADL `time_seconds` of statsbomb
and skillcorner must be identical on both lakes, and equal known source values. The gradientsports tracking gains
extra-time (period 3) frames in memory (Review Focus 3), and the statsbomb events gain one Pass at a known clock
(the fixture has only "Starting XI" events, which SPADL drops); the fixture files are not changed.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from computekit import PARSERS, fixture_adapters, read_fixture
from lakekit import LakeKit, lake_roots
from runkit import NOW, active, save_settings

from lakehouse.catalog.reader import CatalogReader
from lakehouse.compute.registry import DEFAULT_COMPUTE_STEPS
from lakehouse.runs.run import run_ingestion
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake

_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
_MODULES = ("statsbomb_public", "idsse_public", "skillcorner_public", "gradientsports_public")
_KNOWN_EVENT = "00000000-0000-4000-8000-0000000000aa"
_GS_FRAME, _GS_SECONDS = 102, 0.066408  # tracking_9001.jsonl: frameNum 102, periodElapsedTime 0.066408
_SC_EVENT, _SC_SECONDS = "8_1", 2.0  # dynamic_events_1899585_slice.csv: event 8_1, time_start "00:02.0"


_ACTIONS_OF = (
    "SELECT original_event_id, period_id, time_seconds FROM lake.main.fct_actions WHERE canonical_match_key IN "
    "(SELECT canonical_match_key FROM lake.main.dim_matches WHERE provider = ?) ORDER BY ALL"
)


def _tracking_with_extra_time() -> bytes:
    """The GS tracking fixture plus a period-3 copy of every frame (frame numbers shifted, clock kept)."""
    lines = read_fixture("gradientsports", "tracking_9001.jsonl").decode("utf-8").splitlines()
    extra: list[str] = []
    for line in lines:
        record: dict[str, Any] = json.loads(line)
        record["period"] = 3
        record["frameNum"] = int(record["frameNum"]) + 100_000
        extra.append(json.dumps(record))
    return ("\n".join([*lines, *extra]) + "\n").encode("utf-8")


def _events_with_known_pass() -> bytes:
    """The statsbomb events fixture plus one plain Pass at `00:00:02.250` (Ana Example, team 771)."""
    events: list[dict[str, Any]] = json.loads(read_fixture("statsbomb", "events_9100001.json"))
    template = next(event for event in events if "pass" in event)
    known_pass: dict[str, Any] = {
        key: template[key]
        for key in ("period", "possession", "possession_team", "play_pattern", "team", "player", "position")
    }
    known_pass |= {
        "id": _KNOWN_EVENT,
        "index": max(int(event["index"]) for event in events) + 1,
        "timestamp": "00:00:02.250",
        "minute": 0,
        "second": 2,
        "type": {"id": 30, "name": "Pass"},
        "location": [60.0, 40.0],
        "duration": 1.0,
        "pass": {
            "end_location": [70.0, 40.0],
            "length": 10.0,
            "angle": 0.0,
            "height": {"id": 1, "name": "Ground Pass"},
            "body_part": {"id": 40, "name": "Right Foot"},
        },
    }
    return json.dumps([*events, known_pass]).encode("utf-8")


def _new_kit(base: Path) -> LakeKit:
    roots = lake_roots(base)
    init_lake(ResolvedRoots(roots=roots, source="cli"), bootstrap_file=base / "boot.json")
    return LakeKit(base=base, roots=roots, storage=LocalStorage(roots))


def _ingest(kit: LakeKit, env: dict[str, str]) -> dict[str, list[tuple[Any, ...]]]:
    save_settings(kit.storage, [active(module) for module in _MODULES])
    adapters = fixture_adapters(
        gradientsports_tracking=_tracking_with_extra_time(), statsbomb_events=_events_with_known_pass()
    )
    result = run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={module: adapters[module] for module in _MODULES},  # type: ignore[index]
        parsers=PARSERS,
        now=lambda: NOW,
        work_dir=kit.base / "work",
        compute=DEFAULT_COMPUTE_STEPS,
    )
    assert result.status == "ok", "\n".join(f"{e.kind}: {e.message}" for e in result.errors)
    with CatalogReader(kit.storage, env=env).session() as conn:
        return {
            "ball": conn.execute(
                "SELECT canonical_match_key, period, frame, time_seconds FROM lake.main.fct_tracking_ball ORDER BY ALL"
            ).fetchall(),
            "statsbomb": conn.execute(_ACTIONS_OF, ["statsbomb"]).fetchall(),
            "skillcorner": conn.execute(_ACTIONS_OF, ["skillcorner"]).fetchall(),
        }


@pytest.fixture(scope="module")
def both(tmp_path_factory: pytest.TempPathFactory) -> dict[str, dict[str, list[tuple[Any, ...]]]]:
    plain_kit = _new_kit(tmp_path_factory.mktemp("plain"))
    plain = _ingest(plain_kit, env={})
    encrypted_kit = _new_kit(tmp_path_factory.mktemp("encrypted"))
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv(ENCRYPTION_KEY_ENV, _KEY)
        encrypted = _ingest(encrypted_kit, env={ENCRYPTION_KEY_ENV: _KEY})
    return {"plain": plain, "encrypted": encrypted}


def test_tracking_ball_time_is_equal_on_both_write_paths(both: dict[str, dict[str, list[tuple[Any, ...]]]]) -> None:
    plain, encrypted = both["plain"]["ball"], both["encrypted"]["ball"]
    assert plain and plain == encrypted
    assert any(row[1] == 3 for row in plain), "the gradientsports extra-time frames are missing"
    gs_match = {row[0] for row in plain if row[1] == 3}  # only the gradientsports match has period-3 frames
    assert len(gs_match) == 1, gs_match
    known = [row[3] for row in plain if row[0] in gs_match and row[2] in (_GS_FRAME, _GS_FRAME + 100_000)]
    assert known == [pytest.approx(_GS_SECONDS, abs=1e-9)] * 2, known  # period 1 and its period-3 copy


def test_statsbomb_spadl_time_is_equal_on_both_write_paths(both: dict[str, dict[str, list[tuple[Any, ...]]]]) -> None:
    plain, encrypted = both["plain"]["statsbomb"], both["encrypted"]["statsbomb"]
    assert plain and plain == encrypted
    # A dropped `unit=` reads int64 ms as NANOSECONDS: 2250 ms would become 2.25e-06 s.
    assert [row[2] for row in plain if row[0] == _KNOWN_EVENT] == [pytest.approx(2.25, abs=1e-9)]


def test_skillcorner_spadl_time_is_equal_on_both_write_paths(both: dict[str, dict[str, list[tuple[Any, ...]]]]) -> None:
    plain, encrypted = both["plain"]["skillcorner"], both["encrypted"]["skillcorner"]
    assert plain and plain == encrypted
    assert [row[2] for row in plain if row[0] == _SC_EVENT] == [pytest.approx(_SC_SECONDS, abs=1e-9)]

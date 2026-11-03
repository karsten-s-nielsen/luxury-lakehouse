"""The tracking benchmark harness (spec §10.9, §23.4, §26 item 13; plan Task 17).

`measure` parses one match and writes its tracking files in a child process, samples the child's resident memory
with psutil, and returns the parse time, the peak RSS and the bytes written. The child runs `parse_and_write`.
"""

from __future__ import annotations

import datetime as dt
import json
import multiprocessing
import time
from pathlib import Path
from typing import Final, Literal

import psutil

from lakehouse.bronze.writer import write_tracking_file
from lakehouse.entries import SourceEntry
from lakehouse.ids.keys import match_key
from lakehouse.parsers.idsse import TRACKING_FRAMES as IDSSE_FRAMES
from lakehouse.parsers.idsse import TRACKING_PLAYERS as IDSSE_PLAYERS
from lakehouse.parsers.idsse import IdsseParser
from lakehouse.parsers.skillcorner import TRACKING_FRAMES as SC_FRAMES
from lakehouse.parsers.skillcorner import TRACKING_PLAYERS as SC_PLAYERS
from lakehouse.parsers.skillcorner import SkillCornerParser
from lakehouse.raw.archive import Delivery

__all__ = ["BENCH_MATCHES", "BenchProvider", "measure", "parse_and_write"]

BenchProvider = Literal["idsse", "skillcorner"]
BENCH_MATCHES: Final[dict[BenchProvider, str]] = {"idsse": "J03WPY", "skillcorner": "1899585"}
_SAMPLE_S: Final = 0.05
_MODULES: Final = {"idsse": "idsse_public", "skillcorner": "skillcorner_public"}


def _entry(provider: BenchProvider, match: str) -> SourceEntry:
    return SourceEntry(
        module_id=_MODULES[provider],  # type: ignore[arg-type]
        native_match_id=match,
        competition_id=None,
        competition_name=None,
        season_id=None,
        season_name=None,
        visibility="public",
        artifacts={},
        listed_at=dt.datetime.now(dt.UTC),
    )


def parse_and_write(provider: BenchProvider, match: str, files: dict[str, str], out_dir: str, result: str) -> None:
    """Parse one match from local raw files and write its players and frames files; the timings go to `result`."""
    now = dt.datetime.now(dt.UTC)
    started = time.perf_counter()
    deliveries = {
        artifact: Delivery.received(
            provider=provider,
            channel="public",
            native_match_id=match,
            artifact=artifact,
            raw=Path(path).read_bytes(),
            visibility="public",
            change_signal=None,
            fetched_at=now,
        )
        for artifact, path in files.items()
    }
    parser = IdsseParser() if provider == "idsse" else SkillCornerParser()
    parsed = parser.parse(_entry(provider, match), deliveries, run_id="bench")
    parse_s = time.perf_counter() - started
    players, frames = (IDSSE_PLAYERS, IDSSE_FRAMES) if provider == "idsse" else (SC_PLAYERS, SC_FRAMES)
    key = match_key(provider, match)
    written = 0
    for table in (players, frames):
        path = write_tracking_file(
            table, parsed.tables[table], match_key=key, content_version="bench", out_dir=Path(out_dir)
        )
        written += path.stat().st_size
    Path(result).write_text(
        json.dumps(
            {
                "parse_s": parse_s,
                "total_s": time.perf_counter() - started,
                "mb_per_match": written / 1e6,
                "player_rows": parsed.tables[players].num_rows,
                "frame_rows": parsed.tables[frames].num_rows,
            }
        ),
        encoding="utf-8",
    )


def measure(provider: BenchProvider, match: str, files: dict[str, str], out_dir: Path) -> dict[str, float]:
    """Run `parse_and_write` in a child process and add its peak RSS in MB."""
    out_dir.mkdir(parents=True, exist_ok=True)
    result = out_dir / f"{provider}-result.json"
    result.unlink(missing_ok=True)
    child = multiprocessing.get_context("spawn").Process(
        target=parse_and_write, args=(provider, match, files, str(out_dir), str(result))
    )
    child.start()
    peak = 0
    watched = psutil.Process(child.pid)
    while child.is_alive():
        try:
            peak = max(peak, watched.memory_info().rss)
        except psutil.NoSuchProcess:
            break
        time.sleep(_SAMPLE_S)
    child.join()
    if child.exitcode != 0:
        raise RuntimeError(f"the {provider} bench child failed with exit code {child.exitcode}")
    measured: dict[str, float] = json.loads(result.read_text(encoding="utf-8"))
    measured["peak_rss_mb"] = peak / 1e6
    return measured

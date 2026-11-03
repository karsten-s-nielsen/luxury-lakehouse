"""The bench harness works on the small fixtures, so the Mac Mini run measures, not debugs (plan Task 17, Task 30)."""

from __future__ import annotations

from pathlib import Path

import pytest
from benchkit import measure

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


@pytest.mark.parametrize(
    ("provider", "match", "files", "players", "frames"),
    [
        (
            "idsse",
            "J03WPY",
            {"metadata": "idsse/matchinformation_J03WPY.xml", "positions": "idsse/positions_J03WPY_slice.xml"},
            2650,
            550,
        ),
        (
            "skillcorner",
            "1899585",
            {"match": "skillcorner/match_1899585.json", "tracking": "skillcorner/tracking_1899585_slice.jsonl"},
            None,
            182,
        ),
    ],
)
def test_measure_runs_in_a_child(
    provider: str, match: str, files: dict[str, str], players: int | None, frames: int, tmp_path: Path
) -> None:
    measured = measure(provider, match, {k: str(FIXTURES / v) for k, v in files.items()}, tmp_path)  # type: ignore[arg-type]
    assert measured["peak_rss_mb"] > 10  # a Python child with pyarrow loaded
    assert measured["parse_s"] > 0
    assert measured["mb_per_match"] > 0
    assert measured["frame_rows"] == frames
    if players is not None:
        assert measured["player_rows"] == players
    assert {p.parent.name for p in tmp_path.rglob("*.parquet")} == {
        f"{provider}_tracking_players",
        f"{provider}_tracking_frames",
    }

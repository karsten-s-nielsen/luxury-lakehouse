"""SkillCorner RM (private) multi-format ingestion — pure unit tests.

Covers the format-reader change (spec 2026-07-02-skillcorner-rm-private-format-ingestion):
artifact-manifest format detection, the events parquet reader, the tracking gzip-JSON
reader, and the metric-validity-critical frame-rate derivation.

All fixtures are SYNTHETIC — real RM data is `access_tier=restricted` and must never be
committed to this public repo (that would be the exact leak H1/ADR-064 prevents). The
synthetic fixtures mirror the *structure* verified live against RM match 1021404, not its
data.
"""

from __future__ import annotations

import gzip
import io
import json
from datetime import datetime, timezone

import pandas as pd
import pytest

from ingestion.skillcorner_common import FMT_ALEAGUE, FMT_RM, MatchInfo, resolve_artifact_plan
from ingestion.skillcorner_events import parse_events_csv, parse_events_parquet
from ingestion.skillcorner_matches import parse_match_json
from ingestion.skillcorner_tracking import derive_frame_rate, parse_tracking_gz


def _match(mid: str, artifacts: dict[str, str], *, visibility: str = "public") -> MatchInfo:
    return MatchInfo(
        id=mid,
        artifacts=artifacts,
        home="Home FC",
        away="Away FC",
        date="2023-08-12",
        updated_at=datetime(2023, 8, 13, tzinfo=timezone.utc),
        visibility=visibility,
    )


# ── Format detection (§5.1) ───────────────────────────────────────────────────


def test_resolve_artifact_plan_aleague() -> None:
    m = _match(
        "1886347",
        {
            "1886347_match": "1886347_match.json",
            "1886347_dynamic_events": "1886347_dynamic_events.csv",
            "1886347_tracking_extrapolated": "1886347_tracking_extrapolated.jsonl",
            "1886347_phases_of_play": "1886347_phases_of_play.csv",
        },
    )
    plan = resolve_artifact_plan(m)
    assert plan.fmt == FMT_ALEAGUE
    assert plan.match_key == "1886347_match"
    assert plan.events_key == "1886347_dynamic_events"
    assert plan.tracking_key == "1886347_tracking_extrapolated"


def test_resolve_artifact_plan_rm() -> None:
    m = _match(
        "1021404",
        {
            "metadata": "metadata.json",
            "events": "events.parquet",
            "tracking": "tracking.json.gz",
            "freeze_frames": "freeze_frames.parquet",
            "physical": "physical.parquet",
        },
        visibility="private",
    )
    plan = resolve_artifact_plan(m)
    assert plan.fmt == FMT_RM
    assert (plan.match_key, plan.events_key, plan.tracking_key) == ("metadata", "events", "tracking")


def test_resolve_artifact_plan_unknown_raises() -> None:
    # A manifest matching neither layout must fail loud (never silently mis-fetch).
    m = _match("999", {"something_else": "x.bin"})
    with pytest.raises(ValueError, match="Unrecognized SkillCorner artifact manifest"):
        resolve_artifact_plan(m)


# ── frame_rate derivation — the metric-validity gate (§5.3) ────────────────────


def test_derive_frame_rate_snaps_25_from_noisy_cadence() -> None:
    # 0.04004 s steps → 24.97 fps raw → must SNAP to 25 (bare equality would false-trip).
    ts = [round(i * 0.04004, 5) for i in range(200)]
    assert derive_frame_rate({1: ts}) == 25


def test_derive_frame_rate_snaps_10() -> None:
    ts = [round(i * 0.1, 5) for i in range(100)]
    assert derive_frame_rate({1: ts}) == 10


def test_derive_frame_rate_uses_within_period_deltas_only() -> None:
    # Period 2 starts long after period 1 ends; the boundary gap must NOT skew the median
    # (Δt is computed WITHIN each period, then pooled).
    p1 = [round(i * 0.04, 5) for i in range(100)]
    p2 = [round(2700.0 + i * 0.04, 5) for i in range(100)]
    assert derive_frame_rate({1: p1, 2: p2}) == 25


def test_derive_frame_rate_out_of_band_raises() -> None:
    # 0.02 s → 50 fps, not within ±5% of any allowed rate {10,25,30}.
    ts = [round(i * 0.02, 5) for i in range(100)]
    with pytest.raises(ValueError, match="not within"):
        derive_frame_rate({1: ts})


def test_derive_frame_rate_insufficient_timestamps_raises() -> None:
    with pytest.raises(ValueError, match="no within-period timestamp deltas"):
        derive_frame_rate({1: [5.0]})


def test_derive_frame_rate_never_defaults_on_empty() -> None:
    with pytest.raises(ValueError, match="no within-period timestamp deltas"):
        derive_frame_rate({})


# ── tracking gzip-JSON reader (§5.3) ───────────────────────────────────────────


def _synthetic_tracking_frames() -> list[dict]:
    """RM tracking frame structure (verified live): frame/period/timestamp/ball_data/player_data
    (+ extra ignorable keys). Synthetic data only."""
    frames = []
    for i in range(4):
        frames.append(
            {
                "frame": i,
                "period": 1,
                "timestamp": f"00:00:{i * 0.04:05.2f}",
                "ball_data": {"x": 1.0 * i, "y": -2.0, "z": 0.5, "is_detected": True},
                "possession": {"group": "home"},  # extra key — ignored
                "image_corners_projection": [],  # extra key — ignored
                "player_data": [
                    {"x": 10.0 + i, "y": 5.0, "player_id": 13899, "is_detected": True},
                    {"x": -10.0, "y": -5.0, "player_id": 24680, "is_detected": False},
                ],
            }
        )
    return frames


def test_parse_tracking_gz(tmp_path) -> None:
    path = tmp_path / "tracking.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        json.dump(_synthetic_tracking_frames(), fh)

    df = parse_tracking_gz(str(path), match_id="1021404")

    # 4 frames x 2 players = 8 narrow rows; same columns as the JSONL reader.
    assert len(df) == 8
    assert {"match_id", "period", "frame", "timestamp", "player_id", "x", "y", "is_visible"} <= set(df.columns)
    assert (df["match_id"] == "1021404").all()
    # is_detected → is_visible rename preserved.
    assert df["is_visible"].tolist() == [True, False] * 4
    # frame_rate derived from the 0.04 s cadence (25 fps), NOT the A-League default of 10.
    assert (df["frame_rate"] == 25).all()
    assert "_ingested_at" in df.columns


# ── events parquet reader (§5.2) ───────────────────────────────────────────────


def test_parse_events_parquet_matches_csv_columns_and_dtypes() -> None:
    # Same logical data via both readers must yield identical columns AND pandas dtypes — the
    # parity that makes spark.createDataFrame produce ONE schema for RM + A-League rows on the
    # shared bronze table. (Real schema is 294 cols; the reader is column-agnostic.)
    src = pd.DataFrame(
        {
            "event_id": [1, 2],
            "event_type": ["pass", "shot"],
            "player_id": [13899, 24680],
            "x_start": [50.0, 88.0],
            "penalty_area_start": [True, False],
        }
    )
    # Parquet-native nullable int would drift to Int32 vs the CSV path's int64 WITHOUT the
    # round-trip coercion — this column is the regression guard for the exact dtype risk.
    src["player_id"] = src["player_id"].astype("Int32")

    pq = io.BytesIO()
    src.to_parquet(pq)
    csv = io.StringIO()
    src.to_csv(csv, index=False)
    csv.seek(0)

    from_parquet = parse_events_parquet(pq.getvalue(), match_id="1021404")
    from_csv = parse_events_csv(csv, match_id="1886347")

    assert list(from_parquet.columns) == list(from_csv.columns)
    # dtype parity on the shared source columns (match_id/_ingested_at differ by value only).
    shared = [c for c in from_csv.columns if c not in ("match_id", "_ingested_at")]
    assert from_parquet[shared].dtypes.to_dict() == from_csv[shared].dtypes.to_dict()
    # coercion normalized the parquet-native Int32 to the CSV-inferred int64.
    assert str(from_parquet["player_id"].dtype) == "int64"
    assert (from_parquet["match_id"] == "1021404").all()
    assert from_parquet["event_type"].tolist() == ["pass", "shot"]


# ── metadata: parse_match_json reused as-is on RM structure (§5.4 regression) ───


def _synthetic_rm_metadata() -> str:
    """RM metadata.json structure verified live (stadium=dict, competition/season with id+name,
    player_role, NO match_periods). Synthetic ids only."""
    return json.dumps(
        {
            "id": 1021404,
            "date_time": "2023-08-12T19:30:00",
            "stadium": {"id": 84, "name": "Synthetic Arena"},
            "pitch_length": 105,
            "pitch_width": 68,
            "home_team": {"id": 1, "name": "Home FC", "short_name": "HOM", "acronym": "HFC"},
            "away_team": {"id": 2, "name": "Away FC", "short_name": "AWY", "acronym": "AFC"},
            "competition_edition": {
                "id": 900,
                "name": "Synthetic League 23/24",
                "competition": {"id": 10, "name": "Synthetic League"},
                "season": {"id": 20, "name": "2023/2024"},
            },
            "players": [
                {
                    "id": 1001,
                    "team_id": 1,
                    "short_name": "Player One",
                    "first_name": "Player",
                    "last_name": "One",
                    "number": 7,
                    "player_role": {"name": "Defensive Midfield", "acronym": "DM"},
                },
                {
                    "id": 1002,
                    "team_id": 2,
                    "short_name": "Player Two",
                    "first_name": "Player",
                    "last_name": "Two",
                    "number": 1,
                    "player_role": {"name": "Goalkeeper", "acronym": "GK"},
                },
            ],
        }
    )


def test_parse_match_json_handles_rm_metadata_shape() -> None:
    df = parse_match_json(_synthetic_rm_metadata(), match_id="1021404", visibility="private")
    assert len(df) == 2
    assert df["player_id"].notna().all()
    assert set(df["team_name"]) == {"Home FC", "Away FC"}
    assert df["stadium_name"].iloc[0] == "Synthetic Arena"
    # Absent match_periods degrades gracefully to an empty JSON list.
    assert df["period_boundaries"].iloc[0] == "[]"

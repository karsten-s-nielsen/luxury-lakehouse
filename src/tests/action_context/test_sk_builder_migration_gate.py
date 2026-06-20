"""TF-23 migration gate — the silly-kicks SkillCorner adapter on a committed REAL bronze slice.

This is a diff-explainer / regression guard (NOT the correctness oracle — that is
``test_frame_orientation_golden``). It runs ``convert_skillcorner_bronze_to_frames`` on the
committed real SkillCorner bronze (match 1886347, period 2) and asserts the migration-relevant
invariants achievable without a fresh data extract:

* the converted frames are home-LTR oriented (home GK defends low x),
* coordinates land in SPADL 105x68,
* the output matches the AC result-frame schema,
* the B' clock is the dispatcher's period-relative clock (== bronze timestamp here), not the
  builder's internal re-base.

**Data-extract limitations (tracked, Phase B):** the committed fixture predates the SkillCorner
``ball_z`` carry (migrated in-place to ``ball_z = NaN``), so the ``z``-population diff cannot be
shown here — it is covered on synthetic real-z bronze by ``test_sk_frame_adapters``. The fixture is
also a single period spanning one frame batch, so the multi-batch metrica clock case is covered by
``test_sk_frame_adapters.test_metrica_adapter_overwrites_clock_on_mid_period_batch`` instead. A
denser real-ball_z, multi-batch bronze extract would let this gate add the old-vs-new byte-diff.
"""

from __future__ import annotations

import pandas as pd
import pytest

from analytics.action_context.sk_frame_adapters import _AC_FRAME_COLUMNS, convert_skillcorner_bronze_to_frames

_FIXTURE = "src/tests/fixtures/action_context/skillcorner/1886347_p2"


def _load() -> tuple[pd.DataFrame, pd.Series]:
    bronze = pd.read_parquet(f"{_FIXTURE}/frames.parquet")
    meta = pd.read_parquet(f"{_FIXTURE}/meta.parquet").iloc[0]
    return bronze, meta


def test_skillcorner_adapter_on_real_bronze_orients_and_matches_schema() -> None:
    bronze, meta = _load()
    prt = bronze[["frame", "period", "timestamp"]].drop_duplicates()
    prt = prt.rename(columns={"frame": "frame_id", "period": "period_id", "timestamp": "time_seconds"})
    frames, _ = convert_skillcorner_bronze_to_frames(
        bronze, game_id=1886347, home_team_id=meta["home_team_id"], period_relative_time=prt
    )

    assert set(frames.columns) == _AC_FRAME_COLUMNS
    # Coords must be finite and in a tolerant pitch envelope. NOT strict [0,105]x[0,68): real
    # SkillCorner tracking captures players/ball slightly out of bounds (e.g. x=106.37, ~1.4m past
    # the goal line) and the silly-kicks builder faithfully preserves raw positions (no clamp). A
    # tolerant envelope still catches a gross rescale bug (e.g. StatsBomb 120x80 or center-origin).
    assert frames["x"].notna().all() and frames["y"].notna().all()
    assert frames["x"].between(-5, 110).all() and frames["y"].between(-5, 73).all()

    gk = frames[(~frames["is_ball"]) & (frames["is_goalkeeper"])]
    assert not gk.empty, "no goalkeeper rows in converted real-bronze frames"
    home_gk_x = gk[gk["team_id"].astype(str) == str(meta["home_team_id"])]["x"].median()
    assert home_gk_x < 52.5, f"home GK must defend low x after LTR (got {home_gk_x})"


def test_skillcorner_adapter_clock_is_dispatcher_period_relative() -> None:
    """B': output time_seconds equals the dispatcher clock (the bronze period-relative timestamp),
    not the silly-kicks builder's internal re-base."""
    bronze, meta = _load()
    prt = bronze[["frame", "period", "timestamp"]].drop_duplicates()
    prt = prt.rename(columns={"frame": "frame_id", "period": "period_id", "timestamp": "time_seconds"})
    frames, _ = convert_skillcorner_bronze_to_frames(
        bronze, game_id=1886347, home_team_id=meta["home_team_id"], period_relative_time=prt
    )
    expected = dict(zip(prt["frame_id"], prt["time_seconds"], strict=False))
    got = frames.groupby("frame_id")["time_seconds"].first()
    assert all(got.loc[f] == pytest.approx(expected[f]) for f in got.index), "clock != dispatcher period-relative"

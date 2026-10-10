"""Build the DEDICATED tracking-marts preflight canary fixture (ADR-087 amendment 3) — no Databricks.

Curates a self-consistent COMPLETE idsse unit from the committed public ``J03WMX_p1`` fixture and writes
it under the ``ingestion`` wheel package so the preflight's fixture Probe C
(``ingestion.tracking_marts_canary.run_fixture_probe_c``) can score it offline, in CI, and from the
installed wheel. idsse is public access-tier (ADR-064) — the distributed wheel ships no restricted data.

Why a [0, 130] s window (not the whole 300 s frame window, not a 3-action mini)
------------------------------------------------------------------------------
``J03WMX_p1`` is a windowed-frames + whole-match-actions unit (ADR-067): its frames cover timestamp
[0, 300] s (the first ~300 s), its actions cover the whole period. Slicing the ACTIONS to the frame
window makes the unit self-consistent (link-rate ~0.99) — the proven ``build_ac1_mini_golden`` pattern,
just a wider window than the 3-action mini (which no-ops 3 of the 6 marts). The single shot in [0, 300]
is at t≈102 s, so ``gk_decision`` needs the window to reach ~105 s; [0, 130] is the smallest window where
``gk_decision`` hits its stable value (2, not the [0, 110] knife-edge 1) and every other mart is ≥ 1 row,
at ~1.26 MB frames parquet (vs 2.6 MB for the full window). A sliced unit stays COMPLETE + self-consistent
because frames and their matching actions are both inside the window.

Regenerate (after an INTENTIONAL fixture change) and commit the updated parquet set::

    uv run python scripts/build_tracking_marts_canary_fixture.py
    git add src/ingestion/canary_fixture/
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

import pandas as pd

from analytics.action_context.work_unit import WorkUnit
from ingestion.tracking_marts_canary import MART_KEYS, build_inputs, score_all_marts

logger = logging.getLogger(__name__)

_SRC_ROOT = Path("src/tests/fixtures/action_context")
_SRC_UNIT = _SRC_ROOT / "idsse" / "J03WMX_p1"

_DST_ROOT = Path("src/ingestion/canary_fixture")
_PROVIDER = "idsse"
_MATCH = "J03WMXcanary"
_PERIOD = 1
_DST_UNIT = _DST_ROOT / _PROVIDER / f"{_MATCH}_p{_PERIOD}"

#: Frame-window upper bound (seconds) — see the module docstring for why 130.
_WINDOW_HI_S = 130.0
#: Action-window margin (seconds) for actor-pre-window / DAS carrier hysteresis (mirrors the mini-golden).
_ACT_MARGIN_S = 2.0


def _write_slice() -> None:
    frames = pd.read_parquet(_SRC_UNIT / "frames.parquet")
    actions = pd.read_parquet(_SRC_UNIT / "actions.parquet")

    win_frames = frames[frames["timestamp"] <= _WINDOW_HI_S].copy()
    win_actions = actions[
        (actions["period_id"] == _PERIOD)
        & (actions["time_seconds"] >= -_ACT_MARGIN_S)
        & (actions["time_seconds"] <= _WINDOW_HI_S + _ACT_MARGIN_S)
    ].copy()

    if _DST_ROOT.exists():
        shutil.rmtree(_DST_ROOT)
    _DST_UNIT.mkdir(parents=True, exist_ok=True)
    win_frames.to_parquet(_DST_UNIT / "frames.parquet", index=False)
    win_actions.to_parquet(_DST_UNIT / "actions.parquet", index=False)
    shutil.copy(_SRC_UNIT / "meta.parquet", _DST_UNIT / "meta.parquet")
    shutil.copy(_SRC_ROOT / "xt_grid.parquet", _DST_ROOT / "xt_grid.parquet")
    print(
        f"canary frames: {len(win_frames)} rows ({win_frames['frame'].nunique()} frames, "
        f"timestamp <= {_WINDOW_HI_S}s); actions: {len(win_actions)} rows (period {_PERIOD})"
    )


def _verify_non_vacuous() -> None:
    """Fail LOUD if any of the six marts no-ops on the written fixture (the R-N hard gate)."""
    wu = WorkUnit(provider=_PROVIDER, match_id=_MATCH, period=_PERIOD)
    inp = build_inputs(str(_DST_ROOT), wu)
    from analytics.action_context.local.parquet_sources import ParquetMatchMetadataSource

    meta = ParquetMatchMetadataSource(str(_DST_ROOT)).metadata(wu)
    outputs = score_all_marts(_PROVIDER, inp, inp.frames, match_id=_MATCH, meta=meta)
    counts = {m: len(outputs[m]) for m in MART_KEYS}
    empty = [m for m, n in counts.items() if n == 0]
    print(f"mart row counts: {counts}")
    if empty:
        msg = f"canary fixture is VACUOUS: mart(s) {empty} produced 0 rows — widen the window before committing"
        raise RuntimeError(msg)
    print("OK — all six marts >= 1 row")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    _write_slice()
    _verify_non_vacuous()
    print(f"Done. Canary fixture at {_DST_ROOT}")


if __name__ == "__main__":
    main()

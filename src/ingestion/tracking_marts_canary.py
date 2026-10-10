"""Bundled-fixture Probe C for the tracking-marts preflight canary (ADR-087 amendment 3).

The ADR-087 two-probe canary dry-runs the real processor before the fan-out so a systemic compute-path
defect (the ``gk_decision is_actor`` crash class) fails preflight cheaply. But a full two-stage score of
ANY real unit ``≈`` one drain unit (pitch-control / defensive-credit dominate), and every tracking
provider's smallest unit is still a dense half — so a real-unit Probe C cannot fit the 1200 s planning
budget (R5, 2026-10-10: ``--providers idsse`` alone timed out at ~1240 s, driver stack pinned the Stage-2
``scored_sdf.count()``). A sub-unit slice is not an option: the scorers are whole-unit (the global
action→frame link is not window-decomposable), so a 1 % slice leaves ~99 % of actions unlinked.

This module replaces the real-unit Probe C with a **bundled, COMPLETE, tiny idsse fixture** scored through
the six PURE scorer cores. A complete small unit (few frames + their MATCHING few actions, self-consistent,
link-rate ~0.99) exercises every scorer's real code path (schema, wiring, the ``is_actor`` class) in
seconds, corpus-independent. It drops live per-provider DATA-shape coverage (the runtime circuit-breaker is
the backstop) and the per-provider BUILD path for non-fixture providers (covered by ``test_transport_oracle``
+ the per-provider ingest tests + Probe M's real build).

SINGLE SOURCE of the build+score CODE: ``src/tests/tracking_marts/_oracle.py`` already builds a unit's
oriented ``(actions, frames, xt)`` via ``build_unit_inputs`` and runs all six scorers. That mechanics is
lifted HERE (parameterized by fixture root + unit) so the test oracle and this wheel-shipped canary share
one builder — ``_oracle`` imports it. It lives in ``ingestion`` (NOT ``analytics``): the six scorer cores
are ``ingestion.*_writer`` and ``analytics ⇏ ingestion`` (import-linter), while ``ingestion → analytics``
is allowed. Pure-pandas, no Spark; the scorer imports are function-local so the module stays importable
offline (``tracking_marts_drain``'s discipline) regardless of any transitive Spark import.

The DATA is NOT shared: this ships its OWN dedicated fixture at ``src/ingestion/canary_fixture/`` (one copy,
single consumer — no drift), curated from public idsse ``J03WMX_p1`` by
``scripts/build_tracking_marts_canary_fixture.py``. ``_oracle`` keeps its own multi-provider ``src/tests``
transport fixtures. idsse is public-tier (ADR-064), so the distributed wheel ships no restricted data.
"""

from __future__ import annotations

import importlib.resources
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from analytics.action_context.local.parquet_sources import (
    ParquetActionsSource,
    ParquetFrameSource,
    ParquetMatchMetadataSource,
    ParquetXtSource,
)
from analytics.action_context.unit_inputs import build_unit_inputs
from analytics.action_context.work_unit import WorkUnit

if TYPE_CHECKING:  # pragma: no cover - typing only
    import logging

    from analytics.action_context.unit_inputs import UnitInputs
    from analytics.action_context.work_unit import MatchMeta

#: The six tracking-marts output keys (aligned with ``tracking_marts_dispatch.make_mart_scorers``).
#: Single source — ``_oracle.MART_KEYS`` imports this.
MART_KEYS: tuple[str, ...] = (
    "off_ball_runs",
    "action_defensive",  # defensive_credit aggregate
    "defensive_credit_attributions",  # defensive_credit long
    "gk_decision",
    "rest_defense",
    "gkdv_observations",
)

#: The bundled canary fixture unit (public idsse tier). Built by
#: ``scripts/build_tracking_marts_canary_fixture.py`` as a self-consistent [0, 130] s window of
#: ``J03WMX_p1`` (~3251 frames + their 44 matching actions, link-rate ~0.99) — the smallest window whose
#: single shot (t≈102 s) keeps ``gk_decision`` at its stable value and every other mart ≥ 1 row.
_CANARY_PROVIDER = "idsse"
_CANARY_MATCH = "J03WMXcanary"
_CANARY_PERIOD = 1


def canary_fixture_root() -> str:
    """The bundled canary fixture root, resolved from the installed ``ingestion`` package.

    Works in both the editable dev tree and an installed wheel (the parquet set ships as ``ingestion``
    package-data). Read via ``importlib.resources`` — NEVER a ``src/tests`` path.
    """
    return str(importlib.resources.files("ingestion") / "canary_fixture")


def canary_work_unit() -> WorkUnit:
    """The bundled fixture's ``WorkUnit`` (idsse / J03WMXcanary / period 1)."""
    return WorkUnit(provider=_CANARY_PROVIDER, match_id=_CANARY_MATCH, period=_CANARY_PERIOD)


def build_inputs(fixture_root: str, wu: WorkUnit) -> UnitInputs:
    """Build oriented ``(actions, frames, xt)`` for one fixture unit via the driver's ``build_unit_inputs``.

    Parameterized by ``fixture_root`` + ``wu`` so BOTH the ``src/tests`` transport oracle and the
    bundled-wheel canary share this ONE builder (no drift). Pure-pandas, no Spark.
    """
    grid, xt_l, xt_w = ParquetXtSource(fixture_root).grid()
    return build_unit_inputs(
        wu,
        frame_bundle=ParquetFrameSource(fixture_root).frames(wu),
        actions_df=ParquetActionsSource(fixture_root).actions(wu),
        meta=ParquetMatchMetadataSource(fixture_root).metadata(wu),
        xt_grid_data=grid,
        xt_l=xt_l,
        xt_w=xt_w,
    )


def synthetic_xg_preds(inp: UnitInputs) -> pd.DataFrame:
    """Per-shot synthetic xG predictions (``fct_shot_xg`` is absent in fixtures). Shared by the baseline
    oracle and the dispatch-closure test so both ``attach_xg`` the identical preds."""
    import silly_kicks.spadl.config as cfg

    shot_id = cfg.actiontype_id["shot"]
    shots = inp.actions[inp.actions["type_id"] == shot_id]
    return pd.DataFrame(
        {
            "data_source": shots["data_source"].to_numpy(),
            "match_id_native": shots["match_id_native"].to_numpy(),
            "action_id": shots["action_id"].to_numpy(),
            "xg": np.full(len(shots), 0.12),
        }
    )


def actions_with_synthetic_xg(inp: UnitInputs) -> pd.DataFrame:
    """Per-shot synthetic xG via the production ``attach_xg`` LEFT-JOIN (mirrors the defensive-credit test)."""
    from ingestion.defensive_credit_writer import attach_xg

    return attach_xg(inp.actions, synthetic_xg_preds(inp))


def _access_tier(inp: UnitInputs) -> str | None:
    _at = inp.actions["access_tier"].iloc[0] if "access_tier" in inp.actions.columns else None
    return None if _at is None or (isinstance(_at, float) and _at != _at) else str(_at)


def score_all_marts(
    provider: str,
    inp: UnitInputs,
    frames: pd.DataFrame,
    *,
    match_id: str,
    meta: MatchMeta,
    include_gkdv: bool = True,
) -> dict[str, pd.DataFrame]:
    """Run the six pure scorers on a GIVEN ``frames`` (one signature source — no drift).

    Parameterized by ``match_id`` + ``meta`` (vs reading a hardcoded ``UNITS`` table) so the ``src/tests``
    oracle and the bundled-wheel canary share it. Identity columns are stamped exactly as
    ``TrackingMartsProcessor.process`` stamps. The scorer imports are function-local so importing THIS
    module stays offline-safe irrespective of any transitive Spark import in a writer module.
    """
    from ingestion.defensive_credit_writer import compute_action_defensive_credit, compute_defensive_credit_long
    from ingestion.gk_decision_writer import _bundled_completion_model, score_gk_decision_unit
    from ingestion.gkdv_writer import score_gkdv_unit
    from ingestion.off_ball_runs_writer import compute_off_ball_runs
    from ingestion.restdefense_writer import compute_rest_defense_samples

    actions_xg = actions_with_synthetic_xg(inp)
    access_tier = _access_tier(inp)

    out: dict[str, pd.DataFrame] = {
        "off_ball_runs": compute_off_ball_runs(inp.actions, frames, inp.xt),
        "action_defensive": compute_action_defensive_credit(actions_xg, frames, inp.xt),
        "defensive_credit_attributions": compute_defensive_credit_long(actions_xg, frames, inp.xt),
        "gk_decision": score_gk_decision_unit(
            inp.actions,
            frames,
            _bundled_completion_model(),
            data_source=provider,
            match_id=match_id,
            access_tier=access_tier,
        ),
        "rest_defense": compute_rest_defense_samples(inp.actions, frames, inp.xt, access_tier=access_tier),
    }
    if include_gkdv:
        obs = score_gkdv_unit(
            frames,
            meta.home_team_id,
            inp.xt,
            data_source=provider,
            match_id=match_id,
            competition_id=None,
            season_id=None,
        )
        obs = obs.copy()
        obs["match_id"] = match_id  # the processor stamps this for the per-unit replaceWhere
        out["gkdv_observations"] = obs
    return out


def score_fixture_marts(*, include_gkdv: bool = True) -> dict[str, pd.DataFrame]:
    """Build + score the BUNDLED canary fixture through all six pure scorer cores (Probe C's work).

    Reads the wheel-bundled fixture via ``importlib.resources`` — proves the package-data path works
    offline and in the installed wheel. Deterministic.
    """
    root = canary_fixture_root()
    wu = canary_work_unit()
    inp = build_inputs(root, wu)
    meta = ParquetMatchMetadataSource(root).metadata(wu)
    return score_all_marts(
        _CANARY_PROVIDER, inp, inp.frames, match_id=_CANARY_MATCH, meta=meta, include_gkdv=include_gkdv
    )


def run_fixture_probe_c(logger: logging.Logger) -> None:
    """Probe C (tracking-marts): the bundled-fixture pure-core smoke.

    Runs the six scorers on the bundled complete idsse fixture and asserts each mart is produced AND has
    **≥ 1 row** (R3 — NON-EMPTY is the bar, not merely non-crash: a 0-row mart with a valid schema would
    let a scorer defect ship past the preflight, the vacuous-green the canary exists to prevent). Raises a
    ``canary FAILED`` ``RuntimeError`` — naming the offending mart — on a scorer crash OR an empty mart, so
    ``main_tracking_marts_preflight`` aborts before the enqueue / fan-out (the dependent ``compute_*`` task
    is skipped). A legitimately-empty mart on the fixture is a fixture-curation failure, not a pass.
    """
    try:
        outputs = score_fixture_marts()
    except Exception as exc:
        logger.error("drain_canary_failed probe=C(fixture) err=%s", exc, exc_info=True)
        raise RuntimeError(f"tracking-marts drain canary FAILED (fixture correctness): {exc}") from exc

    missing = [m for m in MART_KEYS if m not in outputs]
    empty = [m for m in MART_KEYS if m in outputs and len(outputs[m]) == 0]
    if missing or empty:
        raise RuntimeError(
            "tracking-marts drain canary FAILED (fixture correctness): "
            + (f"scorer(s) missing from output: {missing}; " if missing else "")
            + (f"mart(s) produced 0 rows: {empty}; " if empty else "")
            + "the bundled canary fixture must yield >= 1 row for every mart "
            "(regenerate via scripts/build_tracking_marts_canary_fixture.py)."
        )
    logger.info(
        "drain_canary_ok probe=C(fixture) marts=%d rows=%s",
        len(outputs),
        {m: len(outputs[m]) for m in MART_KEYS},
    )


__all__ = [
    "MART_KEYS",
    "actions_with_synthetic_xg",
    "build_inputs",
    "canary_fixture_root",
    "canary_work_unit",
    "run_fixture_probe_c",
    "score_all_marts",
    "score_fixture_marts",
    "synthetic_xg_preds",
]

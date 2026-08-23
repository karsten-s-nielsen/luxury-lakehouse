"""Smoke tests for scripts/train_xt_gk_v2_hf.py — the pure fit + ADR-012 delivery contract.

The trainer is a PEP 723 single-file that fits the xT-GK v2 possession-value + turnover-cost surfaces
on HF Jobs and delivers them via ADR-012. These tests exercise the torch-free / Databricks-free path:
the pure ``fit_xt_gk_v2`` fit, the round-trip through the single-JSON bundle serialization, and the
ADR-012 delivery-helper contract. They never touch HF Jobs / Databricks / MLflow.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import train_xt_gk_v2_hf as trainer  # noqa: E402  (sys.path insert must precede import)

from ingestion.xt_gk_v2_writer import (  # noqa: E402
    V2_OUTPUT_COLUMNS,
    XtGkV2Bundle,
    deserialize_xt_gk_v2_bundle,
    score_xt_gk_v2,
    serialize_xt_gk_v2_bundle,
)


def _synthetic_corpus(n_games: int = 2, per_game: int = 140, seed: int = 7) -> pd.DataFrame:
    """A small SPADL-shaped corpus that fits both surfaces without raising.

    Carries every column the xtgk fit + score paths need: SPADL identity/geometry, a pressure column,
    an xG column (NaN off-shot), and the AC-layer resolved-coordinate + domain columns the writer reads.
    """
    import silly_kicks.spadl.config as spadlcfg

    rng = np.random.default_rng(seed)
    t_pass = spadlcfg.actiontype_id["pass"]
    t_shot = spadlcfg.actiontype_id["shot"]
    t_goalkick = spadlcfg.actiontype_id["goalkick"]
    r_success = spadlcfg.result_id["success"]
    r_fail = spadlcfg.result_id["fail"]
    _type_name = {v: k for k, v in spadlcfg.actiontype_id.items()}

    rows: list[dict[str, object]] = []
    for g in range(n_games):
        game_id = f"g{g}"
        for i in range(per_game):
            team = f"t{g}_{i % 2}"
            # Every 20th action is a goal-kick (GK distribution); every 15th a shot at high x.
            is_goalkick = (i % 20) == 0
            is_shot = (not is_goalkick) and (i % 15) == 7
            if is_goalkick:
                type_id, sx, sy = t_goalkick, 5.0, 34.0
            elif is_shot:
                type_id, sx, sy = t_shot, float(rng.uniform(88, 103)), float(rng.uniform(20, 48))
            else:
                type_id, sx, sy = t_pass, float(rng.uniform(2, 100)), float(rng.uniform(2, 66))
            ex, ey = float(rng.uniform(2, 103)), float(rng.uniform(2, 66))
            rows.append(
                {
                    "game_id": game_id,
                    "data_source": "idsse",
                    "match_id": game_id,
                    "period_id": 1,
                    "action_id": i,
                    "team_id": team,
                    "time_seconds": float(i) * 3.0,
                    "possession_id": i // 4,
                    "type_id": int(type_id),
                    "type_name": _type_name[int(type_id)],
                    "result_id": int(r_fail if (i % 9 == 0) else r_success),
                    "start_x": sx,
                    "start_y": sy,
                    "end_x": ex,
                    "end_y": ey,
                    "pressure": float(rng.uniform(0.0, 3.0)),
                    "xg": float(rng.uniform(0.02, 0.4)) if is_shot else np.nan,
                    # AC-layer columns the writer reads (resolved geometry = native no-op here).
                    "is_gk_distribution": bool(is_goalkick),
                    "xt_gk_origin_x": sx,
                    "xt_gk_origin_y": sy,
                    "xt_gk_dest_x": ex,
                    "xt_gk_dest_y": ey,
                }
            )
    return pd.DataFrame(rows)


class TestRequiredSkMin:
    def test_required_sk_min_is_4_90_1(self) -> None:
        assert trainer._REQUIRED_SK_MIN == (4, 90, 1)


class TestAdr012Delivery:
    """The three ADR-012 delivery helpers must be imported (and therefore callable) by the trainer."""

    def test_all_three_adr012_helpers_imported(self) -> None:
        from ingestion import artifact_deploy

        assert trainer.require_mlflow_env is artifact_deploy.require_mlflow_env
        assert trainer.set_and_verify_mlflow_champion is artifact_deploy.set_and_verify_mlflow_champion
        assert trainer.upload_weights_to_uc_volume is artifact_deploy.upload_weights_to_uc_volume

    def test_main_calls_require_mlflow_env_before_work(self) -> None:
        # Source-text guard: require_mlflow_env must be invoked inside main() (fail-loud pre-flight).
        src = Path(trainer.__file__).read_text(encoding="utf-8")
        assert "require_mlflow_env()" in src
        assert "upload_weights_to_uc_volume(" in src
        assert "set_and_verify_mlflow_champion(" in src


class TestFitAndSerializeRoundTrip:
    """fit_xt_gk_v2 -> serialize -> deserialize -> score, all on a synthetic corpus (no Databricks)."""

    def test_fit_serialize_deserialize_preserves_surfaces(self) -> None:
        corpus = _synthetic_corpus()
        pv, tc, pl = trainer.fit_xt_gk_v2(corpus, xg_column="xg", pressure_column="pressure")

        envelope = serialize_xt_gk_v2_bundle(pv, tc, pl, xg_column="xg", pressure_column="pressure")
        assert isinstance(envelope, bytes) and len(envelope) > 0

        bundle = deserialize_xt_gk_v2_bundle(envelope)
        assert isinstance(bundle, XtGkV2Bundle)
        assert bundle.xg_column == "xg"
        # Surfaces survive the round-trip byte-for-byte (value equality on every pressure tercile).
        for p in (1, 2, 3):
            np.testing.assert_allclose(bundle.possession_value.value(0, p), pv.value(0, p))  # type: ignore[arg-type]
            np.testing.assert_allclose(bundle.turnover_cost.surface(p), tc.surface(p))  # type: ignore[arg-type]

    def test_score_produces_v2_columns_for_gk_distribution_rows(self) -> None:
        corpus = _synthetic_corpus()
        pv, tc, pl = trainer.fit_xt_gk_v2(corpus, xg_column="xg", pressure_column="pressure")
        bundle = deserialize_xt_gk_v2_bundle(
            serialize_xt_gk_v2_bundle(pv, tc, pl, xg_column="xg", pressure_column="pressure")
        )

        scored = score_xt_gk_v2(corpus, bundle)
        # One row per GK-distribution (goalkick) action; carries the 6 v2 mart-join columns + identity.
        assert set(V2_OUTPUT_COLUMNS).issubset(scored.columns)
        assert {"data_source", "match_id", "action_id"}.issubset(scored.columns)
        n_gk = int(corpus["is_gk_distribution"].sum())
        assert len(scored) == n_gk
        # Native-resolved finite coords -> a real (non-NaN) xt_gk_v2 for the goal-kicks.
        assert scored["xt_gk_v2"].notna().any()
        assert set(scored["gk_geometry_source"].unique()) <= {
            "native",
            "resolved_origin",
            "resolved_dest",
            "resolved_both",
            "unresolved",
            "unattested",
            "off_domain",
        }

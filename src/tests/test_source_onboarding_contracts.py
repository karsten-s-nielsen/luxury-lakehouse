"""S6 — per-source onboarding invariant tests.

Validates that every SPADL source satisfies the cross-source contracts
established in PR-LL3. Fixture-based (offline CI, no Databricks).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "silly_kicks_boundary"


@pytest.mark.parametrize(
    "source,fixture",
    [
        ("statsbomb", "sb_match_7298.parquet"),
        ("wyscout", "ws_match_2576335.parquet"),
        ("idsse", "idsse_J03WMX.parquet"),
        ("metrica", "metrica_sample_game_1.parquet"),
        ("skillcorner", "sc_match_1886347.parquet"),
        ("gradientsports", "gs_match_10502.parquet"),
    ],
)
class TestSourceOnboardingContracts:
    """Per-source invariants that must hold for any SPADL source."""

    def test_native_id_columns_present(self, source: str, fixture: str) -> None:
        """SPADL output must include all 6 native ID columns."""
        from ingestion.spadl_conversion import (
            _make_gradientsports_spadl_udf,
            _make_idsse_spadl_udf,
            _make_metrica_spadl_udf,
            _make_sb_spadl_udf,
            _make_skillcorner_spadl_udf,
            _make_ws_spadl_udf,
        )

        if source == "skillcorner":
            match_metadata = {"id": "1886347", "pitch_length": 105, "pitch_width": 68, "home_team": {"id": 4177}}
            udf = _make_skillcorner_spadl_udf(match_metadata=match_metadata)
        elif source == "gradientsports":
            udf = _make_gradientsports_spadl_udf(gs_comp_season=None)
        else:
            udf_map = {
                "statsbomb": _make_sb_spadl_udf,
                "wyscout": _make_ws_spadl_udf,
                "idsse": _make_idsse_spadl_udf,
                "metrica": _make_metrica_spadl_udf,
            }
            udf = udf_map[source]()

        empty_result = udf(pd.DataFrame())
        native_cols = {
            "team_id_native",
            "home_team_id_native",
            "competition_native_id",
            "season_native_id",
            "match_id_native",
            "player_id_native",
        }
        assert native_cols.issubset(set(empty_result.columns)), (
            f"{source}: missing native ID columns: {native_cols - set(empty_result.columns)}"
        )

    def test_native_id_format_contract(self, source: str, fixture: str) -> None:
        """native player_id values follow the format contract in identifiers.py."""
        import silly_kicks.spadl.metrica
        import silly_kicks.spadl.skillcorner
        import silly_kicks.spadl.sportec
        import silly_kicks.spadl.statsbomb
        import silly_kicks.spadl.wyscout

        from shared.identifiers import (
            gradientsports_native_player_id,
            idsse_native_player_id,
            metrica_native_player_id,
            skillcorner_native_player_id,
            statsbomb_native_player_id,
            wyscout_native_player_id,
        )

        df = pd.read_parquet(_FIXTURE_DIR / fixture)

        if source == "statsbomb":
            from ingestion.spadl_adapter import adapt_statsbomb_events

            home_team_id = int(df["team_id"].dropna().iloc[0])
            adapted = adapt_statsbomb_events(df, home_team_id)
            actions, _ = silly_kicks.spadl.statsbomb.convert_to_actions(adapted, home_team_id)
        elif source == "wyscout":
            from ingestion.spadl_adapter import adapt_wyscout_events

            home_team_id = int(df["teamId"].dropna().iloc[0])
            adapted = adapt_wyscout_events(df)
            actions, _ = silly_kicks.spadl.wyscout.convert_to_actions(adapted, home_team_id)
        elif source == "idsse":
            # IDSSE shaper + deriver moved to the silly-kicks DFL parse port
            # under delete-and-depend (ADR-031 T3 / Gate B).
            from silly_kicks.providers.sportec import (
                derive_idsse_home_team_start_left,
                shape_events_to_native,
            )

            df = df[~((df["period"] == 2) & (df["timestamp_seconds"] < 0))].reset_index(drop=True)
            adapted = shape_events_to_native(df)
            htid = str(df["home_team_id_native"].dropna().iloc[0])
            hsl = derive_idsse_home_team_start_left(adapted, htid)
            actions, _ = silly_kicks.spadl.sportec.convert_to_actions(
                adapted,
                home_team_id="home",
                home_team_start_left=hsl,
            )
        elif source == "skillcorner":
            import json

            meta_path = _FIXTURE_DIR / "sc_match_1886347_meta.json"
            with open(meta_path) as f:
                match_metadata = json.load(f)
            actions, _ = silly_kicks.spadl.skillcorner.convert_to_actions(df, match_metadata)
        elif source == "gradientsports":
            import silly_kicks.spadl.gradientsports

            from ingestion.spadl_adapter import (
                adapt_gradientsports_events,
                extract_gradientsports_match_metadata,
            )

            metadata = extract_gradientsports_match_metadata(df)
            adapted = adapt_gradientsports_events(df)
            actions, _ = silly_kicks.spadl.gradientsports.convert_to_actions(
                adapted,
                home_team_id=metadata["home_team_id"],
                home_team_start_left=metadata["home_team_start_left"],
                home_team_start_left_extratime=metadata["home_team_start_left_extratime"],
            )
        else:  # metrica
            from ingestion.spadl_adapter import (
                adapt_metrica_events_for_silly_kicks,
                derive_metrica_home_team_start_left,
            )

            adapted = adapt_metrica_events_for_silly_kicks(df)
            hsl = derive_metrica_home_team_start_left(adapted, home_team_value="Home")
            actions, _ = silly_kicks.spadl.metrica.convert_to_actions(
                adapted,
                home_team_id="Home",
                home_team_start_left=hsl,
            )

        validators = {
            "statsbomb": lambda v: statsbomb_native_player_id(int(v)),
            "wyscout": lambda v: wyscout_native_player_id(int(v)),
            "idsse": lambda v: idsse_native_player_id(str(v)),
            "metrica": lambda v: metrica_native_player_id(str(v)),
            "skillcorner": lambda v: skillcorner_native_player_id(int(v)),
            "gradientsports": lambda v: gradientsports_native_player_id(int(v)),
        }

        non_null = actions["player_id"].dropna()
        if len(non_null) == 0:
            pytest.skip(f"{source}: no non-null player_id in fixture")

        validator = validators[source]
        for val in non_null.head(10):
            validator(val)

    def test_spadl_schema_parity(self, source: str, fixture: str) -> None:
        """Output column set matches the canonical _SPADL_SCHEMA DDL."""
        from ingestion.spadl_conversion import (
            _make_gradientsports_spadl_udf,
            _make_idsse_spadl_udf,
            _make_metrica_spadl_udf,
            _make_sb_spadl_udf,
            _make_skillcorner_spadl_udf,
            _make_ws_spadl_udf,
        )

        if source == "skillcorner":
            match_metadata = {"id": "1886347", "pitch_length": 105, "pitch_width": 68, "home_team": {"id": 4177}}
            udf = _make_skillcorner_spadl_udf(match_metadata=match_metadata)
        elif source == "gradientsports":
            udf = _make_gradientsports_spadl_udf(gs_comp_season=None)
        else:
            udf_map = {
                "statsbomb": _make_sb_spadl_udf,
                "wyscout": _make_ws_spadl_udf,
                "idsse": _make_idsse_spadl_udf,
                "metrica": _make_metrica_spadl_udf,
            }
            udf = udf_map[source]()

        empty_result = udf(pd.DataFrame())
        udf_cols = set(empty_result.columns)

        from ingestion.spadl_vaep import _SPADL_SCHEMA
        from tests.test_spadl_vaep_writer_parity import _parse_ddl

        ddl_cols = set(_parse_ddl(_SPADL_SCHEMA).keys())

        # UDF cols are a subset of DDL (DDL also has _ingested_at)
        missing_from_udf = ddl_cols - udf_cols - {"_ingested_at"}
        assert not missing_from_udf, f"{source}: UDF output missing DDL columns: {missing_from_udf}"

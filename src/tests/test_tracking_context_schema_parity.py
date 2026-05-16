"""TC-1 — Bronze DDL ↔ UDF output column-set AND type parity test.

Same pattern as test_spadl_vaep_writer_parity.py: parse the DDL string,
compare columns against the StructType used by applyInPandas.

Additionally validates that the pandas dtypes produced by _enrich_match are
Arrow-serializable to the declared DDL types. This catches the class of bug
where a column like defending_gk_player_id is declared DOUBLE in the DDL but
contains string player IDs at runtime — only exploding at the Spark→Arrow
boundary during applyInPandas serialization.
"""

from __future__ import annotations

import re

import numpy as np
import pandas as pd
import pytest

_DDL_TYPE_TO_SPARK_NAME = {
    "STRING": "string",
    "BIGINT": "bigint",
    "INT": "int",
    "DOUBLE": "double",
    "FLOAT": "float",
    "TIMESTAMP": "timestamp",
    "BOOLEAN": "boolean",
}

# pandas dtypes that are Arrow-compatible with each Spark/DDL type.
# Key = DDL type name, Value = set of acceptable pandas dtype kinds/names.
_ARROW_COMPATIBLE_DTYPES: dict[str, set[str]] = {
    "STRING": {"object", "string", "str"},
    "BIGINT": {"int64", "Int64", "float64", "Float64", "object"},
    "INT": {"int32", "int64", "Int32", "Int64", "float64", "Float64", "object"},
    "DOUBLE": {"float64", "Float64", "float32", "int64", "Int64"},
    "FLOAT": {"float32", "float64", "Float64"},
    "BOOLEAN": {"bool", "boolean", "object"},
    "TIMESTAMP": {"datetime64[ns]", "object"},
}


def _parse_ddl(ddl: str) -> dict[str, str]:
    """Return {col_name: ddl_type} from a CREATE-TABLE-style DDL."""
    out: dict[str, str] = {}
    for raw in ddl.split(","):
        tok = raw.strip()
        if not tok:
            continue
        m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s+([A-Z]+)\b", tok)
        if not m:
            raise AssertionError(f"unparseable DDL fragment: {tok!r}")
        col, ddl_type = m.group(1), m.group(2)
        if ddl_type not in _DDL_TYPE_TO_SPARK_NAME:
            raise AssertionError(f"unknown DDL type {ddl_type!r} for column {col!r}")
        out[col] = ddl_type
    return out


def _is_arrow_compatible(series: pd.Series, ddl_type: str) -> bool:  # type: ignore[type-arg]
    """Check if a pandas Series can be Arrow-serialized to the given DDL type.

    Goes beyond dtype name matching: also checks actual values for object-typed
    Series (which can contain strings, ints, or mixed types).
    """
    dtype_str = str(series.dtype)
    compatible = _ARROW_COMPATIBLE_DTYPES.get(ddl_type, set())

    if dtype_str in compatible:
        return True

    # object dtype needs value-level inspection: it's Arrow-compatible with
    # STRING but NOT with DOUBLE/BIGINT if it contains non-numeric strings.
    if dtype_str == "object":
        if ddl_type == "STRING":
            return True
        if ddl_type in ("DOUBLE", "FLOAT"):
            # All non-null values must be numeric (int or float)
            non_null = series.dropna()
            if non_null.empty:
                return True
            return all(isinstance(v, (int, float, np.integer, np.floating)) for v in non_null)
        if ddl_type in ("BIGINT", "INT"):
            non_null = series.dropna()
            if non_null.empty:
                return True
            return all(isinstance(v, (int, np.integer)) for v in non_null)
        if ddl_type == "BOOLEAN":
            non_null = series.dropna()
            if non_null.empty:
                return True
            return all(isinstance(v, (bool, np.bool_)) for v in non_null)

    return False


def _make_string_id_actions(n: int = 20) -> pd.DataFrame:
    """Synthetic SPADL actions with STRING player/team IDs (DFL-native format).

    This reproduces the runtime conditions for IDSSE where player_id contains
    native DFL strings like 'DFL-OBJ-0002HE'. The enrichment test fixtures use
    integer IDs which masks the type mismatch bug.
    """
    rng = np.random.default_rng(42)
    team_ids_native = rng.choice(["DFL-CLU-000001", "DFL-CLU-000002"], n)
    player_ids_native = [f"DFL-OBJ-{i:06X}" for i in rng.choice(range(1, 23), n)]
    return pd.DataFrame(
        {
            "game_id": [1] * n,
            "action_id": list(range(n)),
            "period_id": [1] * n,
            "time_seconds": np.linspace(0, 90 * 60, n),
            "team_id": team_ids_native,
            "player_id": player_ids_native,
            "team_id_native": team_ids_native,
            "player_id_native": player_ids_native,
            "type_id": rng.choice([0, 1, 2, 3], n),
            "result_id": rng.choice([0, 1], n),
            "bodypart_id": [0] * n,
            "start_x": rng.uniform(0, 105, n),
            "start_y": rng.uniform(0, 68, n),
            "end_x": rng.uniform(0, 105, n),
            "end_y": rng.uniform(0, 68, n),
            "original_event_id": [f"evt_{i}" for i in range(n)],
        }
    )


def _make_string_id_frames(n_frames: int = 100) -> pd.DataFrame:
    """Tracking frames with STRING player/team IDs matching DFL-native format."""
    rng = np.random.default_rng(42)
    rows = []
    player_ids = [f"DFL-OBJ-{i:06X}" for i in range(1, 23)]
    for f in range(n_frames):
        t = f * 0.04
        for idx, pid in enumerate(player_ids):
            team = "DFL-CLU-000001" if idx < 11 else "DFL-CLU-000002"
            rows.append(
                {
                    "game_id": 1,
                    "frame_id": f,
                    "period_id": 1,
                    "time_seconds": t,
                    "player_id": pid,
                    "team_id": team,
                    "x": rng.uniform(0, 105),
                    "y": rng.uniform(0, 68),
                    "vx": rng.uniform(-5, 5),
                    "vy": rng.uniform(-5, 5),
                    "is_goalkeeper": idx in (0, 11),
                    "is_ball": False,
                }
            )
        rows.append(
            {
                "game_id": 1,
                "frame_id": f,
                "period_id": 1,
                "time_seconds": t,
                "player_id": None,
                "team_id": None,
                "x": rng.uniform(0, 105),
                "y": rng.uniform(0, 68),
                "vx": rng.uniform(-5, 5),
                "vy": rng.uniform(-5, 5),
                "is_goalkeeper": False,
                "is_ball": True,
            }
        )
    df = pd.DataFrame(rows)
    df["source_provider"] = "sportec"
    df["is_goalkeeper_source"] = "native"
    df["frame_rate"] = 25.0
    df["z"] = np.nan
    df["speed"] = np.sqrt(df["vx"] ** 2 + df["vy"] ** 2)
    df["speed_source"] = "derived"
    df["ball_state"] = "alive"
    df["team_attacking_direction"] = None
    df["confidence"] = None
    df["visibility"] = None
    return df


class TestTrackingContextSchemaParity:
    """Bronze DDL constant must match UDF output schema."""

    def test_ddl_columns_match_result_columns(self) -> None:
        from ingestion.tracking_context import _RESULT_COLUMNS, _TRACKING_CONTEXT_DDL

        ddl_cols = set(_parse_ddl(_TRACKING_CONTEXT_DDL).keys())
        result_cols = set(_RESULT_COLUMNS)
        assert ddl_cols == result_cols, (
            f"DDL vs _RESULT_COLUMNS mismatch.\n"
            f"  In DDL only: {ddl_cols - result_cols}\n"
            f"  In _RESULT_COLUMNS only: {result_cols - ddl_cols}"
        )

    def test_ddl_has_no_duplicates(self) -> None:
        from ingestion.tracking_context import _TRACKING_CONTEXT_DDL

        cols = [tok.strip().split()[0] for tok in _TRACKING_CONTEXT_DDL.split(",") if tok.strip()]
        seen: set[str] = set()
        dupes = [c for c in cols if c in seen or seen.add(c)]  # type: ignore[func-returns-value]
        assert not dupes, f"Duplicate columns in DDL: {dupes}"

    def test_column_count(self) -> None:
        from ingestion.tracking_context import _RESULT_COLUMNS

        assert len(_RESULT_COLUMNS) == 83, f"Expected 83 columns, got {len(_RESULT_COLUMNS)}"


class TestTrackingContextDtypeParity:
    """Verify pandas output dtypes are Arrow-serializable to the declared DDL types.

    Reproduces the runtime conditions (string player IDs) that caused the
    defending_gk_player_id DOUBLE vs object mismatch which crashed all 18
    iterations of compute_tracking_context on 2026-05-16.
    """

    @pytest.fixture
    def enrichment_result(self) -> pd.DataFrame:
        """Run _enrich_match with string-ID fixtures (IDSSE-like)."""
        pytest.importorskip("silly_kicks")
        from silly_kicks.xthreat import ExpectedThreat

        from ingestion.tracking_context import _enrich_match

        actions = _make_string_id_actions()
        frames = _make_string_id_frames()

        xt = ExpectedThreat(l=16, w=12)
        xt.fit(actions)

        return _enrich_match(
            actions=actions,
            frames=frames,
            xt=xt,
            home_team_id="DFL-CLU-000001",
            match_id_native="J03TEST",
            data_source="idsse",
        )

    def test_all_columns_arrow_compatible_with_ddl(self, enrichment_result: pd.DataFrame) -> None:
        """Every column produced by _enrich_match must be serializable to its DDL type."""
        from ingestion.tracking_context import _TRACKING_CONTEXT_DDL

        ddl_types = _parse_ddl(_TRACKING_CONTEXT_DDL)
        failures: list[str] = []

        for col_name, ddl_type in ddl_types.items():
            if col_name == "_ingested_at":
                continue
            if col_name not in enrichment_result.columns:
                continue

            series = enrichment_result[col_name]
            if not _is_arrow_compatible(series, ddl_type):
                non_null_vals = series.dropna().head(3).tolist()
                failures.append(f"  {col_name}: dtype={series.dtype}, DDL={ddl_type}, sample_values={non_null_vals}")

        assert not failures, (
            "Arrow serialization would fail for these columns "
            "(pandas dtype incompatible with DDL type):\n" + "\n".join(failures)
        )

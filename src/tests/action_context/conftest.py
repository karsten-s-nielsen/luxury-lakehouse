from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

_FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "action_context"
_ANCHOR = _FIXTURE_DIR / "idsse" / "J03WMX_p1"


@pytest.fixture(scope="session")
def anchor_dir() -> Path:
    return _ANCHOR


@pytest.fixture(scope="session")
def golden_df() -> pd.DataFrame:
    return pd.read_parquet(_ANCHOR / "golden.parquet")


@pytest.fixture(scope="session")
def oracle_tracking_context() -> pd.DataFrame:
    return pd.read_parquet(_ANCHOR / "oracle_fct_tracking_context.parquet")


@pytest.fixture(scope="session")
def oracle_pausa() -> pd.DataFrame:
    return pd.read_parquet(_ANCHOR / "oracle_fct_pausa_values.parquet")


# NOTE: oracle_elastic_sync_results.parquet is retained as committed evidence of the legacy
# IDSSE frame-origin bug (see oracle_map.py docstring) but is NOT loaded as a fixture: it is
# not a valid oracle, so elastic_* columns are INVARIANT_ONLY range-checked instead.

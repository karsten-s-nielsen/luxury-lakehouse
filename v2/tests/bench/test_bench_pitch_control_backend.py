"""numba vs numpy pitch control on a REAL match (perf spec §7.1, plan Task 2 Step 6). Mac Mini only.

Markers `bench` and `heavy`. It opens the PUBLISHED catalog read-only (the lake roots resolve as for any `lakehouse`
command), builds the largest gradientsports match, scores it under both backends, records the largest absolute
`control_fraction` difference in `benchmark.extra_info`, and asserts it is within `BACKEND_TOLERANCE`.
"""

# pandas/silly_kicks are stub-less; relax the strict "unknown"-family rules (see src/lakehouse/compute/frames.py).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, cast

import pytest
from silly_kicks.tracking.pitch_control import (  # pyright: ignore[reportMissingTypeStubs]
    _spearman,  # pyright: ignore[reportPrivateUsage] - the backend flag; pinned silly-kicks 4.139.1
)

from lakehouse.catalog.reader import CatalogReader
from lakehouse.compute.backend import BACKEND_TOLERANCE, numba_active
from lakehouse.compute.frames import build_match_frame
from lakehouse.compute.input_key import match_record_keys
from lakehouse.compute.pitch_control import pitch_control_values
from lakehouse.storage.roots import default_bootstrap_file, open_storage, resolve_roots

if TYPE_CHECKING:
    from lakehouse.catalog.working import WorkingCatalog

pytestmark = [pytest.mark.bench, pytest.mark.heavy]

_KEYS = ["canonical_match_key", "period", "frame", "canonical_team_key"]


class _ReadOnlyCatalog:
    def __init__(self, conn: Any) -> None:
        self.conn = conn


def test_bench_backend_tolerance_real_match(benchmark: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    assert numba_active(), "numba is not active in this environment (perf spec §7.1)"
    env = os.environ
    resolved = resolve_roots(cli_lake=None, cli_restricted=None, env=env, bootstrap_file=default_bootstrap_file(env))
    reader = CatalogReader(open_storage(resolved, env))
    with reader.session() as conn:
        row = conn.execute(
            "SELECT canonical_match_key FROM lake.main.fct_tracking_frames WHERE provider = 'gradientsports' "
            "GROUP BY canonical_match_key ORDER BY count(*) DESC LIMIT 1"
        ).fetchone()
        assert row is not None, "no gradientsports tracking in the published catalog"
        match_key = int(row[0])
        cat = cast("WorkingCatalog", _ReadOnlyCatalog(conn))
        record_keys = match_record_keys(conn, match_key)
        frame = build_match_frame(cat, match_key, provider="gradientsports", record_keys=record_keys)
        home_row = conn.execute(
            "SELECT home_team_key FROM lake.main.fct_match_params WHERE canonical_match_key = ?", [match_key]
        ).fetchone()
    assert home_row is not None
    home = int(home_row[0])
    teams = {int(t) for t in frame.loc[~frame["is_ball"], "team_id"].dropna().unique()}
    away = next(t for t in teams if t != home)

    compiled: Any = pitch_control_values(frame, home_team_id=home, away_team_id=away).to_pandas()
    monkeypatch.setattr(_spearman, "_HAS_NUMBA", False)
    fallback: Any = pitch_control_values(frame, home_team_id=home, away_team_id=away).to_pandas()

    compiled = compiled.sort_values(_KEYS).reset_index(drop=True)
    fallback = fallback.sort_values(_KEYS).reset_index(drop=True)
    max_abs_diff = float((compiled["control_fraction"] - fallback["control_fraction"]).abs().max())
    benchmark.extra_info.update({"match_key": match_key, "rows": len(compiled), "max_abs_diff": max_abs_diff})
    benchmark.pedantic(lambda: None, rounds=1, iterations=1)  # the timed work ran above; its numbers are extra_info
    assert compiled[_KEYS].equals(fallback[_KEYS])
    assert max_abs_diff <= BACKEND_TOLERANCE

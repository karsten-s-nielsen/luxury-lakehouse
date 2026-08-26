"""Task 7 — ``ingestion.tracking_marts_drain._pool_gkdv``: the single-driver gkdv pooling reduce.

gkdv pooling is cross-game (a keeper's ``n_games`` spans every match), so it CANNOT run per-unit in the
drain — the drain writes per-frame observations to ``bronze.gkdv_observations`` and this reduce pools them
into ``gkdv_keeper_pooled`` afterwards. Mirrors ``gkdv_writer.run_pipeline``'s end-stage write loop.

No Spark: the observation read is stubbed, ``pool_keepers`` is stubbed, and the writes are captured.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest


class _FakeTable:
    def __init__(self, pdf: pd.DataFrame) -> None:
        self._pdf = pdf

    def toPandas(self) -> pd.DataFrame:  # noqa: N802 — pyspark API name
        return self._pdf


class _FakeSpark:
    def __init__(self, obs_pdf: pd.DataFrame) -> None:
        self._obs = obs_pdf
        self.tables_read: list[str] = []

    def table(self, name: str) -> _FakeTable:
        self.tables_read.append(name)
        return _FakeTable(self._obs)

    def createDataFrame(self, pdf: pd.DataFrame, schema: Any = None) -> tuple[str, pd.DataFrame, Any]:  # noqa: N802
        return ("SDF", pdf, schema)


def test_pool_gkdv_reads_observations_pools_and_writes_per_provider_replacewhere(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ingestion.gkdv_writer as gkw
    import ingestion.utils as utils
    from ingestion.tracking_marts_drain import _pool_gkdv

    obs_pdf = pd.DataFrame({"player_id": ["gk1", "gk2"], "data_source": ["idsse", "skillcorner"]})
    spark = _FakeSpark(obs_pdf)

    # Stub the whole-corpus pool: 2 idsse keeper rows + 1 skillcorner, none for metrica/gradientsports.
    pooled = pd.DataFrame(
        {
            "data_source": ["idsse", "idsse", "skillcorner"],
            "player_id": ["a", "b", "c"],
        }
    )
    seen_pool_args: dict[str, Any] = {}

    def _fake_pool_keepers(observations: pd.DataFrame, *, min_nonzero: int, min_games: int, want_threat: bool):
        seen_pool_args["observations"] = observations
        seen_pool_args["min_nonzero"] = min_nonzero
        seen_pool_args["min_games"] = min_games
        seen_pool_args["want_threat"] = want_threat
        return pooled

    monkeypatch.setattr(gkw, "pool_keepers", _fake_pool_keepers)
    monkeypatch.setattr(gkw, "_pooled_struct_type", lambda: "POOLED_SCHEMA")

    writes: list[dict[str, Any]] = []

    def _fake_write(sdf: Any, catalog: str, schema: str, table_name: str, *, replace_where=None, logger=None) -> int:
        _tag, pdf, _struct = sdf
        writes.append(
            {"catalog": catalog, "schema": schema, "table": table_name, "where": replace_where, "rows": len(pdf)}
        )
        return len(pdf)

    monkeypatch.setattr(utils, "write_delta_table", _fake_write)

    total = _pool_gkdv(spark, "cat")

    # (a) the observation intermediate was read to the driver and pooled with the registered floors.
    assert any(name.endswith("gkdv_observations") for name in spark.tables_read)
    assert seen_pool_args["observations"] is obs_pdf
    assert (seen_pool_args["min_nonzero"], seen_pool_args["min_games"], seen_pool_args["want_threat"]) == (20, 2, True)

    # (b) one per-provider replaceWhere write to gkdv_keeper_pooled, in provider order.
    assert [w["table"] for w in writes] == ["gkdv_keeper_pooled"] * 4
    assert [w["where"] for w in writes] == [
        "data_source = 'idsse'",
        "data_source = 'metrica'",
        "data_source = 'skillcorner'",
        "data_source = 'gradientsports'",
    ]
    assert all(w["schema"] == "bronze" for w in writes)

    # (c) row counts per provider slice, and the summed total.
    assert [w["rows"] for w in writes] == [2, 0, 1, 0]
    assert total == 3

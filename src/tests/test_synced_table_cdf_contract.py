"""Every TRIGGERED synced-table source dbt model must persist CDF (spec F2).

A `dbt build --full-refresh` of a mart drops + recreates the Delta table. If the model does
not set ``delta.enableChangeDataFeed='true'`` in its tblproperties, the recreated table loses
CDF — and a TRIGGERED Lakebase synced table cannot be (re)created over a CDF-off source. This
contract test prevents anyone from silently reopening F2 by adding a TRIGGERED synced table over
a CDF-off source. It also fails *gracefully* (clear message) for a TRIGGERED source that is not
exactly one dbt model (e.g. a Python-written observability table), since such a source cannot be
CDF-guaranteed here and needs explicit handling (P7).
"""

from __future__ import annotations

import re
from pathlib import Path

from ingestion.refresh_synced_tables import SYNCED_TABLES

_MODELS = Path(__file__).resolve().parents[2] / "dbt_project" / "models"
_CDF_RE = re.compile(r"delta\.enableChangeDataFeed['\"]\s*:\s*['\"]true['\"]")


def test_triggered_sources_persist_cdf() -> None:
    missing: list[str] = []
    non_model: list[tuple[str, str, int]] = []
    for cfg in SYNCED_TABLES:
        if cfg.scheduling_policy != "TRIGGERED":
            continue
        hits = list(_MODELS.rglob(f"{cfg.source_table}.sql"))
        if len(hits) != 1:
            non_model.append((cfg.name, cfg.source_table, len(hits)))  # P7: clear, not a crash
            continue
        if not _CDF_RE.search(hits[0].read_text(encoding="utf-8")):
            missing.append(cfg.source_table)
    assert not non_model, (
        "TRIGGERED synced tables whose source is not exactly one dbt model — a TRIGGERED "
        f"non-mart source cannot be CDF-guaranteed by this contract and needs explicit handling: {non_model}"
    )
    assert not missing, (
        "TRIGGERED synced-table sources missing delta.enableChangeDataFeed tblproperty "
        f"(a full-refresh strips CDF -> the synced table becomes unrecreatable): {sorted(missing)}"
    )

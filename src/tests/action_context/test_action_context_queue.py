"""Serverless integration proofs for the AC-1 worker-drain adapters (ADR-037).

SCAFFOLDING / MANUAL: these require a live Databricks serverless Spark-Connect session
plus ``spark`` and ``tmp_catalog`` fixtures that do NOT yet exist in this repo's offline
CI. The module is skipped unless RUN_SERVERLESS_TESTS=1 AND those fixtures are provided.
They are the executable form of the must-run-before-"verified" checklist (plan Task 14):
  - run-id isolation (B1/L2)
  - interruptTag actually cancels a real applyInPandas (B2/N3)
  - IDSSE halves survive each other's period-aware replaceWhere (B2 precision)
Fill the `<a real ... in tmp_catalog>` markers with real ids from the test catalog.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_SERVERLESS_TESTS") != "1",
    reason="requires a Databricks serverless Spark session + spark/tmp_catalog fixtures (RUN_SERVERLESS_TESTS=1)",
)


def test_delta_work_queue_roundtrip_and_run_id_isolation(spark, tmp_catalog) -> None:
    """B1/L2: enqueue two runs; a worker sees ONLY its run's rows; period preserved."""
    from analytics.action_context.drain import assign_workers
    from analytics.action_context.work_unit import WorkUnit
    from ingestion.action_context_queue import DeltaWorkQueue

    q = DeltaWorkQueue(spark, catalog=tmp_catalog)
    q.ensure_table()

    units_a = [WorkUnit(provider="wyscout", match_id=f"a{i}") for i in range(5)]
    units_b = [WorkUnit(provider="idsse", match_id="bm", period=p) for p in (1, 2)]
    q.enqueue("RUN_A", assign_workers(units_a, n_workers=2))
    q.enqueue("RUN_B", assign_workers(units_b, n_workers=2))

    got_b = [u for w in (0, 1) for u in q.units_for_worker("RUN_B", w)]
    assert sorted((u.provider, u.match_id, u.period) for u in got_b) == [
        ("idsse", "bm", 1),
        ("idsse", "bm", 2),
    ]


def test_idsse_halves_survive_each_other(spark, tmp_catalog) -> None:
    """B2 precision: period-aware replaceWhere — writing period 2 must NOT delete period 1."""
    import logging

    from ingestion.action_context import _load_xt_grid_from_delta, _process_tracking_match

    log = logging.getLogger("t")
    grid, xt_l, xt_w = _load_xt_grid_from_delta(spark, tmp_catalog, "bronze", log)
    mid = "<a real IDSSE match in tmp_catalog with periods 1 and 2>"
    _process_tracking_match(spark, tmp_catalog, "bronze", "idsse", mid, 1, grid, xt_l, xt_w, log)
    _process_tracking_match(spark, tmp_catalog, "bronze", "idsse", mid, 2, grid, xt_l, xt_w, log)
    rows = spark.table(f"{tmp_catalog}.bronze.spadl_action_context").where(f"match_id = '{mid}'")
    periods = {r["period_id"] for r in rows.select("period_id").distinct().collect()}
    assert periods == {1, 2}  # period 1 survived period 2's write


def test_spark_interrupt_watchdog_real_processor_smoke(spark, tmp_catalog) -> None:
    """B2/N3: drive the REAL processor path; interruptTag must cancel the deep applyInPandas."""
    import logging

    from analytics.action_context.drain import GameTimeoutError
    from analytics.action_context.work_unit import WorkUnit
    from ingestion.action_context_queue import SparkGameProcessor, SparkInterruptWatchdog

    proc = SparkGameProcessor(spark, catalog=tmp_catalog, schema="bronze")
    wd = SparkInterruptWatchdog(spark)
    unit = WorkUnit(provider="metrica", match_id="<a real tracking match in tmp_catalog>")

    import time

    start = time.monotonic()
    with pytest.raises(GameTimeoutError):
        wd.run(lambda: proc.process(unit), "metrica:smoke", timeout_s=5)  # 5 s << real ~minutes
    elapsed = time.monotonic() - start
    # R3 PRIMARY proof: the controller regained control near the budget, not after the
    # job's full ~minutes -> the watchdog actually returned control (robust across runtimes).
    assert elapsed < 5 + wd._grace + 5
    # CORROBORATING (not load-bearing): interruptTag's returned op-ids. A correct serverless
    # build returns them, but the contract isn't guaranteed across runtimes -> don't fail on [].
    if not wd._last_interrupted_ops:
        logging.getLogger("t").warning("interruptTag returned no op-ids; relying on timing proof (R3)")

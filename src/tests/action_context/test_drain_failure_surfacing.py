"""D2: a drain that swallowed a unit failure must NOT report success.

2026-07-11: ``skillcorner:1552423:2`` raised inside the UDF; ``drain_worker`` caught it
(``failed=1``) and continued; ``main_drain_worker`` logged the summary and returned -- so the
Databricks task exited 0 and the job reported SUCCESS while 550 actions were missing from
``fct_action_context``.

The swallow itself is CORRECT and stays (``drain.py:170-181``): one bad unit must not destroy a
5.5h drain, and the worker's remaining slice rolls forward. Exiting 0 afterwards is not.

Timeouts are deliberately EXCLUDED: they roll forward by design (a capacity signal, not a
correctness one).
"""

from __future__ import annotations

import pytest

from analytics.action_context.drain import DrainSummary
from ingestion.action_context import raise_on_failed_units


def test_failed_units_raise_with_labels() -> None:
    summary = DrainSummary(worker_id=5, processed=46, failed=1, failed_units=["skillcorner:1552423:2"])

    with pytest.raises(RuntimeError, match="skillcorner:1552423:2"):
        raise_on_failed_units(summary, run_id="85619159042760")


def test_clean_drain_does_not_raise() -> None:
    raise_on_failed_units(DrainSummary(worker_id=0, processed=47), run_id="r1")


def test_timeouts_alone_do_not_raise() -> None:
    """Timeouts roll forward to the next run BY DESIGN. Only ``failed`` means a unit produced no
    rows and never will."""
    summary = DrainSummary(
        worker_id=1,
        processed=40,
        timed_out=7,
        timed_out_units=["gradientsports:10502:1"],
    )

    raise_on_failed_units(summary, run_id="r1")

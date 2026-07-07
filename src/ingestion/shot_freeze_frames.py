"""Bronze writer for the pre-shot tracking freeze-frame snapshots (Task 0.5).

Driver-side persistence for the per-(shot, player) snapshots produced by
``analytics.action_context.tracking_snapshots``. This lives in the INGESTION layer
(not analytics) because it calls ``ingestion.utils.write_delta_table`` — ``analytics``
must not import ``ingestion`` (import-linter contract; the allowed direction is
ingestion -> analytics). The canonical column list + table name are the data-shape
source of truth in the analytics builder, so they are imported from there.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from analytics.action_context.tracking_snapshots import _SHOT_FF_COLUMNS, _TABLE_NAME

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def write_shot_freeze_frames(
    snapshots_df: DataFrame,
    catalog: str,
    schema: str,
    match_keys: Sequence[int],
    *,
    row_count: int | None = None,
) -> int:
    """Persist the collected per-(shot, player) snapshot set to ``bronze.shot_freeze_frames``.

    Driver-side persistence step: the per-(match, period) cogroup runs
    :func:`analytics.action_context.tracking_snapshots.build_tracking_snapshots_spark` inside its
    ``applyInPandas`` UDF (no Delta writes from executors — serverless forbids it) and yields
    ``snapshots_df``, a Spark DataFrame conforming to :data:`_SHOT_FF_COLUMNS`. We select the
    canonical column order and write it idempotently, ``replaceWhere`` keyed on ``match_key``
    (mirrors ``xg_model_v2.run_pipeline``'s per-competition bulk write). All periods of a match land
    in the same bulk write, so a ``match_key``-only predicate is safe.

    Parameters
    ----------
    snapshots_df :
        Spark DataFrame with (at least) the :data:`_SHOT_FF_COLUMNS` columns.
    catalog, schema :
        Unity Catalog target (e.g. ``soccer_analytics`` / ``bronze``).
    match_keys :
        The ``match_key`` set covered by this write — the run's work units. Used to build the
        ``replaceWhere`` predicate so a re-run overwrites exactly these matches.
    row_count :
        Optional pre-computed row count forwarded to ``write_delta_table`` to skip a redundant
        ``df.count()`` DAG recomputation.

    Returns
    -------
    int
        Number of rows written.

    Raises
    ------
    ValueError
        If ``match_keys`` is empty (a ``replaceWhere`` with no keys would be a no-op predicate
        that silently writes nothing).
    """
    from ingestion.utils import write_delta_table

    keys = sorted({int(k) for k in match_keys})
    if not keys:
        msg = "write_shot_freeze_frames requires a non-empty match_keys set for the replaceWhere predicate"
        raise ValueError(msg)

    ordered = snapshots_df.select(*_SHOT_FF_COLUMNS)
    key_list = ", ".join(str(k) for k in keys)
    return write_delta_table(
        ordered,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=f"match_key IN ({key_list})",
        logger=logger,
        row_count=row_count,
    )

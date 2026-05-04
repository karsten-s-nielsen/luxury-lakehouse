"""SK3-MIG-B telemetry — schema constants + StructType factory + writer helper.

Per ADR-002 §4 schema-drift guard: the column list is the single source of truth
for both the DDL migration (scripts/migrations/2026-05-03-create-bronze-sk3-mig-b-runs.sql)
and the writer code (orchestrator + smoke gates). Drift is caught by
src/tests/test_sk3_mig_b_runs_schema_parity.py.

Writer contract:
- Every row MUST include cycle_id, cycle_item, cycle_item_kind, recorded_at.
- Trained-model rows: hf_job_id, champion_set_at, pre/post_mart_version, smoke_*.
- Compute-only rows: pre/post_mart_version, smoke_*. NULL for hf/champion fields.
- Publish rows: pre_hf_revision_sha. NULL for mart/champion fields.
- Meta-event rows (pre_state, baseline_rebase): cycle_item_kind="meta_event"; mostly NULL.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


# Single source of truth — every column in the bronze.sk3_mig_b_runs table.
# Order matches the migration DDL exactly (parity-tested).
_SK3_MIG_B_RUNS_COLUMNS: list[tuple[str, str]] = [
    ("cycle_id", "STRING"),
    ("cycle_started_at", "TIMESTAMP"),
    ("cycle_finished_at", "TIMESTAMP"),
    ("wheel_at_start", "STRING"),
    ("wheel_at_end", "STRING"),
    ("silly_kicks_version", "STRING"),
    ("cost_cap_usd", "DOUBLE"),
    ("walltime_cap_hours", "DOUBLE"),
    ("cycle_item", "STRING"),
    ("cycle_item_kind", "STRING"),
    ("hf_job_id", "STRING"),
    ("champion_set_at", "TIMESTAMP"),
    ("pre_mart_version", "BIGINT"),
    ("post_mart_version", "BIGINT"),
    ("pre_hf_revision_sha", "STRING"),
    ("smoke_pass", "BOOLEAN"),
    ("smoke_metrics", "MAP<STRING, DOUBLE>"),
    ("smoke_metrics_str", "MAP<STRING, STRING>"),
    ("wall_clock_seconds", "DOUBLE"),
    ("cost_usd", "DOUBLE"),
    ("recorded_at", "TIMESTAMP"),
]

# Cycle-item enums — used by writers to set cycle_item_kind correctly.
_TRAINED_MODEL_ITEMS: frozenset[str] = frozenset(
    {
        "vaep",
        "xg_v2",
        "ext_v2_p0",
        "ext_v2_p1",
        "f2v_v1",
        "f2v_v2",
        "f2v_360",
        "scoutgpt",
    }
)
_COMPUTE_ONLY_ITEMS: frozenset[str] = frozenset(
    {
        "defcon_lite",
        "obso",
        "pausa",
    }
)
_PUBLISH_ITEMS: frozenset[str] = frozenset(
    {
        "spadl_vaep_publish",
        "xg_shots_publish",
        "freeze_frame_publish",
        "shots_on_target_publish",
        "obso_pausa_inputs_publish",
        "obso_trained_grids_publish",
        "obso_pausa_values_publish",
        "f2v_embeddings_publish",
    }
)
_META_EVENT_ITEMS: frozenset[str] = frozenset(
    {
        "pre_state",
        "baseline_rebase",
        "xg1_retire_runtime",
        "scoutgpt_export",
        "heartbeat",
    }
)


def get_sk3_mig_b_runs_struct_type() -> StructType:
    """Lazy factory — converts _SK3_MIG_B_RUNS_COLUMNS to a Spark StructType.

    Lazy import of pyspark.sql.types so this module imports cleanly outside
    a Spark context (e.g., during pytest collection on a non-Spark host).
    """
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    type_map: dict[str, object] = {
        "STRING": StringType(),
        "TIMESTAMP": TimestampType(),
        "DOUBLE": DoubleType(),
        "BIGINT": LongType(),
        "BOOLEAN": BooleanType(),
        "MAP<STRING, DOUBLE>": MapType(StringType(), DoubleType()),
        "MAP<STRING, STRING>": MapType(StringType(), StringType()),
    }

    fields = [StructField(name, type_map[ddl_type], nullable=True) for name, ddl_type in _SK3_MIG_B_RUNS_COLUMNS]
    return StructType(fields)


def classify_cycle_item(cycle_item: str) -> str:
    """Return the cycle_item_kind for a given cycle_item name.

    Raises ValueError if cycle_item is not in any registered set.
    """
    if cycle_item in _TRAINED_MODEL_ITEMS:
        return "trained_model"
    if cycle_item in _COMPUTE_ONLY_ITEMS:
        return "compute_only"
    if cycle_item in _PUBLISH_ITEMS:
        return "publish"
    if cycle_item in _META_EVENT_ITEMS:
        return "meta_event"
    raise ValueError(
        f"Unknown cycle_item: {cycle_item!r}. "
        f"Add it to one of _TRAINED_MODEL_ITEMS / _COMPUTE_ONLY_ITEMS / "
        f"_PUBLISH_ITEMS / _META_EVENT_ITEMS in src/ingestion/sk3_mig_b_telemetry.py."
    )

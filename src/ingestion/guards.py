"""Port/adapter infrastructure for pipeline skip guards.

Each workflow exposes a :class:`SkipGuard` adapter whose ``check()``
method returns a :class:`FilterResult` describing whether the workflow
has new work and how to chunk it for fan-out.

Each pipeline's ``main()`` calls its guard's ``check()`` at startup
and raises ``WorkflowSkippedError`` when ``count == 0``.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import ingestion as _ingestion

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass(frozen=True)
class FilterResult:
    """Result of a single workflow's skip guard check.

    Attributes:
        workflow_id: The ``wf-xxx`` identifier matching the workflow card.
        count: Number of unprocessed items.  ``0`` means skip entirely.
        chunks: Pre-computed fan-out partitions — a list of ID lists.
            ``None`` means single-task execution (no fan-out).
            ``len(chunks) > 1`` triggers ``for_each_task``.
            The adapter owns chunk sizing (knows its data shape).
        metadata: Pass-through context for the pipeline — avoids
            re-computing what the guard already discovered (e.g.,
            ``need_global`` flag, competitions DataFrame).
        guard_duration_seconds: Wall-clock time the guard check took.
            Populated by :func:`timed_check`, ``None`` for legacy callers.
    """

    workflow_id: str
    count: int
    chunks: list[list[str]] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    guard_duration_seconds: int | None = None


def timed_check(guard: SkipGuard, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
    """Run a guard's ``check()`` and record its wall-clock duration.

    Returns a new :class:`FilterResult` with ``guard_duration_seconds``
    populated.  All fields from the guard's result (including ``metadata``
    with pre-computed IDs) are preserved unchanged.
    """
    start = time.monotonic()
    result = guard.check(spark, catalog, schema)
    elapsed = round(time.monotonic() - start)
    return FilterResult(
        workflow_id=result.workflow_id,
        count=result.count,
        chunks=result.chunks,
        metadata=result.metadata,
        guard_duration_seconds=elapsed,
    )


def ensure_table(spark: SparkSession, table_name: str, schema_ddl: str) -> None:
    """Create a Delta table if it does not exist.

    Called by guards before ``find_new_ids()`` to guarantee the results
    table exists.  On first-ever pipeline run the table is empty and the
    anti-join correctly returns all source IDs.  After the first write,
    this is a metadata-only no-op (~100 ms).

    Args:
        spark: Active SparkSession.
        table_name: Fully-qualified table name (``catalog.schema.table``).
        schema_ddl: SQL column definitions (e.g., ``"match_id STRING, value DOUBLE"``).
    """
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_ddl}) USING DELTA"
        " TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true',"
        " 'delta.autoOptimize.optimizeWrite' = 'true')"
    )


def find_new_ids(
    spark: SparkSession,
    source_table: str,
    results_table: str,
    id_column: str = "match_id",
    *,
    results_id_column: str | None = None,
    source_filter: str | None = None,
    results_filter: str | None = None,
) -> list[str]:
    """Spark-native anti-join to find IDs present in source but not in results.

    Pushes the set-difference to Spark executors via LEFT ANTI JOIN,
    collecting only the (small) list of new IDs to the driver.
    All IDs are cast to string for consistent cross-system normalization.

    The results table **must exist** before calling this function.  Use
    :func:`ensure_table` in the guard's ``check()`` to create it on first run.

    Args:
        spark: Active SparkSession.
        source_table: Fully-qualified source table (e.g., ``catalog.schema.table``).
        results_table: Fully-qualified results table (must exist).
        id_column: Column name for the join key in the source table (default ``match_id``).
        results_id_column: Column name for the join key in the results table.
            Defaults to ``id_column`` when source and results use the same name.
            Use when comparing bronze (raw schema) against gold (canonical schema),
            e.g., ``id_column="matchId", results_id_column="match_id"``.
        source_filter: Optional SQL filter expression for source table.
        results_filter: Optional SQL filter expression for results table.

    Returns:
        List of string IDs present in source but absent from results.
        Empty list if source is empty or all IDs are already processed.

    Raises:
        AnalysisException: If the results table does not exist (call
            ``ensure_table`` first).
    """
    from pyspark.sql import functions as F  # noqa: N812

    res_col = results_id_column or id_column
    join_alias = "_join_id"

    source_df = spark.table(source_table)
    if source_filter:
        source_df = source_df.filter(source_filter)
    source_df = source_df.select(F.col(id_column).cast("string").alias(join_alias)).distinct()

    results_df = spark.table(results_table)
    if results_filter:
        results_df = results_df.filter(results_filter)
    results_df = results_df.select(F.col(res_col).cast("string").alias(join_alias)).distinct()

    new_df = source_df.join(results_df, on=join_alias, how="left_anti")
    rows = new_df.collect()
    return [str(row[join_alias]) for row in rows]


_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HF Hub dataset freshness — SHA-based skip guard for import pipelines
# ---------------------------------------------------------------------------

_IMPORT_CHECKSUMS_DDL = (
    "workflow_id STRING NOT NULL, "
    "source_repo STRING NOT NULL, "
    "repo_type STRING NOT NULL, "
    "last_imported_sha STRING NOT NULL, "
    "imported_at TIMESTAMP NOT NULL"
)
_IMPORT_CHECKSUMS_TABLE = "workflow_import_checksums"


def check_hf_dataset_freshness(
    spark: SparkSession,
    catalog: str,
    workflow_id: str,
    hf_repo: str,
    repo_type: str = "dataset",
) -> FilterResult:
    """Compare the current HF Hub commit SHA against the last imported SHA.

    Returns ``count=0`` (skip) when the stored SHA matches the live repo,
    ``count=1`` (run) when they differ or no stored SHA exists.

    Fails open: if HF Hub is unreachable, returns ``count=1`` so the
    pipeline still runs (network issues should not silently skip work).

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        workflow_id: The ``wf-xxx`` identifier for this workflow.
        hf_repo: HF Hub repo ID (e.g., ``luxury-lakehouse/obso-pausa-values``).
        repo_type: HF Hub repo type (default ``"dataset"``).

    Returns:
        FilterResult with ``count=0`` (skip) or ``count=1`` (run).
        When ``count=1``, ``metadata["commit_sha"]`` contains the live SHA
        for downstream ``record_import_sha`` write-back.
    """
    # Import HfApi inside function body to avoid import-time dep on
    # huggingface_hub — conformance test runner doesn't have it installed.
    from huggingface_hub import HfApi

    # 1. Fetch current commit SHA from HF Hub
    try:
        info = HfApi().repo_info(hf_repo, repo_type=repo_type)
        current_sha = info.sha
    except Exception:  # noqa: BLE001 — fail open on any HF Hub error (network, auth, 404)
        _logger.warning(
            "Could not reach HF Hub for %s — failing open (count=1)",
            hf_repo,
            exc_info=True,
        )
        return FilterResult(workflow_id=workflow_id, count=1)

    if not current_sha:
        _logger.warning("HF Hub returned empty SHA for %s — failing open", hf_repo)
        return FilterResult(workflow_id=workflow_id, count=1)

    # 2. Ensure the checksums table exists
    table = f"{catalog}.observability.{_IMPORT_CHECKSUMS_TABLE}"
    ensure_table(spark, table, _IMPORT_CHECKSUMS_DDL)

    # 3. Look up the last imported SHA for this workflow
    rows = spark.sql(
        f"SELECT last_imported_sha FROM {table} "  # noqa: S608
        f"WHERE workflow_id = '{workflow_id}'"
    ).collect()

    if rows:
        stored_sha: str = rows[0]["last_imported_sha"]
        if stored_sha == current_sha:
            _logger.info(
                "HF repo %s SHA unchanged (%s) — skipping %s",
                hf_repo,
                current_sha[:8],
                workflow_id,
            )
            return FilterResult(workflow_id=workflow_id, count=0)
        _logger.info(
            "HF repo %s SHA changed (%s → %s) — running %s",
            hf_repo,
            stored_sha[:8],
            current_sha[:8],
            workflow_id,
        )
    else:
        _logger.info(
            "No stored SHA for %s (%s) — first run",
            workflow_id,
            hf_repo,
        )

    return FilterResult(
        workflow_id=workflow_id,
        count=1,
        metadata={"commit_sha": current_sha},
    )


def record_import_sha(
    spark: SparkSession,
    catalog: str,
    workflow_id: str,
    source_repo: str,
    commit_sha: str | None,
    repo_type: str = "dataset",
) -> None:
    """Write back the imported commit SHA to the checksums table via MERGE.

    Called after a successful pipeline write to record which HF Hub commit
    was imported.  Subsequent guard runs compare against this stored SHA
    to decide whether new work exists.

    No-ops when ``commit_sha`` is ``None`` (e.g., when the guard failed
    open and no SHA was available).

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        workflow_id: The ``wf-xxx`` identifier for this workflow.
        source_repo: HF Hub repo ID that was imported.
        commit_sha: The commit SHA to record, or ``None`` to skip.
        repo_type: HF Hub repo type (default ``"dataset"``).
    """
    if commit_sha is None:
        return

    table = f"{catalog}.observability.{_IMPORT_CHECKSUMS_TABLE}"

    spark.sql(
        f"MERGE INTO {table} AS t "  # noqa: S608 — values are internal workflow constants, not user input
        f"USING (SELECT '{workflow_id}' AS workflow_id) AS s "
        f"ON t.workflow_id = s.workflow_id "
        f"WHEN MATCHED THEN UPDATE SET "
        f"  source_repo = '{source_repo}', "
        f"  repo_type = '{repo_type}', "
        f"  last_imported_sha = '{commit_sha}', "
        f"  imported_at = current_timestamp() "
        f"WHEN NOT MATCHED THEN INSERT "
        f"  (workflow_id, source_repo, repo_type, last_imported_sha, imported_at) "
        f"  VALUES ('{workflow_id}', '{source_repo}', '{repo_type}', "
        f"  '{commit_sha}', current_timestamp())"
    )

    _logger.info(
        "Recorded import SHA %s for %s (%s)",
        commit_sha[:8],
        workflow_id,
        source_repo,
    )


class SkipGuard(Protocol):
    """Port: each workflow exposes its freshness check.

    Implementations live alongside their pipeline module as a
    module-level ``skip_guard`` object or function.
    """

    workflow_id: str

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Run the skip guard and return a FilterResult.

        Must be safe to call from the ``default`` environment —
        only Spark SQL, no analytics imports.
        """
        ...


_GUARD_MODULES: list[str] = [
    "ingestion.pitch_control_batch",
    "ingestion.off_ball_xt",
    "ingestion.defcon_lite_360",
    "ingestion.defcon_lite_tracking",
    "ingestion.elastic_sync",
    "ingestion.pausa",
    "ingestion.line_breaking",
    "ingestion.formations_efpi",
    "ingestion.formations_shape_graph",
    "ingestion.spadl_vaep",
    "ingestion.player_embeddings_v1",
    # ingestion.xg_model (v1) retired SK3-MIG-B 2026-05-03 per ADR-023.
    "ingestion.xg_model_v2",
    "ingestion.expected_threat",
    "ingestion.export_embeddings_training_data",
    "ingestion.prepare_360_training_data",
    "ingestion.entity_resolution",
    "ingestion.statsbomb",
    "ingestion.statsbomb_backfill_extra",
    "ingestion.statsbomb_backfill_360",
    "ingestion.metrica",
    "ingestion.wyscout",
    "ingestion.idsse",
    "ingestion.idsse_events",
    "ingestion.skillcorner",
    "ingestion.import_obso_results",
    "ingestion.import_psxg_predictions",
    "ingestion.import_space_creation",
    "ingestion.tracking_metadata",
    "ingestion.model_validation",
    "ingestion.dbt_runner",
    "ingestion.refresh_synced_tables",
    "ingestion.sync_hf_costs",
    "ingestion.player_embeddings_v2",
    "ingestion.hf_sync",
]


def get_workflow_guards() -> dict[str, SkipGuard]:
    """Build the guard registry with per-module imports.

    Returns a dict mapping ``workflow_id`` to its ``SkipGuard`` adapter.
    Each module is imported individually so that a missing third-party
    dependency (e.g., ``statsbombpy`` in the ``default`` environment)
    skips that guard rather than crashing the entire registry.
    """
    import importlib
    import logging

    _logger = logging.getLogger(__name__)
    guards: dict[str, SkipGuard] = {}

    for module_path in _GUARD_MODULES:
        try:
            mod = importlib.import_module(module_path)
            guard = mod.skip_guard  # type: ignore[attr-defined]
            guards[guard.workflow_id] = guard
        except (ImportError, ModuleNotFoundError, AttributeError):
            # ImportError: guard module itself failed to import (missing dep).
            # AttributeError: guard module exists but doesn't expose `skip_guard`.
            # Both are legitimate "skip this guard" cases on first install.
            _logger.error("Skipping guard from %s (import failed)", module_path, exc_info=True)

    return guards


# ---------------------------------------------------------------------------
# Watermark-based skip guard — "has any upstream Delta table changed?"
# ---------------------------------------------------------------------------

_DATA_CHANGING_OPS: frozenset[str] = frozenset(
    {
        "WRITE",
        "MERGE",
        "DELETE",
        "UPDATE",
        "CREATE TABLE AS SELECT",
        "CREATE OR REPLACE TABLE AS SELECT",
        "RESTORE",
    }
)

# Logical PK: (workflow_id, upstream_table) — enforced by MERGE ON clause,
# not by Delta constraints (Delta Lake does not enforce PKs at write time).
_WATERMARKS_DDL = (
    "workflow_id STRING NOT NULL, upstream_table STRING NOT NULL, "
    "last_seen_version BIGINT NOT NULL, checked_at TIMESTAMP NOT NULL"
)


def _load_stored_watermarks(
    spark: SparkSession,
    watermarks_table: str,
    workflow_id: str,
) -> dict[str, int]:
    """Load stored watermarks for a workflow. Returns {table_fqn: version}."""
    rows = spark.sql(
        f"SELECT upstream_table, last_seen_version "  # noqa: S608
        f"FROM {watermarks_table} "
        f"WHERE workflow_id = '{workflow_id}'"
    ).collect()
    return {row.upstream_table: row.last_seen_version for row in rows}


def _get_latest_data_version(
    spark: SparkSession,
    table: str,
) -> int | None:
    """Get the latest data-changing version from DESCRIBE HISTORY.

    Returns None if no data-changing operations found.
    """
    rows = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 20").collect()
    data_versions = [row.version for row in rows if row.operation in _DATA_CHANGING_OPS]
    return max(data_versions) if data_versions else None


def check_upstream_freshness(
    spark: SparkSession,
    catalog: str,
    workflow_id: str,
    upstream_tables: list[str],
) -> FilterResult:
    """Check if any upstream Delta table has changed since last recorded watermark.

    Returns ``FilterResult(count=0)`` if all upstream tables are at the same
    version as the last recorded watermark.  Returns ``count=1`` (fail open)
    on first run, version mismatch, or any error.
    """
    watermarks_table = f"{catalog}.observability.workflow_watermarks"
    try:
        ensure_table(spark, watermarks_table, _WATERMARKS_DDL)
        stored = _load_stored_watermarks(spark, watermarks_table, workflow_id)
    except Exception:  # noqa: BLE001 — fail open if watermarks table is inaccessible
        _logger.warning("Watermark table inaccessible for %s — failing open", workflow_id)
        return FilterResult(workflow_id=workflow_id, count=1)

    for table in upstream_tables:
        try:
            current_version = _get_latest_data_version(spark, table)
        except Exception:  # noqa: BLE001 — fail open on DESCRIBE HISTORY errors
            _logger.warning("DESCRIBE HISTORY failed for %s — failing open", table)
            return FilterResult(workflow_id=workflow_id, count=1)

        stored_version = stored.get(table)

        if current_version is None:
            # No data-changing ops in the last 20 history entries.
            # If we have a stored watermark, the data hasn't changed — skip.
            # If no stored watermark (first run), fail open.
            if stored_version is None:
                return FilterResult(workflow_id=workflow_id, count=1)
            continue

        if stored_version is None or current_version != stored_version:
            return FilterResult(workflow_id=workflow_id, count=1)

    return FilterResult(workflow_id=workflow_id, count=0)


def record_watermarks(
    spark: SparkSession,
    catalog: str,
    workflow_id: str,
    upstream_tables: list[str],
) -> None:
    """Record current upstream versions after successful pipeline completion."""
    watermarks_table = f"{catalog}.observability.workflow_watermarks"
    ensure_table(spark, watermarks_table, _WATERMARKS_DDL)

    for table in upstream_tables:
        current_version = _get_latest_data_version(spark, table)
        # If no data-changing ops in recent history, record version 0 as sentinel.
        # This prevents a livelock where the guard perpetually fails open because
        # no watermark is ever stored for rarely-updated tables.
        if current_version is None:
            current_version = 0
        spark.sql(
            f"MERGE INTO {watermarks_table} AS target "
            f"USING (SELECT '{workflow_id}' AS workflow_id, "
            f"'{table}' AS upstream_table, "
            f"{current_version} AS last_seen_version, "
            f"current_timestamp() AS checked_at) AS source "
            f"ON target.workflow_id = source.workflow_id "
            f"AND target.upstream_table = source.upstream_table "
            f"WHEN MATCHED THEN UPDATE SET "
            f"target.last_seen_version = source.last_seen_version, "
            f"target.checked_at = source.checked_at "
            f"WHEN NOT MATCHED THEN INSERT *"
        )


# Reference the installed ``ingestion`` package to anchor wheel-path
# resolution.  Uses ``_ingestion.__file__`` (package ``__init__.py``), NOT
# ``__file__`` (this module), so the anchor is resilient to guards.py
# moving within the package tree.  Same pattern as ``_WHEEL_INGESTION_FILE``
# in ``ingestion.hf_publish``.  Exposed as a module-level attribute so tests
# can monkeypatch it to simulate a site-packages layout.
# The ``import ingestion as _ingestion`` statement is at the top of this file.
_WHEEL_INGESTION_FILE: Path = Path(_ingestion.__file__).resolve()


def _default_cards_dir() -> Path:
    """Resolve workflow-cards using dual-mode resolution.

    Dual-mode so the same code works at runtime inside both a wheel install
    (Databricks workflow task) and a source-tree checkout (local dev, tests):

      1. **Wheel install**: the wheel force-includes ``workflow-cards/`` as
         ``workflow_cards/`` (sibling of the ``ingestion`` package).  Resolves
         via ``Path(ingestion.__file__).parent.parent / "workflow_cards"``.

      2. **Source-tree fallback**: when the wheel-side candidate does not
         exist, walks up from this module to the repo root and descends into
         ``workflow-cards/``.

    Follows the ``get_hf_card_path`` precedent in ``ingestion.hf_publish``.
    """
    # Wheel-first: site-packages layout where workflow_cards/ is a sibling of ingestion/.
    wheel_candidate = _WHEEL_INGESTION_FILE.parent.parent / "workflow_cards"
    if wheel_candidate.is_dir():
        return wheel_candidate

    # Dev fallback: walk up from this module to repo root.
    # src/ingestion/guards.py -> parents[2] = repo root.
    return Path(__file__).resolve().parents[2] / "workflow-cards"


def resolve_upstream_tables_from_card(
    workflow_id: str,
    catalog: str,
    schema: str,
    cards_dir: Path | None = None,
) -> list[str]:
    """Load upstream Delta table FQNs from a workflow card's inputs section.

    Reads ``inputs.tables`` and ``inputs.datasets`` entries where
    ``source == "delta-table"``, substitutes ``{catalog}`` and ``{schema}``
    placeholders in the ``id`` field, and returns the resolved list.

    ``cards_dir`` defaults to dual-mode resolution: wheel-install path first
    (``workflow_cards/`` force-included in the wheel), then source-tree
    fallback (``workflow-cards/`` at repo root).  Tests can pass an explicit
    ``cards_dir`` to override.
    """
    if cards_dir is None:
        cards_dir = _default_cards_dir()

    card_path = cards_dir / f"{workflow_id}.yaml"
    with open(card_path, encoding="utf-8") as f:
        import yaml

        # Workflow cards have YAML front matter delimited by ---
        content = f.read()
        # Split on --- and take the first YAML document
        parts = content.split("---")
        if len(parts) >= 3:
            card = yaml.safe_load(parts[1])
        else:
            card = yaml.safe_load(content)

    tables: list[str] = []
    inputs = card.get("inputs", {})
    for section in ("tables", "datasets"):
        for entry in inputs.get(section, []):
            if entry.get("source") == "delta-table":
                fqn = entry["id"].replace("{catalog}", catalog).replace("{schema}", schema)
                tables.append(fqn)
    return tables


def _repo_cards_dir() -> Path:
    """Resolve workflow-cards/ from the repo root (for local/test use)."""
    return Path(__file__).resolve().parent.parent.parent / "workflow-cards"

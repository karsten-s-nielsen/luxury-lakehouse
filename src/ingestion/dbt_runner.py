"""D59: programmatic dbt invocation entry point for the daily Databricks job.

Resolves the wheel-bundled ``dbt_project`` location at runtime via
``importlib.resources``, invokes
``dbtRunner().invoke(['build', '--project-dir', ..., '--profiles-dir', ..., '--target', 'serverless'])``,
and returns a non-zero exit code on failure so the Databricks task fails fast.

Auth: queries ``WorkspaceClient.config`` to resolve ``host`` (workspace URL)
and selects the configured SQL warehouse via ``DATABRICKS_SQL_WAREHOUSE_ID``
env var (set in the ``dbt`` environment block of the Terraform workflows
module). Sets ``DATABRICKS_HOST`` and ``DATABRICKS_HTTP_PATH`` env vars
before invoking dbt so ``profiles.yml`` env_var() lookups resolve. The
``serverless`` target uses ``auth_type: oauth`` so dbt-databricks 1.10+
auto-discovers runtime SP identity via the SDK.

Bundling: ``pyproject.toml`` ``[tool.hatch.build.targets.wheel.force-include]``
installs ``dbt_project/`` as ``luxury_lakehouse_dbt_project/`` inside the wheel.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# dbt-core is an optional runtime dependency — only installed via the ``dbt``
# extra (used by the Databricks daily job and local developer workflows).
# Unit tests that mock ``dbtRunner`` via ``@patch("ingestion.dbt_runner.dbtRunner")``
# work against ``dbtRunner = None`` because unittest.mock replaces the module
# attribute regardless of its current value. CI's ``lint-and-test`` job
# installs analytics/embeddings/mlflow/jax but not dbt, so this module must
# import cleanly without dbt present so the helper tests
# (TestExtractWarehouseIdFromHttpPath) can run.
try:
    from dbt.cli.main import dbtRunner
except ImportError:  # pragma: no cover — CI lint-and-test path
    dbtRunner = None  # type: ignore[assignment,misc]  # noqa: N816

from ingestion.guards import (
    FilterResult,
    check_upstream_freshness,
    record_watermarks,
    resolve_upstream_tables_from_card,
    timed_check,
)
from ingestion.utils import get_spark_session

logger = logging.getLogger(__name__)

# Keys are frozensets of selector tags so lookup is order-independent.
# The selector parser normalizes by stripping whitespace and sorting.
_SELECTOR_TO_CARD: dict[frozenset[str], str] = {
    frozenset({"+tag:input_mart", "+tag:dimension"}): "wf-dbt-build-input-marts",
    frozenset({"+tag:intermediate_mart"}): "wf-dbt-build-intermediate-marts",
    frozenset({"tag:output_mart"}): "wf-dbt-build-output-marts",
}


class _DbtWatermarkGuard:
    """Watermark guard parameterized by workflow card ID."""

    def __init__(self, card_id: str) -> None:
        self.workflow_id = card_id

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        try:
            upstream = resolve_upstream_tables_from_card(self.workflow_id, catalog, schema)
        except FileNotFoundError:
            return FilterResult(workflow_id=self.workflow_id, count=1)
        return check_upstream_freshness(spark, catalog, self.workflow_id, upstream)


# Default guard for _GUARD_MODULES registration — uses the first dbt stage.
# The actual guard in main() is parameterized per --select value.
skip_guard = _DbtWatermarkGuard("wf-dbt-build-input-marts")


def _resolve_bundled_dbt_project() -> Path:
    """Return the filesystem path to the wheel-bundled ``dbt_project`` directory.

    The bundled directory ships as ``luxury_lakehouse_dbt_project/`` inside
    the wheel (per ``[tool.hatch.build.targets.wheel.force-include]``). Because
    the directory has no ``__init__.py``, ``importlib.resources.files`` returns
    a ``MultiplexedPath`` that is NOT a real filesystem path — we resolve it
    by taking the parent of any concrete child entry.

    Falls back to the repo-root ``dbt_project`` during local source-checkout
    development.
    """
    try:
        pkg_files = resources.files("luxury_lakehouse_dbt_project")
        # MultiplexedPath has no .parent or __fspath__, but its iterdir yields
        # concrete pathlib.Path objects whose .parent is the real directory.
        children = list(pkg_files.iterdir())
        if children:
            real_parent = Path(str(children[0])).parent
            if (real_parent / "dbt_project.yml").is_file():
                return real_parent
    except (ModuleNotFoundError, ImportError, FileNotFoundError):
        pass

    # Local source checkout fallback: resolve relative to this file
    repo_root = Path(__file__).resolve().parents[2]
    local_path = repo_root / "dbt_project"
    if local_path.is_dir() and (local_path / "dbt_project.yml").is_file():
        return local_path

    msg = (
        "Cannot locate dbt_project — expected the wheel-bundled "
        "luxury_lakehouse_dbt_project package or a local dbt_project/ directory."
    )
    raise RuntimeError(msg)


_PROJECT_WAREHOUSE_NAME_PREFIX = "soccer-analytics-warehouse"
"""Prefix matching the project's SQL warehouse name (per Terraform module)."""

_WAREHOUSE_ID_RE = re.compile(r"^[a-f0-9]{8,}$")
"""Databricks serverless SQL warehouse IDs are 16-char lowercase hex strings.
The regex is intentionally permissive (8+ hex chars) to tolerate any legitimate
variation without accepting obvious garbage like ``xyz`` from a mis-pasted
cluster path (``/sql/protocolv1/o/123/xyz``)."""


def _extract_warehouse_id_from_http_path(http_path: str) -> str:
    """Extract and validate the Databricks SQL warehouse ID from an HTTP path.

    Accepts the canonical ``/sql/1.0/warehouses/<id>`` form and the
    ``//sql/...`` MSYS-safe form (per CLAUDE.md DATABRICKS_HTTP_PATH rule).
    Rejects anything that doesn't end in a hex-looking warehouse ID so that
    downstream ``WorkspaceClient.warehouses.get(id=...)`` doesn't blow up
    with a cryptic 404 when the env var was set to a cluster path or
    other malformed value.

    Raises:
        RuntimeError: When the trailing path segment is not a valid hex
            warehouse ID. The message includes the original path and the
            extracted segment so operators can diagnose the env var.
    """
    candidate = http_path.rsplit("/", 1)[-1]
    if not _WAREHOUSE_ID_RE.match(candidate):
        msg = (
            f"Cannot extract warehouse ID from DATABRICKS_HTTP_PATH={http_path!r}: "
            f"expected trailing segment to be an 8+ hex-char warehouse ID, got {candidate!r}. "
            "Either set DATABRICKS_SQL_WAREHOUSE_ID explicitly or use the "
            "'/sql/1.0/warehouses/<id>' format."
        )
        raise RuntimeError(msg)
    return candidate


def _ensure_databricks_env_vars() -> None:
    """Populate ``DATABRICKS_HOST`` and ``DATABRICKS_HTTP_PATH`` if absent and
    ensure the project SQL warehouse is RUNNING.

    The wheel-bundled ``profiles.yml`` references these via
    ``{{ env_var('DATABRICKS_HOST') }}`` / ``{{ env_var('DATABRICKS_HTTP_PATH') }}``.
    On Databricks serverless they are not auto-injected, so we resolve:

    - ``host`` from ``WorkspaceClient.config.host`` (SDK-discovered).
    - ``http_path`` by listing warehouses via the SDK and picking the one
      whose name starts with the project prefix
      (``soccer-analytics-warehouse``).
    - And we explicitly resume the warehouse via ``start_and_wait`` if it is
      STOPPED, because dbt-databricks' auto-resume retry is unreliable.

    Print-based logging (not ``logger.info``) so output reaches Databricks
    task logs unbuffered — the standard ``logging`` module's stderr handler
    is buffered through Python's wheel-task layer.
    """
    print("[dbt_runner] _ensure_databricks_env_vars: start", file=sys.stderr, flush=True)
    from databricks.sdk import WorkspaceClient

    print("[dbt_runner] creating WorkspaceClient...", file=sys.stderr, flush=True)
    client = WorkspaceClient()
    print("[dbt_runner] WorkspaceClient created", file=sys.stderr, flush=True)

    if not os.environ.get("DATABRICKS_HOST"):
        host = client.config.host
        if not host:
            msg = "Cannot resolve DATABRICKS_HOST: WorkspaceClient.config.host is empty"
            raise RuntimeError(msg)
        os.environ["DATABRICKS_HOST"] = host
        print(f"[dbt_runner] Resolved DATABRICKS_HOST: {host}", file=sys.stderr, flush=True)

    if not os.environ.get("DATABRICKS_TOKEN"):
        # CRITICAL: dbt-databricks falls through to external-browser auth when
        # neither token nor client_secret is set, which hangs indefinitely on
        # a serverless task with no display. Resolve a short-lived OAuth M2M
        # token from the runtime SP context via the SDK's authentication
        # provider chain.
        print("[dbt_runner] resolving DATABRICKS_TOKEN from SDK auth provider...", file=sys.stderr, flush=True)
        try:
            headers = client.config.authenticate()
            auth_header = headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                token = auth_header[len("Bearer ") :]
                os.environ["DATABRICKS_TOKEN"] = token
                print(f"[dbt_runner] Resolved DATABRICKS_TOKEN (length={len(token)})", file=sys.stderr, flush=True)
            else:
                snippet = auth_header[:30]
                print(f"[dbt_runner] WARNING: SDK auth header is not Bearer: {snippet}...", file=sys.stderr, flush=True)
        except Exception as exc:
            print(f"[dbt_runner] ERROR resolving token: {exc}", file=sys.stderr, flush=True)
            raise

    warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")
    if not os.environ.get("DATABRICKS_HTTP_PATH"):
        if not warehouse_id:
            print("[dbt_runner] listing warehouses to find project warehouse...", file=sys.stderr, flush=True)
            warehouses = list(client.warehouses.list())
            print(f"[dbt_runner] found {len(warehouses)} warehouses", file=sys.stderr, flush=True)
            project_wh = next(
                (wh for wh in warehouses if wh.name and wh.name.startswith(_PROJECT_WAREHOUSE_NAME_PREFIX)),
                None,
            )
            if project_wh is None or not project_wh.id:
                msg = (
                    "No SQL warehouse with name starting with "
                    f"{_PROJECT_WAREHOUSE_NAME_PREFIX!r} found via WorkspaceClient.warehouses.list()"
                )
                raise RuntimeError(msg)
            warehouse_id = project_wh.id
            print(f"[dbt_runner] resolved warehouse: {project_wh.name} id={warehouse_id}", file=sys.stderr, flush=True)
        http_path = f"/sql/1.0/warehouses/{warehouse_id}"
        os.environ["DATABRICKS_HTTP_PATH"] = http_path
        print(f"[dbt_runner] DATABRICKS_HTTP_PATH={http_path}", file=sys.stderr, flush=True)
    elif not warehouse_id:
        http_path = os.environ["DATABRICKS_HTTP_PATH"]
        warehouse_id = _extract_warehouse_id_from_http_path(http_path)

    # Explicitly verify warehouse state and resume if STOPPED.
    from databricks.sdk.service.sql import State

    print(f"[dbt_runner] checking warehouse {warehouse_id} state...", file=sys.stderr, flush=True)
    wh = client.warehouses.get(id=warehouse_id)
    print(f"[dbt_runner] warehouse state: {wh.state}", file=sys.stderr, flush=True)
    if wh.state != State.RUNNING:
        print(f"[dbt_runner] warehouse is {wh.state} — starting and waiting...", file=sys.stderr, flush=True)
        client.warehouses.start_and_wait(id=warehouse_id)
        print("[dbt_runner] warehouse is now RUNNING", file=sys.stderr, flush=True)
    print("[dbt_runner] _ensure_databricks_env_vars: done", file=sys.stderr, flush=True)


def run_pipeline(extra_args: list[str] | None = None) -> int:
    """Execute ``dbt build`` against the daily-job target and return the model count.

    Args:
        extra_args: Additional CLI arguments appended to the dbt command
            (e.g. ``["--select", "dim_competitions"]`` to build a single
            model for diagnostics).

    Raises:
        RuntimeError: When dbt reports failure (``success=False``) or when
            required env vars cannot be resolved.
    """
    print("[dbt_runner] run_pipeline: start", file=sys.stderr, flush=True)
    _ensure_databricks_env_vars()

    print("[dbt_runner] resolving bundled dbt_project...", file=sys.stderr, flush=True)
    project_dir = _resolve_bundled_dbt_project()
    profiles_dir = project_dir  # profiles.yml is co-located with the project
    print(f"[dbt_runner] project_dir={project_dir}", file=sys.stderr, flush=True)

    args = [
        "build",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
        "--target",
        "serverless",
    ]
    if extra_args:
        args.extend(extra_args)

    print(f"[dbt_runner] invoking dbt: {' '.join(args)}", file=sys.stderr, flush=True)
    if dbtRunner is None:
        msg = "dbt-core is not installed — install via the 'dbt' extra (e.g. `uv sync --extra dbt`)"
        raise RuntimeError(msg)
    runner = dbtRunner()
    result = runner.invoke(args)
    print(f"[dbt_runner] dbt invoke complete, success={result.success}", file=sys.stderr, flush=True)

    if not result.success:
        msg = f"dbt build failed: {getattr(result, 'exception', None)}"
        logger.error(msg)
        raise RuntimeError(msg)

    node_count = 0
    run_result = getattr(result, "result", None)
    if run_result is not None:
        # dbt build returns a RunExecutionResult with a .results list of per-node outcomes.
        # The `result` attribute on dbtRunnerResult is a Union across all command types
        # (parse → Manifest, docs-generate → CatalogArtifact, etc.) so pyright cannot
        # narrow the type — we access `.results` defensively via getattr.
        node_results = getattr(run_result, "results", None)
        if node_results is not None:
            node_count = len(node_results)

    logger.info("dbt build complete — %d nodes processed", node_count)
    return node_count


def main() -> int:
    """CLI entry point for the Databricks ``python_wheel_task``.

    Forwards any ``sys.argv[1:]`` arguments to dbt (e.g. ``--select dim_competitions``
    for a diagnostic single-model build). The Databricks ``python_wheel_task``
    `parameters` array becomes ``sys.argv[1:]`` when the wheel entry point runs.

    Supports ``--no-watermark`` to bypass the watermark guard — required for
    schema migrations (e.g. BIGINT sweeps where physical tables were dropped and
    need recreation) and manual full-refresh runs.  The flag is consumed by this
    function and NOT forwarded to dbt.

    Returns 0 on success. On failure, the underlying ``RuntimeError`` from
    ``run_pipeline`` propagates out of this function — Databricks treats an
    uncaught exception in a ``python_wheel_task`` as task failure, but a
    function that returns ``1`` is silently treated as success. Do NOT catch
    here. Returning a non-zero int does NOT fail the task.

    NOTE: ``@workflow`` is intentionally NOT applied. dbt_runner invokes dbt
    via ``dbtRunner().invoke(args)`` — no Spark infrastructure, no Delta writes.
    The watermark guard below is the only Spark touchpoint, used purely for
    metadata reads (DESCRIBE HISTORY + watermarks table).
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raw_args = sys.argv[1:] if len(sys.argv) > 1 else []

    # --no-watermark: bypass the watermark guard (consumed here, not forwarded to dbt).
    skip_watermark = "--no-watermark" in raw_args
    extra_args = [a for a in raw_args if a != "--no-watermark"] or None
    if skip_watermark:
        logger.info("--no-watermark: watermark guard bypassed")

    # Resolve selector from args to find matching workflow card
    selector_str = ""
    if extra_args:
        select_args: list[str] = []
        capture = False
        for arg in extra_args:
            if arg == "--select":
                capture = True
                continue
            if capture and not arg.startswith("--"):
                select_args.append(arg)
            else:
                capture = False
        selector_str = " ".join(select_args)

    card_id = _SELECTOR_TO_CARD.get(frozenset(selector_str.split()) if selector_str else frozenset())
    spark: SparkSession | None = None
    if card_id is not None and not skip_watermark:
        spark = get_spark_session()
        watermark_guard = _DbtWatermarkGuard(card_id)
        fr = timed_check(watermark_guard, spark, "soccer_analytics", "dev_gold")
        if fr.count == 0:
            logger.info("Watermark skip: %s — no upstream changes", card_id)
            return 0

    run_pipeline(extra_args=extra_args)

    # Record watermarks after successful dbt build.
    # FileNotFoundError catch: if card resolution fails (e.g. local CLI run
    # outside wheel and source tree), the dbt build already succeeded and
    # should not be marked as failed.  Next run re-processes (same as first
    # run).  All other exceptions (Spark, Delta) propagate — ADR-002.
    if card_id is not None:
        if spark is None:
            spark = get_spark_session()
        try:
            upstream = resolve_upstream_tables_from_card(card_id, "soccer_analytics", "dev_gold")
            record_watermarks(spark, "soccer_analytics", card_id, upstream)
        except FileNotFoundError:
            logger.error("Failed to record watermarks for %s — card file not found", card_id, exc_info=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())

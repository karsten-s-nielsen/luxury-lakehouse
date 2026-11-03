"""The dbt hand-off of a run (spec §10.3, §11, §21; plan Task 23).

A DuckDB catalog file allows one attacher, so `run_dbt` detaches the working copy, lets dbt-duckdb attach it as the
only attacher (`dbt build` on the project in `v2/dbt`), closes dbt-duckdb's DuckDB environment — it would otherwise
keep the file attached in this process — and attaches the working copy again. The run's `run_results.json` is kept
at `runs/dbt/<run_id>/run_results.json` in the lake root (spec §21, guess 38). dbt never hashes: every key comes from
bronze or from resolution (ADR-003).
"""

from __future__ import annotations

import fnmatch
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, cast

import jinja2

from lakehouse.bronze.writer import BRONZE_COMMENT
from lakehouse.catalog.duck import EXTENSION_DIR_ENV, writer_data_path
from lakehouse.catalog.working import WorkingCatalog, catalog_extensions
from lakehouse.config.model import ResourceCaps
from lakehouse.logging import get_logger
from lakehouse.observability import dbt_manifest_key
from lakehouse.storage.base import Storage
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV, resolve_encryption_key

__all__ = [
    "NATIVE_TABLE_ALLOWLIST",
    "DbtNodeResult",
    "DbtResult",
    "allowed_native_table",
    "parse_manifest",
    "project_dir",
    "render_profile",
    "run_dbt",
    "run_results_key",
    "tables_outside_allowlist",
]

# The only tables that DuckDB itself may write into the lake root (spec §9): dimensions, links, lineage and ops.
NATIVE_TABLE_ALLOWLIST: Final = ("dim_*", "*_links", "ops_*", "lineage_*")


def allowed_native_table(name: str, comment: str | None, *, encrypted_local: bool) -> bool:
    """§23.6: whether DuckDB may write table `name` natively into the lake root. A dim/link/lineage/ops table is
    always allowed. A bronze table (its comment is `BRONZE_COMMENT`) is allowed ONLY on an encrypted-local lake
    (plan T12), where bronze is native by design and `ENCRYPTED` protects its row-level data; on any other mode a
    native bronze table is a leak of row-level data into the lake root. Anything else — e.g. a rogue dbt-
    materialized row-level table (no bronze comment) — is forbidden in every mode."""
    if any(fnmatch.fnmatchcase(name, pattern) for pattern in NATIVE_TABLE_ALLOWLIST):
        return True
    return encrypted_local and comment == BRONZE_COMMENT


_PACKAGED: Final = "_dbt"  # the project inside the installed wheel (pyproject force-include)
_PROFILE_TEMPLATE: Final = "profiles.yml.j2"
_LOG_ARGS: Final = ("--log-format", "json", "--log-level", "warn")

_log = get_logger(__name__)


@dataclass(frozen=True)
class DbtNodeResult:
    unique_id: str
    status: str
    message: str | None


@dataclass(frozen=True)
class DbtResult:
    success: bool
    results: list[DbtNodeResult]
    run_results_key: str


def project_dir() -> Path:
    """The dbt project: `v2/dbt` in a source tree, `lakehouse/_dbt` in an installed wheel."""
    source = Path(__file__).resolve().parents[3] / "dbt"
    if (source / "dbt_project.yml").is_file():
        return source
    packaged = Path(__file__).resolve().parents[1] / _PACKAGED
    if (packaged / "dbt_project.yml").is_file():
        return packaged
    raise FileNotFoundError(
        "the dbt project is neither in the source tree (v2/dbt) nor in the package (lakehouse/_dbt)"
    )


def run_results_key(run_id: str) -> str:
    return f"runs/dbt/{run_id}/run_results.json"


def render_profile(*, catalog_path: Path, caps: ResourceCaps, storage: Storage, env: Mapping[str, str]) -> str:
    """The dbt profile of a run: the DuckLake attach of `catalog_path` as `lake`, the caps, the extensions and the
    storage's secrets as environment-variable references (no secret value)."""
    template = jinja2.Environment(
        loader=jinja2.FileSystemLoader(project_dir()),
        undefined=jinja2.StrictUndefined,
        keep_trailing_newline=True,
        autoescape=False,  # noqa: S701 - renders YAML, not HTML
    ).get_template(_PROFILE_TEMPLATE)
    key = resolve_encryption_key(storage.roots, env)
    return template.render(
        catalog_path=catalog_path.as_posix(),
        threads=caps.threads,
        memory_limit=caps.memory_limit,
        extensions=list(catalog_extensions(storage)),
        extension_directory=env.get(EXTENSION_DIR_ENV, ""),
        secrets=storage.dbt_secrets(),
        # Encrypted-local: dbt opens the catalog through a ducklake SECRET whose key is an env_var reference, so the
        # rendered profile holds no key value. dbt-duckdb renders the nested map; the attach names the secret.
        encrypted=key is not None,
        data_path=writer_data_path(storage.roots) if key is not None else "",
        encryption_key_ref="{{ env_var('" + ENCRYPTION_KEY_ENV + "') }}",
        # Keyed dbt session: encrypt spill too (spec §13/§spill, plan T7) — dbt is the heaviest transform, so an
        # over-memory_limit build must not spill a private match's silver/gold rows as plaintext. A cleaned temp dir
        # beside the catalog keeps the spill local and removable.
        temp_directory=(catalog_path.parent / "dbt_tmp").as_posix() if key is not None else "",
    )


def _invoke(args: Sequence[str]) -> Any:
    """`dbtRunner().invoke`, then close dbt-duckdb's DuckDB environment, which keeps the catalog attached."""
    from dbt.adapters.duckdb.connections import DuckDBConnectionManager  # dbt imports are slow: only when needed
    from dbt.cli.main import dbtRunner

    try:
        runner: Any = dbtRunner()
        return runner.invoke(list(args))
    finally:
        manager: Any = DuckDBConnectionManager
        environment: Any = manager._ENV  # dbt-duckdb has no public close; close_all drops it only
        if environment is not None:
            environment.close()
        manager.close_all_connections()


def _write_profile(folder: Path, text: str) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "profiles.yml").write_text(text, encoding="utf-8")
    return folder


def parse_manifest(
    work_dir: Path, *, caps: ResourceCaps, project: Path | None = None, storage: Storage | None = None
) -> dict[str, Any]:
    """`dbt parse` of the project (no connection is opened) and its `manifest.json`."""
    chosen = project if project is not None else project_dir()
    if storage is None:
        text = (
            'lakehouse:\n  target: run\n  outputs:\n    run:\n      type: duckdb\n      path: ":memory:"\n'
            "      schema: main\n"
        )
    else:
        text = render_profile(catalog_path=work_dir / "lake.ducklake", caps=caps, storage=storage, env=os.environ)
    profiles = _write_profile(work_dir / "profiles", text)
    target = work_dir / "target"
    result = _invoke(
        [
            "parse",
            "--project-dir",
            str(chosen),
            "--profiles-dir",
            str(profiles),
            "--target-path",
            str(target),
            "--log-path",
            str(work_dir / "logs"),
            *_LOG_ARGS,
        ]
    )
    if not result.success:
        raise RuntimeError(f"dbt parse failed: {result.exception}")
    return cast("dict[str, Any]", json.loads((target / "manifest.json").read_text(encoding="utf-8")))


def tables_outside_allowlist(manifest: Mapping[str, Any]) -> list[str]:
    """The models materialised as a table or incremental whose name is not in `NATIVE_TABLE_ALLOWLIST` (§23.6)."""
    found: list[str] = []
    for node in cast("Mapping[str, Mapping[str, Any]]", manifest["nodes"]).values():
        if node["resource_type"] != "model":
            continue
        materialized = node["config"]["materialized"]
        name = str(node["name"])
        if materialized in ("table", "incremental") and not any(
            fnmatch.fnmatchcase(name, pattern) for pattern in NATIVE_TABLE_ALLOWLIST
        ):
            found.append(name)
    return sorted(found)


def _results(target: Path) -> tuple[list[DbtNodeResult], bytes]:
    path = target / "run_results.json"
    if not path.is_file():
        return [], b""
    raw = path.read_bytes()
    document = cast("dict[str, Any]", json.loads(raw))
    results = [
        DbtNodeResult(str(r["unique_id"]), str(r["status"]), None if r.get("message") is None else str(r["message"]))
        for r in cast("list[dict[str, Any]]", document.get("results", []))
    ]
    return results, raw


def run_dbt(
    cat: WorkingCatalog,
    *,
    run_id: str,
    caps: ResourceCaps,
    select: str | None = None,
    project: Path | None = None,
) -> DbtResult:
    """`dbt build` on the working copy (see the module docstring). A failed model gives `success=False` with its
    node result; the working copy is attached again in every case."""
    chosen = project if project is not None else project_dir()
    catalog_path = cat.detach()
    folder = catalog_path.parent / "dbt"
    target = folder / "target"
    try:
        profiles = _write_profile(
            folder / "profiles",
            render_profile(catalog_path=catalog_path, caps=caps, storage=cat.storage, env=os.environ),
        )
        args = [
            "build",
            "--project-dir",
            str(chosen),
            "--profiles-dir",
            str(profiles),
            "--target-path",
            str(target),
            "--log-path",
            str(folder / "logs"),
            *_LOG_ARGS,
        ]
        if select is not None:
            args += ["--select", select]
        outcome = _invoke(args)
    finally:
        cat.attach()
    results, raw = _results(target)
    key = run_results_key(run_id)
    if raw:
        cat.storage.put("lake", key, raw)
    manifest_path = target / "manifest.json"
    if manifest_path.is_file():  # retained beside run_results.json for the dependency view (plan Task 4)
        cat.storage.put("lake", dbt_manifest_key(run_id), manifest_path.read_bytes())
    success = bool(outcome.success) and outcome.exception is None
    if outcome.exception is not None:
        results = [*results, DbtNodeResult("dbt", "error", str(outcome.exception))]
    _log.info(
        "dbt build",
        extra={
            "source": "dbt",
            "success": success,
            "rows": len(results),
            "failed": [r.unique_id for r in results if r.status in ("error", "fail")],
        },
    )
    return DbtResult(success=success, results=results, run_results_key=key)

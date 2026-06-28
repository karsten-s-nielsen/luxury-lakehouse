"""Guardrail: task + environment + depends_on blocks stay alphabetical for
every `databricks_job` resource declared under `terraform/`.

The Databricks Terraform provider matches nested blocks positionally against
state. State stores blocks sorted alphabetically by key; declaring them in
any other order produces phantom drift in every `terraform plan`. This test
keeps every `databricks_job` aligned so CI plan reviews stay signal, not
noise.

Originally written for `databricks_job.data_ingestion` (the only resource
with 29 task blocks at the time). Generalized in the PR #128 follow-up
cycle to walk ALL `databricks_job` resources in `terraform/**/*.tf`, so
any future job resource is automatically covered without a new test.

Empirical note: the same positional-matching behavior does NOT affect
`access_control` blocks inside `databricks_permissions` resources —
verified via `terraform plan -target=databricks_permissions.sql_warehouse`
which reported `No changes` despite that resource having heterogeneous,
unsorted principal blocks. The provider appears to match `access_control`
by principal identity, not position. A generic ACL-ordering test is
therefore intentionally omitted; the existing
`test_sec4_ci_sp_job_owner.py` still asserts ordering on the two daily-job
ACLs as a defensive per-resource guardrail.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_TERRAFORM_ROOT = _REPO / "terraform"

_JOB_RESOURCE_RE = re.compile(r'^resource\s+"databricks_job"\s+"(\w+)"\s*\{', re.MULTILINE)


def _find_all_databricks_jobs() -> list[tuple[Path, str]]:
    """Return the ordered list of (tf_file, resource_name) for every
    `databricks_job` resource declared anywhere under `terraform/`."""
    found: list[tuple[Path, str]] = []
    for tf_file in sorted(_TERRAFORM_ROOT.rglob("*.tf")):
        text = tf_file.read_text(encoding="utf-8")
        for m in _JOB_RESOURCE_RE.finditer(text):
            found.append((tf_file, m.group(1)))
    return found


def _extract_top_level_block_keys(
    text: str,
    resource_type: str,
    resource_name: str,
    block_type: str,
    key_field: str,
) -> list[str]:
    """Return the ordered list of ``{key_field} = "..."`` values for every
    top-level ``{block_type} {{ ... }}`` block inside the named resource
    body.

    Uses brace-depth tracking so nested blocks (e.g. ``depends_on`` inside a
    ``task``) are skipped — only depth-2 blocks count (depth 1 = resource body,
    depth 2 = top-level child block).
    """
    lines = text.splitlines()
    keys: list[str] = []
    depth = 0
    in_resource = False
    current_block: str | None = None
    block_start_depth: int | None = None
    key_pattern = re.compile(rf'^\s*{re.escape(key_field)}\s*=\s*"([^"]+)"')
    resource_start = re.compile(rf'^resource\s+"{re.escape(resource_type)}"\s+"{re.escape(resource_name)}"\s*\{{')
    block_start = re.compile(rf"^\s*{re.escape(block_type)}\s*\{{")

    for line in lines:
        if not in_resource:
            if resource_start.search(line):
                in_resource = True
                depth = 1
            continue
        open_braces = line.count("{")
        close_braces = line.count("}")
        if current_block is None and depth == 1 and block_start.match(line):
            current_block = block_type
            block_start_depth = depth + open_braces
        if current_block is not None and key_pattern.match(line) and depth == block_start_depth:
            m = key_pattern.match(line)
            assert m is not None
            keys.append(m.group(1))
            current_block = None
            block_start_depth = None
        depth += open_braces - close_braces
        if depth <= 0:
            break
    return keys


_DEPENDS_ON_RE = re.compile(r'depends_on\s*\{\s*task_key\s*=\s*"([^"]+)"', re.MULTILINE)


def _extract_depends_on_by_task(text: str, resource_name: str) -> dict[str, list[str]]:
    """Walk the named `databricks_job` resource body; for each top-level
    `task { ... }` block, collect the ordered list of `task_key` values
    declared under each nested `depends_on { ... }` block.

    Handles BOTH syntaxes:
      depends_on {
        task_key = "X"
      }
      depends_on { task_key = "X" }

    Returns {outer_task_key: [dep_task_key_1, dep_task_key_2, ...]}.
    """
    lines = text.splitlines(keepends=True)
    result: dict[str, list[str]] = {}
    depth = 0
    in_resource = False
    task_start_depth: int | None = None
    task_start_idx: int | None = None
    task_outer_key: str | None = None
    resource_start = re.compile(rf'^resource\s+"databricks_job"\s+"{re.escape(resource_name)}"\s*\{{')
    task_open = re.compile(r"^  task\s*\{\s*$")
    # Outer task_key is at indent 4 (inside resource at indent 2, task at indent 4).
    outer_task_key_re = re.compile(r'^    task_key\s*=\s*"([^"]+)"')

    for idx, line in enumerate(lines):
        if not in_resource:
            if resource_start.search(line):
                in_resource = True
                depth = 1
            continue
        opens = line.count("{")
        closes = line.count("}")
        if task_start_depth is None and depth == 1 and task_open.match(line):
            task_start_depth = depth + opens
            task_start_idx = idx
            task_outer_key = None
        elif task_start_depth is not None and task_outer_key is None:
            m = outer_task_key_re.match(line)
            if m:
                task_outer_key = m.group(1)

        depth += opens - closes

        if task_start_depth is not None and depth < task_start_depth:
            assert task_start_idx is not None
            body = "".join(lines[task_start_idx : idx + 1])
            deps = _DEPENDS_ON_RE.findall(body)
            if task_outer_key and deps:
                result[task_outer_key] = deps
            task_start_depth = None
            task_start_idx = None
            task_outer_key = None
        if depth <= 0:
            break
    return result


def _rel(path: Path) -> str:
    return str(path.relative_to(_REPO)).replace("\\", "/")


def test_at_least_one_databricks_job_exists() -> None:
    """Sanity: the walker finds at least one databricks_job. A future regex
    regression producing an empty list would otherwise make every per-job
    assertion vacuously pass."""
    jobs = _find_all_databricks_jobs()
    assert jobs, "no `databricks_job` resources found anywhere under terraform/ — parser regression?"


def test_all_databricks_jobs_task_blocks_alphabetical() -> None:
    """For every `databricks_job` resource in terraform/, the top-level
    task blocks must be sorted alphabetically by task_key to match
    Databricks provider positional-matching behavior against state."""
    jobs = _find_all_databricks_jobs()
    errors: list[str] = []
    for tf_file, resource_name in jobs:
        text = tf_file.read_text(encoding="utf-8")
        task_keys = _extract_top_level_block_keys(text, "databricks_job", resource_name, "task", "task_key")
        if not task_keys:
            continue
        if task_keys != sorted(task_keys):
            errors.append(
                f"{_rel(tf_file)}:databricks_job.{resource_name}: "
                f"task blocks not sorted alphabetically. "
                f"Got {task_keys}, expected {sorted(task_keys)}."
            )
    assert not errors, "\n".join(errors)


def test_all_databricks_jobs_environment_blocks_alphabetical() -> None:
    """For every `databricks_job` resource in terraform/, the top-level
    environment blocks must be sorted alphabetically by environment_key."""
    jobs = _find_all_databricks_jobs()
    errors: list[str] = []
    for tf_file, resource_name in jobs:
        text = tf_file.read_text(encoding="utf-8")
        env_keys = _extract_top_level_block_keys(
            text, "databricks_job", resource_name, "environment", "environment_key"
        )
        if not env_keys:
            continue
        if env_keys != sorted(env_keys):
            errors.append(
                f"{_rel(tf_file)}:databricks_job.{resource_name}: "
                f"environment blocks not sorted alphabetically. "
                f"Got {env_keys}, expected {sorted(env_keys)}."
            )
    assert not errors, "\n".join(errors)


def test_all_databricks_jobs_depends_on_blocks_alphabetical() -> None:
    """For every `databricks_job` resource, every nested `depends_on` block
    sequence inside a task must be sorted alphabetically by referenced
    task_key. The Databricks provider matches `depends_on` positionally too."""
    jobs = _find_all_databricks_jobs()
    errors: list[str] = []
    for tf_file, resource_name in jobs:
        text = tf_file.read_text(encoding="utf-8")
        deps = _extract_depends_on_by_task(text, resource_name)
        for task_key, dep_list in deps.items():
            if dep_list != sorted(dep_list):
                errors.append(
                    f"{_rel(tf_file)}:databricks_job.{resource_name} task {task_key!r}: "
                    f"depends_on order {dep_list} is not sorted; expected {sorted(dep_list)}"
                )
    assert not errors, "\n".join(errors)


def test_data_ingestion_parser_count_anchor() -> None:
    """Anchor the parser against known counts on the `data_ingestion` job
    so a future regex regression producing an empty list is caught as a
    parser bug, not a false pass. Counts verified live 2026-04-16 against
    PR #128 state."""
    tf_file = _REPO / "terraform" / "modules" / "workflows" / "main.tf"
    text = tf_file.read_text(encoding="utf-8")
    env_keys = _extract_top_level_block_keys(text, "databricks_job", "data_ingestion", "environment", "environment_key")
    task_keys = _extract_top_level_block_keys(text, "databricks_job", "data_ingestion", "task", "task_key")
    # 7 → 6 in SkillCorner ingestion rewrite (2026-05-16): removed `tracking`
    # environment block (was kloppy-only for SkillCorner open data); tasks
    # moved to `default`.
    # 6 → 7 (2026-06-05): added `lakebase` environment (wheel + databricks-sdk) so
    # refresh_synced_tables can call ws.postgres.* — see
    # test_refresh_synced_tables_env_ships_databricks_sdk.
    assert len(env_keys) == 7, f"expected 7 environment blocks on data_ingestion, parser found {len(env_keys)}"
    # 29 → 30 in PR-Cycle-A (2026-04-30): added `preflight_idsse` for runtime
    # chunk discovery feeding the `ingest_idsse` for_each_task fan-out.
    # 30 → 31 in PR-Cycle-B (2026-05-01): split `import_obso_results` out of
    # hf_sync into its own scheduled task so compute_pausa can declare an
    # explicit dependency on the OBSO import.
    # 31 → 33 in PR-Cycle-C PR-β (2026-05-02): replace single `dbt_build`
    # task with three sequential tasks (`dbt_build_input_marts`,
    # `_intermediate_marts`, `_output_marts`) per ADR-019. Net +2 tasks
    # (3 added, 1 removed).
    # 33 → 32 in SK3-MIG-B PR-alpha (2026-05-03): XG1-RETIRE removed
    # `compute_xg_model` (per ADR-023).
    # 32 → 31 in f2v-dim-fix (2026-05-07): removed `compute_embeddings_v1`
    # (v1 Doc2Vec deprecated — zero downstream consumers).
    # 31 → 33 in TC-1 (2026-05-12): added `preflight_tracking_context` +
    # `compute_tracking_context_iteration` inner for_each_task block
    # (existing `compute_tracking_context` becomes a for_each_task parent).
    # 33 → 34 in SPADL-VAEP-chunked (2026-05-17): added `preflight_spadl_vaep`
    # + `compute_spadl_vaep_iteration` inner for_each_task block. Net +1
    # because old monolithic `compute_spadl_vaep` becomes the for_each_task
    # parent (no new top-level key for it) but `preflight_spadl_vaep` is new.
    # 34 → 35 in Gradient Sports ingestion (2026-05-19): added `ingest_gradientsports`.
    # 35 → 36 in Gradient Sports fan-out (2026-05-20): monolithic task split into
    # `preflight_gradientsports` + `ingest_gradientsports` (for_each_task parent)
    # + `ingest_gradientsports_iteration` (inner). Net +1 top-level block.
    # 38 → 39 (ADR-058, 2026-06-17): added `compute_action_context_statsbomb` (statsbomb sb360
    # exits the per-match drain into a single distributed cogroup job).
    assert len(task_keys) == 40, f"expected 40 task blocks on data_ingestion, parser found {len(task_keys)}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

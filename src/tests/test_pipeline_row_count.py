"""Pipeline conformance tests: row count returns + workflow mapping coverage.

TestRunPipelineReturnsInt:
    Every run_pipeline() must return int (row count) so CostEstimateHook
    can populate the row_count column in fct_workflow_costs.

TestTaskWorkflowMappingCoverage:
    Every task_key in the Terraform job definitions must be present in the
    task_workflow_mapping.csv seed. Without this, the warm-tier join in
    fct_workflow_costs silently produces NULL enrichment columns.
"""

from __future__ import annotations

import ast
import csv
import re
from pathlib import Path

import pytest

_INGESTION_DIR = Path(__file__).resolve().parent.parent / "ingestion"


def _find_run_pipeline_modules() -> list[tuple[str, Path]]:
    """Find all ingestion modules with a run_pipeline function."""
    modules: list[tuple[str, Path]] = []
    for py_file in sorted(_INGESTION_DIR.glob("*.py")):
        if py_file.name.startswith("_"):
            continue
        try:
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "run_pipeline":
                modules.append((py_file.stem, py_file))
                break
    return modules


_MODULES = _find_run_pipeline_modules()


@pytest.mark.parametrize(
    "module_name,module_path",
    _MODULES,
    ids=[m[0] for m in _MODULES],
)
def test_run_pipeline_returns_int(module_name: str, module_path: Path) -> None:
    """run_pipeline() must have return type annotation ``int``.

    This ensures the @workflow lifecycle runner can pass the row count
    to CostEstimateHook.on_complete(). Without this, the row_count
    column in fct_workflow_costs is always NULL.
    """
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "run_pipeline":
            assert node.returns is not None, (
                f"{module_name}.run_pipeline() has no return type annotation. "
                "Add ``-> int`` and return the row count from write_delta_table()."
            )
            ret_source = ast.unparse(node.returns)
            assert ret_source == "int", (
                f"{module_name}.run_pipeline() return type is ``{ret_source}``, expected ``int``. "
                "Change to ``-> int`` and return the row count (0 for skipped runs)."
            )
            return

    pytest.fail(f"run_pipeline not found in {module_name}")


# ---------------------------------------------------------------------------
# TestTaskWorkflowMappingCoverage
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_SEED_CSV = _PROJECT_ROOT / "dbt_project" / "seeds" / "task_workflow_mapping.csv"
_TF_WORKFLOW_MODULE = _PROJECT_ROOT / "terraform" / "modules" / "workflows" / "main.tf"
_TF_DEV_MAIN = _PROJECT_ROOT / "terraform" / "environments" / "dev" / "main.tf"

# Regex matches top-level task_key assignments (4-space indent), not depends_on refs (6+).
_TASK_KEY_RE = re.compile(r"^\s{4}task_key\s+=\s+\"(\w+)\"", re.MULTILINE)


def _terraform_task_keys() -> set[str]:
    """Extract all top-level task_keys from Terraform job definitions."""
    keys: set[str] = set()
    for tf_file in (_TF_WORKFLOW_MODULE, _TF_DEV_MAIN):
        if tf_file.exists():
            keys.update(_TASK_KEY_RE.findall(tf_file.read_text(encoding="utf-8")))
    return keys


def _seed_task_keys() -> set[str]:
    """Extract all task_keys from the dbt seed CSV."""
    keys: set[str] = set()
    with _SEED_CSV.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = row.get("task_key", "").strip()
            if key:
                keys.add(key)
    return keys


class TestTaskWorkflowMappingCoverage:
    """Every Terraform task_key must have a workflow_id in the seed.

    Without a mapping entry, the warm-tier LEFT JOIN in fct_workflow_costs
    cannot match the pipeline's CostEstimateHook data, and enrichment
    columns (duration, entity_count, cold_start, etc.) are silently NULL.

    If this test fails, add the missing task_key to:
        dbt_project/seeds/task_workflow_mapping.csv
    then run: ``dbt seed --select task_workflow_mapping``
    """

    def test_all_terraform_tasks_mapped_in_seed(self) -> None:
        tf_keys = _terraform_task_keys()
        seed_keys = _seed_task_keys()

        assert tf_keys, "No task_keys found in Terraform — check file paths"
        assert seed_keys, "No task_keys found in seed CSV — check file path"

        missing = tf_keys - seed_keys
        assert not missing, (
            "Terraform task_key(s) missing from task_workflow_mapping.csv "
            "(warm-tier join will produce NULL enrichment):\n"
            + "\n".join(f"  - {k}" for k in sorted(missing))
            + "\n\nAdd entries to: dbt_project/seeds/task_workflow_mapping.csv"
        )

"""XG1-RETIRE regression test — prevents accidental re-introduction of v1.

Three assertions:
1. ``import ingestion.xg_model`` raises ModuleNotFoundError.
2. Glob across 7 layers (src/, scripts/, notebooks/, dbt_project/, terraform/,
   workflow-cards/, hf_taipy_app/) returns zero hits for v1 names.
3. pyproject.toml [project.scripts] has no v1 entry-point.
4. No code in src/ imports from ``ingestion.xg_model``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_ingestion_xg_model_module_does_not_exist() -> None:
    """Direct import attempt — survives any leftover __init__.py re-export."""
    with pytest.raises(ModuleNotFoundError):
        import ingestion.xg_model  # noqa: F401


# Source-control invariants. Note: `xg_model.py` is intentionally NOT in this list
# because `src/analytics/xg_model.py` is load-bearing for v2 (shared serialization
# + freeze-frame parsing). The retired v1 module is `src/ingestion/xg_model.py`,
# checked separately below via `test_no_ingestion_xg_model_module`.
_FORBIDDEN_NAMES = (
    "fct_xg_predictions.sql",  # dbt mart
    "stg_xg__predictions.sql",  # dbt staging
    "wf-xg-v1.yaml",  # workflow card
    "xg-model-card.md",  # v1 HF model card (NB: actual filename, not the
    # plan's xg-model-statsbomb-wyscout.md which is F2V v1)
    "train_xg_model_hf.py",  # v1 HF trainer
    "train_xg_model.py",  # v1 Databricks-notebook trainer
)

_LAYER_DIRS = (
    "src",
    "scripts",
    "notebooks",
    "dbt_project",
    "terraform",
    "workflow-cards",
    "hf_taipy_app",
)

# Build artifacts / generated content that should be ignored. dbt's target/
# carries compiled SQL from prior runs; cleaning it is operator-driven.
_IGNORED_PATH_FRAGMENTS = (
    "target",  # dbt_project/target/
    "__pycache__",
    ".pytest_cache",
    "node_modules",
)


def _is_ignored(path: Path) -> bool:
    return any(frag in path.parts for frag in _IGNORED_PATH_FRAGMENTS)


@pytest.mark.parametrize("layer_dir", _LAYER_DIRS)
@pytest.mark.parametrize("forbidden", _FORBIDDEN_NAMES)
def test_no_v1_files_in_layer(layer_dir: str, forbidden: str) -> None:
    """Recursive glob across each layer dir for each forbidden filename."""
    layer_path = REPO_ROOT / layer_dir
    if not layer_path.exists():
        pytest.skip(f"Layer dir does not exist: {layer_path}")
    matches = [p for p in layer_path.rglob(forbidden) if not _is_ignored(p)]
    assert matches == [], (
        f"Forbidden v1 file found post-XG1-RETIRE: "
        f"{[str(m.relative_to(REPO_ROOT)) for m in matches]} in {layer_dir}/. "
        "Verify XG1-RETIRE drop ordering completed (spec §6.1)."
    )


def test_no_ingestion_xg_model_module() -> None:
    """The retired v1 module path must not exist."""
    candidate = REPO_ROOT / "src" / "ingestion" / "xg_model.py"
    assert not candidate.exists(), (
        f"Retired v1 module still present at {candidate.relative_to(REPO_ROOT)}. Verify Task 4.3 deletion."
    )


def test_pyproject_has_no_v1_entry_point() -> None:
    """pyproject.toml [project.scripts] must not contain a v1 entry-point."""
    pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    forbidden_lines = [
        line
        for line in pyproject.splitlines()
        if "ingestion.xg_model:" in line or line.strip().startswith("compute_xg_model =")
    ]
    assert not forbidden_lines, f"pyproject.toml still contains v1 entry-point line(s): {forbidden_lines}"


def test_no_xg_v1_imports_in_src() -> None:
    """No code in src/ imports from ingestion.xg_model.

    Self-exempt: this regression test file's pytest.raises block deliberately
    references `import ingestion.xg_model` to assert the module is gone.
    """
    src_dir = REPO_ROOT / "src"
    self_path = Path(__file__).resolve()
    forbidden_imports: list[str] = []
    for py_file in src_dir.rglob("*.py"):
        if py_file.resolve() == self_path:
            continue
        text = py_file.read_text(encoding="utf-8")
        if "from ingestion.xg_model " in text or "import ingestion.xg_model" in text:
            forbidden_imports.append(str(py_file.relative_to(REPO_ROOT)))
    assert not forbidden_imports, f"v1 imports found in src/: {forbidden_imports}"

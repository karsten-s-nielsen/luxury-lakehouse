"""B-ac elastic-retirement structural gate (sk4118 cycle, spec §5.4 Option B-ac).

The standalone ELASTIC producer (`wf-elastic-sync` / `ingestion.elastic_sync`), the local greedy
engine (`analytics.elastic_sync`), and the `bronze.elastic_sync_results` table are retired. Elastic
frame linkage now rides on the AC drain's action-grain columns (`elastic_frame_id`/`elastic_receive_*`)
in `fct_action_context` / `stg_action_context__values`.

This gate is the "watch it fail first" guard for that retirement: it asserts every registration is
gone so a half-removed task cannot silently break mega-job parity, and it is non-vacuous (it proves the
anchor files it scans actually exist). Consumers were re-pointed in the same PR; see the DROP migration
`scripts/migrations/2026-09-20-drop-elastic-sync-results.sql` (operator-applied).
"""

from __future__ import annotations

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Files that may legitimately still NAME the retired artifacts — historical notes, the DROP
# migration, the committed legacy oracle, and the point-in-time bronze schema snapshot (which
# records live state as of its capture date, before the operator DROP runs).
_ALLOWED_MENTIONS = {
    "scripts/migrations/2026-09-20-drop-elastic-sync-results.sql",
    "src/tests/action_context/oracle_map.py",
    "src/tests/action_context/conftest.py",
    "src/tests/test_elastic_retirement.py",
    "src/tests/fixtures/idsse_bronze_schema_snapshot.json",
    "src/tests/fixtures/action_context/idsse/J03WMX_p1/oracle_elastic_sync_results.parquet",
}


def _exists(rel: str) -> Path:
    p = _REPO_ROOT / rel
    assert p.exists(), f"anchor {rel} missing — this gate would be vacuous"
    return p


def test_elastic_sync_modules_deleted() -> None:
    for rel in ("src/ingestion/elastic_sync.py", "src/analytics/elastic_sync.py"):
        assert not (_REPO_ROOT / rel).exists(), f"{rel} must be deleted (B-ac elastic retirement)"


def test_no_importers_of_the_retired_modules() -> None:
    offenders: list[str] = []
    for base in ("src", "scripts", "hf_taipy_app"):
        root = _REPO_ROOT / base
        if not root.exists():
            continue
        for py in root.rglob("*.py"):
            if "__pycache__" in py.parts:
                continue
            text = py.read_text(encoding="utf-8")
            if "analytics.elastic_sync" in text or "ingestion.elastic_sync" in text:
                rel = py.relative_to(_REPO_ROOT).as_posix()
                if rel not in _ALLOWED_MENTIONS:
                    offenders.append(rel)
    assert not offenders, f"live importers of the retired elastic modules remain: {offenders}"


def test_wf_elastic_sync_card_deleted_and_undepended() -> None:
    """The card is gone and no OTHER card DEPENDS on it. A historical comment that names the retired
    card is fine (that is documentation); a live ``depends_on: - wf-elastic-sync`` edge is not — so
    this checks the parsed ``depends_on`` list, not raw text."""
    import re

    import yaml

    cards_dir = _exists("workflow-cards")
    assert not (cards_dir / "wf-elastic-sync.yaml").exists(), "wf-elastic-sync.yaml must be deleted"
    # Workflow cards are YAML frontmatter + a Markdown body — parse only the frontmatter block
    # (mirrors _load_card in test_card_parity_with_terraform.py; the body is prose, not YAML).
    frontmatter = re.compile(r"^---\s*\n(.*?\n)---\s*\n", re.DOTALL)
    offenders: list[str] = []
    for c in cards_dir.glob("*.yaml"):
        m = frontmatter.match(c.read_text(encoding="utf-8"))
        if not m:
            continue
        doc = yaml.safe_load(m.group(1)) or {}
        if isinstance(doc, dict) and "wf-elastic-sync" in (doc.get("depends_on") or []):
            offenders.append(c.name)
    assert not offenders, f"cards still declare a depends_on edge to wf-elastic-sync: {offenders}"


def test_compute_elastic_sync_task_deregistered() -> None:
    seed = _exists("dbt_project/seeds/task_workflow_mapping.csv").read_text(encoding="utf-8")
    assert "compute_elastic_sync" not in seed, "seed still maps compute_elastic_sync"

    pyproject = _exists("pyproject.toml").read_text(encoding="utf-8")
    assert "compute_elastic_sync =" not in pyproject, "pyproject still declares the compute_elastic_sync entry point"

    tf = _exists("terraform/modules/workflows/main.tf").read_text(encoding="utf-8")
    assert 'task_key        = "compute_elastic_sync"' not in tf, "TF still defines the compute_elastic_sync task"
    assert 'task_key = "compute_elastic_sync"' not in tf, "TF still has a depends_on edge to compute_elastic_sync"


_REFERENCE_TOKENS = ("from ", "join ", "source(", ".table(", "spark.sql", "- name:")


@pytest.mark.parametrize("base", ["src", "scripts", "dbt_project/models"])
def test_no_live_elastic_sync_results_reader(base: str) -> None:
    """No live reader/decl of the dropped table. A LINE that both names ``elastic_sync_results`` and
    carries a SQL/dbt reference token (FROM/JOIN/source()/.table()/spark.sql) or a ``- name:`` source
    decl is a real consumer; a bare prose mention in a docstring/comment is not. Matching per-line on
    the reference tokens keeps the historical retirement notes from tripping the gate while still
    catching an actual read."""
    root = _REPO_ROOT / base
    assert root.exists(), f"anchor {base} missing — this gate would be vacuous"
    offenders: list[str] = []
    for f in root.rglob("*"):
        if not f.is_file() or f.suffix not in {".py", ".sql", ".yml", ".yaml"}:
            continue
        if "__pycache__" in f.parts:
            continue
        rel = f.relative_to(_REPO_ROOT).as_posix()
        if rel in _ALLOWED_MENTIONS:
            continue
        for line in f.read_text(encoding="utf-8").splitlines():
            if "elastic_sync_results" not in line:
                continue
            low = line.lower()
            if any(tok in low for tok in _REFERENCE_TOKENS):
                offenders.append(f"{rel}: {line.strip()[:100]}")
    assert not offenders, f"live references to bronze.elastic_sync_results remain under {base}: {offenders}"

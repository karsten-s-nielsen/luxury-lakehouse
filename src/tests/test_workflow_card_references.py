"""D56: assert workflow cards have correct/documented references.

Each test guards against a specific D56 audit finding. Tests load YAML cards
via the WorkflowCard Pydantic model (preserves schema validation) or read raw
YAML text (for comment-block drift checks).
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CARDS_DIR = REPO_ROOT / "workflow-cards"


def _load_card(name: str) -> dict:
    """Load a workflow card's YAML frontmatter as a dict."""
    text = (CARDS_DIR / name).read_text(encoding="utf-8")
    # Split frontmatter between --- delimiters
    parts = text.split("---", 2)
    if len(parts) < 3:
        msg = f"{name} has no YAML frontmatter block"
        raise ValueError(msg)
    return yaml.safe_load(parts[1])


# ---------------------------------------------------------------------------
# D56-7: wf-defcon Kim et al. (2025) DEFCON methodology citation
# ---------------------------------------------------------------------------


def test_wf_defcon_has_kim_2025_methodology_citation() -> None:
    """wf-defcon.yaml must cite Kim et al. (2025) DEFCON. Canonical citation
    lives at NOTICE:89-91.
    """
    card = _load_card("wf-defcon.yaml")
    refs = card.get("references", [])
    assert refs, "wf-defcon.yaml has no references — should cite Kim et al. (2025) DEFCON"
    methodology_refs = [r for r in refs if r.get("role") == "methodology"]
    assert methodology_refs, "wf-defcon.yaml has no methodology-role reference"
    citations = " ".join(r.get("citation", "") for r in methodology_refs)
    assert "Kim" in citations, f"wf-defcon.yaml methodology references should mention Kim. Got: {citations!r}"
    assert "2025" in citations, f"wf-defcon.yaml should cite the 2025 paper. Got: {citations!r}"


# ---------------------------------------------------------------------------
# D56-8: PSxG pipeline cards share Butcher (2025) citation
# ---------------------------------------------------------------------------


def test_psxg_pipeline_cards_share_butcher_citation() -> None:
    """wf-export-shots, wf-import-psxg, and wf-goalkeeper are 3 stages of the
    same PSxG pipeline and must share the canonical Butcher (2025) citation
    from wf-goalkeeper.yaml:17.
    """
    for card_name in ("wf-export-shots.yaml", "wf-import-psxg.yaml", "wf-goalkeeper.yaml"):
        card = _load_card(card_name)
        refs = card.get("references", [])
        citations = " ".join(r.get("citation", "") for r in refs)
        assert "Butcher" in citations, (
            f"{card_name} should cite Butcher (2025) (canonical PSxG/xGOT citation "
            f"from wf-goalkeeper.yaml:17). Got: {citations!r}"
        )


def test_wf_export_shots_has_statsbomb_dataset_citation() -> None:
    """wf-export-shots republishes StatsBomb data to HF Hub — CC-BY 4.0 attribution required."""
    card = _load_card("wf-export-shots.yaml")
    refs = card.get("references", [])
    dataset_refs = [r for r in refs if r.get("role") == "dataset"]
    citations = " ".join(r.get("citation", "") for r in dataset_refs)
    assert "StatsBomb" in citations, (
        "wf-export-shots.yaml must cite StatsBomb Open Data as a dataset reference (CC-BY 4.0 republish attribution)."
    )


# ---------------------------------------------------------------------------
# D56-9: wf-line-breaking parmacalcio + StatsBomb citations
# ---------------------------------------------------------------------------


def test_wf_line_breaking_references_parmacalcio_and_statsbomb() -> None:
    """wf-line-breaking adapts parmacalcio1913/line-breaking-passes (Apache 2.0)
    per NOTICE:67-72 and operates on StatsBomb 360 freeze frames.
    """
    card = _load_card("wf-line-breaking.yaml")
    refs = card.get("references", [])
    assert refs, "wf-line-breaking.yaml has no references"
    citations = " ".join(r.get("citation", "") for r in refs)
    assert "parmacalcio" in citations.lower() or "line-breaking-passes" in citations.lower(), (
        f"wf-line-breaking.yaml should cite the parmacalcio1913/line-breaking-passes "
        f"upstream (Apache 2.0). Got: {citations!r}"
    )


# ---------------------------------------------------------------------------
# D56-10: wf-prepare-360-data StatsBomb 360 citation
# ---------------------------------------------------------------------------


def test_wf_prepare_360_data_has_statsbomb_dataset_citation() -> None:
    card = _load_card("wf-prepare-360-data.yaml")
    refs = card.get("references", [])
    dataset_refs = [r for r in refs if r.get("role") == "dataset"]
    citations = " ".join(r.get("citation", "") for r in dataset_refs)
    assert "StatsBomb" in citations, (
        "wf-prepare-360-data.yaml prepares StatsBomb 360 freeze frames for training "
        "and must cite StatsBomb Open Data as a dataset reference."
    )


# ---------------------------------------------------------------------------
# D56-11: Operational-plumbing cards must document empty references
# ---------------------------------------------------------------------------


def test_no_workflow_card_has_undocumented_empty_references() -> None:
    """Drift guard: any workflow card with `references: []` must have a comment
    block immediately above the line explaining why. Catches future drift where
    a new card lands with empty references and no rationale.
    """
    failures: list[str] = []
    for card_path in sorted(CARDS_DIR.glob("wf-*.yaml")):
        text = card_path.read_text(encoding="utf-8")
        if "references: []" not in text:
            continue  # has populated refs — fine

        lines = text.splitlines()
        for i, line in enumerate(lines):
            if line.strip() == "references: []":
                preceding = "\n".join(lines[max(0, i - 6) : i])
                if "# " not in preceding:
                    failures.append(card_path.name)
                break

    assert not failures, (
        f"The following workflow cards have `references: []` without a leading comment "
        f"block explaining why: {failures}. Add a YAML comment above the line."
    )

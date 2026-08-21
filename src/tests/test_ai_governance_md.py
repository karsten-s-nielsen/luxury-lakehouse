"""SEC1 / REG-01 enforcement: AI_GOVERNANCE.md must remain structurally complete,
must enumerate every per-player evaluative ML system in the repo, must cite the
originating audit finding, and must not go more than 30 days stale.

Parallels src/tests/test_architecture_md_appendix.py — together these two tests
form the "root-level governance documents are automatically reviewed" mechanism
described in AI_GOVERNANCE.md §14.
"""

from __future__ import annotations

import datetime as dt
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DOC = REPO_ROOT / "AI_GOVERNANCE.md"
WORKFLOW_CARDS_DIR = REPO_ROOT / "workflow-cards"
MODEL_CARDS_DIR = REPO_ROOT / "docs" / "huggingface" / "model-cards"

# Workflow cards whose outputs are evaluative about individual player performance,
# or that are load-bearing for systems that produce per-player evaluations.
# Adding a new workflow card with this property requires extending this set AND
# the "Scope — Systems in Scope" section of AI_GOVERNANCE.md, AND creating a
# matching HuggingFace model card under docs/huggingface/model-cards/.
PER_PLAYER_EVALUATIVE_CARDS: frozenset[str] = frozenset(
    {
        # wf-xg-v1 retired 2026-05-03 (SK3-MIG-B XG1-RETIRE).
        "wf-xg-v2",
        "wf-vaep",
        "wf-goalkeeper",  # PSxG + goalkeeper aggregation
        "wf-pitch-control",
        "wf-defcon",
        "wf-off-ball-xt",
        "wf-obso-pausa",
        "wf-space-creation",
        "wf-football2vec",  # v1 — deprecated but still referenced
        "wf-football2vec-v2",
        "wf-football2vec-360",
        "wf-scoutgpt",
        # silly-kicks 4.87.0 full-adoption new evaluative families (Rev 6 / GOV-A).
        # run-values folds into wf-off-ball-xt and obso_epv_source into wf-obso-pausa
        # (both already members) — they are NOT separate cards. xt-gk-v2's methodology
        # card is the member; its future writer/inference card (wf-xt-gk-v2-writer,
        # governed_by: wf-xt-gk-v2) is created at scheduling time and is NOT a member.
        "wf-packing",
        "wf-press-commitment",
        "wf-defensive-credit",
        "wf-bravery",
        "wf-gkdv",
        "wf-xt-gk-v2",
    }
)

# Mapping from workflow card ID to its corresponding HuggingFace model card
# filename under docs/huggingface/model-cards/. The naming is not mechanical
# (historical reasons: some cards use "-model-card.md", others "-model.md",
# and some are method cards for heuristics), so the mapping is explicit.
WORKFLOW_TO_MODEL_CARD: dict[str, str] = {
    "wf-xg-v2": "xg-v2-model-card.md",
    "wf-vaep": "vaep-model.md",
    "wf-goalkeeper": "psxg-model.md",
    "wf-pitch-control": "pitch-control.md",
    "wf-defcon": "defcon.md",
    "wf-off-ball-xt": "off-ball-xt.md",
    "wf-obso-pausa": "obso-pausa.md",
    "wf-space-creation": "space-creation.md",
    "wf-football2vec": "football2vec-statsbomb-wyscout.md",
    "wf-football2vec-v2": "football2vec-v2-model-card.md",
    "wf-football2vec-360": "football2vec-360-model-card.md",
    "wf-scoutgpt": "scoutgpt.md",
    # silly-kicks 4.87.0 full-adoption new evaluative families (Rev 6 / GOV-A).
    "wf-packing": "packing.md",
    "wf-press-commitment": "press-commitment.md",
    "wf-defensive-credit": "defensive-credit.md",
    "wf-bravery": "bravery.md",
    "wf-gkdv": "gkdv.md",
    "wf-xt-gk-v2": "xt-gk-v2.md",
}
assert set(WORKFLOW_TO_MODEL_CARD.keys()) == set(PER_PLAYER_EVALUATIVE_CARDS), (
    "WORKFLOW_TO_MODEL_CARD keys must exactly match PER_PLAYER_EVALUATIVE_CARDS"
)

REQUIRED_SECTION_HEADINGS: tuple[str, ...] = (
    "## 1. Executive Summary",
    "## 2. Operating Context",
    "## 3. Regulatory Scope Boundary",
    "## 4. Legal Framework Primer",
    "## 5. Scope — Systems in Scope",
    "## 6. Risk Classification Per System",
    "## 7. Conformity Assessment Obligations",
    "## 8. Technical Documentation Mapping",
    "## 9. Human Oversight Mechanisms",
    "## 10. Fairness Analysis",
    "## 11. Gap Summary",
    "## 12. Recommended Governance Actions",
    "## 13. Re-Classification Triggers",
    "## 14. Maintenance",
    "## 15. References",
)

REVIEW_GRACE_PERIOD_DAYS = 30


def _load_doc() -> str:
    assert DOC.exists(), (
        "AI_GOVERNANCE.md is missing. It is the remediation artifact for "
        "SEC-AUDIT-v1.12.0 REG-01 and must remain present at the project root."
    )
    return DOC.read_text(encoding="utf-8")


def test_ai_governance_exists() -> None:
    _load_doc()


def test_required_sections_present() -> None:
    text = _load_doc()
    missing = [h for h in REQUIRED_SECTION_HEADINGS if h not in text]
    assert not missing, f"AI_GOVERNANCE.md missing required section(s): {missing}"


def test_every_per_player_evaluative_card_is_listed() -> None:
    """Every workflow card named in PER_PLAYER_EVALUATIVE_CARDS must exist on
    disk and must be mentioned by ID in AI_GOVERNANCE.md. Adding a new card to
    the set without updating the document is forbidden.
    """
    text = _load_doc()
    missing_on_disk: list[str] = []
    missing_in_doc: list[str] = []
    for card_id in sorted(PER_PLAYER_EVALUATIVE_CARDS):
        card_path = WORKFLOW_CARDS_DIR / f"{card_id}.yaml"
        if not card_path.exists():
            missing_on_disk.append(card_id)
            continue
        if card_id not in text:
            missing_in_doc.append(card_id)
    assert not missing_on_disk, f"PER_PLAYER_EVALUATIVE_CARDS references missing workflow cards: {missing_on_disk}"
    assert not missing_in_doc, (
        "AI_GOVERNANCE.md must mention every per-player evaluative workflow "
        f"card by ID in the Scope inventory. Missing: {missing_in_doc}"
    )


def test_reg01_provenance_cited() -> None:
    text = _load_doc()
    assert "SEC-AUDIT-v1.12.0 REG-01" in text, (
        "AI_GOVERNANCE.md must cite SEC-AUDIT-v1.12.0 REG-01 as the originating "
        "audit finding. Do not silently rename or drop the provenance tag. Note: "
        "TODO.md no longer tracks SEC1 — this document is the remediation artifact "
        "and carries the full provenance itself."
    )


def test_next_review_date_is_fresh() -> None:
    """The 'Next review' date in the frontmatter must be parseable and must not
    be more than REVIEW_GRACE_PERIOD_DAYS in the past. The grace period allows
    the annual review to slide by a month before CI turns red.
    """
    text = _load_doc()
    match = re.search(r"\*\*Next review\*\*\s*\|\s*(\d{4}-\d{2}-\d{2})", text)
    assert match, (
        "AI_GOVERNANCE.md frontmatter must contain a row of the form "
        "'**Next review** | YYYY-MM-DD'. Missing or malformed."
    )
    next_review = dt.date.fromisoformat(match.group(1))
    grace_cutoff = dt.date.today() - dt.timedelta(days=REVIEW_GRACE_PERIOD_DAYS)
    assert next_review >= grace_cutoff, (
        f"AI_GOVERNANCE.md Next review date ({next_review}) is more than "
        f"{REVIEW_GRACE_PERIOD_DAYS} days stale. Re-run the gap analysis per "
        "the Maintenance section, update the Next review row, and commit before "
        "proceeding."
    )


def test_every_evaluative_system_has_a_model_card() -> None:
    """Every workflow card in PER_PLAYER_EVALUATIVE_CARDS must have a
    corresponding HuggingFace model card on disk under docs/huggingface/model-cards/.
    Adding a new per-player evaluative workflow card requires creating a matching
    model card in the same commit.
    """
    missing: list[str] = []
    for card_id, model_card_filename in sorted(WORKFLOW_TO_MODEL_CARD.items()):
        model_card_path = MODEL_CARDS_DIR / model_card_filename
        if not model_card_path.exists():
            missing.append(f"{card_id} → {model_card_filename}")
    assert not missing, (
        "Every per-player evaluative workflow card must have a matching model "
        f"card under docs/huggingface/model-cards/. Missing: {missing}"
    )


def test_every_evaluative_workflow_card_has_governance_block() -> None:
    """Every workflow card in PER_PLAYER_EVALUATIVE_CARDS must carry a
    'governance:' YAML block that references AI_GOVERNANCE.md. This guarantees
    the operational-surface parity described in AI_GOVERNANCE.md §12.5.
    """
    missing_block: list[str] = []
    missing_reference: list[str] = []
    for card_id in sorted(PER_PLAYER_EVALUATIVE_CARDS):
        card_path = WORKFLOW_CARDS_DIR / f"{card_id}.yaml"
        if not card_path.exists():
            missing_block.append(f"{card_id} (file missing)")
            continue
        text = card_path.read_text(encoding="utf-8")
        if "governance:" not in text:
            missing_block.append(card_id)
            continue
        if "AI_GOVERNANCE.md" not in text:
            missing_reference.append(card_id)
    assert not missing_block, (
        f"Every per-player evaluative workflow card must contain a 'governance:' YAML block. Missing: {missing_block}"
    )
    assert not missing_reference, (
        "Every per-player evaluative workflow card's governance: block must "
        f"reference AI_GOVERNANCE.md. Missing reference: {missing_reference}"
    )


def test_every_model_card_has_governance_stanza() -> None:
    """Every model card named in WORKFLOW_TO_MODEL_CARD must carry the
    'EU AI Act — Intended Use and Non-Use' stanza and must reference
    AI_GOVERNANCE.md. This guarantees public-surface parity for HuggingFace Hub
    consumers.
    """
    missing_stanza: list[str] = []
    missing_reference: list[str] = []
    for model_card_filename in sorted(WORKFLOW_TO_MODEL_CARD.values()):
        path = MODEL_CARDS_DIR / model_card_filename
        if not path.exists():
            missing_stanza.append(f"{model_card_filename} (file missing)")
            continue
        text = path.read_text(encoding="utf-8")
        if "EU AI Act — Intended Use and Non-Use" not in text:
            missing_stanza.append(model_card_filename)
            continue
        if "AI_GOVERNANCE.md" not in text:
            missing_reference.append(model_card_filename)
    assert not missing_stanza, (
        "Every model card must contain an 'EU AI Act — Intended Use and Non-Use' "
        f"stanza (H2 heading). Missing: {missing_stanza}"
    )
    assert not missing_reference, (
        f"Every model card's governance stanza must link to AI_GOVERNANCE.md. Missing reference: {missing_reference}"
    )

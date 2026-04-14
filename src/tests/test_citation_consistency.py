"""D56: assert citation consistency across UI pages, NOTICE, and source code.

Each test guards against a specific historical mismatch (Spearman 2017 with the
2018 Beyond Expected Goals URL, Rathke 2017 with a 2019 DOI, etc.).
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Issue 1: Spearman 2017 must NOT link to the 2018 "Beyond Expected Goals" URL
# ---------------------------------------------------------------------------


def test_pitch_control_page_spearman_citation_correct() -> None:
    """pitch_control.py UI Citation must use the 'Physics-Based Modeling of
    Pass Probabilities in Soccer' title, not 'Beyond Expected Goals'.

    The 2017 Spearman paper is 'Physics-Based Modeling...'; 'Beyond Expected
    Goals' is the 2018 paper. The implementation at src/analytics/pitch_control.py
    references the 2017 framework (time-to-intercept). See spec § Item 3 Issue 1.
    """
    src = _read("hf_taipy_app/src/pages/pitch_control.py")
    assert "Beyond_Expected_Goals" not in src, (
        "pitch_control.py still links to the 2018 ResearchGate URL for 'Beyond Expected Goals'. "
        "The 2017 paper is 'Physics-Based Modeling of Pass Probabilities in Soccer'."
    )
    assert "Physics-Based Modeling" in src, (
        "pitch_control.py UI Citation should reference the 2017 'Physics-Based Modeling of "
        "Pass Probabilities in Soccer' title, matching wf-pitch-control.yaml:16."
    )


def test_movement_analysis_page_spearman_citation_correct() -> None:
    src = _read("hf_taipy_app/src/pages/movement_analysis.py")
    assert "Beyond_Expected_Goals" not in src, "movement_analysis.py still links to the 2018 ResearchGate URL."
    assert "Physics-Based Modeling" in src


def test_notice_spearman_2017_title_correct() -> None:
    """NOTICE file must use the 'Physics-Based Modeling of Pass Probabilities'
    title for the Spearman 2017 citation, matching the implementation source.

    Verified at NOTICE:54-58 — original text incorrectly says 'Beyond Expected Goals'.
    """
    notice = _read("NOTICE")
    spearman_idx = notice.find("Spearman")
    assert spearman_idx != -1, "NOTICE has no Spearman citation block"
    window = notice[spearman_idx : spearman_idx + 300]
    assert "Physics-Based Modeling" in window, (
        f"NOTICE Spearman block should reference 'Physics-Based Modeling of Pass Probabilities'. Got: {window[:200]!r}"
    )
    assert "Beyond Expected Goals" not in window, (
        "NOTICE Spearman block still says 'Beyond Expected Goals' (the 2018 paper title)."
    )


def test_pitch_control_source_code_docstring_correct() -> None:
    """src/analytics/pitch_control.py module docstring must reference the correct
    Spearman 2017 paper title. Issue 1c in spec — third site of the same bug.
    """
    src = _read("src/analytics/pitch_control.py")
    head = src[:500]
    assert "Beyond Expected Goals" not in head, (
        "pitch_control.py module docstring still says 'Beyond Expected Goals' (2018 paper)."
    )
    assert "Physics-Based Modeling" in head, (
        "pitch_control.py module docstring should reference 'Physics-Based Modeling of "
        "Pass Probabilities in Soccer' (the 2017 paper that the time-to-intercept "
        "framework comes from)."
    )


# ---------------------------------------------------------------------------
# Issue 2: Rathke citation is decorative-only — no anchor in src/analytics/.
# Replaced with the project-canonical xG citation (Robberechts & Davis 2020)
# from workflow-cards/wf-xg-v1.yaml:16. D56-4, Option A (approved 2026-04-13).
# ---------------------------------------------------------------------------


def test_match_summary_no_rathke_citation() -> None:
    """Issue 2: Rathke is decorative-only (no source-code anchor). Replaced with
    Robberechts & Davis (2020) per Option A approval (2026-04-13).
    """
    src = _read("hf_taipy_app/src/pages/match_summary.py")
    assert "Rathke" not in src, (
        "match_summary.py still references Rathke. Replaced with Robberechts & Davis (2020) "
        "per spec § Item 3 Issue 2 Option A."
    )
    assert "Robberechts" in src, (
        "match_summary.py should now cite 'Robberechts & Davis (2020)' — the project-canonical "
        "xG citation from wf-xg-v1.yaml:16."
    )


def test_shot_map_no_rathke_citation() -> None:
    src = _read("hf_taipy_app/src/pages/shot_map.py")
    assert "Rathke" not in src
    assert "Robberechts" in src


# ---------------------------------------------------------------------------
# Issue 5: Sotudeh citation must reference the ETH Zurich PhD thesis, not the
# University of Twente MSc work. Implementation src is the PhD. D56-5.
# ---------------------------------------------------------------------------


def test_sotudeh_citation_uses_eth_zurich_phd_thesis() -> None:
    """Issue 5: Sotudeh's PhD thesis is at ETH Zurich (DISS. ETH NO. 31732),
    not the University of Twente MSc work. Implementation references the PhD.
    """
    src = _read("hf_taipy_app/src/pages/tactical_positions.py")
    assert "essay.utwente.nl" not in src, "tactical_positions.py still links the University of Twente MSc thesis."
    assert "ETH Zurich" in src or "DISS. ETH NO. 31732" in src or "s44260" in src, (
        "tactical_positions.py should reference Sotudeh's ETH Zurich PhD thesis."
    )


# ---------------------------------------------------------------------------
# Issue 4: Danesi citation must use the canonical title from source code
# (src/analytics/football2vec_transformer.py:20). D56-6.
# ---------------------------------------------------------------------------


def test_danesi_ui_uses_canonical_title() -> None:
    """Issue 4: Standardize on the source-code canonical title
    'Football2Vec: Transformer-Based Player Embeddings' (verified at
    src/analytics/football2vec_transformer.py:20).
    """
    src = _read("hf_taipy_app/src/pages/player_similarity.py")
    assert "Football2Vec: Transformer-Based Player Embeddings" in src, (
        "player_similarity.py UI Citation should expand the abbreviated 'Football2Vec' "
        "title to match the implementation source-code citation."
    )


def test_danesi_workflow_card_uses_canonical_title() -> None:
    src = _read("workflow-cards/wf-football2vec-v2.yaml")
    assert "Football2Vec: Transformer-Based Player Embeddings" in src, (
        "wf-football2vec-v2.yaml should use the 'Football2Vec: Transformer-Based Player Embeddings' "
        "title (matching src/analytics/football2vec_transformer.py:20), not "
        "'The Imposter on the Pitch'."
    )
    assert "Imposter on the Pitch" not in src

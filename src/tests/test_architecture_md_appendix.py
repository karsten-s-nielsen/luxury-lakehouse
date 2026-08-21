"""D56-12 Issue 6: ARCHITECTURE.md must contain Appendix D with all academic
references cited across the UI pages and analytics modules.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_architecture_md_has_appendix_d_academic_references() -> None:
    """ARCHITECTURE.md must contain the academic-references appendix under the
    existing ``## 8. Appendices`` section (the convention is ``### D. ...``,
    not a top-level ``## Appendix D``).
    """
    text = (REPO_ROOT / "ARCHITECTURE.md").read_text(encoding="utf-8")
    assert "D. Academic References" in text, (
        "ARCHITECTURE.md must contain a '### D. Academic References' heading "
        "under the '## 8. Appendices' section, listing all UI and analytics citations."
    )

    # Authors that must appear in the appendix per the D56 audit scope.
    expected_authors = [
        "Anzer",
        "Suzuki",
        "Robberechts",  # replaces Rathke per Option A (D56-4)
        "Trainor",
        "Pena",
        "Frencken",
        "Bourbousson",
        "Singh",  # Karun Singh — short surname
        "Donnelly",
        "Eyestone",  # xT-GK (Expected Threat for Goalkeepers) — Goalkeeper Analytics distribution view
        "Danesi",
        "Sotudeh",
        "Spearman",
        "Butcher",
        "Kim",  # Kim et al. 2025 for ELASTIC and DEFCON
        "Tancik",  # Fourier Features (scoutgpt fourier_cross_attention)
        "Shazeer",  # SwiGLU variants (scoutgpt swiglu)
        "Pipping-Gamón",  # xShotOccurrence (xS) in fct_action_context (ADR-039)
        "Karakus",  # structural pass (structural_lbs/sgm/sdi) in fct_action_context (ADR-042)
        "Cao",  # xCrossAttempt (xcross_attempt) in fct_action_context (ADR-042)
        # silly-kicks 4.87.0 full-adoption new-methodology citations (ADR-077).
        # Only genuinely-PUBLISHED methods are gated; press-commitment and bravery are
        # silly-kicks-native ORIGINAL metrics with no published paper — no appendix entry.
        "Goes",  # packing formalization: Goes, Kempe, Meerhoff & Lemmink (2019) — wf-packing
        "Power",  # off-ball-run detection: Power, Ruiz, Wei & Lucey (2017) KDD '17 — wf-off-ball-xt
        "Vidal-Codina",  # possession-hysteresis for run detection (2022) — wf-off-ball-xt
        "Esposito",  # off-ball run valuation framing-only (2026) — wf-off-ball-xt
        "Bischofberger",  # defensive-credit xT(origin) sizing + GKDV delta_das (2026)
        "Baca",  # co-author on both Bischofberger 2026 papers (defensive-credit, gkdv)
        "Le",  # GKDV ghost-substitution: Le, Yue, Carr & Lucey (2017) — short surname — wf-gkdv
        "Shaw",  # GKDV threat-suppression: Shaw & Sudarshan (2020) — wf-gkdv
        "Sudarshan",  # GKDV threat-suppression: Shaw & Sudarshan (2020) — wf-gkdv
    ]
    appendix_idx = text.find("D. Academic References")
    appendix = text[appendix_idx:]
    missing = [a for a in expected_authors if a not in appendix]
    assert not missing, f"D. Academic References appendix is missing entries for: {missing}"

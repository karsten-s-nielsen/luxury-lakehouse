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
        "Danesi",
        "Sotudeh",
        "Spearman",
        "Butcher",
        "Kim",  # Kim et al. 2025 for ELASTIC and DEFCON
    ]
    appendix_idx = text.find("D. Academic References")
    appendix = text[appendix_idx:]
    missing = [a for a in expected_authors if a not in appendix]
    assert not missing, f"D. Academic References appendix is missing entries for: {missing}"

"""Gradient Sports HF-publish protection guard.

Gradient Sports tracking/event data is computed internally (it flows through
``bronze.spadl_action_context`` and the SPADL/VAEP + tracking-context feature
tables) but is NOT licensed for publication to the public HuggingFace Hub.

Protection rests on two layers:

1. UI-facing / derived marts (``fct_tracking_frames``, ``fct_shots``,
   ``fct_passes``, ...) never union the Gradient Sports staging models, so
   gradientsports rows structurally never reach them — publishers that read
   those marts cannot leak it.
2. Publishers that DO read marts carrying restricted rows
   (``fct_action_context``, ``fct_action_values``, ``fct_tracking_frames``,
   ``fct_tracking_context``, ``fct_shot_psxg``) gate redistribution via the
   **per-match access_tier split** (spec 2026-06-29 §6.5, generalizing ADR-049):
   pull ALL providers + the per-row ``access_tier``, split via
   ``ingestion.hf_publish.split_restricted(df, column="access_tier")``, publish
   the restricted side to the PRIVATE companion repo, and assert the PUBLIC frame
   is leak-free via ``ingestion.hf_leak_guard.assert_no_private_leak``. These
   publishers must NOT carry a SQL-side provider filter — a SQL filter is exactly
   what the VAEP trainer silently inherited (Champions v10-and-earlier trained
   without GS), and it cannot express the per-match SkillCorner boundary.

The legacy ``WHERE data_source != 'gradientsports'`` SQL-exclusion mode is now
fully retired (spec D6 — tracking_context migrated). This test locks in: every
split publisher splits on ``access_tier`` + calls the leak guard + has no SQL
provider filter, and NO publisher in EITHER ``scripts/`` or ``src/ingestion/``
(B2: the wired entry points are the ``src/ingestion/`` twins) keys the
redistribution decision on ``data_source`` — neither a SQL filter nor
``split_restricted(column="data_source")``. The dim tables intentionally include
restricted providers (needed for internal joins), so the publisher-side split is
the boundary that keeps licensed data out of the public repo.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parent.parent.parent
_SCRIPTS_DIR = _REPO_ROOT / "scripts"
_SRC_INGESTION_DIR = _REPO_ROOT / "src" / "ingestion"

# Publishers migrated to the per-match access_tier split (spec 2026-06-29 §6.5): SQL pulls ALL
# providers + the per-row access_tier; the redistribution gate is ingestion.hf_publish.split_restricted
# keyed on access_tier, and the public frame is enforced by the fail-closed leak guard. NO SQL-side
# provider filter — a SQL filter silently shrinks the restricted repo (and the training corpus).
_ADR049_SPLIT_PUBLISHERS: tuple[str, ...] = (
    "publish_action_context_hf.py",
    "publish_spadl_vaep_hf.py",
    "publish_psxg_shots_hf.py",
    "publish_pitch_control_tracking_hf.py",
    "publish_tracking_context_hf.py",
    # Pre-Shot xG v3 delivery Task 1.2 (spec §A3, 2026-07-07): tabular shot corpus publisher.
    "publish_xg_shot_data_v3_hf.py",
    # Pre-Shot xG v3 delivery Task 1.3 (spec §A4, 2026-07-07): freeze-frame (context) corpus publisher.
    "publish_shot_freeze_frames_hf.py",
)

# Legacy SQL-side exclusion mode is now EMPTY: tracking_context (the last legacy publisher)
# migrated to the per-match access_tier split (spec D6). No publisher may gate redistribution by a
# `data_source != '<provider>'` SQL filter any more — see test_no_publisher_restricts_by_data_source.
_GS_GATED_PUBLISHERS: tuple[str, ...] = ()

# Matches `data_source != '<provider>'` tolerant of whitespace, quote style, and the SQL `<>`
# inequality spelling (the legacy restriction-by-SQL pattern this feature retires).
_EXCLUSION_RE = re.compile(
    r"""data_source \s* (?: != | <> ) \s* ['"][a-z_]+['"]""",
    re.IGNORECASE | re.VERBOSE,
)


def _imports_split_restricted(source: str) -> bool:
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "ingestion.hf_publish":
            if any(alias.name == "split_restricted" for alias in node.names):
                return True
    return False


def _all_publisher_paths() -> list[Path]:
    """Every HF publisher in BOTH dirs (B2: the src/ingestion/ twins are the wired entry points)."""
    return sorted(_SCRIPTS_DIR.glob("publish_*_hf.py")) + sorted(_SRC_INGESTION_DIR.glob("publish_*_hf.py"))


def test_publisher_mode_sets_are_disjoint() -> None:
    """A publisher is gated in exactly one mode — never both, never ambiguous."""
    overlap = set(_ADR049_SPLIT_PUBLISHERS) & set(_GS_GATED_PUBLISHERS)
    assert not overlap, f"Publishers listed in both gating modes: {sorted(overlap)}"


@pytest.mark.parametrize("publisher", _ADR049_SPLIT_PUBLISHERS)
def test_split_publisher_uses_access_tier_split_and_leak_guard(publisher: str) -> None:
    """Each split publisher: imports split_restricted, splits on access_tier, calls the leak
    guard, and carries NO SQL provider filter (spec §6.5/D1/C3)."""
    path = _SCRIPTS_DIR / publisher
    assert path.exists(), f"Expected publisher script {path} to exist"
    source = path.read_text(encoding="utf-8")
    assert _imports_split_restricted(source), (
        f"{publisher} must import split_restricted from ingestion.hf_publish — the restricted gate is missing."
    )
    assert 'column="access_tier"' in source, (
        f'{publisher} must split on access_tier — `split_restricted(df, column="access_tier")` '
        f"(per-match redistribution boundary, spec §6.5). A provider-level split leaks restricted SkillCorner."
    )
    assert "assert_no_private_leak" in source, (
        f"{publisher} must call assert_no_private_leak(public_df, publisher=...) on the PUBLIC frame "
        f"before upload (fail-closed leak guard, spec C3/§9.7)."
    )
    assert not _EXCLUSION_RE.search(source), (
        f"{publisher} uses a SQL-side `data_source != '<provider>'` filter — the redistribution gate "
        f"must be the access_tier split, never SQL (a SQL filter silently shrinks the restricted repo)."
    )


def test_legacy_sql_exclusion_mode_is_empty() -> None:
    """The legacy `WHERE data_source != '<provider>'` gate is fully retired (spec D6)."""
    assert _GS_GATED_PUBLISHERS == (), (
        f"Publishers still on the legacy SQL exclusion: {_GS_GATED_PUBLISHERS}. Migrate each to the "
        f"per-match access_tier split and add it to _ADR049_SPLIT_PUBLISHERS."
    )


@pytest.mark.parametrize("path", _all_publisher_paths(), ids=lambda p: f"{p.parent.name}/{p.name}")
def test_no_publisher_restricts_by_data_source(path: Path) -> None:
    """No publisher (scripts/ OR src/ingestion/) may key the redistribution decision on data_source —
    neither a SQL `data_source != '<provider>'` filter nor `split_restricted(column="data_source")`.
    The per-match boundary is access_tier (B2/C5/D6). Partitioning the OUTPUT by data_source for
    layout is fine; restricting WHO is published by data_source is not."""
    source = path.read_text(encoding="utf-8")
    assert not _EXCLUSION_RE.search(source), (
        f"{path.parent.name}/{path.name} restricts redistribution via a SQL data_source filter — "
        f"use the per-match access_tier split (spec §6.5/D6)."
    )
    assert 'column="data_source"' not in source and "column='data_source'" not in source, (
        f"{path.parent.name}/{path.name} calls split_restricted(column='data_source') — the restriction "
        f"decision must key on access_tier, not provider (spec §6.5; provider-level leaks restricted SkillCorner)."
    )


def test_src_ingestion_spadl_vaep_twin_splits_on_access_tier() -> None:
    """C5/B2: the wired src/ingestion/ spadl_vaep twin reads a SkillCorner-carrying mart and must
    NOT survive as a no-split path — it splits on access_tier and calls the leak guard."""
    source = (_SRC_INGESTION_DIR / "publish_spadl_vaep_hf.py").read_text(encoding="utf-8")
    assert _imports_split_restricted(source), "src/ingestion/publish_spadl_vaep_hf.py must import split_restricted"
    assert 'column="access_tier"' in source, "src/ingestion/publish_spadl_vaep_hf.py must split on access_tier"
    assert "assert_no_private_leak" in source, "src/ingestion/publish_spadl_vaep_hf.py must call the leak guard"

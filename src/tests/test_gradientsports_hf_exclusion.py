"""Gradient Sports HF-publish protection guard.

Gradient Sports tracking/event data is computed internally (it flows through
``bronze.spadl_action_context`` and the SPADL/VAEP + tracking-context feature
tables) but is NOT licensed for publication to the public HuggingFace Hub.

Protection rests on two layers:

1. UI-facing / derived marts (``fct_tracking_frames``, ``fct_shots``,
   ``fct_passes``, ...) never union the Gradient Sports staging models, so
   gradientsports rows structurally never reach them — publishers that read
   those marts cannot leak it.
2. Publishers that DO read marts carrying gradientsports
   (``fct_action_context``, ``fct_action_values``, ``fct_tracking_context``)
   gate it in exactly ONE of two ways:

   - **ADR-049 restricted split** (the long-term pattern): pull ALL providers
     in SQL, split via ``ingestion.hf_publish.split_restricted``, publish the
     restricted side to the PRIVATE companion repo. These publishers must NOT
     carry a SQL-side provider filter — a SQL filter is exactly what the VAEP
     trainer silently inherited (Champions v10-and-earlier trained without GS).
   - **Legacy SQL exclusion**: ``WHERE data_source != 'gradientsports'``.
     Remaining publishers migrate to the ADR-049 split when next touched.

This test locks in layer (2): every GS-carrying publisher is in exactly one
mode, split-mode publishers import the shared helper and have no SQL filter,
and legacy publishers retain the exclusion clause. If anyone removes a gate
without adopting the other mode, the dataset would silently start leaking
licensed data to HF on the next publish run with nothing else to catch it.
The dim tables intentionally include gradientsports (needed for internal
joins), so the publisher-side gate is the only remaining one.

Remove a publisher from BOTH sets only when the Gradient Sports HF license is
secured — and then via ADR-049's one-edit migration (drop the provider from
``RESTRICTED_HF_PROVIDERS``), not by deleting the machinery.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parent.parent.parent
_SCRIPTS_DIR = _REPO_ROOT / "scripts"

# Publishers migrated to the ADR-049 restricted split: SQL pulls ALL
# providers; the license gate is ingestion.hf_publish.split_restricted.
_ADR049_SPLIT_PUBLISHERS: tuple[str, ...] = (
    "publish_action_context_hf.py",
    "publish_spadl_vaep_hf.py",
)

# Publishers still on the legacy SQL-side exclusion. Migrate each to the
# ADR-049 split when next touched (tracking_context is slated for
# deprecation).
_GS_GATED_PUBLISHERS: tuple[str, ...] = ("publish_tracking_context_hf.py",)

# Matches `data_source != 'gradientsports'` tolerant of whitespace, quote style,
# and the SQL `<>` inequality spelling.
_EXCLUSION_RE = re.compile(
    r"""data_source \s* (?: != | <> ) \s* ['"]gradientsports['"]""",
    re.IGNORECASE | re.VERBOSE,
)


def _imports_split_restricted(source: str) -> bool:
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "ingestion.hf_publish":
            if any(alias.name == "split_restricted" for alias in node.names):
                return True
    return False


def test_publisher_mode_sets_are_disjoint() -> None:
    """A publisher is gated in exactly one mode — never both, never ambiguous."""
    overlap = set(_ADR049_SPLIT_PUBLISHERS) & set(_GS_GATED_PUBLISHERS)
    assert not overlap, f"Publishers listed in both gating modes: {sorted(overlap)}"


@pytest.mark.parametrize("publisher", _ADR049_SPLIT_PUBLISHERS)
def test_split_publisher_uses_restricted_split_not_sql_filter(publisher: str) -> None:
    """ADR-049 publishers import split_restricted and carry NO SQL provider filter."""
    path = _SCRIPTS_DIR / publisher
    assert path.exists(), f"Expected publisher script {path} to exist"
    source = path.read_text(encoding="utf-8")
    assert _imports_split_restricted(source), (
        f"{publisher} is listed as an ADR-049 split publisher but does not import "
        f"split_restricted from ingestion.hf_publish — the restricted gate is missing."
    )
    assert not _EXCLUSION_RE.search(source), (
        f"{publisher} uses the ADR-049 split AND a SQL-side gradientsports filter. "
        f"The SQL filter shrinks the restricted repo (and thus the training corpus) "
        f"silently — remove it; split_restricted is the only gate (ADR-049)."
    )


@pytest.mark.parametrize("publisher", _GS_GATED_PUBLISHERS)
def test_legacy_publisher_excludes_gradientsports(publisher: str) -> None:
    """Each legacy-gated publisher's SQL must filter out gradientsports."""
    path = _SCRIPTS_DIR / publisher
    assert path.exists(), f"Expected publisher script {path} to exist"
    source = path.read_text(encoding="utf-8")
    assert _EXCLUSION_RE.search(source), (
        f"{publisher} is missing the gradientsports HF-license exclusion "
        f"(`WHERE data_source != 'gradientsports'`). Gradient Sports data is "
        f"computed internally but not licensed for public HF publication. "
        f"Restore the filter, or migrate the publisher to the ADR-049 "
        f"restricted split and move it to _ADR049_SPLIT_PUBLISHERS."
    )

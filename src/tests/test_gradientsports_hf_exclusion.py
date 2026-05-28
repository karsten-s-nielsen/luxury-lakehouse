"""Gradient Sports HF-publish exclusion guard.

Gradient Sports tracking/event data is computed internally (it flows through
``bronze.spadl_action_context`` and the SPADL/VAEP + tracking-context feature
tables) but is NOT licensed for publication to the public HuggingFace Hub.

Protection rests on two layers:

1. UI-facing / derived marts (``fct_tracking_frames``, ``fct_shots``,
   ``fct_passes``, ...) never union the Gradient Sports staging models, so
   gradientsports rows structurally never reach them — publishers that read
   those marts cannot leak it.
2. The three feature-table publishers DO read marts that carry gradientsports
   (``fct_action_context``, ``fct_action_values``, ``fct_tracking_context``),
   so each must filter it out with ``WHERE data_source != 'gradientsports'``.

This test locks in layer (2). If anyone edits/removes the exclusion clause in
one of those publisher SQL constants, the dataset would silently start leaking
licensed data to HF on the next publish run with nothing else to catch it.
The dim tables intentionally include gradientsports (needed for internal
joins), so the publisher WHERE clause is the only remaining gate.

Remove a publisher from ``_GS_GATED_PUBLISHERS`` only when the Gradient Sports
HF license is secured (and update the publisher SQL in the same change).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parent.parent.parent
_SCRIPTS_DIR = _REPO_ROOT / "scripts"

# Publishers whose source mart carries gradientsports rows and therefore MUST
# filter it out before uploading to HF.
_GS_GATED_PUBLISHERS: tuple[str, ...] = (
    "publish_action_context_hf.py",
    "publish_spadl_vaep_hf.py",
    "publish_tracking_context_hf.py",
)

# Matches `data_source != 'gradientsports'` tolerant of whitespace, quote style,
# and the SQL `<>` inequality spelling.
_EXCLUSION_RE = re.compile(
    r"""data_source \s* (?: != | <> ) \s* ['"]gradientsports['"]""",
    re.IGNORECASE | re.VERBOSE,
)


@pytest.mark.parametrize("publisher", _GS_GATED_PUBLISHERS)
def test_publisher_excludes_gradientsports(publisher: str) -> None:
    """Each gated publisher's SQL must filter out gradientsports."""
    path = _SCRIPTS_DIR / publisher
    assert path.exists(), f"Expected publisher script {path} to exist"
    source = path.read_text(encoding="utf-8")
    assert _EXCLUSION_RE.search(source), (
        f"{publisher} is missing the gradientsports HF-license exclusion "
        f"(`WHERE data_source != 'gradientsports'`). Gradient Sports data is "
        f"computed internally but not licensed for public HF publication. "
        f"Restore the filter, or — only if the license is now secured — remove "
        f"{publisher} from _GS_GATED_PUBLISHERS in this test."
    )

"""Per-match HF redistribution policy — the SINGLE source of truth.

Pure: stdlib only (sits beside ``src/shared/identifiers.py``; ``src/shared/`` has zero external deps,
enforced by import-linter). No Spark, no HF, no I/O. Inputs are the ingestion-time signals; output is
the redistribution tier. See ``docs/superpowers/specs/2026-06-29-per-match-hf-redistribution-restriction.md``.
"""

from __future__ import annotations

from enum import Enum


class AccessTier(str, Enum):
    """Redistribution tier of a match's data. ``.value`` is the canonical bronze/mart string."""

    PUBLIC = "public"
    RESTRICTED = "restricted"


# Providers whose matches default to RESTRICTED when they carry NO per-match visibility signal.
# GradientSports today; SkillCorner has a real `visibility` feed (pining) so it is NOT defaulted here.
# This set is the NULL-fallback for `classify_access_tier`. It lives in this pure core (not
# ingestion.hf_publish, which imports pandas/HF) so the stdlib-only core never imports an adapter —
# hf_publish.py re-exports it from here (no zero-dep violation, no import cycle).
RESTRICTED_HF_PROVIDERS: frozenset[str] = frozenset({"gradientsports"})


def classify_access_tier(*, provider: str, visibility: str | None) -> AccessTier:
    """Classify a match's redistribution tier from its ingestion-time signals.

    pining ``visibility`` is ``"public" | "private"`` (pining canonical model pins
    ``pattern=r"^(public|private)$"``). Mapping:

    - ``"private"``        -> ``RESTRICTED``  (the positive trigger — matched on the LITERAL value)
    - ``"public"``         -> ``PUBLIC``
    - ``None`` (no feed)   -> provider default (``RESTRICTED`` if in ``RESTRICTED_HF_PROVIDERS`` else ``PUBLIC``)
    - anything else        -> ``RESTRICTED``  (fail-safe — never leak an unrecognized value)
    """
    if visibility is None:
        return AccessTier.RESTRICTED if provider in RESTRICTED_HF_PROVIDERS else AccessTier.PUBLIC
    if visibility == "public":
        return AccessTier.PUBLIC
    # "private" AND any unrecognized value both route to RESTRICTED (fail-safe).
    return AccessTier.RESTRICTED

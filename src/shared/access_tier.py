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


# ALLOWLIST (ADR-064 amendment 2026-06-30, review P1): providers whose data is PUBLIC BY LICENSE (open data).
# A no-per-match-signal match defaults PUBLIC *only* if its provider is on this list. EVERYTHING else — any
# unknown/new provider, AND any provider with a `visibility` feed but no signal on this row (skillcorner, GS) —
# fails SAFE to RESTRICTED. This is the highest-stakes default in the system: a wrong-restrict is recoverable
# with one line here; a wrong-public of private data (e.g. the restricted SkillCorner Real Madrid games) is an
# irreversible licence breach. A denylist ("these providers are restricted, else public") would leak any provider
# nobody remembered to classify — the allowlist closes that. Lives in this stdlib-only core (hf_publish re-exports).
PUBLIC_BY_LICENSE_PROVIDERS: frozenset[str] = frozenset({"statsbomb", "wyscout", "idsse", "metrica"})

# Back-compat: still imported by ingestion.hf_publish / the VAEP trainer gate as "providers that produce
# restricted rows by default". Kept as {gradientsports} — the allowlist above is the authoritative no-signal default.
RESTRICTED_HF_PROVIDERS: frozenset[str] = frozenset({"gradientsports"})


def classify_access_tier(*, provider: str, visibility: str | None) -> AccessTier:
    """Classify a match's redistribution tier from its ingestion-time signals (fail-safe-for-privacy).

    pining ``visibility`` is ``"public" | "private"`` (pining canonical model pins
    ``pattern=r"^(public|private)$"``). Mapping:

    - ``"public"``                          -> ``PUBLIC``  (explicit per-match public signal, any provider)
    - ``None`` AND provider on allowlist    -> ``PUBLIC``  (open-data provider, no per-match feed)
    - ``"private"`` / unknown value /
      ``None`` off-allowlist / unknown
      provider                              -> ``RESTRICTED``  (FAIL SAFE — never leak)
    """
    if visibility == "public":
        return AccessTier.PUBLIC
    if visibility is None and provider in PUBLIC_BY_LICENSE_PROVIDERS:
        return AccessTier.PUBLIC
    # "private", any unrecognized visibility, a no-signal off-allowlist provider, OR an unknown provider:
    # all fail safe to RESTRICTED.
    return AccessTier.RESTRICTED

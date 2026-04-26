"""Hash-based deterministic match-stratified holdout split for ExT v2.

A match (identified by ``competition_id`` + ``match_key``) lands in holdout iff::

    int(sha256(f"{competition_id}|{match_key}").hexdigest(), 16) % 100 < holdout_fraction * 100

Properties:

- Deterministic across runs (no RNG state).
- Stable as data evolves (new matches naturally distribute).
- Per-competition stratified (each competition's matches split independently).
- Disjoint at the match level (one match → one bucket → one split).
- Cross-machine reproducible (hash is a pure function of identifiers).
- Order-invariant (independent of input row ordering).
- Subset-invariant (a match's bucket doesn't depend on other matches present).

For competitions with very few matches (1-3), the binomial nature of the hash
split means a comp's matches may all land in train or all in holdout; per-comp
NLL reporting (see ``analytics.ext_v2.fitness``) skips comps with empty
holdout gracefully.
"""

from __future__ import annotations

import hashlib

import pandas as pd

DEFAULT_HOLDOUT_FRACTION = 0.15
"""Phase 0 default per design spec §5.3."""

REQUIRED_COLUMNS: tuple[str, ...] = ("competition_id", "match_key")


def _bucket(competition_id: object, match_key: object) -> int:
    """Hash ``(competition_id, match_key)`` to an integer bucket in ``[0, 100)``.

    sha256 is used as a non-cryptographic hash (``usedforsecurity=False``);
    the only requirement is uniform distribution over buckets, which sha256
    trivially satisfies. Stability across machines and Python versions is
    guaranteed by the hashlib spec. (sha256 over sha1 here purely to clear
    the global Semgrep insecure-hash-algorithm rule; mod 100 makes any
    cryptographic strength irrelevant for this use.)
    """
    key = f"{competition_id}|{match_key}".encode()
    digest = hashlib.sha256(key, usedforsecurity=False).hexdigest()
    return int(digest, 16) % 100


def holdout_split(
    actions: pd.DataFrame,
    *,
    holdout_fraction: float = DEFAULT_HOLDOUT_FRACTION,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split actions into ``(train, holdout)`` by hash on match identity.

    Args:
        actions: DataFrame with at minimum ``competition_id`` and
            ``match_key`` columns. All other columns pass through unchanged.
        holdout_fraction: Fraction in ``[0, 1]`` to send to holdout. Default
            0.15 (15%) per design spec §5.3.

    Returns:
        ``(train, holdout)`` — each a copy of the rows from ``actions`` whose
        match falls in the corresponding split.

    Raises:
        ValueError: if ``holdout_fraction`` is outside ``[0, 1]`` or required
            columns are missing.
    """
    if not 0 <= holdout_fraction <= 1:
        msg = f"holdout_fraction must be in [0, 1], got {holdout_fraction}"
        raise ValueError(msg)
    missing = [col for col in REQUIRED_COLUMNS if col not in actions.columns]
    if missing:
        msg = f"actions missing required columns: {missing}"
        raise ValueError(msg)
    if actions.empty:
        return actions.copy(), actions.copy()

    threshold = round(holdout_fraction * 100)

    # Hash each unique match identifier once; map back to rows. For 8M+ action
    # rows over ~5K matches this is dominantly the unique extraction + dict
    # lookup, both vectorised in pandas.
    composite_key = actions["competition_id"].astype(str) + "|" + actions["match_key"].astype(str)
    bucket_map = {k: _bucket_from_str(k) for k in composite_key.unique()}
    is_holdout = composite_key.map(bucket_map) < threshold

    return actions[~is_holdout].copy(), actions[is_holdout].copy()


def _bucket_from_str(composite_key: str) -> int:
    """Hash a pre-built ``f"{cid}|{mk}"`` string to a bucket in ``[0, 100)``."""
    digest = hashlib.sha256(composite_key.encode(), usedforsecurity=False).hexdigest()
    return int(digest, 16) % 100

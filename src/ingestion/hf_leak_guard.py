"""Fail-closed leak guard for public HF publishers (spec §9.7 / C3).

Every public-HF publisher calls ``assert_no_private_leak(public_df, publisher=<name>)`` immediately
before uploading its PUBLIC artifact. The registry enumerates every publisher + its tier-handling mode,
so a new publisher with no entry fails ``test_registry_covers_every_publisher_module`` (it cannot be
silently omitted) and the guard refuses to run for it. ERROR-level + raise on any non-public row
(spec C3 / the CLAUDE.md telemetry rule — alerts are ERROR, never warning).
"""

from __future__ import annotations

import logging

import pandas as pd

from shared.access_tier import AccessTier

logger = logging.getLogger(__name__)


class LeakDetectedError(RuntimeError):
    """A public artifact contains a non-public (restricted / NULL / unknown) row, or an unregistered publisher."""


# Tier-handling mode per publisher (keyed by module basename — same for the scripts/ and src/ingestion/ twins):
#   "split"       — row-level, publishes both repos; the public frame must be all-public.
#   "fail_closed" — safe-by-absence today (no restricted provider in its mart); still asserted so absence
#                   can never silently become a leak.
#   "derived"     — built public-only upstream; the publisher asserts its source separately (§6.8).
PUBLISHER_REGISTRY: dict[str, str] = {
    "publish_spadl_vaep_hf": "split",
    "publish_action_context_hf": "split",
    "publish_psxg_shots_hf": "split",
    "publish_pitch_control_tracking_hf": "split",
    "publish_tracking_context_hf": "split",
    "publish_line_breaking_passes_hf": "fail_closed",
    "publish_xg_shots_hf": "fail_closed",
    "publish_freeze_frame_hf": "fail_closed",
    "publish_obso_pausa_inputs_hf": "fail_closed",
    "publish_shots_on_target_hf": "fail_closed",
    "publish_football2vec_embeddings_hf": "derived",
}


def assert_no_private_leak(public_df: pd.DataFrame, *, publisher: str) -> None:
    """Raise ``LeakDetectedError`` if ``public_df`` contains any row whose ``access_tier`` is not exactly 'public'.

    Call this on the PUBLIC frame AFTER ``split_restricted`` (so the column is still present) and BEFORE
    dropping ``access_tier`` for upload (spec R2). Fail-closed on an unregistered publisher or a missing
    ``access_tier`` column — the guard never assumes safety it cannot prove.
    """
    if publisher not in PUBLISHER_REGISTRY:
        raise LeakDetectedError(f"publisher {publisher!r} not in PUBLISHER_REGISTRY — add it (fail-closed)")
    if "access_tier" not in public_df.columns:
        raise LeakDetectedError(f"{publisher}: public frame has no access_tier column — cannot prove it is public")
    non_public = public_df[public_df["access_tier"] != AccessTier.PUBLIC.value]
    if len(non_public) > 0:
        by_tier = non_public["access_tier"].fillna("<null>").value_counts().to_dict()
        logger.error("LEAK BLOCKED: %s public artifact has %d non-public rows: %s", publisher, len(non_public), by_tier)
        raise LeakDetectedError(f"{publisher}: {len(non_public)} non-public rows in public artifact: {by_tier}")
    logger.info("leak guard OK: %s public artifact is all-public (%d rows)", publisher, len(public_df))

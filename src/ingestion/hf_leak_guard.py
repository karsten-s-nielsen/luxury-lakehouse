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

from shared.access_tier import PUBLIC_BY_LICENSE_PROVIDERS, AccessTier

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
    "publish_xg_shot_data_v3_hf": "split",
    "publish_shot_freeze_frames_hf": "split",
    "publish_psxg_shots_hf": "split",
    "publish_pitch_control_tracking_hf": "split",
    "publish_line_breaking_passes_hf": "fail_closed",
    "publish_xg_shots_hf": "fail_closed",
    "publish_freeze_frame_hf": "fail_closed",
    "publish_obso_pausa_inputs_hf": "fail_closed",
    "publish_shots_on_target_hf": "fail_closed",
    "publish_football2vec_embeddings_hf": "derived",
}


def assert_publishable_frame(df: pd.DataFrame, *, publisher: str) -> None:
    """Fail closed unless ``df`` is a frame this publisher is permitted to attempt to publish.

    Registry membership + tier-column presence — the preconditions of any tier decision. Extracted
    (ADR-072) so ``ingestion.hf_upload_seam.prepare_public_upload`` can run them BEFORE
    ``split_restricted``, which subscripts the column directly and would otherwise surface a missing
    column as a bare ``KeyError`` on the split path. ``assert_no_private_leak`` calls this too, so
    there is one owner for the question "what makes a frame publishable".
    """
    if publisher not in PUBLISHER_REGISTRY:
        raise LeakDetectedError(f"publisher {publisher!r} not in PUBLISHER_REGISTRY — add it (fail-closed)")
    if "access_tier" not in df.columns:
        raise LeakDetectedError(f"{publisher}: public frame has no access_tier column — cannot prove it is public")


def assert_no_private_leak(public_df: pd.DataFrame, *, publisher: str) -> None:
    """Raise ``LeakDetectedError`` if ``public_df`` contains any row whose ``access_tier`` is not exactly 'public'.

    Call this on the PUBLIC frame AFTER ``split_restricted`` (so the column is still present) and BEFORE
    dropping ``access_tier`` for upload (spec R2). Fail-closed on an unregistered publisher or a missing
    ``access_tier`` column — the guard never assumes safety it cannot prove.
    """
    assert_publishable_frame(public_df, publisher=publisher)
    non_public = public_df[public_df["access_tier"] != AccessTier.PUBLIC.value]
    if len(non_public) > 0:
        by_tier = non_public["access_tier"].fillna("<null>").value_counts().to_dict()
        logger.error("LEAK BLOCKED: %s public artifact has %d non-public rows: %s", publisher, len(non_public), by_tier)
        raise LeakDetectedError(f"{publisher}: {len(non_public)} non-public rows in public artifact: {by_tier}")
    _assert_no_access_tier_visibility_divergence(public_df, publisher=publisher)
    logger.info("leak guard OK: %s public artifact is all-public (%d rows)", publisher, len(public_df))


def _assert_no_access_tier_visibility_divergence(public_df: pd.DataFrame, *, publisher: str) -> None:
    """Fail closed if a NON-allowlisted provider's row reached `access_tier='public'` with a true `visibility`
    that is not 'public' (H1.3 / review-P1 approach A — keyed on the SHARED `PUBLIC_BY_LICENSE_PROVIDERS`).

    Why per-row `visibility` threading is NOT needed (ADR-064 amendment 2026-06-30): after the allowlist flip, a
    provider NOT on `PUBLIC_BY_LICENSE_PROVIDERS` (skillcorner, gradientsports) can only reach `access_tier='public'`
    via an explicit per-match `visibility='public'` (or the verified confirmed-public backfill override) — never a
    default. So **`access_tier` already encodes the per-row visibility decision**, and the all-public check above is
    the primary enforcement. This is the on-the-publish-path, fail-closed backstop against a *stamp divergence*
    (`access_tier='public'` on a row whose true `visibility` is not public). It fires when the frame carries a
    provider column + `visibility` (a publisher that joins dim_matches attaches it).
    The COMPREHENSIVE, build-gating enforcement is the dbt consistency test
    (`assert_access_tier_visibility_consistency.sql`) — it covers `dim_matches` (the source for dim-resolved marts) AND
    the row-level facts (`fct_action_context`/`fct_action_values`, joined to dim_matches), so every mart is gated
    before publish regardless of which publishers carry `visibility`. Allowlisted (open-data) providers need no
    `visibility` signal. A provider MIS-TAG (private row wrongly stamped with an allowlisted provider) is an upstream
    ingestion-integrity concern, out of scope here by design.
    """
    provider_col = next((c for c in ("data_source", "source_provider", "provider") if c in public_df.columns), None)
    if provider_col is None or "visibility" not in public_df.columns:
        return  # nothing to cross-check on this frame; access_tier all-public (above) + the source dbt test cover it
    non_allowlisted = public_df[~public_df[provider_col].isin(PUBLIC_BY_LICENSE_PROVIDERS)]
    diverged = non_allowlisted[non_allowlisted["visibility"] != "public"]
    if len(diverged) > 0:
        by = diverged[provider_col].fillna("<null>").value_counts().to_dict()
        logger.error(
            "LEAK BLOCKED (divergence): %s has %d non-allowlisted public rows whose visibility != 'public': %s",
            publisher,
            len(diverged),
            by,
        )
        raise LeakDetectedError(
            f"{publisher}: {len(diverged)} non-allowlisted public rows with visibility != 'public' "
            f"(access_tier/visibility divergence): {by}"
        )

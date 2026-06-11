"""Frame-batch sizing for the action-context pipeline (ADR-047 amendment 2).

The frame batch size is part of the DOMAIN CONTRACT, not a Spark dispatch
detail: window-dependent features (elastic_sync, OBSO peak, sync_score,
pre_window, pressure) see the batch's window, and M13 single-owner action
de-dup assigns each action to ``floor(frame / size)`` — so prod (Spark
mapInPandas dispatch) and local (``run_work_unit`` loop) MUST resolve the
identical size for a given provider (H3 lockstep). This module is the single
source of truth both sides import.

Per-provider defaults (evidence — the FULL census of the 2026-06-11 scoped prod
test at 2500, run 883267532931612, 24 units):

- 13 of 16 TRACKING units OOMed the 1 GB serverless UDF cap
  (``UDF_PYSPARK_ERROR.OOM``): gradientsports 4/4, idsse 4/4, metrica 3/4,
  skillcorner 2/4. The 3 tracking units that passed (metrica Sample_Game_1 p2,
  both skillcorner p2 halves) are the exception, not the rule — 2500 is NOT
  memory-safe for ANY tracking provider on the current column set.
- Every provider therefore defaults to 250, the universally prod-proven
  pre-ADR-047 value (the map below is intentionally EMPTY — it exists as the
  documented seam for re-earning larger sizes per provider).

The ADR-047 local A/B that motivated 2500 measured throughput only; its memory
envelope predated the 4.22 column families (xT-GK incl. five philosophy presets,
gk_completion — PR #368). Re-earn a larger size per provider with scoped
override runs (e.g. ``{"provider":"metrica","max_units":"4",
"frame_batch_size":"1000"}``) walking the envelope up, then add a map entry
citing the passing run id.

Override precedence (highest wins):

1. Explicit ``override`` argument — plumbed from the ``frame_batch_size`` job
   parameter via the drain worker's ``--frame-batch-size`` flag (run-scoped,
   e.g. for a memory-envelope A/B without a wheel release).
2. ``AC_FRAME_BATCH_SIZE`` env var — local runs / profiling
   (``scripts/profile_ac1_local.py``).
3. ``FRAME_BATCH_SIZE_BY_PROVIDER`` — the per-provider defaults above.
4. ``DEFAULT_FRAME_BATCH_SIZE`` — the conservative prod-proven floor.

Stdlib only — importable by the Spark driver, the executor UDF closure, the
local hexagon, and the fixture extractor alike.
"""

from __future__ import annotations

import os
from collections.abc import Mapping

# Universally prod-proven floor: every pre-ADR-047 production run (all
# providers, full halves) ran at 250 against the 1 GB serverless UDF cap.
DEFAULT_FRAME_BATCH_SIZE = 250

# Providers with prod evidence at a LARGER batch (see module docstring).
# INTENTIONALLY EMPTY since the 2026-06-11 OOM census (13/16 tracking units
# OOMed at 2500, every provider affected): an entry requires a passing scoped
# prod run at that size on the CURRENT column set, cited in a comment.
FRAME_BATCH_SIZE_BY_PROVIDER: Mapping[str, int] = {}

# Run-scoped escape hatch for local runs / profiling sweeps.
ENV_VAR = "AC_FRAME_BATCH_SIZE"


def resolve_frame_batch_size(provider: str, override: int | None = None) -> int:
    """Resolve the frame batch size for ``provider`` (see module docstring).

    Args:
        provider: data_source of the work unit (e.g. ``"idsse"``).
        override: run-scoped explicit size (job parameter / CLI flag). Takes
            precedence over everything; ``None`` means "not set".

    Returns:
        The batch size to use for both Spark ``frame_batch_id`` assignment and
        the in-batch single-owner action math.

    Raises:
        ValueError: if the resolved value (override or env) is not a positive
            integer — fail loud rather than batch with a nonsense size.
    """
    if override is not None:
        if override <= 0:
            msg = f"frame_batch_size override must be > 0, got {override}"
            raise ValueError(msg)
        return override

    env_raw = os.environ.get(ENV_VAR, "").strip()
    if env_raw:
        try:
            env_val = int(env_raw)
        except ValueError:
            msg = f"{ENV_VAR} must be a positive integer, got {env_raw!r}"
            raise ValueError(msg) from None
        if env_val <= 0:
            msg = f"{ENV_VAR} must be > 0, got {env_val}"
            raise ValueError(msg)
        return env_val

    return FRAME_BATCH_SIZE_BY_PROVIDER.get(provider, DEFAULT_FRAME_BATCH_SIZE)

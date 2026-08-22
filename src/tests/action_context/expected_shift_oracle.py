"""Expected-shift oracle for the silly-kicks 4.87.0 live recompute (spec §8, Task 13).

The rebaselined goldens (Task 12) are a REGRESSION guard: they pin whatever 4.87.0 produces, so a
wrong-but-stable value is invisible to a golden regenerated from the same code. The correctness
question — "did the values move the way the changelog says they should?" — is answered by the
Part-B §11.1b pre-wipe shadow diff, which compares the OLD live distributions against the NEW
recomputed ones. That diff is only as strong as an explicit MODEL of what *should* change. This
module is that model.

Per value-shifting column it declares, per provider, a :class:`ShiftBand`:
  * ``cohort`` — the row cohort expected to move: ``"away"`` (the direction-cycle away-team rows),
    ``"home_y_mirror"`` (home rows only, where a y-mirror bug applied), or ``"all"`` (a corpus-wide
    surface change).
  * ``min_change_rate`` / ``max_change_rate`` — the fraction of rows in the cohort expected to change
    (0-1), a deliberately GENEROUS band. The changelog deltas are point estimates on different
    corpora, so a too-tight band false-halts a 5.5 h recompute; err wide (Task 13 / §11.1b Step 2
    calibration note). Widening on the FIRST live run is legitimate ONLY with a recorded mechanism.
  * ``direction`` — the qualitative mechanism where one is known (e.g. space-creation columns were
    exchanged for away actions pre-fix).

Consumed against LIVE data in Part B §11.1b (Task 20). Here it is a self-tested, correct deliverable:
``test_expected_shift_oracle.py`` asserts the bands are well-formed and cover every value-shifting
column the migration is expected to move.

Changelog sources (spec §7.2/§7.3/§8, §6.6; silly-kicks 4.43→4.87.0):
  * OBSO/PAUSA values — the MANDATORY ``xt=`` switch (§7.3) replaces the synthetic EPV ramp with the
    real fitted xT grid → a corpus-wide value change on the OBSO/PAUSA surface (all rows).
  * space_created_m2 / space_denied_m2_opponent — the direction-of-play cycle exchanged ``created``
    and ``denied`` for AWAY actions pre-fix (§8, §11.1b): measured away-row change ~47% (GS) / ~60%
    (IDSSE). (These columns also ride the xT surface change, but the documented per-provider delta is
    the away-exchange signal, so that is the characteristic band.)
  * ghost_gk_x / ghost_gk_y — the 4.87.0 ghost-GK default path is ``predict_mean`` (params-only), not
    the old KDE argmax (§6.6) → a ~0.20 m shift on essentially every GK-domain row (all tracking).
  * cross_blocked — StatsBomb 4.86.0 un-deferred the mask: all-``pd.NA`` → a real open-play-cross
    mask (§7.2), so the non-null rate on StatsBomb goes 0% → ~base-rate. (SPADL surface column, diffed
    over ``bronze.spadl_actions`` / ``vaep_action_values`` in §11.1b.)
"""

from __future__ import annotations

from dataclasses import dataclass

VALID_COHORTS: frozenset[str] = frozenset({"away", "home_y_mirror", "all"})

# The four open-data providers whose SPADL surface carries cross_blocked; the tracking providers that
# compute OBSO/space-creation/ghost-GK. Kept here so the self-test can assert coverage per provider.
TRACKING_PROVIDERS: frozenset[str] = frozenset({"idsse", "metrica", "skillcorner", "gradientsports"})


@dataclass(frozen=True)
class ShiftBand:
    """Expected old-vs-new shift for one (column, provider) under the 4.87.0 recompute."""

    cohort: str
    min_change_rate: float
    max_change_rate: float
    direction: str | None = None


# Per-column, per-provider expected shift. Provider ``"*"`` is the wildcard fallback used by
# :func:`band_for` when no provider-specific band exists.
EXPECTED: dict[str, dict[str, ShiftBand]] = {
    # --- OBSO / PAUSA: synthetic EPV ramp -> real fitted xT (§7.3), corpus-wide on all rows. ---
    "obso_actual": {"*": ShiftBand("all", 0.70, 1.0, "synthetic_to_xt")},
    "obso_peak": {"*": ShiftBand("all", 0.70, 1.0, "synthetic_to_xt")},
    "obso_optimal": {"*": ShiftBand("all", 0.70, 1.0, "synthetic_to_xt")},
    "pausa_temporal": {"*": ShiftBand("all", 0.60, 1.0, "synthetic_to_xt")},
    "pausa_spatial": {"*": ShiftBand("all", 0.60, 1.0, "synthetic_to_xt")},
    "pausa_composite": {"*": ShiftBand("all", 0.60, 1.0, "synthetic_to_xt")},
    # --- space-creation: created/denied exchanged for AWAY actions pre-fix (§8). Measured away-row
    #     change ~0.47 (GS) / ~0.60 (IDSSE); generic away band for the other tracking providers. ---
    "space_created_m2": {
        "gradientsports": ShiftBand("away", 0.30, 0.65, "exchanged"),
        "idsse": ShiftBand("away", 0.45, 0.75, "exchanged"),
        "*": ShiftBand("away", 0.30, 0.75, "exchanged"),
    },
    "space_denied_m2_opponent": {
        "gradientsports": ShiftBand("away", 0.30, 0.65, "exchanged"),
        "idsse": ShiftBand("away", 0.45, 0.75, "exchanged"),
        "*": ShiftBand("away", 0.30, 0.75, "exchanged"),
    },
    # --- ghost-GK: predict_mean default replaces KDE argmax (§6.6), ~0.20 m shift on most GK rows. ---
    "ghost_gk_x": {"*": ShiftBand("all", 0.50, 1.0, "predict_mean_shift")},
    "ghost_gk_y": {"*": ShiftBand("all", 0.50, 1.0, "predict_mean_shift")},
    # --- cross_blocked: StatsBomb 4.86.0 NA -> real open-play-cross mask (§7.2), 0% -> ~base-rate. ---
    "cross_blocked": {"statsbomb": ShiftBand("all", 0.005, 0.08, "na_to_real")},
}

# The columns the 4.87.0 recompute is EXPECTED to move (the "differential flagged as moved" set the
# §11.1b shadow diff must cover). The self-test asserts EXPECTED has a band for every one of these.
VALUE_SHIFTING_COLUMNS: frozenset[str] = frozenset(EXPECTED)


def band_for(column: str, provider: str) -> ShiftBand | None:
    """Return the expected shift band for ``(column, provider)``, or ``None`` if the column is not
    expected to move. Falls back to the provider-agnostic ``"*"`` band when no provider-specific one
    is declared."""
    per = EXPECTED.get(column)
    if per is None:
        return None
    return per.get(provider) or per.get("*")

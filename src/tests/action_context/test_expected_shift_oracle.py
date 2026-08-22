"""Self-test for the expected-shift oracle (Task 13 Step 2, spec §8).

The oracle is consumed against LIVE data in Part B §11.1b; here it must be a CORRECT, self-tested
deliverable. This asserts every band is well-formed (rates in [0,1], min<=max, valid cohort) and
that the oracle covers every column the 4.87.0 migration is independently known to move — so a band
dropped for a real mover fails here, not silently at the live gate.
"""

from __future__ import annotations

from tests.action_context.expected_shift_oracle import (
    EXPECTED,
    VALID_COHORTS,
    VALUE_SHIFTING_COLUMNS,
    ShiftBand,
    band_for,
)

# Independent reference list of the columns the 4.87.0 recompute is expected to move, transcribed
# from the spec (NOT derived from EXPECTED — so the coverage check below is non-vacuous):
#   * OBSO/PAUSA values      — mandatory xt= synthetic->fitted-xT switch (§7.3)
#   * space_created/denied   — created/denied exchanged for away actions pre-fix (§8)
#   * ghost_gk_x/y           — predict_mean replaces KDE argmax (§6.6)
#   * cross_blocked          — StatsBomb NA -> real mask (§7.2)
_EXPECTED_MOVERS: frozenset[str] = frozenset(
    {
        "obso_actual",
        "obso_peak",
        "obso_optimal",
        "pausa_temporal",
        "pausa_spatial",
        "pausa_composite",
        "space_created_m2",
        "space_denied_m2_opponent",
        "ghost_gk_x",
        "ghost_gk_y",
        "cross_blocked",
    }
)


def test_all_bands_are_well_formed() -> None:
    assert EXPECTED, "oracle is empty — no value-shifting columns declared"
    for column, per_provider in EXPECTED.items():
        assert per_provider, f"{column}: no provider bands"
        for provider, band in per_provider.items():
            assert isinstance(band, ShiftBand), f"{column}/{provider}: not a ShiftBand"
            assert band.cohort in VALID_COHORTS, f"{column}/{provider}: invalid cohort {band.cohort!r}"
            assert 0.0 <= band.min_change_rate <= 1.0, f"{column}/{provider}: min out of [0,1]"
            assert 0.0 <= band.max_change_rate <= 1.0, f"{column}/{provider}: max out of [0,1]"
            assert band.min_change_rate <= band.max_change_rate, f"{column}/{provider}: min > max"
            assert band.direction is None or isinstance(band.direction, str), (
                f"{column}/{provider}: direction must be None or str"
            )


def test_oracle_covers_every_expected_mover() -> None:
    """Non-vacuous coverage: every independently-listed value-shifting column has a band."""
    uncovered = sorted(_EXPECTED_MOVERS - set(EXPECTED))
    assert not uncovered, f"oracle missing a band for known value-shifting column(s): {uncovered}"
    # VALUE_SHIFTING_COLUMNS is the exported view of the oracle keys; keep it in sync with EXPECTED.
    assert VALUE_SHIFTING_COLUMNS == frozenset(EXPECTED)


def test_band_for_resolves_provider_specific_then_wildcard() -> None:
    # Provider-specific band wins over the wildcard.
    idsse = band_for("space_created_m2", "idsse")
    assert idsse is not None and idsse.cohort == "away" and idsse.min_change_rate == 0.45
    # A tracking provider with no specific band falls back to "*".
    sc = band_for("space_created_m2", "skillcorner")
    assert sc is not None and sc.cohort == "away" and sc.min_change_rate == 0.30
    # A corpus-wide column resolves purely via the wildcard.
    metrica_obso = band_for("obso_actual", "metrica")
    assert metrica_obso is not None and metrica_obso.cohort == "all"
    # cross_blocked is StatsBomb-only with no wildcard -> other providers get None.
    assert band_for("cross_blocked", "statsbomb") is not None
    assert band_for("cross_blocked", "wyscout") is None
    # An unknown column is not expected to move.
    assert band_for("pitch_control_at_target__spearman", "idsse") is None


def test_direction_labels_are_from_the_known_set() -> None:
    """Directions are qualitative mechanism labels — keep them a closed, documented set so a typo
    (which would silently weaken the §11.1b direction check) is caught here."""
    known = {"synthetic_to_xt", "exchanged", "predict_mean_shift", "na_to_real"}
    seen = {b.direction for per in EXPECTED.values() for b in per.values() if b.direction is not None}
    assert seen <= known, f"unknown direction label(s): {sorted(seen - known)}"

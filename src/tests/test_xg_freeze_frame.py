import numpy as np

from analytics.xg_freeze_frame import SPADL_PITCH, PitchDims, normalize_freeze_frame


def _players():
    # (x, y, is_keeper, is_teammate) in SPADL meters, home-LTR
    return np.array(
        [
            [105.0, 34.0, 1.0, 0.0],  # opponent keeper on goal line, centre
            [90.0, 20.0, 0.0, 0.0],  # opponent defender
            [95.0, 40.0, 0.0, 1.0],  # teammate
        ],
        dtype=np.float64,
    )


def test_spadl_normalization_matches_statsbomb_fractional_position():
    # SPADL / 105, / 68 lands on the same fractional [0,1] point StatsBomb / 120, / 80 would.
    out = normalize_freeze_frame(_players(), SPADL_PITCH, shooter_attacks_high_x=True)
    assert out.shape == (3, 4)
    # keeper at x=105 -> x_norm 1.0 ; y=34 -> 0.5
    np.testing.assert_allclose(out[0, :2], [1.0, 0.5], atol=1e-9)
    # flags preserved
    np.testing.assert_array_equal(out[:, 2], [1.0, 0.0, 0.0])
    np.testing.assert_array_equal(out[:, 3], [0.0, 0.0, 1.0])


def test_away_shooter_is_point_reflected_to_attack_high_x():
    out = normalize_freeze_frame(_players(), SPADL_PITCH, shooter_attacks_high_x=False)
    # x -> (105-x)/105 ; keeper x=105 -> 0.0 ; y -> (68-y)/68 ; y=34 -> 0.5
    np.testing.assert_allclose(out[0, :2], [0.0, 0.5], atol=1e-9)


def test_empty_set_returns_zero_by_four():
    out = normalize_freeze_frame(np.empty((0, 4)), SPADL_PITCH, shooter_attacks_high_x=True)
    assert out.shape == (0, 4)


def test_pitch_dims_is_frozen_dataclass():
    dims = PitchDims(105.0, 68.0)
    assert (dims.length, dims.width) == (105.0, 68.0)

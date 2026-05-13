"""CVD (color-vision-deficiency) accessibility test for chart color palettes.

Asserts minimum perceptual distance between semantically-distinct color pairs
under deuteranopia and protanopia simulation. Uses CIELAB ΔE*ab distance —
pairs below JND threshold 20 are indistinguishable to CVD users.

No external dependency — uses hand-rolled sRGB→CIELAB conversion via the
standard D65 illuminant. The CVD simulation matrices are from Brettel et al.
(1997) / Viénot et al. (1999), the same source used by colorspacious.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))

from render import AWAY_COLOR, DEFCON_COLORS, HOME_COLOR

# ── sRGB → linear RGB → XYZ → CIELAB pipeline ──────────────────────────────


def _hex_to_linear_rgb(hex_color: str) -> tuple[float, float, float]:
    """Convert #RRGGBB to linear RGB (0-1 range, gamma-decoded)."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16) / 255, int(h[2:4], 16) / 255, int(h[4:6], 16) / 255

    # sRGB gamma decode
    def _linearize(c: float) -> float:
        return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4

    return _linearize(r), _linearize(g), _linearize(b)


def _linear_rgb_to_xyz(r: float, g: float, b: float) -> tuple[float, float, float]:
    """Linear sRGB to CIE XYZ (D65 illuminant)."""
    x = 0.4124564 * r + 0.3575761 * g + 0.1804375 * b
    y = 0.2126729 * r + 0.7151522 * g + 0.0721750 * b
    z = 0.0193339 * r + 0.1191920 * g + 0.9503041 * b
    return x, y, z


def _xyz_to_lab(x: float, y: float, z: float) -> tuple[float, float, float]:
    """CIE XYZ to CIELAB (D65 reference white)."""
    xn, yn, zn = 0.95047, 1.0, 1.08883

    def _f(t: float) -> float:
        return t ** (1 / 3) if t > 0.008856 else 7.787 * t + 16 / 116

    fx, fy, fz = _f(x / xn), _f(y / yn), _f(z / zn)
    l_star = 116 * fy - 16
    a_star = 500 * (fx - fy)
    b_star = 200 * (fy - fz)
    return l_star, a_star, b_star


# ── CVD simulation (Viénot et al. 1999 / Brettel et al. 1997) ──────────────

# Protanopia simulation matrix (applied in linear RGB space)
_PROTAN_MATRIX = [
    (0.152286, 1.052583, -0.204868),
    (0.114503, 0.786281, 0.099216),
    (-0.003882, -0.048116, 1.051998),
]

# Deuteranopia simulation matrix
_DEUTAN_MATRIX = [
    (0.367322, 0.860646, -0.227968),
    (0.280085, 0.672501, 0.047414),
    (-0.011820, 0.042940, 0.968881),
]


def _apply_cvd_matrix(
    matrix: list[tuple[float, float, float]], r: float, g: float, b: float
) -> tuple[float, float, float]:
    """Apply a 3x3 CVD simulation matrix to linear RGB."""
    r2 = matrix[0][0] * r + matrix[0][1] * g + matrix[0][2] * b
    g2 = matrix[1][0] * r + matrix[1][1] * g + matrix[1][2] * b
    b2 = matrix[2][0] * r + matrix[2][1] * g + matrix[2][2] * b
    return max(0, min(1, r2)), max(0, min(1, g2)), max(0, min(1, b2))


def _hex_to_lab_cvd(hex_color: str, matrix: list[tuple[float, float, float]]) -> tuple[float, float, float]:
    """Convert hex color to CIELAB after CVD simulation."""
    r, g, b = _hex_to_linear_rgb(hex_color)
    r2, g2, b2 = _apply_cvd_matrix(matrix, r, g, b)
    x, y, z = _linear_rgb_to_xyz(r2, g2, b2)
    return _xyz_to_lab(x, y, z)


def _delta_e(lab1: tuple[float, float, float], lab2: tuple[float, float, float]) -> float:
    """CIE76 ΔE*ab — Euclidean distance in CIELAB space."""
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(lab1, lab2, strict=True)))


# ── Test data ───────────────────────────────────────────────────────────────

# Semantically-distinct color pairs that MUST be distinguishable
_CRITICAL_PAIRS: list[tuple[str, str, str, str]] = [
    ("HOME_COLOR", HOME_COLOR, "AWAY_COLOR", AWAY_COLOR),
    ("Intercept", DEFCON_COLORS["Intercept"], "Concede", DEFCON_COLORS["Concede"]),
    ("Intercept", DEFCON_COLORS["Intercept"], "Disturb", DEFCON_COLORS["Disturb"]),
    ("Intercept", DEFCON_COLORS["Intercept"], "Deter", DEFCON_COLORS["Deter"]),
    ("Concede", DEFCON_COLORS["Concede"], "Disturb", DEFCON_COLORS["Disturb"]),
    ("Concede", DEFCON_COLORS["Concede"], "Deter", DEFCON_COLORS["Deter"]),
    ("Disturb", DEFCON_COLORS["Disturb"], "Deter", DEFCON_COLORS["Deter"]),
]

# JND threshold — pairs below this ΔE are considered indistinguishable.
# WCAG-adjacent: typical JND for CVD users is ~10-15; we use 20 as a
# conservative threshold (some research suggests 25 for deuteranopia).
_JND_THRESHOLD = 20.0


@pytest.mark.parametrize(
    ("name_a", "color_a", "name_b", "color_b"),
    _CRITICAL_PAIRS,
    ids=[f"{p[0]}_vs_{p[2]}" for p in _CRITICAL_PAIRS],
)
@pytest.mark.parametrize(
    ("cvd_name", "cvd_matrix"),
    [("protanopia", _PROTAN_MATRIX), ("deuteranopia", _DEUTAN_MATRIX)],
)
def test_color_pair_distinguishable_under_cvd(
    name_a: str,
    color_a: str,
    name_b: str,
    color_b: str,
    cvd_name: str,
    cvd_matrix: list[tuple[float, float, float]],
) -> None:
    """Assert minimum perceptual distance between semantically-distinct colors
    under CVD simulation. Hard assert — failing pairs must be fixed before merge.
    Shape markers (WCAG 1.4.1) provide a secondary cue but do not excuse
    indistinguishable color pairs."""
    lab_a = _hex_to_lab_cvd(color_a, cvd_matrix)
    lab_b = _hex_to_lab_cvd(color_b, cvd_matrix)
    de = _delta_e(lab_a, lab_b)
    assert de >= _JND_THRESHOLD, (
        f"{name_a} ({color_a}) vs {name_b} ({color_b}) under {cvd_name}: "
        f"ΔE = {de:.1f} < {_JND_THRESHOLD} — colors are too similar for CVD users"
    )

"""The single source of the Ops colour palette (spec S06 / A-SPEC-03).

Taipy-free by design: both the Taipy stylekit (passed to `gui.run(stylekit=…)` from `shell`) and the anonymous public
page (`web/public.py`, which must import no Taipy) derive their `--color-*` tokens from the one `PALETTE` here, so the
operator pages and the public page cannot drift when the brand colour changes. This module imports no Taipy and no
lake package.
"""

from __future__ import annotations

from typing import Final

__all__ = ["PALETTE", "css_root_block", "palette_stylekit"]

# The amber-on-dark-paper lakehouse palette (ported from the v1 app). The keys are the Taipy stylekit names; the public
# page maps them to `--color-*` CSS variables through `css_root_block`.
PALETTE: Final[dict[str, str]] = {
    "color_primary": "#f59e0b",
    "color_secondary": "#d97706",
    "color_paper": "#1a1a2e",
    "color_background": "#0e1117",
    "color_background_dark": "#0a0d12",
    "color_background_light": "#1a1d23",
    "font_family": "Source Sans Pro, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif",
}

# The stylekit color keys that map to `--color-*` CSS variables (the font family is set separately by the stylekit).
_COLOR_KEYS: Final[tuple[str, ...]] = (
    "color_primary",
    "color_secondary",
    "color_paper",
    "color_background",
    "color_background_dark",
    "color_background_light",
)


def palette_stylekit() -> dict[str, str]:
    """The Taipy stylekit dict for `gui.run(stylekit=…)` (a copy, so a caller cannot mutate the source)."""
    return dict(PALETTE)


def css_root_block() -> str:
    """The `:root { --color-*: … }` CSS block for the public page, derived from `PALETTE` so it cannot drift from the
    operator stylekit. One CSS custom property per color key (`color_primary` -> `--color-primary`)."""
    body = "".join(f"--{key.replace('_', '-')}:{PALETTE[key]};" for key in _COLOR_KEYS)
    return f":root{{{body}}}"

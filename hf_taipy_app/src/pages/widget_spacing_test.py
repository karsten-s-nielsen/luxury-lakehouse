"""Widget spacing test — De Bruijn sequence covering all 9 widget-pair transitions.

Uses build_sidebar_section() from the template. The test passes only if the
gap between EVERY two consecutive widgets is identical, regardless of type.

Sequence: D1, D2, S1, T1, T2, S2, S1, D1, T1, D2
Pairs:    DD, DS, ST, TT, TS, SS, SD, DT, TD  — all 9 permutations covered.
"""

from __future__ import annotations

from page_template import SidebarWidget, build_sidebar_section

# State variables (wst_ prefixed)
wst_drop_a: str | None = None
wst_drop_a_lov: list[str] = ["Option A", "Option B", "Option C"]
wst_drop_b: str | None = None
wst_drop_b_lov: list[str] = ["Option X", "Option Y", "Option Z"]
wst_slider_a: int = 5
wst_slider_b: int = 50
wst_toggle_a: bool = True
wst_toggle_b: bool = False


def _noop(state, var, val):
    pass


__all__ = [
    "_noop",
    "wst_drop_a",
    "wst_drop_a_lov",
    "wst_drop_b",
    "wst_drop_b_lov",
    "wst_slider_a",
    "wst_slider_b",
    "wst_toggle_a",
    "wst_toggle_b",
]

# Widget definitions — data only, zero styling
_D1 = SidebarWidget("dropdown", "wst_drop_a", "Dropdown A", "_noop", lov="wst_drop_a_lov")
_D2 = SidebarWidget("dropdown", "wst_drop_b", "Dropdown B", "_noop", lov="wst_drop_b_lov")
_S1 = SidebarWidget(
    "slider", "wst_slider_a", "Slider A", "_noop", slider_min="1", slider_max="10", slider_range_labels=("1", "10")
)
_S2 = SidebarWidget(
    "slider", "wst_slider_b", "Slider B", "_noop", slider_min="0", slider_max="100", slider_range_labels=("0", "100")
)
_T1 = SidebarWidget("toggle", "wst_toggle_a", "Toggle A", "_noop")
_T2 = SidebarWidget("toggle", "wst_toggle_b", "Toggle B", "_noop")

# De Bruijn sequence B(3,2): D D S T T S S D T D
# Covers all 9 consecutive pairs: DD DS ST TT TS SS SD DT TD
_ALL_WIDGETS = [_D1, _D2, _S1, _T1, _T2, _S2, _S1, _D1, _T1, _D2]

_section = build_sidebar_section("Spacing Test", _ALL_WIDGETS, f_string=False)

page_md = f"""
## Widget Spacing Test

De Bruijn sequence: D1 D2 S1 T1 T2 S2 S1 D1 T1 D2

Covers all 9 consecutive-pair permutations: DD DS ST TT TS SS SD DT TD

**Pass criteria:** the gap between every two consecutive widgets is identical.

---

<|layout|columns=300px 1fr|gap=0.75rem|

<|part|class_name=sidebar|
{_section}
|>

<|part|
*Measure gaps between all 9 consecutive widget pairs. All must be equal.*
|>

|>
"""

"""Chart + RawHtml builder tests for the GK Analytics views."""

import pytest

pytest.importorskip("plotly")

from services.gk_insight import Verdict
from state.gk_analytics_charts import (
    _AMBER,
    _POS_COLOR,
    build_distribution_scatter_figure,
    build_line_height_figure,
    build_sweeper_profile_figure,
)
from state.gk_analytics_render import (
    render_big_story_html,
    render_honest_secondary_html,
)


def _profile_points():
    # (name, progress_m, share, n, is_selected)
    return [
        ("Andre Onana", 22.0, 0.24, 51, False),
        ("Alisson", 14.0, 0.18, 67, False),
        ("Keylor Navas", 26.0, 0.06, 80, False),
        ("Matt Turner", 23.0, 0.15, 110, True),
    ]


def test_distribution_scatter_quadrant_crosshair_and_selected_highlight():
    fig = build_distribution_scatter_figure(_profile_points(), share_median=0.15, progress_median=22.0)
    assert fig is not None
    d = fig.to_dict()
    # cohort-median crosshair (one vertical at progress_median, one horizontal at share%)
    shapes = d.get("layout", {}).get("shapes", ())
    assert any(s.get("x0") == 22.0 and s.get("x1") == 22.0 for s in shapes)  # vertical median line
    # quadrant corner labels + the selected-keeper label present
    anns = " ".join(a.get("text", "") for a in d.get("layout", {}).get("annotations", ()))
    assert "secure recycler" in anns and "proactive" in anns
    assert "Matt Turner" in anns  # selected keeper is labelled (arrow annotation)
    # the selected keeper has its own amber-marker trace (drawn on top), distinct from the grey cohort
    sel_traces = [t for t in d["data"] if t.get("marker", {}).get("color") == _AMBER]
    assert sel_traces, "selected keeper should have a distinct amber marker trace"
    # y axis is the % adds-threat (0..100 scale, share*100)
    assert "add threat" in d["layout"]["yaxis"]["title"]["text"].lower()


def test_distribution_scatter_none_on_empty():
    assert build_distribution_scatter_figure([], share_median=None, progress_median=None) is None


def test_distribution_scatter_degrades_without_cohort_medians():
    # cohort too small -> no crosshair, but still plots the points
    fig = build_distribution_scatter_figure(_profile_points()[:1], share_median=None, progress_median=None)
    assert fig is not None
    assert len(fig.to_dict()["data"][0]["x"]) == 1


def test_sweeper_profile_strip_and_highlight():
    cohort = [220.0, 240.0, 250.0, 255.0, 260.0, 270.0, 280.0, 290.0, 300.0]  # 9 keepers, median 260
    metrics = [
        ("Reachable area", 298.0, "298 m²", cohort, False),  # 298 > median -> better (blue)
        ("Closing time · 6-yd", 1.5, "1.5 s", [], True),  # empty cohort -> no strip, degrade note
    ]
    fig = build_sweeper_profile_figure(metrics)
    assert fig is not None
    d = fig.to_dict()
    anns = " ".join(a.get("text", "") for a in d["layout"].get("annotations", ()))
    assert "298 m²" in anns and "1.5 s" in anns and "better than cohort" in anns
    assert "cohort too small" in anns  # empty-cohort metric degrades
    # cohort drawn as an individual-keeper STRIP (faint grey dots), not a box
    grey = [t for t in d["data"] if t.get("marker", {}).get("color") == "rgba(150,165,185,0.45)"]
    assert grey and len(grey[0]["x"]) == 9
    # selected keeper highlighted blue (better than median)
    assert _POS_COLOR in [t.get("marker", {}).get("color") for t in d["data"]]


def test_line_height_figure_strip_descriptive():
    cohort = [30.0, 33.0, 35.0, 36.0, 37.0, 38.0, 40.0, 42.0, 45.0]  # 9 keepers, median 37
    fig = build_line_height_figure(40.0, cohort)
    assert fig is not None
    d = fig.to_dict()
    anns = " ".join(a.get("text", "") for a in d["layout"].get("annotations", ()))
    title = d["layout"]["title"]["text"]
    assert "40 m" in anns and "above cohort" in anns and "cohort median 37 m" in anns
    assert "Deep block" not in anns and "High line" not in anns and "unused" not in (anns + title).lower()
    grey = [t for t in d["data"] if t.get("marker", {}).get("color") == "rgba(150,165,185,0.45)"]
    assert grey and len(grey[0]["x"]) == 9  # cohort strip


def test_line_height_figure_none_when_no_value():
    assert build_line_height_figure(None, []) is None


def test_honest_secondary_two_boxes():
    s = render_honest_secondary_html(
        ghost_dev="1.9 m",
        ghost_n="n=12 shots",
        goals_prevented="+0.40 ± 1.50",
        gp_note="inconclusive",
        low_sample=True,
    ).html
    assert "Ghost-positioning deviation" in s and "Goals prevented" in s and "1.9 m" in s and "+0.40 ± 1.50" in s


def test_honest_secondary_badge_is_conditional_not_hardcoded():
    shown = render_honest_secondary_html(
        ghost_dev="1.0 m", ghost_n="n=32", goals_prevented="+0.28 ± 1.82", gp_note="x", low_sample=True
    ).html
    hidden = render_honest_secondary_html(
        ghost_dev="1.0 m", ghost_n="n=32", goals_prevented="+0.28 ± 1.82", gp_note="x", low_sample=False
    ).html
    assert "low sample" in shown and "low sample" not in hidden


def test_big_story_contains_label_and_phrase():
    html = render_big_story_html(
        Verdict("Proactive distributor", "adds threat at volume, playing forward"),
        body="Adds threat on 15% of his distributions.",
    )
    s = html.html
    assert "★ Big story" in s and "Proactive distributor" in s

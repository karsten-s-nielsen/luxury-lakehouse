"""Player Comparison page — config only, layout from page_template."""

from __future__ import annotations

from page_template import Citation, PageConfig, build_page

_CONTENT = """\
<|part|render={pr_comp_selected and pr_player_count == 0 and len(pr_no_data_warning) == 0}|
Select 1\u20133 players to compare.
|>

<|part|render={len(pr_no_data_warning) > 0}|
<|{pr_no_data_warning}|text|>
|>

<|part|render={pr_player_count > 0 and len(pr_radar_image) > 0}|

<|part|render={len(pr_no_physical_note) > 0}|
<|{pr_no_physical_note}|text|class_name=ll-reference|>
|>

<|part|render={len(pr_low_minute_warning) > 0}|
<|{pr_low_minute_warning}|text|>
|>

<|{pr_radar_image}|image|label=Player Comparison Radar|width=100%|>

<|part|render={len(pr_spoke_caption) > 0}|class_name=ll-reference|
<|{pr_spoke_caption}|text|>
|>

|>

<|part|render={len(pr_stats_table) > 0}|
<|part|class_name=ll-subtitle|
Full Stats
|>
<|{pr_stats_table}|table|page_size=25|>
|>

<|part|render={len(pr_data_freshness_text) > 0}|class_name=ll-reference|
<|{pr_data_freshness_text}|text|>
|>"""

page_md = build_page(
    PageConfig(
        title="Player Comparison",
        icon="radar",
        description="Multi-metric player comparison using radar chart. Metrics from VAEP and tracking data.",
        citations=[
            Citation("mplsoccer", "https://mplsoccer.readthedocs.io/"),
            Citation("Decroos et al. (2019)", "https://doi.org/10.1007/s10994-021-05989-6"),
        ],
        image_var="",
        empty_message="Select a competition to begin.",
        empty_condition="not pr_comp_selected",
        pre_image_content=_CONTENT,
    )
)

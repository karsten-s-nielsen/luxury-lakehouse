"""Player Similarity page — config only, layout from page_template."""

from __future__ import annotations

from page_template import Citation, PageConfig, build_page

_CONTENT = """\
<|part|render={len(ps_status_message) > 0}|
<|{ps_status_message}|text|>
|>

<|part|render={len(ps_results_data) > 0}|

<|part|class_name=ll-subtitle|
Similar Players
|>

<|{ps_results_data}|table|>

<|part|class_name=ll-subtitle|
Radar Comparison
|>

<|part|render={len(ps_radar_image) > 0}|
<|{ps_radar_image}|image|label=Radar Comparison|width=100%|>
|>

|>"""

page_md = build_page(
    PageConfig(
        title="Player Similarity",
        icon="search",
        description=(
            "Find similar players using pgvector cosine distance on behavioral (32-d) "
            "or statistical (13-d) embedding vectors."
        ),
        citations=[
            Citation("Theiner et al. (2022)", "https://doi.org/10.1007/978-3-031-02044-5_2"),
            Citation("Doc2Vec (Le & Mikolov 2014)", "https://arxiv.org/abs/1405.4053"),
            Citation(
                "luxury-lakehouse/football2vec-statsbomb-wyscout",
                "https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout",
            ),
        ],
        image_var="",
        empty_message="Select a player to begin.",
        empty_condition="ps_selected_player is None and len(ps_results_data) == 0",
        pre_image_content=_CONTENT,
    )
)

"""Player Similarity page — config only, layout from page_template."""

from __future__ import annotations

from page_template import Citation, ContentBlock, ContentRow, PageConfig, build_page

page_config = PageConfig(
    title="Player Similarity",
    icon="search",
    nav_section="Player Analysis",
    freshness_var="ps_data_freshness",
    description=(
        "Find similar players using pgvector cosine distance on behavioral (32-d) "
        "or statistical (13-d) embedding vectors. "
        "Behavioral embeddings via Theiner et al. (2022) football2vec with Doc2Vec (Le & Mikolov 2014). "
        "Model: luxury-lakehouse/football2vec-statsbomb-wyscout."
    ),
    citations=[
        Citation("Theiner et al. (2022)", "https://doi.org/10.1007/978-3-031-02044-5_2"),
        Citation("Doc2Vec (Le & Mikolov 2014)", "https://arxiv.org/abs/1405.4053"),
        Citation(
            "luxury-lakehouse/football2vec-statsbomb-wyscout",
            "https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout",
        ),
    ],
    warning_var="ps_warning_text",
    content=[
        ContentRow([ContentBlock("text", "ps_status_message")]),
        ContentRow([ContentBlock("text", "ps_threshold_caption")]),
        ContentRow([ContentBlock("table", "ps_results_data", header="Similar Players")]),
        ContentRow(
            [
                ContentBlock(
                    "image",
                    "ps_radar_image",
                    header="Radar Comparison",
                    condition="len(ps_results_data) > 0 and len(ps_radar_image) > 0",
                )
            ]
        ),
        ContentRow([ContentBlock("text", "ps_spoke_caption", condition="len(ps_spoke_caption) > 0")]),
    ],
    empty_message="Select a player to begin.",
    empty_condition="ps_selected_player is None and len(ps_results_data) == 0",
)
page_md = build_page(page_config)

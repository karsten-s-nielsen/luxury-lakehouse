"""Player Similarity page — config only, layout from page_template."""

from __future__ import annotations

from page_template import NAV_PLAYER_ANALYSIS, Citation, ContentBlock, ContentRow, PageConfig, build_page

page_config = PageConfig(
    title="Player Similarity",
    icon="search",
    nav_section=NAV_PLAYER_ANALYSIS,
    freshness_var="ps_data_freshness",
    description=(
        "Find similar players using pgvector cosine distance on behavioral (128-d) "
        "or statistical (13-d) embedding vectors. "
        "Behavioral embeddings via football2vec v2 transformer encoder with adversarial "
        "team debiasing (Ganin et al. 2016). "
        "Model: luxury-lakehouse/football2vec-v2."
    ),
    citations=[
        Citation("Ganin et al. (2016) — Gradient Reversal", "https://jmlr.org/papers/v17/15-239.html"),
        Citation("Danesi (2025) — Football2Vec", "https://doi.org/10.1007/978-3-031-02044-5_2"),
        Citation(
            "luxury-lakehouse/football2vec-v2",
            "https://huggingface.co/luxury-lakehouse/football2vec-v2",
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
                    "chart",
                    "ps_neighborhood_chart",
                    header="Embedding Neighborhood",
                    condition="len(ps_results_data) > 0 and ps_neighborhood_chart is not None",
                )
            ]
        ),
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

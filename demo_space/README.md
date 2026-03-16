---
title: Soccer Analytics Explorer
emoji: ⚽
colorFrom: yellow
colorTo: gray
sdk: gradio
sdk_version: "5.23.3"
app_file: app.py
pinned: false
license: apache-2.0
thumbnail: https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo/resolve/main/thumbnail.png
tags:
  - soccer
  - football
  - analytics
  - embeddings
  - mplsoccer
  - statsbomb
  - pitch-control
  - defcon
short_description: Pass quality, pitch control, similarity, DEFCON, PAUSA
---

# Soccer Analytics Explorer

Interactive demo for the [Luxury Lakehouse](https://huggingface.co/luxury-lakehouse) soccer analytics platform.

## Tabs

| Tab | Description |
|-----|-------------|
| **Pass Quality** | Pass origins with line-breaking pass highlighting (Ward clustering on 360 freeze frames) |
| **Pitch Control** | Physics-based pitch control (Spearman 2017) with frame slider and velocity arrows |
| **Player Similarity** | Doc2Vec behavioral embedding search — find players with similar styles |
| **Shot Map** | Shot locations on a half-pitch, colored by outcome |
| **Defensive Impact** | Defensive contribution breakdown per match — Intercept/Concede/Disturb/Deter |
| **Pass Timing** | PAUSA pass timing analysis — temporal judgment vs spatial selection with OBSO heatmap |

**Theme:** Luxury flagship — dark surfaces, amber/gold accents, sharp corners, Inter font, prominent tab navigation.

## Data

All data is pre-cached as Parquet files (no live database connectivity):

- `career_embeddings.parquet` — Doc2Vec career embeddings (~8,950 players)
- `sample_shots.parquet` — 1,000 shots from StatsBomb Open Data
- `sample_passes.parquet` — 2,000 passes with line-breaking detection
- `sample_tracking.parquet` — Metrica Sports tracking at 1fps (3 matches)
- `defcon_pressure.parquet` — DEFCON pressure aggregates with player names
- `sample_pausa.parquet` — PAUSA pass timing scores with OBSO decomposition

## Sources

- [StatsBomb Open Data](https://github.com/statsbomb/open-data) (CC-BY 4.0)
- [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) (CC-BY-NC 4.0)
- [Metrica Sports Sample Data](https://github.com/metrica-sports/sample-data) (CC-BY 4.0)

**Published artifacts:**
[SPADL/VAEP](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) |
[Line-Breaking Passes](https://huggingface.co/datasets/luxury-lakehouse/line-breaking-passes) |
[Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) |
[Pitch Control](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) |
[football2vec model](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout)

**Source:** [github.com/karsten-s-nielsen/luxury-lakehouse](https://github.com/karsten-s-nielsen/luxury-lakehouse)

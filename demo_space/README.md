---
title: Soccer Analytics Explorer
emoji: "\u26BD"
colorFrom: green
colorTo: blue
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
short_description: Player similarity, shot maps, and pass analysis
---

# Soccer Analytics Explorer

Interactive demo for the [Luxury Lakehouse](https://huggingface.co/luxury-lakehouse) soccer analytics platform.

**Features:**
- **Player Similarity** — Find players with similar playing styles using Doc2Vec behavioral embeddings and cosine similarity search
- **Shot Map** — Visualize shot locations by competition with mplsoccer pitch rendering (StatsBomb coordinate system)
- **Pass Quality** — Full-pitch pass arrows with line-breaking pass detection highlighted (Ward clustering on 360 freeze frames)

**Data:** [StatsBomb Open Data](https://github.com/statsbomb/open-data) (CC-BY 4.0)

**Published artifacts:**
[SPADL/VAEP](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) |
[Line-Breaking Passes](https://huggingface.co/datasets/luxury-lakehouse/line-breaking-passes) |
[Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) |
[Pitch Control](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) |
[football2vec model](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout)

**Source:** [github.com/karsten-s-nielsen/luxury-lakehouse](https://github.com/karsten-s-nielsen/luxury-lakehouse)

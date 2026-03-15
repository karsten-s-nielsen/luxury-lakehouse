---
title: README
emoji: ⚽
colorFrom: yellow
colorTo: gray
sdk: static
pinned: false
---

<p align="center">
  <img src="luxury-lakehouse.jpg" alt="Luxury Lakehouse" width="400">
</p>

# (Right! Luxury!) Lakehouse

> *"Luxury! We used to dream of serverless!"*

Open-source soccer analytics platform built on **Databricks Lakebase** &mdash; replacing a 6-service traditional AWS pipeline with a unified lakehouse architecture that scales to zero.

## What We Publish

| Artifact | Type | Description |
|----------|------|-------------|
| [football2vec-statsbomb-wyscout](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout) | Model | 32-dim Doc2Vec player behavioral embeddings trained on ~3,000 professional matches |
| [xg-model-statsbomb-wyscout](https://huggingface.co/luxury-lakehouse/xg-model-statsbomb-wyscout) | Model | Calibrated XGBoost + logistic xG model trained on 87,999 shots |
| [spadl-vaep-action-values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | Dataset | Per-action offensive/defensive VAEP valuations across ~5M events |
| [line-breaking-passes](https://huggingface.co/datasets/luxury-lakehouse/line-breaking-passes) | Dataset | All passes with defensive line-breaking labels via Ward clustering |
| [football2vec-player-embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) | Dataset | Pre-computed behavioral + statistical vectors (career/season/match) |
| [pitch-control-tracking](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) | Dataset | Per-player per-frame Spearman (2017) pitch control from tracking data |
| [expected-threat-grids](https://huggingface.co/datasets/luxury-lakehouse/expected-threat-grids) | Dataset | Data-driven 12x8 Expected Threat grid computed from 2.2M SPADL actions |
| [soccer-analytics-demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) | Space | Interactive Gradio demo: pass quality, pitch control, player similarity, shot maps, DEFCON pressure |
| DEFCON pressure data | Dataset (planned) | Defensive pressure values per action and per match period. Available in the [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) but not yet published as a standalone dataset. |

## The Platform

The platform ingests open-source match data from five providers (StatsBomb, Metrica Sports, Wyscout, IDSSE, SkillCorner), transforms it through a medallion architecture (Bronze &rarr; Silver &rarr; Gold), and serves interactive dashboards for coaches, scouts, and analysts.

**Analytics include**: Expected Goals (xG), Expected Threat (xT), VAEP action valuation, physics-based pitch control, line-breaking pass detection, player embeddings with pgvector similarity search, and DEFCON-lite defensive pressure.

## Links

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

- **License**: Apache 2.0

<sub>Named after Monty Python's <em>Four Yorkshiremen</em> sketch. In data engineering, moving from hand-managed EC2 instances to serverless Lakebase truly is... right luxury.</sub>

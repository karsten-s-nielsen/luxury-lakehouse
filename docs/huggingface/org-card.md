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

## The Platform

The platform ingests open-source match data from five providers (StatsBomb, Metrica Sports, Wyscout, IDSSE, SkillCorner), transforms it through a medallion architecture (Bronze &rarr; Silver &rarr; Gold), and serves interactive dashboards for coaches, scouts, and analysts.

**Analytics include**: Expected Goals (xG), Expected Threat (xT), VAEP action valuation, physics-based pitch control, line-breaking pass detection, player embeddings with pgvector similarity search, and DEFCON-lite defensive pressure.

## Links

- **License**: Apache 2.0

<sub>Named after Monty Python's <em>Four Yorkshiremen</em> sketch. In data engineering, moving from hand-managed EC2 instances to serverless Lakebase truly is... right luxury.</sub>

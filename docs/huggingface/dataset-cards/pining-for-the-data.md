---
language: [en]
license: mit
task_categories:
  - other
tags:
  - sports-analytics
  - soccer
  - football
  - tracking-data
  - skillcorner
pretty_name: "Pining for the Data — Open Soccer Tracking"
size_categories:
  - 1K<n<10K
---

# Pining for the Data

Soccer tracking data in SkillCorner V3 format (match JSON + tracking JSONL at 10 fps), redistributed as-is from [SkillCorner open data](https://github.com/SkillCorner/opendata) under the MIT license. Companion dataset to the (Right! Luxury!) Lakehouse soccer analytics platform, hosted separately so downstream consumers can pull just the tracking payload without the rest of the lakehouse stack.

> *"It's not pinin', it's passed on! This parrot is no more!"*
> — Monty Python's Flying Circus, Dead Parrot sketch

## What This Is

Ten SkillCorner-provided open match files in native V3 format, redistributed from the upstream repo under MIT. No de-identification, no schema transformation, no Kimball conformance — this dataset is a transport-layer convenience mirror.

- **Format:** SkillCorner V3 (match JSON + tracking JSONL)
- **Frame rate:** 10 fps
- **Data:** Redistributed as-is (MIT license)
- **Matches:** ~10 (mirrors the upstream repo's open set)

## Usage

```python
import json

with open("match.json") as f:
    match = json.load(f)

with open("tracking.jsonl") as f:
    frames = [json.loads(line) for line in f]
```

## Why a Separate Dataset?

SkillCorner's open set is the only free source of 10 fps tracking data for professional matches on HuggingFace Hub. The `luxury-lakehouse` org keeps it as a stand-alone mirror so that:

- downstream researchers can consume SkillCorner data without pulling the lakehouse's Kimball-conformed gold tables, and
- the lakehouse pipeline itself can ingest from this repo (on infrastructure without GitHub outbound access) instead of re-downloading from the upstream GitHub.

## Source

Tracking data from [SkillCorner open data](https://github.com/SkillCorner/opendata) (MIT license). See [NOTICE](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/NOTICE) for the full attribution chain.

## License

- **Data:** [MIT](https://opensource.org/licenses/MIT) (redistributed from SkillCorner open data)
- **Tooling:** [MIT](https://opensource.org/licenses/MIT)

Companion repository: [pining-for-the-data](https://github.com/karstenskyt/pining-for-the-data) — the re-packaging scripts + upload tooling.

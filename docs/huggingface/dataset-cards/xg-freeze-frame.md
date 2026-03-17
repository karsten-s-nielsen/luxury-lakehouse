---
license: cc-by-4.0
task_categories:
  - tabular-classification
tags:
  - sports-analytics
  - soccer
  - expected-goals
  - freeze-frames
size_categories:
  - 100K<n<1M
---

# xG Freeze-Frame Data

Player positions at the moment of each shot, parsed from StatsBomb open data inline freeze frames.

## Schema

| Column | Type | Description |
|--------|------|-------------|
| event_id | string | Shot event ID (FK to shots) |
| match_id | bigint | Match identifier |
| competition_id | bigint | Competition identifier |
| season_id | bigint | Season identifier |
| player_x_norm | double | Player x position normalized to [0, 1] |
| player_y_norm | double | Player y position normalized to [0, 1] |
| is_keeper | boolean | Whether player is goalkeeper |
| is_teammate | boolean | Whether player is on the shooting team |

## Source

StatsBomb open data (CC-BY 4.0). Positions normalized from StatsBomb 120×80 coordinate system.

## Usage

Used as training input for the xG v2 set encoder model (`luxury-lakehouse/xg-v2-model-set-encoder`).

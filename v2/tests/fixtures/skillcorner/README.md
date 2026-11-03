# SkillCorner fixtures (real slices, MIT)

These files are small slices of the SkillCorner open data, GitHub `SkillCorner/opendata` (branch `master`), read on
2026-10-27. Copyright (c) 2020 SkillCorner (<https://skillcorner.com>), MIT License: the copyright notice and the
permission notice of the repository's `LICENSE` apply to these slices. Changes from the source files are listed
below. Every kept line is byte-identical to its source line.

| File | Source file | Change |
|---|---|---|
| `match_1899585.json`, `match_1874553.json` | `data/matches/<id>/<id>_match.json` | none (byte-identical); 1874553 holds `last_modified` and `playing_time.sequences`, which 1899585 has not |
| `matches.json` | `data/matches.json` | none (byte-identical) |
| `github_tree.json` | `https://api.github.com/repos/SkillCorner/opendata/git/trees/master?recursive=1` | none (the recorded listing) |
| `tracking_1899585_slice.jsonl` | `data/matches/1899585/1899585_tracking_extrapolated.jsonl` (Git LFS, 85,550,410 bytes) | 182 whole lines from three HTTP range reads: frames 0–120 (pre-match frames 0–9 and the kick-off), 29755–29775 (the end of period 1, the half-time frames 29761–29769 and the start of period 2) and 60490–60529 (the end of the match) |
| `dynamic_events_1899585_slice.csv` | `data/matches/1899585/1899585_dynamic_events.csv` | the header, the first 25 records and a greedy cover: every column has a value, and every boolean column has `True` and `False` where any of the 20 matches has both (59 records) |
| `phases_of_play_1899585_slice.csv` | `data/matches/1899585/1899585_phases_of_play.csv` | the same rule (27 records) |

Facts the tests use, from the source:

- The README ("Tracking Data Description") and `assets/field.jpg` give the coordinate system: metres, origin at the
  centre of the pitch, x along the long side (to the right in the diagram), y along the short side (up).
- The tracking `timestamp` is the match clock: period 2 starts at `00:45:00.00`. Frames outside every period have
  `period: null`, `timestamp: null`, null ball values and no player data.

## Format 2 files (pining owner tier, pining ADR 0011)

- `tracking_1899585_slice.parquet` and `dynamic_events_1899585_slice.parquet` were made from the JSONL and CSV slices
  above with the pining producer's own transforms (`formats.skillcorner_canonical.tracking_to_parquet` and
  `events_csv_to_parquet`, 2026-10-27). They hold the same public data (MIT) in the owner tier's format.
- `physical_1899585_synthetic.parquet` holds invented values on the 41 columns that the owner tier's
  `physical.parquet` files hold (measured on 492 files, 2026-10-27; only names, types and counts were read). Its
  match, player and team ids are those of the public 1899585 fixture.

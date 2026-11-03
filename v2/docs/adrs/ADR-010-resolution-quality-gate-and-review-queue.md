# ADR-010: The resolution quality gate and the review queue

## Status

Accepted (2026-10-27). Owner decisions: the targets and the labelling (spec §15.4, §29.3), the review queue (§15.5)
and "Accept recommended" (§15.6). Built in plan Task 22. **The bands and margins are not measured yet**: plan Task 34
(Gate G-E) measures them on the owner's truth set and records them here and in `ops_resolution_bands`.

## Context

- Resolution joins records of five providers across players, teams, competitions and matches (§15.2). A wrong link
  merges two real people; a missed link splits one.
- The IRIS resolver gives scores from 0 to 100, but no measured statement of how good a score is.
- Demo users must be able to take the most likely links without reviewing each one (§15.6). The owner must be able
  to trust the automatic links and to undo a batch.
- The catalog has one writer (the run); config-side objects have one writer kind (the operator side) (§8.3).

## Decision

**Matchers** (`lakehouse.resolve`), each over every provider pair, each giving a confidence, a layer and a method:

- players: the IRIS three layers (name, birth date, jersey and team; name and birth date with a swap check; name and
  position), TF-IDF candidates (scikit-learn, sparse_dot_topn), rapidfuzz scores, mutual best only;
- teams: the IRIS name matcher (rapidfuzz `token_sort_ratio`, at least 70, one-to-one);
- competitions: the same name matcher, layer 1 when the area agrees;
- matches: the date (one day of tolerance) and the teams — linked teams (layer 1), team names (layer 2), team names
  with home and away swapped (layer 3).

Each candidate also carries its runner-up score: the best score of a competing candidate of either record.

**Bands.** `Bands(auto_link_min, reject_max, margin)` per entity type, from `ops_resolution_bands` (measured values
only). A score at or above `auto_link_min` links automatically; at or below `reject_max` it is dropped; between
them it goes to the review queue (`ops_review_queue`). **An entity type without measured bands gets no automatic
link and no recommendation**: every candidate goes to the queue, and the queue item says why.

**Recommended** means the top candidate, and only when `score - runner_up >= margin` (a tie never is).

**Decisions and requests** are config-side objects under `ConfigLock` (`config/decisions/`, `config/requests/`):
must-link and cannot-link with who, when and why; "Accept recommended" (all, one entity type, or one provider pair)
and "Undo batch" requests. The run reads them once and never writes them. It records each applied request in
`ops_applied_requests`, so a request is applied once.

**Order of application** in each pass (§15.6): (1) hand decisions — the latest decision on a pair wins; (2) undo
requests — the batch's links go and its pairs are held; the links of earlier batches stay; then the bands; (3) accept
requests; (4) the `auto_accept_recommended` setting. Steps 3 and 4 skip held pairs and pairs without a measured
margin. A held pair stays in the queue until a person confirms or rejects it.

**Links** go to `gold_<entity>_links` with confidence, layer, method, `accepted_by` (`auto`, `hand` or
`recommended`), the batch id of a recommended link, and the operator of a hand decision. The names follow the
lake-root allowlist `{dim_*, *_links, ops_*, lineage_*}` (so `gold_<entity>_links`, not `gold_links_<entity>`).

**The truth set** holds ids only (`tests/fixtures/truthset/<entities>.csv`). Only the owner labels it; the owner's
hand-checked decisions grow it (`truthset_rows_from_decisions`); a link accepted as recommended never enters it.
`measure` gives the precision of all links and of the automatic band, and the recall after queue review.

**Targets** (owner decision, per entity type): precision of at least 0.99 for links in the automatic band, recall of
at least 0.95 after queue review, and a separate no-regression rule against the last recorded values
(`tests/live/test_resolution_quality.py`). If the IRIS matchers cannot meet the targets after band tuning, the
builder evaluates splink 5.0.0 on the same truth set and brings the result to the owner.

## Measured values

To be filled by plan Task 34 for each entity type: the truth-set size and scope, `auto_link_min`, `reject_max`,
`margin`, the precision and the recall.

| Entity | Truth rows | auto_link_min | reject_max | margin | precision (auto) | recall |
|---|---|---|---|---|---|---|
| players | — | — | — | — | — | — |
| teams | — | — | — | — | — | — |
| competitions | — | — | — | — | — | — |
| matches | — | — | — | — | — | — |

## Consequences

- Until Task 34, every candidate waits in the queue, on every install, whatever the setting says. That is the
  owner's rule (§15.4), and the queue items say why.
- An undo is one request; its pairs stay held until a person decides them, so the setting cannot undo the undo.
- The resolver depends on scikit-learn, sparse-dot-topn and Unidecode (GPL-2.0-or-later), as v1 and IRIS do.

# Spec: SkillCorner private-match (RM) ingestion — multi-format artifact support

**Date:** 2026-07-02 · **From:** lakehouse session · **Status:** APPROVED by the xT-GK analysis session (2026-07-02),
review elevations incorporated below. **Depends on:** the shipped SkillCorner missing-discovery + `--max-matches`
change (PR #418, `2c6e7d4`, wheel 0.5.61) and H1 (per-match `access_tier`, ADR-064 / PR #414-#415, live).
**Blocks:** RM-5 → RM-94 → the RM metric test.

## 0. Why this is metric-validity-critical (analysis-side reframe, 2026-07-02)

RM is not "another dataset to ingest" — it is the **independent cohort that decides whether the xT-GK degeneracy is
intrinsic or GS-specific** (GS degeneracy re-confirmed on clean data, CV 5.9%). For that test to be valid, **RM `xt_gk`
must be geometrically comparable** to GS/A-League: same coordinate handling and same frame-rate semantics. A subtle
geometry- or rate-mismatch **would not crash** — it would silently make RM `xt_gk` non-comparable and we would draw the
wrong conclusion about the metric. Therefore **frame-rate derivation (§5.3) and the coordinate-comparability golden
(§8.6) are BLOCKING gates on RM-5**, not ingestion hygiene.

## 1. Problem

The 98 private Real-Madrid (RM) SkillCorner matches are now **discoverable** (owner token; the missing-discovery
guard surfaces them) and **safe to ingest** (H1 live → `visibility=private` ⇒ `access_tier=RESTRICTED`). But the RM-5
probe (`ingest_skillcorner --max-matches 5`) **failed** on the first match:

```
HTTPError: 404 Not Found — GET /skillcorner/matches/1021404/1021404_match
```

Root cause: `ingest_skillcorner` **hardcodes the public A-League artifact-key names** (`{id}_match`,
`{id}_dynamic_events`, `{id}_tracking_extrapolated`) and their **serializations** (JSON / CSV / JSONL). The private RM
matches ship the **same SkillCorner data model in a different serialization + artifact-key layout**. The RM path was
never exercised until the guard surfaced it.

## 2. Key finding — SAME data model, DIFFERENT serialization (the de-risker)

Verified live against RM match `1021404` (owner token) vs the ingested public A-League match `1886347`:

| Artifact | Public A-League | Private RM | Schema compatibility (measured) |
|---|---|---|---|
| match metadata | `{id}_match` → **.json** | `metadata` → **.json** | RM has every field `parse_match_json` reads: `home_team{id,name,short_name}`, `away_team`, `competition_edition{competition,season}`, `players[{id,team_id,short_name,first_name,last_name,number,player_role}]`, `date_time`, `stadium`, `pitch_length/width` ✓ |
| events | `{id}_dynamic_events` → **.csv** | `events` → **.parquet** | **294 columns, byte-identical set** to `bronze.skillcorner_events` (0 columns differ either way) ✓ |
| tracking | `{id}_tracking_extrapolated` → **.jsonl** | `tracking` → **.json.gz** | frames carry `frame/period/timestamp/ball_data{x,y,z,is_detected}/player_data[{player_id,x,y,is_detected}]` — exactly what `parse_tracking_jsonl` reads ✓ (+ extra ignorable keys `possession`, `image_corners_projection`) |
| (extra) | `{id}_phases_of_play`.csv | `freeze_frames`.parquet, `physical`.parquet | not consumed by SPADL/AC → out of scope |

RM artifacts fetch **200** by their own keys (`metadata`/`events`/`tracking`). The token is confirmed owner-tier
(discovery returns all **98 private + 10 public**).

**Consequence:** this is a **format-reader change, not a re-model.** RM normalizes into the **exact same bronze
schema** (`skillcorner_matches` / `skillcorner_events` / `skillcorner_tracking`), so **everything downstream is
unchanged** — SPADL conversion (silly-kicks `skillcorner`), the AC pipeline, the 4.39.0 goal-kick actor override, the
mart contamination guard, and the `access_tier` split all key on `provider='skillcorner'` and are serialization-blind.

## 3. Goal / Non-goals

**Goal:** ingest RM (and any future SkillCorner "full-format" delivery) into the existing bronze schema, so the RM-5 →
RM-94 rollout runs through the unchanged downstream. Private rows carry `access_tier=RESTRICTED` (H1).

**Non-goals:**
- **No re-model / no new bronze tables.** Same three bronze tables, same columns.
- **No downstream changes.** SPADL/AC/mart/publishers untouched (verified serialization-blind).
- **No A-League behavior change.** The public CSV/JSONL path stays byte-for-byte identical.
- **`freeze_frames` / `physical` artifacts** — not ingested (not consumed by SPADL/AC). Note as a future option.

## 4. Current state (what already exists)

- **Discovery + cap shipped** (PR #418): the guard does full-discovery + missing-anti-join; `--max-matches` caps + walks forward. RM-5 = `{"max_matches":"5"}`. This half works — the failure is purely in the per-match artifact fetch/parse.
- **Ingest loop** (`ingest_skillcorner`): per match → `fetch_artifact({id}_match)` → `parse_match_json` → `write_matches`; `{id}_dynamic_events` → `parse_events_csv` → `write_events`; `{id}_tracking_extrapolated` (stream to temp) → `parse_tracking_jsonl` → `write_tracking`. All A-League-format-hardcoded.
- **`MatchInfo.artifacts`** (`dict[str,str]`) already carries the per-match artifact manifest — the basis for format detection (below). It is currently ignored by the ingest.

## 5. Design — changes by layer (all in `src/ingestion/skillcorner*.py`)

### 5.1 Format detection — from the artifact manifest (recommended)
Resolve format per match from `MatchInfo.artifacts` keys, NOT from `visibility` (decouples format from tier — robust if the mapping ever crosses):
- **A-League format** iff `f"{id}_match"` in `artifacts` (equivalently `{id}_dynamic_events` / `{id}_tracking_extrapolated`).
- **RM/full format** iff `metadata` / `events` / `tracking` in `artifacts`.
- Neither/ambiguous → **raise loud** (unknown SkillCorner delivery format — never silently skip; surfaces a producer change, mirroring the pining visibility-vocabulary contract).

A small `_resolve_artifacts(match) -> ArtifactPlan` returns the three `(artifact_key, format)` triples to fetch.

### 5.2 events — add a parquet reader (with DTYPE parity, not just column-name parity)
`parse_events_parquet(content_bytes, *, match_id) -> pd.DataFrame`: `pd.read_parquet(io.BytesIO(...))` → same
all-columns-preserved passthrough + `match_id` + `_ingested_at`.

**DTYPE parity is required (review elevation).** `parse_events_csv` uses `pd.read_csv(..., low_memory=False)` — pandas
*infers* dtypes (int64/float64/object) — and `write_events` does `spark.createDataFrame(df)`, inferring the Spark
schema from those pandas dtypes. Parquet carries its own **typed** columns (int/float/bool/nullable), which will differ
from CSV-inference (e.g. a NaN-bearing int → float64 under CSV but Int32 under parquet; bool vs object). Writing RM rows
with parquet-native dtypes into the shared `bronze.skillcorner_events` would schema-conflict or land inconsistent
column types across A-League vs RM rows. So `parse_events_parquet` MUST coerce each column to the **fixed
`bronze.skillcorner_events` Delta schema** — NOT to "whatever `parse_events_csv` inferred on a given fixture" (review #2:
CSV inference is file-dependent — a column all-int in one file but NaN-bearing in another infers differently).

**Two-layer coercion (final, review #3):**
1. **Pure (PR-CI-testable):** `parse_events_parquet` normalizes the parquet frame through the **same `pd.read_csv`
   inference** the bronze schema was built from — it reads the parquet, writes an in-memory CSV, and delegates to
   `parse_events_csv`. So its pandas dtypes equal the A-League reader's by construction, and since
   `spark.createDataFrame` infers the Spark schema from pandas dtypes, the two formats produce ONE Spark schema. This
   makes the coercion assertable in a pure test (`parse_events_parquet(x).dtypes == parse_events_csv(x).dtypes`) rather
   than only behind the Spark write. Events are ~6k rows/match, so the round-trip is negligible.
2. **Authoritative (e2e):** `write_events → _conform_to_bronze_schema` still casts the Spark DF to the **exact live
   `bronze.skillcorner_events` Delta schema** (both formats; A-League no-op) — the fixed-contract backstop from review #2,
   verified in the RM-5 e2e. Defense-in-depth if pandas inference ever diverges from the live schema.

The 294-column NAME set is identical (measured); layer 1 closes the TYPE gap with fast coverage, layer 2 guarantees the
live-schema match.

### 5.3 tracking — add a gzip-JSON-array reader (memory-aware)
The A-League path streams the response to a temp file and reads JSONL **line-by-line** (memory-safe). RM tracking is a
**single gzipped JSON array** (`1021404` = **59,586 frames × 22 players ≈ 1.3M rows**).

**DECIDED — stream-parse (review): build it once, no load-all.** Stream the gz response to a temp file, then iterate
frames with a streaming JSON parser (`ijson.items(f, "item")` over a `gzip.open` handle) → the **same per-frame reshape**
as `parse_tracking_jsonl`. Bounded memory; scales to RM-94. Adds an `ijson` dep. (Load-all — `json.loads(gzip.decompress)`
— is explicitly rejected: the footgun is real for large/uncapped runs.)

Refactor the per-frame reshape out of `parse_tracking_jsonl` into a shared `_frame_to_rows(frame, match_id, frame_rate)`
so both the JSONL and the gz-array readers call it (single reshape definition; A-League output byte-identical). Bronze
schema (`skillcorner_tracking` narrow: match_id/period/frame/timestamp/player_id/x/y/is_visible/ball_x/y/z/ball_is_detected/frame_rate)
unchanged.

**`frame_rate` — DERIVE, never default (review: BLOCKING metric-validity gate).** Do NOT inherit the A-League
`_FRAME_RATE=10` for RM. Compute it from the actual cadence — `1 / median(Δtimestamp)` **per period** (robust; independent
of any metadata field being present/correct). **Snap to the nearest plausible rate** from a small allow-set (`{10, 25,
30}` fps) within a **±5% band** — the raw derivation won't be exactly nominal (0.04004 s → 24.97, not 25.0), so a bare
equality check would false-trip a healthy stream (review #2). **Cross-check against a metadata rate if one exists (same
±5%); fail loud only when the derived cadence falls outside every allowed band, when metadata and derived disagree beyond
tolerance, or when no usable cadence exists** — never silently default. It is load-bearing twice: (1) velocity/DAS, and
(2) the goal-kick origin resolution's
`round(frame_rate × 1 s)` keeper-detection window — a 10-vs-25 error silently narrows/widens that window and degrades
origin resolution. If RM is 25 fps full-tracking vs A-League's 10 fps broadcast, this MUST be right for RM `xt_gk` to be
comparable. `_FRAME_RATE=10` stays as the A-League-path literal only.

### 5.4 metadata — reuse `parse_match_json` as-is (verify)
RM metadata.json has every field `parse_match_json` reads (§2). Expectation: it works unmodified. The spec REQUIRES a
schema-parity test on a real RM fixture (§8) to confirm no sub-field drift (e.g. `player_role`, `match_periods` — RM may
name period boundaries differently; the current code `data.get("match_periods", [])` degrades gracefully but verify).

### 5.5 ingest loop
`ingest_skillcorner`: per match, call `_resolve_artifacts(match)` and dispatch to the format-appropriate
fetch+parse for each of the three artifacts. `visibility=match.visibility` continues to flow to
`parse_match_json` → `access_tier` stamp (H1). Everything else (write_*, gc, per-match loop, `--max-matches`,
missing-discovery) is unchanged.

## 6. Data flow (unchanged downstream)

```
pining owner token ─ MatchInfo.artifacts ─▶ _resolve_artifacts (A-League CSV/JSONL | RM parquet/gz)
                                                     │  (visibility → access_tier, H1)
      bronze.skillcorner_{matches,events,tracking}  ◀┘   ← SAME schema, both formats
                                                     ▼
        silly-kicks skillcorner SPADL → bronze.spadl_actions → AC (4.39.0 goal-kick override, mart guard)
                                                     ▼
                     fct_action_context / fct_shot_psxg …  (access_tier split → -restricted repo)
```

## 7. Edge cases / risks

- **Tracking memory/timeout (primary residual risk):** 1.3M rows/match; the `ingest_skillcorner` task timeout is
  **1200 s**. RM-5 (5 matches) may fit; RM-94 will not — batch via `--max-matches` (each batch small) and/or raise the
  timeout. Stream-parse (§5.3) keeps memory bounded regardless.
- **`frame_rate`** — resolved: derive-and-fail-loud (§5.3). Elevated to a blocking metric-validity gate (§0).
- **Parquet dtype drift** — resolved: `parse_events_parquet` coerces to the CSV-inferred / bronze-Delta dtypes; the
  parity test asserts **dtypes**, not only names (§5.2, §8.2).
- **Coordinate comparability** — the largest metric-validity risk; a REQUIRED blocking golden on the first RM match (§8.6).
- **`_check_no_tier_mixing` (A3):** RM re-ingest must preserve `visibility=private` — the existing immutability guard covers it.
- **Unknown format:** raise loud (§5.1) — never silently skip a match whose manifest matches neither layout.

## 8. Test plan (pure-first)

1. **`_resolve_artifacts`** unit test: A-League manifest → CSV/JSONL plan; RM manifest → parquet/gz plan; unknown → raises. (Recorded `MatchInfo.artifacts` fixtures for both — already captured in this investigation.)
2. **`parse_events_parquet`** — synthetic same-data parquet+CSV fixtures → asserts **columns AND per-column dtypes** are
   equal to `parse_events_csv`'s output (the pure proxy for the Spark-schema outcome; review #3), with a deliberately
   drift-prone `Int32` column so the test exercises the coercion (Int32 → int64), not a no-op + `match_id` present. The
   exact-live-Delta-schema conformance (`_conform_to_bronze_schema`) is verified in the RM-5 e2e (needs Spark).
3. **tracking gz-array reader** — a trimmed RM tracking.json.gz fixture → asserts the narrow rows match `parse_tracking_jsonl`'s shape for the same frames (shared `_frame_to_rows`).
4. **`frame_rate` derivation** — synthetic per-period timestamp sequences: asserts `1/median(Δt)` **snaps** to the nearest
   allowed rate within ±5% (e.g. 0.04004 s steps → 25.0), that a metadata-vs-derived disagreement beyond tolerance
   **raises**, that a cadence outside every allowed band **raises**, and that absence of any usable cadence **raises**
   (never silently defaults to 10).
5. **`parse_match_json` on RM metadata** — a real RM metadata.json fixture → roster rows produced, all required fields non-null; `access_tier` derives RESTRICTED.
6. **e2e (recorded, env-gated):** ingest one RM match end-to-end into a scratch schema; assert bronze row counts + `visibility=private`/`access_tier=restricted`; then a SPADL smoke conversion to prove the downstream contract holds.

dbt PR CI is parse-only; tests 1–5 are pure-Python + fixtures (no warehouse). Fixtures for 1–5 were captured live in this investigation (match `1021404`).

### 8.6 Coordinate-comparability golden — REQUIRED, BLOCKING on RM-5 (review elevation)

The geometry analog of the 294-parity de-risker, and the **biggest metric-validity risk**. "Same frame model" (§2) proves
the field NAMES match — it does NOT prove the coordinate VALUES are comparable. On the **first RM match** (during RM-5,
before RM-94), verify:
- **native x/y range/origin** matches A-League (center-origin ≈ ±52.5 / ±34);
- **pitch dims** — A-League is assumed 105×68; RM carries `pitch_length`/`pitch_width`. Confirm the silly-kicks
  skillcorner converter's `x+52.5 / y+34` transform is still correct (RM is also 105×68, OR the converter normalizes to
  the carried dims — do not assume);
- **`home_team_side` / orientation** resolves cleanly through the converter;
- **acceptance (same as A-League passed):** RM goal-kick origins land **≈100% own-box** and **~2 keepers/match** carry
  `xt_gk`. If that holds, RM geometry is confirmed comparable and the degeneracy test on RM is valid. If not, the RM
  `xt_gk` is non-comparable and RM-94 does not proceed.

## 9. Rollout

1. Land this (spec-reviewed → plan → implement → merge → wheel deploy).
2. **RM-5** = a **geometry/pipeline confirmation, NOT a degeneracy verdict** (review #2 framing — shared so RM-5 is not
   over-read either way). `ingest_skillcorner {"max_matches":"5"}` → verify 5 RM matches in bronze (private/restricted)
   → SPADL + AC → run the **§8.6 coordinate-comparability golden** (own-box goal-kicks, ~2 keepers/match, restricted
   tier). This confirms the RM cohort is geometrically trustworthy + gives the analysis side a sanity peek; 5 matches ≈
   a handful of keepers, too thin for the intrinsic-vs-cohort-specific verdict.
3. **RM-94** = where the degeneracy question is actually answered (statistical power). Only proceeds if §8.6 passed.
   Batch via `--max-matches` (e.g. 10–20/run; the missing-anti-join walks forward) or raise the ingest timeout.

## 10. Decisions (analysis-side review, 2026-07-02 — approved)

1. **`frame_rate`** → **derive** from `1/median(Δtimestamp)` per period + cross-check metadata + fail loud (§5.3).
   **BLOCKING metric-validity gate** (§0) — never default to 10 for RM.
2. **Tracking reader** → **stream-parse** (`ijson` over the gz handle), built once; load-all rejected (§5.3).
3. **`freeze_frames` / `physical`** → **defer** (not consumed by SPADL/AC). Note for future: `freeze_frames` → PSxG-on-RM,
   `physical` → the defensive report.
4. **Coordinate comparability** → elevated to a **REQUIRED blocking golden on the first RM match** (§8.6), gating RM-94.
5. **Dtype parity** (new, from review) → `parse_events_parquet` coerces to CSV/bronze dtypes; parity test asserts dtypes,
   not just names (§5.2, §8.2).

Approved as-is otherwise: manifest-based format detection decoupled from `visibility` + raise-loud-on-unknown; the A3
immutability guard covering re-ingest; the e2e `access_tier=restricted` post-ingest leak assertion.

# Lakehouse v2, cycle 1: ingestion — design

| Field | Value |
|---|---|
| Date | 2026-10-26 |
| Status | Round 7 draft. Updated after A/B review rounds 1–6 (`D:\Development\_reviews\2026-10-26-v2-ingestion-spec-{A,B}.md`, `...-spec-r2-` to `...-spec-r6-{A,B}.md`). Both reviewers approved in round 7 (`...-spec-r7-{A,B}.md`). Their four optional CONSIDER notes (A-45, A-46, B37, B38) are applied. All owner decisions are closed (§29). **Approved by the owner on 2026-10-26.** Amended on 2026-10-26 after plan review rounds 2, 3 and 4 with owner-approved decisions §29.9, §29.10, §29.11 and §29.12, and after an owner request with §29.13, refined in plan review round 6 with §29.14; §29.11 also records the owner's approval of the author's added text in §10.3 step 7, §10.6 and §26 item 2. The implementation plan is in `v2/docs/plans/`. |
| Owner | Karsten Nielsen |
| Branch | `feat/v2-ingestion` (local, from `main`) |
| Inputs | IRIS POC handover ("innie"), v1 repo (this clone), storage spike of 2026-10-26 (evidence in §25) |

**How to read this document.**

- **Owner decision** means the owner said it or approved it during the brainstorm on 2026-10-26.
- **Author's call** means the author of this document decided it. The owner can change it during review. §28 lists every author's call.
- **Builder** means the session that writes the plan and the code.
- **Must** is a requirement. **Should** is a strong default that the builder can change only with a written reason in the plan.

---

## 1. Intent

v2 is a new lakehouse that sits side by side with v1 in this repository. v2 replaces v1 over time, and v1 retires later (owner decision). The aim is a gold-standard, long-term, open-source design with best practice in every area. Scope is not a constraint (owner decision).

Why v2 exists:

- **Storage cost.** The IRIS POC measured that a full database is too costly for tracking frames. IRIS bronze was 24.8 MB per SkillCorner match and 557.8 MB per IDSSE match. The v2 layout measured 1.89 MB and 9.07 MB for the same matches (§25.1).
- **Open source and portability.** v1 depends on Databricks and Lakebase. v2 must run on a laptop (Windows, macOS, Linux) and deploy to Hugging Face (HF), with only open-source parts.
- **Reproducibility for others.** Any person can install v2, switch on the public modules and get the same data from the same public sources.

Cycle 1 builds **ingestion only**. Ingestion means: fetch, typed bronze, cross-provider entity resolution and the conformed dimensions (owner decision). The sequence after cycle 1 is: observability (OpenTelemetry), then processing (metrics, silly-kicks) (owner decision).

## 2. Goals

- **G1.** Nine ingestion modules: five public direct modules and four pining modules (§7).
- **G2.** Fully incremental runs. Every run processes only additions and changes (§17).
- **G3.** One canonical stored form per provider, whatever the channel (§9, §11).
- **G4.** Bronze is complete. Every field that a source delivers lands as a typed column (§12).
- **G5.** Tracking frames stored in the measured, tuned Parquet layout in a DuckLake catalog (§10).
- **G6.** The v1 Kimball id layer, carried over unchanged: surrogate keys plus the original provider ids (§14).
- **G7.** Cross-provider entity resolution for players, teams, competitions and matches, with a review queue (§15).
- **G8.** Conformed dimensions and a thin cross-provider tracking view, built with dbt (§11, §16).
- **G9.** Authenticated Operations (Ops) pages on the Taipy shell (§19).
- **G10.** Scheduling: one schedule for all active modules, on HF and on local machines (§18).
- **G11.** Three deployment targets: a laptop install, the HF install and the Mac Mini staging install (§22).
- **G12.** No block on the next two cycles. Observability attaches without call-site changes (§21). The AI/ML Workflows page can come back later (§19).

## 3. Non-goals for cycle 1 (owner decisions)

- Processing: metrics, silly-kicks, SPADL, VAEP, xT and unified event facts.
- The observability backend: OTel SDK, OTel Collector, exporters and dashboards. Cycle 1 calls only the OTel API (§21).
- The AI/ML Workflows page, and every existing v1 Taipy page.
- A common centre-origin coordinate frame, and pitch orientation. Cycle 1 stores coordinates as delivered, with coordinate-system metadata (§11.4).
- Metrica. It is not in the owner's provider list.
- Publication of any data to HF datasets or other public places.
- Changes to v1 code or v1 behaviour.

## 4. Anti-goals

The work fails if any of these happen, even if everything else works:

- A second stored copy of tracking frames in silver or gold.
- A field that the source delivers but bronze drops.
- A visibility value that the code infers from the token instead of the source flag.
- A silent fallback, a silent default or a silent skip. Every degraded state is visible on the Ops page and in the logs.
- A key value that differs from v1 for the same `(provider, native id)`.
- Code that works on one operating system only.
- Restricted (private) data in any public storage location.
- A design that forces call-site changes when observability arrives.

## 5. Constraints

- **C1. Open source only.** Every runtime part has an open-source licence.
- **C2. Platforms.** Windows, macOS and Linux for local installs. HF for the hosted install.
- **C3. Python 3.12.** DuckDB 2.0, pyarrow 26, polars 2 and rapidfuzz 3.14.6 need Python ≥ 3.11. taipy 4.1.1 needs Python < 3.13. Python 3.12 is supported until 2028-10-31 (endoflife.date, checked 2026-10-26). The Databricks Python and dependency limits do not apply to v2 (owner decision).
- **C4. One writer per lake.** Each install owns one lake. Exactly one process writes to it at a time (§10.6).
- **C5. One storage interface.** The lake is on local files or on an HF Storage Bucket, behind one interface (§10.5).
- **C6. Nothing preloaded.** A new install has the default config and an empty lake (owner decision).
- **C7. Machine safety.** Jobs on the owner's AI-PC must stay light, because that machine crashes when its RAM runs hot. Thread and memory caps are config values with safe defaults (§10.9).
- **C8. Repository rules.** The owner's standing rules apply: one feature branch, one commit at the end of the cycle with explicit owner approval, no push without approval, independent A/B reviews of spec, plan and implementation, no subagents unless the owner asks.

## 6. Architecture overview

```
 sources                channel adapters          provider parsers         lake (one per install)
 ───────                ────────────────          ────────────────         ──────────────────────
 GitHub, HF, figshare,  list + change signal  →   one parser per      →   raw zone (archive)
 Google Drive (public)  fetch                     provider, writes the     bronze: tuned Parquet files
 pining API (bearer)    map ids to canonical      canonical bronze form    + DuckLake catalog
                                                                           silver/gold: dbt (DuckLake)
                                                         ↑                         ↓
 config objects  ──────────────── read at run start ─────┘          services layer (typed)
 (modules, filters, schedule,                                        ├─ CLI
  review decisions)  ←───────────── written by ─────────────────────┤
                                                                     └─ Ops pages (Taipy) + sign-in
```

**Separation rules (must):**

- A channel adapter knows its source: listing, change signal, download and the mapping from the channel's id form to the canonical provider id. It does not parse content.
- A provider parser turns raw files into the canonical bronze form. It does not know which channel delivered the files. A SkillCorner match from GitHub and from pining goes through the same parser (owner decision: "transform as early as possible to avoid duplicate code").
- The services layer is the only entry point for the CLI and the Ops pages. It holds no UI code. The Ops pages hold no business logic (Taipy guard 1, §19.6).
- Domain code does not import Taipy, the web server or HF client code. Storage access goes through the storage interface. The builder must enforce these directions with `import-linter` contracts.
- **Read port (declared).** Resolution, the services layer and dbt read the lake through DuckDB SQL over a published catalog version (§10.4). This coupling to DuckDB SQL is a deliberate port, not an accident.

## 7. Providers, channels and modules

### 7.1 The nine modules (owner decision)

| # | Module | Provider name (in keys) | Channel | Source | Change signal | Licence | Laptop default |
|---|---|---|---|---|---|---|---|
| 1 | StatsBomb (incl. 360) | `statsbomb` | public | GitHub `statsbomb/open-data` | git tree API: blob SHA per file (one call lists all 8,988 files) | `NOASSERTION` (repo has `LICENSE.pdf`) | active, default filter |
| 2 | Wyscout | `wyscout` | public | figshare collection 4415000 (12 articles, 92 MB) | md5 per file + article version | CC BY 4.0 | active, default filter |
| 3 | IDSSE | `idsse` | public | HF dataset `pysport/idsse-data` | HF tree API: LFS sha256 per file | CC-BY-4.0 | active, all |
| 4 | SkillCorner | `skillcorner` | public | GitHub `SkillCorner/opendata` (Git LFS) | tree blob SHA + LFS pointer `oid sha256` | MIT | active, all |
| 5 | Gradient Sports | `gradientsports` | public | Google Drive folder `1_a_q1e9CXeEPJ3GdCv_3-rNO3gPqacfa` | Drive file id + last-modified date (day only) | no written licence | inactive |
| 6 | IDSSE (pining) | `idsse` | pining | pining API | per-artifact `artifact_meta.sha256` (`updated_at` + `updatedSince` only as a polling hint) | per entry `source.licence` | inactive |
| 7 | StatsBomb (pining) | `statsbomb` | pining | pining API | same | same | inactive |
| 8 | SkillCorner (pining) | `skillcorner` | pining | pining API | same | same | inactive |
| 9 | Gradient Sports (pining) | `gradientsports` | pining | pining API | same | same | inactive |

Notes:

- **IDSSE public source.** Two public copies exist: HF `pysport/idsse-data` and springernature figshare article 28196177 (md5 per file). Author's call: use HF, because it gives a sha256 per file and kloppy uses it. There are 7 matches and 2.6 GB of raw XML. The match ids are J03WMX, J03WN1, J03WPY, J03WOH, J03WQQ, J03WOY and J03WR9.
- **SkillCorner public.** 20 A-League 2024/25 matches. Each tracking JSONL is about 90 MB in Git LFS. LFS downloads use SkillCorner's bandwidth quota (§26).
- **Gradient Sports public.** The folder has 65 tracking files (for example `10502.jsonl.bz2`). Events sit in dated drop subfolders. `embeddedfolderview?id=<folder>` lists file ids and the last-modified day, with no size and no checksum. Downloads hit a virus-scan confirm page. The change signal is weak, so the adapter must also compare the content hash after download (§17.3). It is inactive by default because of its size (owner decision). The total size is not measured yet (§26).
- **StatsBomb 360.** The StatsBomb module also ingests `data/three-sixty/` files (owner decision: "SB including SB360").
- **Every artifact lands.** Each module ingests every artifact its source delivers, not only tracking: events, lineups, rosters, match metadata, 360 frames, phases of play and so on (completeness, §12).

### 7.2 The pining API (facts, checked 2026-10-26)

The authoritative reference is `docs/api-reference.md` in the pining producer repo (`D:\Development\karstenskyt__pining-for-the-data`). The builder must read it. The facts that shape this design:

- **Base URL:** `https://ozqgk9a3ji.execute-api.us-east-1.amazonaws.com/v1`. This is a config value, not a code constant.
- **Auth:** `Authorization: Bearer <token>`. The public token is `test-token-pining-for-the-data` (documented in the producer README). The owner token comes from the environment variable `PINING_FOR_THE_DATA_TOKEN`. Without an owner token, a pining module uses the public token and sees public data only (owner decision).
- **Providers:** `gradientsports`, `idsse`, `skillcorner`, `statsbomb`. On 2026-10-26 the public tier listed 0, 7, 20 and 0 entries. `statsbomb` is owner-tier only. Under one slug it has two provenances: `original` (commercial 360 club delivery) and `redistributed` (StatsBomb open-data 360 for six tournaments).
- **Discovery:** `GET /v1/{provider}/matches`, optional `updatedSince`, `dateFrom`, `dateTo`. `updatedSince` must match `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$`. Use `strftime("%Y-%m-%dT%H:%M:%SZ")`, not `isoformat()`.
- **Entry fields:** `id`, `artifacts` (name → filename), `visibility` (`public` or `private`; missing means public), `updated_at`, `date`, `home`, `away`, `provenance`, `source` (`name`, `url`, `licence`), `format_version` (absent means legacy; 2 means the canonical Parquet/zstd set).
- **Fields being added by the pining session** (2026-10-26, owner-delegated; the builder confirms each against the live API and the pining `api-reference.md` before use):
  - `competition_id`, `competition_name`, `season_id`, `season_name` (§29.1). A field is absent when the source does not give it: Gradient Sports gives only `season_name` (`"2022"`), while IDSSE gives both a season id and a season name.
  - `artifact_meta`: per artifact key, the `sha256` and `size` of the stored object bytes. It lets v2 skip a download when the bytes did not change (§17.3).
  - `updated_at` now changes only when an entry field or an artifact hash changes. A byte-identical re-upload no longer changes it.
  - A **list-only token tier** (owner decision §29.6): it sees private entries on the list endpoints, like the owner tier, but gets the uniform 404 on every artifact endpoint and on private single-record reads. The v2 Space holds this token. Status (reviewer B, round 5): the pining draft spec treats this tier as a separate pining project with its own branch and spec, which does not exist yet. The tier is a prerequisite for closing cycle 1 (owner decision §29.8). During development, until the tier exists, the Space holds only the public token, and the preview shows private pining tournaments as "not listed here: needs the list-only token", visibly (§19.5). The owner token never goes into the Space as a stopgap.
  - Status of the other fields: reviewer B found them in an untracked pining draft spec on branch `feat/match-competition-season-artifact-meta`, not landed yet. The builder confirms them against the live API before use, and the fallbacks of §17.3 and §29.1 apply until then.
- **Artifacts:** `GET /v1/{provider}/matches/{id}/{artifact_key}`. Use the bare key, not the filename. The API answers 302 with a presigned S3 URL (`api-reference.md`, "Get Artifact"). **The client must not send the `Authorization` header to the S3 URL**, because S3 rejects a request that carries both kinds of auth (IRIS `providers/pining.py` handles this). On 2026-08-13 the owner-tier `statsbomb` `events` and `freeze_frames` artifacts were observed as `.json.gz` objects served as `binary/octet-stream` with no `Content-Encoding`. The pining repo does not document this. The client must therefore detect compression by magic bytes (`\x1f\x8b` for gzip) on every artifact, never by headers.
- **Data shape traps:** ids are strings for `gradientsports` and integers in `statsbomb` metadata. In `statsbomb` rosters, unset fields are absent, not null. Nulls mean "our copy lacks this" and are never zero. The client must never backfill them from another source (pining ADR 0010).
- **Tiers:** private content returns a uniform 404 for the public token. During owner-token rotation, the client must retry 401 with backoff.
- **SkillCorner `format_version` 2** (pining ADR 0011): owner-tier SkillCorner tracking can arrive as nested Parquet instead of JSONL. Both forms must produce the same canonical bronze (§12.5).

### 7.3 Channel id mapping (must)

Each channel adapter maps its id form to the canonical provider id before any key is computed. The same real match must get the same native id on every channel. Known cases:

- Pining IDSSE uses `DFL-MAT-J03WMX`, and the public HF copy uses `J03WMX`. The canonical form is the bare `J03WMX` (v1 `idsse_native_match_id`).
- Pining public SkillCorner ids are bare (`1899585`) and equal the GitHub ids.
- The pining IDSSE content comes from figshare, while the public module reads HF `pysport/idsse-data`. Equal ids do not prove equal content. The channel-equivalence test (§23.6) therefore compares content hashes (§13.1) as well as ids.

The builder must inventory the id forms of every channel pair and cover each one with a format-contract test (§14.4).

## 8. Module configuration and filters

### 8.1 Model (owner decision)

- Each module has an **active** switch and a **filter**.
- A filter is a list of **clauses**, combined with OR.
- A **competition clause** names one competition and either a list of seasons or **all seasons**. "All seasons" is live: a season that appears later is included automatically.
- A **match clause** names single matches.
- A **tournament** means a competition plus one or more seasons.
- Clauses use the provider's own competition, season and match ids. The Ops page shows names, never raw ids (§19.5).
- Author's call: an active module with an empty filter ingests nothing. The Ops page shows a warning for it. This prevents an accidental full download.
- **StatsBomb 360 option (owner decision §29.13).** The two StatsBomb modules (public and pining) have one extra checkbox, "Only matches with 360 freeze frames" (`statsbomb_360_only`), off by default. With it on, the module ingests only matches that have 360 data, and a match without it is not downloaded at all, events included. No other module accepts the option.
- **Narrowing a filter keeps data (owner decision §29.13).** Removing a clause, or turning the 360 option on, never deletes ingested data. A match that the new filter excludes stays in the lake and is no longer updated. "No longer updated" means its content: an excluded match is not downloaded or re-parsed. Its visibility still follows the source flag, so an excluded match that turns private still gets the private move of §9, which needs no download (owner decision §29.14). The save preview says so, for example "+0 matches would be added · 12 would stop updating (their data stays)". A widened filter re-includes kept matches, which then resume updating, and the preview counts them as well ("· 3 would resume updating").

### 8.2 Laptop defaults (author's call, owner decision on the principle)

The owner decided the principle: the public direct modules are active, maybe with default filters; GS may be inactive because of its size; pining modules are inactive. The concrete defaults (author's call):

| Module | Default |
|---|---|
| StatsBomb | active. Clauses: 1. Bundesliga 2023/24 (competition 9, season 281); FIFA World Cup 2018 (competition 43, season 3); FIFA World Cup 2022 (competition 43, season 106). The 360 option is off. |
| Wyscout | active. Clause: FIFA World Cup 2018. |
| IDSSE | active, all. |
| SkillCorner | active, all. |
| Gradient Sports | inactive. |
| All pining modules | inactive. |
| Resolution | `auto_accept_recommended` on (§15.6). |

The reason for these defaults: a small download that still shows cross-provider resolution working. The overlaps that the defaults produce:

- StatsBomb Bundesliga 2023/24 ↔ IDSSE: players (59 names, §15.4).
- StatsBomb WC 2018 ↔ Wyscout WC 2018: matches, teams and players.
- StatsBomb WC 2022 ↔ GS: matches, teams and players, but only after the user switches GS on.

SkillCorner (A-League) overlaps nothing in the open sources. Two of the three StatsBomb defaults have 360 data: Bundesliga 2023/24 and WC 2022. WC 2018 has none (`match_available_360` is null in `competitions.json`). The builder must confirm every default id and every claimed overlap against the live sources, and must record each id in the default config file with its name.

### 8.3 Storage of config (author's call)

- The config is a separate, versioned object in the install's storage. It is **not** a table in the DuckLake catalog. Reason: the catalog has one writer (the run), and an Ops page save during a run must not race with it. Each object family has exactly one kind of writer, and every write to it goes through one lock:
  - **Config-side objects** (config, review decisions, accept and undo requests, §15.6): written by the operator side only, which is the Ops pages and, locally, the CLI. Every write holds the config lock described below. A run only reads them.
  - **The catalog** (bronze, links, dimensions, run history): written only by the lease holder (§10.6).
  - **Run-side objects outside the catalog** (progress, skip records): written by the run that owns them (§10.6, §17.1).
- Every save keeps the previous version with who, when and what changed.
- The config has a schema version. The loader rejects an unknown version.
- Concurrent saves use a version check inside a lock, so the check and the write are one atomic step:
  - **HF:** only the Space process writes config and review decisions. The Space runs as one replica (assumption, verified in §26 item 11). An in-process lock serialises every save from every operator and every tab.
  - **Local:** the app and the CLI can both write. An OS file lock on the local file system (for example the `filelock` package) wraps every read-check-write.
  - A stale save is rejected with a clear message that names the newer version and who saved it.
- A run reads the saved config once at start and never sees an unsaved draft (owner decision, guess 31).

## 9. Visibility, channels and precedence (owner decisions)

- **Trust the source flag.** Each match takes its visibility from the source. Public direct sources are public. Pining entries carry `visibility`. The code never infers visibility from the token.
- **Visibility lives in metadata.** The match metadata table holds visibility, not the frame files, so a change is a cheap row update (guess 20).
- **Lineage.** A match from two channels is stored once. The lineage table has one row per channel delivery: channel, channel id, content hash, first seen, last listed.
- **Last listed.** The match record stores when each source last listed it.
- **Precedence when two channels deliver the same match** (owner decision; the visibility wording confirmed by the owner in review round 1, §29.2):
  - If the pining copy has fields the public copy lacks, the pining copy is kept. Otherwise the public copy is kept.
  - The match takes the visibility flag of the **kept** copy. A pining copy that is flagged private and has extra fields keeps the match private. A pining copy that is flagged public (for example the 7 public pining IDSSE entries) keeps the match public, even when it has extra fields.
- **"Extra fields" (guess 21, approved):** a field that exists in the pining copy, has at least one non-null value and is missing from the public copy. The decision and its reason go into lineage. The rule is re-checked whenever either copy changes, and the result does not depend on arrival order.
- **Visibility change** (public to private or private to public) flows through as an update.
- **Visibility is checked for every match the lake holds (owner decision §29.14).** Every run also lists each pining module that holds ingested matches, active or not, for visibility only: one list call, no download. An excluded or inactive-module match whose flag changed gets the move of the next bullets. Direct public sources never turn private, so they are not listed for this.
- **Match disappears at the source:** leave it as it is. `last listed` shows the age.
- **Restricted data stays private.** Private matches, their bronze rows and their raw files live only in private storage. A raw file takes the visibility flag of **its own delivery**, not of the match, so a private delivery stays private even when the public copy wins (owner decision, §29.2). When a match becomes private, the raw files of its **pining** deliveries move to the restricted root too (owner decision §29.12); raw files of public-channel deliveries stay, because they are public at their source. On HF, the whole lake bucket is private (author's call).
- **Restricted-data root (owner decision, §29.6).** Every install has two storage roots (§10.5): the **lake root** and the **restricted root**. The restricted root holds every data file of a private match (all its bronze files) and every raw file of a private delivery. On HF the restricted root is a second private bucket that the Space's token cannot read.
  - The lake root keeps the catalog, config-side and run-side objects, the files of public matches, and the DuckLake-written tables (dimensions, links, lineage, run history).
  - **Boundary, stated plainly:** dimension and link tables therefore still hold names and metadata of private matches (players, teams, competitions, dates), because the Ops pages and the review queue need them. The lake-root catalog also holds, for every restricted file, its path, size, row count and DuckLake's per-column statistics (minimum and maximum values). None of these is event, tracking, lineup or raw content. Guess 17 already lets signed-in operators see private tournament names.
  - **Row-level data never enters a lake-root table.** Every silver and gold model over row-level bronze (events, tracking, lineups, frames, 360 frames and the like) is a dbt **view**. Only dimension, link, lineage and ops models may be materialised as tables. A test reads the dbt manifest and fails if any other model is materialised as a table or as an incremental model.
  - A **visibility change moves files** between the roots: copy to the new root under a new file name (§10.2), register the new path and remove the old registration in one catalog commit. Every move, in either direction, writes a **move record** per file (match, direction, old canonical key, new canonical key, time) into a catalog table (owner decision §29.10). A private-to-public move copies the match's current restricted files to the lake root and leaves the old restricted copies for the normal cleanup, which is harmless. The delete rules below apply only to public-to-private records. **A public-to-private move works per match, not per registered file (owner decision §29.11):** it covers every lake-root bronze object of the match that the current catalog registers, that any kept version names, or that DuckLake still tracks. Each object that the current catalog or a kept version names gets a restricted copy under a new write id and a public-to-private record. An object that only DuckLake still tracks, or that only exists in storage under the match's own file-name prefix (for example an upload of a crashed run), gets a public-to-private record with no new key, because no restore point needs it. All of them are then deleted (§29.12). The same move copies the lake-root raw files of the match's pining deliveries (from lineage) to the restricted root and records them. A raw file is named by its content hash (§13.2), so the lake-root object is deleted only if no public-channel delivery in lineage has the same `content_sha256`; otherwise it stays, because it is public at its source, and the run-start delete skips it as well (§29.12). Bronze file names carry the match key (§10.2), so the run finds these objects in the manifests and in DuckLake's tables. When a match changes content and becomes private in the same run, the run moves the old files first and replaces them after. For a public-to-private move (owner decision §29.7, option (a)):
    - **Crash-safe order.** First, one catalog commit registers the new restricted path, removes the old registration and writes the move record. Second, right after that commit, the run deletes the old lake-root copy. A crash between the two leaves a lake-root copy. To close that gap early, every run starts by deleting each lake-root object whose key a public-to-private move record names, accepting "not found". DuckLake's own cleanup is the second net, because it still tracks the old key and lists it after the snapshot expiry (the orphan sweep skips keys that DuckLake still tracks, §10.3 step 7). Only the run writes move records. The exposure window is the time between the commit and the delete, normally seconds. After a crash between the two, it lasts until the start of the next run.
    - **Guard (§29.10).** Before the run-start delete and before cleanup, the run checks that no public-to-private old key is registered in the version that `CURRENT` names. If one is, the run stops loudly and deletes nothing. A unique file name per write makes this impossible in normal operation, so the guard catches only a bug.
    - The kept-set check of §10.3 step 7 maps each public-to-private old key to its record's new key, so older kept versions that still name the deleted copy do not block cleanup, and their restricted copy stays kept. About 8 days after the move, DuckLake's dry-run list includes the old key. The delete step then accepts "not found" **only** for keys that a move record names, and fails loudly for any other missing key.
    - A reader still attached to a pre-move version (§10.4) gets `IO Error: Cannot open file` for that match's files until it sees the new `CURRENT`. This is expected. The services layer shows it as an error and re-reads `CURRENT`, and it never shows it as missing data.
    - **A restore gives every match its latest visibility and keeps the restored version's content (§10.8, §29.11).** The latest visibility comes from the version that `CURRENT` names before the restore. For each match, in two passes over the files that the restored copy names: first, every lake-root file that a public-to-private record names is replaced by that record's restricted copy, whatever the latest visibility (a record with no new key stops the restore loudly); second, the match is placed by its latest visibility, so for a match that is now public, each restricted file is copied to the lake root under a new write id with a private-to-public record. Before it publishes, the restore checks that every file the copy names exists, and stops loudly if one does not (§29.12). It uses the same path-rewrite mechanism as a restore across a `relocate`. Every restore point keeps working.

## 10. Storage

### 10.1 Engine (owner decision: approach 1, after a measured spike)

- A **DuckLake 1.1** catalog in a single DuckDB file. DuckDB **2.0.x** LTS (2.0.0 released 2026-10-21, supported to at least October 2027).
- Every bronze table, tracking and all others, is made of **Parquet files that v2 writes itself** with pyarrow (one file per match per table), then registers with `ducklake_add_data_files`. One file per match lets each file live in the root that its match visibility selects (§9, §10.5).
- Reason (measured, §25.2): our tuned files are 1.29 MB per SkillCorner players table. The DuckLake native writer gives 3.45–4.54 MB, Delta 4.59 MB, Iceberg 4.63 MB, a DuckDB file 5.51 MB and Lance 8.32 MB.

### 10.2 Tuned Parquet layout (must)

- **Split each tracking match** into a players table (one row per player per frame) and a frames table (one row per frame: ball, possession, clock and other frame-level fields).
- **Coordinates as exact integer centimetres.** The writer converts x and y to `int32` centimetres. It must fail loudly if any source value has more than 2 decimals, because then the conversion is not exact. Both measured sources have at most 2 decimals. The check runs on the **source representation**:
  - **Amended (owner, 2026-10-27, §29.15):** the unit is integer **millimetres** (`int32`, at most 3 decimals) for every provider, because Gradient Sports delivers 3 decimals. Read "centimetres" and "2 decimals" in this section as millimetres and 3 decimals.
  - **Text sources** (JSON, JSONL, XML, CSV): check the decimal text itself, before any float conversion.
  - **Binary float sources** (for example pining `format_version` 2 Parquet): format each value as the shortest text that round-trips **in its own dtype** (float32 or float64), then check that text. This rule is exact for both dtypes and needs no tolerance. The spike's `1e-6` float64 tolerance (`measure_parquet.py:40-45`) is replaced by this rule.
  - **Parsed by a library:** IDSSE XML reaches v2 as float64 values from silly-kicks (§12.7). silly-kicks rounds parsed values to 4 decimals (`_parse_float_or_none` returns `round(value, 4)`, `silly_kicks/providers/sportec/parse.py:416-426`, reported by reviewer A). A source value such as `12.340049` would then arrive as `12.34` and pass a check on the output. So for IDSSE the check runs on the **text of the XML `X` and `Y` attributes**, in one streaming pass over the positions file, beside the silly-kicks parse. The builder confirms which silly-kicks path the tracking coordinates take.
  - The dtype of `format_version` 2 coordinates and the GS precision are not measured yet (§26 item 12).
- **Sort player-major:** `(period, player_key, frame)`. The spike sorted by the native player id. Rows now carry `player_key` (§11.2), so the benchmark re-measures the bytes with this sort.
- **Encodings:** `DELTA_BINARY_PACKED` on x, y and frame. Dictionary encoding on low-cardinality columns (`match_key`, period, `player_key`, flags). No BYTE_STREAM_SPLIT (measured 2–2.2× larger).
- **Compression:** zstd level 3. Level 9 gains about 1 % and is not worth the CPU.
- **Per-match constants move to metadata.** Examples from IDSSE: frame rate, competition, season, home and away team ids. **Per-player attributes move to dimensions**, for example team and goalkeeper flag. Fields that are identical on all rows are dropped only when the builder proves they are identical (IDSSE `t == ball_t`) and records the proof in a test.
- **Wall clock:** keep the absolute wall clock once per frame as a typed timestamp, so nothing is lost.
- Author's call: one Parquet file per match per table. The file name holds the match key, a content version and a write id that is new for every file write, a move copy included (owner decision §29.10). So a replacement never overwrites a file in place, and no object key is ever registered twice.
- **Amended (T3, owner-approved 2026-10-31, D6):** the encrypted-local native path also writes exactly one file per table per match (measured, and pinned by a regression test). The ops writes insert their rows with one statement per table per write, so a write stores at most one file per ops table it touches (before, one `INSERT` per row stored one file per row: 560,582 one-row files of `ops_copy_fields` on the 2026-10-31 Mac Mini lake). Every writing attach sets the catalog's DuckLake options `parquet_compression = zstd`, `parquet_compression_level = 3` and `parquet_version = 2`, so native writes get zstd level 3, `DELTA_BINARY_PACKED` on the integer columns and dictionary encoding on the others, as this section requires. Existing files keep their format until a replace or a re-parse rewrites them.

### 10.3 Write protocol (must)

Terms: a **catalog commit** makes new lake state visible to readers. It is not a git commit. Git commits follow §23.5 only.

The writer works on a private **working copy** of the catalog, which no reader ever opens (§10.4).

Per match:

1. Write the Parquet file locally. Check row count and schema.
2. Store it through the storage interface: a local move, or on HF a write through the S3 gateway (§10.5.1).
3. Register it: `call ducklake_add_data_files('lake', '<table>', '<path>')`, with the URI that readers use (a local path, or the `hf://buckets/...` URI on HF, §10.4).
4. **Replace a match** with `begin; delete from <table> where match_key = ...; call ducklake_add_data_files(...); commit`. The spike showed this drops the old file without a delete file.
5. **Catalog commit:** `CHECKPOINT`, detach the working copy, publish it as a new immutable catalog version (§10.4), then re-attach. With each version the writer also publishes its **file manifest**: the canonical object keys (§10.3.1) of every file that the version's current (head) snapshot names, from `ducklake_list_files` on every table. A manifest never changes, like its version.
   - **Amended (T3, owner-approved 2026-10-31, D7):** the commit runs no `CHECKPOINT` (on the DuckLake attach it runs DuckLake maintenance, which can merge or delete files outside the guarded cleanup of step 7). `DETACH` flushes the catalog file. A detached catalog file of at least 8 MiB is then compacted: a plain DuckDB open (the keyed plain open on an encrypted-local lake) copies every metadata table into a fresh file (`COPY FROM DATABASE`), checks row counts, content checksums and the schema, and replaces the old file only when every check passes. A failed check publishes the original with a `catalog_compaction` warning. When the commit happens is decided by the checkpoint policy (§17.2).

Once per run, at the end (after resolution and dbt, §17.2):

6. **Merge the DuckLake-managed small files** (**amended T12, owner-approved 2026-11-01, D3**). For every non-bronze DuckLake-managed table (§10.2): rewrite each file that carries a delete file (`ducklake_rewrite_data_files`, delete threshold 0.0), then merge the small files left (`ducklake_merge_adjacent_files`) in creation-time windows bounded in file count, then once more over the whole table when the windows left more than one file. A guard runs first and fails closed: every bronze table must carry `auto_compact = false`. A seconds budget bounds the work; the rest waits for the next run. A table at or below the small-file floor with no delete file is left alone. Neither step deletes a file: the replaced files leave through step 7 once their snapshots expire (§10.8). The working copy carries the changes; the run-end catalog version (step 5) publishes them. A guard stop is recorded as a run error; the run still runs step 7 and publishes its data, and ends `failed`. The merge does not run on a stopped run.
7. Expire the snapshots older than the retention (§10.7, `ducklake_expire_snapshots`), then clean up files, in three parts. All comparisons use canonical object keys (§10.3.1).
   - **Kept-version file set.** The union of the file manifests (step 5) of every kept catalog version (§10.8), in which each key that a public-to-private move record names as its old key is replaced by that record's new key, or removed when the record has no new key (§9, §29.7, §29.10, §29.11). A restore point needs only the head snapshot of its version, so a manifest lists only head-snapshot files. Older snapshots inside a version are not restore points.
   - **DuckLake's own list.** List `ducklake_cleanup_old_files(..., dry_run => true)`. If any listed file is in the kept-version file set, stop with a loud error and delete nothing. Otherwise delete the listed files through the storage interface, then run the non-dry cleanup, so DuckLake clears its own list.
     - Invariant: snapshot retention (§10.7, 8 days) is longer than version retention (§10.8, 7 days). The head snapshot of every kept version is therefore still unexpired in the current catalog, so DuckLake never lists a file that a kept version names. In a linear history the guard never fires. It guards against a bug, not an expected case.
   - **Orphan sweep.** Fenced to the data prefix `data/` of both roots. Only the Job or a local writer runs it, because the Space cannot read the restricted root. The raw zone (`raw/`), the catalog (`catalog/`), config-side objects (`config/`) and run-side objects (`runs/`) are outside it and are never swept here. List the objects under `data/` through the storage interface, and delete those that are not in the kept-version file set (in which moved keys are replaced by their new keys), that DuckLake does not still track, and that are older than the run's start. "Still tracks" means any path in the working catalog's `ducklake_data_file` (current or ended rows) or `ducklake_files_scheduled_for_deletion`. DuckLake deletes those files itself in the part above, so the sweep never deletes one first, which would make DuckLake's later delete fail on "not found" (owner decision §29.11). Both times come from the storage clock: the run's start is the storage time of its lease object (§10.6). This removes files that no catalog lists, for example the files of a run undone by a restore (§10.8) or uploads from a crashed run. Two fail-closed guards run before any delete:
     - Every key in the manifest of the version that `CURRENT` names must appear in the listing. If one does not, the key mapping or the listing is wrong, so the sweep aborts loudly and deletes nothing.
     - If the sweep would delete more than 10 % of the objects under `data/` (author's call), it aborts loudly. An operator can repeat it with an explicit CLI flag after checking the list, for example after the restore of a large backfill.
     - **Amended (T3, owner-approved 2026-10-31, D5):** the 10 % guard does not count a doomed object whose storage time falls inside the interval of a surviving run anchor (§10.6): the unpublished work of a crashed run, a lost-lease writer or a rolled-back transaction. The sweep still deletes those objects. It removes an anchor only after its last delete returned, and only when the anchor's interval ended before the sweeping run started, or its generation has no marker. A third fail-closed guard runs first: the sweep stops and deletes nothing when a doomed key is named by the `CURRENT` manifest. Each sweep logs its exempted count.
8. A final catalog commit (step 5).

#### 10.3.1 Canonical object keys (must)

One function maps every stored file reference to a **canonical object key**: the root name plus the POSIX path relative to that root (for example `lake:data/main/dim_players/ducklake-<uuid>.parquet` or `restricted:data/bronze/skillcorner_players/<file>.parquet`). The mapping rules:

- A DuckLake-written file, stored relative to `data_path`: `lake:data/` + its relative path.
- An `hf://buckets/<ns>/<bucket>/<key>` URI: `<root>:<key>`, where `<root>` is the root whose bucket matches.
- An `s3://<bucket path>/<key>` URI on the gateway: `<root>:<key>` in the same way.
- A local absolute path under a root: `<root>:` + its path relative to that root, with `/` separators on every OS.
- A reference that is under neither root fails loudly. It is never treated as an orphan.

Storage listings are mapped by the same function. Manifests (step 5) hold only canonical keys. A unit test covers every rule on Windows, macOS and Linux paths.

Data files are always stored before the catalog version that names them. A reader never sees a catalog that points to files that do not exist yet.

Tables that hold our own files have `auto_compact = false` (`call lake.set_option('auto_compact', false, table_name => '<table>')`). Reason: DuckLake compaction rewrites files with DuckDB's dictionary-encoded writer. The measured result is 3.5× larger than our files unpartitioned (4.54 MB against 1.29 MB) and 2.7× larger partitioned (3.45 MB). DuckLake exposes no `dictionary_size_limit` option.

**Amended (T12, owner-approved 2026-11-01, D3):** these bronze tables (the `lakehouse:bronze` comment) keep `auto_compact = false` for the reason above. Every other DuckLake-managed table — the ops tables and any native dimension or link — writes one data file per insert and one delete file per partial delete, and v2 never runs DuckLake's compaction (`CHECKPOINT` stays forbidden on the attach, §10.3 step 5). Without maintenance the file count of these tables grows with every run. So the run end merges them (§10.3 step 6).

**dbt hand-off.** A DuckDB catalog file allows one attacher (reviewer B reproduced `Unique file handle conflict` for a second in-process attach). Before dbt runs, the run detaches the working copy. dbt-duckdb then attaches it as the only attacher. After dbt, the run re-attaches. A test proves the hand-off (§23.6).

Small tables (dimensions, links, ops tables) may use the DuckLake native writer through dbt. Compaction is allowed for them. On HF, DuckDB cannot write `hf://`, so these native writes need the path of §10.5.1.

Measured path rules (`SPIKE-FINDINGS.md` §6, local):

- Files that DuckLake writes itself are stored **relative** to `data_path`.
- Files registered with `ducklake_add_data_files` are stored with their **absolute** path. A relative argument is refused.
- Small native inserts (5 rows in the probe) are **inlined** in the catalog and have no data file.

### 10.4 Catalog publication and readers (must)

Reason: a DuckDB catalog file cannot be opened by a second process while the writer has it attached. Reviewer B reproduced this with duckdb 2.0.0 on Windows: `The process cannot access the file because it is being used by another process`. A remote reader on HF must also never range-read an object that is being replaced. One model solves both, locally and on HF:

- **Immutable versions.** Each catalog commit publishes the working copy as a new object `catalog/v/<sequence>.ducklake`. A published version is never changed.
- **Pointer.** A small object `catalog/CURRENT` holds the sequence number, the version object's name and size, the SHA-256 of the version object, and a checksum of `CURRENT`'s own content. It is the only catalog object that is overwritten, and only by compare-and-swap (§10.6, §29.9).
  - A reader verifies `CURRENT`'s own checksum on every read. That detects a torn or partial `CURRENT` and costs nothing extra. On a mismatch it retries, and fails loudly after 3 tries.
  - A reader checks the version object's size against `CURRENT` when it attaches. It does not hash the whole version on every read, because that would mean a full download on HF (§10.4, "no download").
  - The writer verifies the full SHA-256 after each publish. A CLI `verify` command checks every kept version in full.
- **Readers** (Ops pages, the services layer, analysts) read `CURRENT`, then attach that version read-only. Author's call: the services layer keeps one attachment per version and re-attaches only when `CURRENT` names a new version. Each request uses exactly one version, so a page never mixes two versions. Reason: an attach over `hf://` took 1.2–1.3 s in the spike, and a per-request attach would break the 500 ms budget of §19.5. §26 item 3 measures the cached interaction time on HF.
- **Re-rooted reads.** The catalog stores DuckLake-written files relative to the writer's `data_path` (§10.3). A reader attaches with `(read_only, data_path '<storage root>/data/', override_data_path true)`, so those files resolve under the reader's storage root. Measured locally (`SPIKE-FINDINGS.md` §6). Files that v2 registers itself keep their absolute storage URI, so v2 registers them with the URI that readers use: a local path on local installs, and an `hf://buckets/...` URI on HF (§10.5.1).
- **Local:** publication is a file copy followed by a replace of `CURRENT`. On Windows a reader can hold a file open. So the writer retries a locked replace of `CURRENT` with backoff (at most 10 tries, then a loud failure), deletes old versions only when they are past retention (§10.8), and retries a locked delete later.
- **HF:** publication uploads the version, then `CURRENT`. A reader attaches `ducklake:hf://buckets/<ns>/<bucket>/catalog/v/<sequence>.ducklake` read-only over `hf://`, with no download, after `create secret hf (type huggingface, provider credential_chain)`. The reader needs only its scoped HF token. It never holds the S3 gateway key (§10.5.1). Native files resolve through `override_data_path` with an `hf://` root, and v2's own files through their registered `hf://` URIs. The spike measured 1.2–1.3 s for the attach and a 43–79 ms median for a frame-window query over the network. The spike attached a fixed path. The versioned path, the pointer and the re-rooted read are verified in §26 item 3.
- **Freshness.** Readers see the state of the last catalog commit, which is at most one match behind the run. Live progress comes from the progress object (§17.1).
  - **Amended (T3, owner-approved 2026-10-31, D1):** at most one checkpoint interval behind the run (T minutes or M pending units, whichever comes first), not one match (§17.2).
- **Amended (T6, owner-approved 2026-10-31, D3):** after its compare-and-swap of `CURRENT`, the writer reads `CURRENT` back and raises `LeaseLostError` when another pointer is there (the HF gateway's `If-Match` is not atomic under concurrency). On HF the catalog version is uploaded as one part, so a catalog copy larger than the measured single-part limit (2,048 MiB) cannot be published; the `catalog_growth` warning fires at half that limit at the latest.

### 10.5 Storage interface (must)

- One interface with two implementations: local file system and HF Storage Bucket.
- Operations: put, get, list, delete, exists, plus a DuckDB path for reads.
- **Two roots per install** (§9, §29.6): `lake` and `restricted`. Every call names its root. On HF they are two private buckets. Locally they are two folders, by default `<lake>/` and `<lake>-restricted/`, and either can be configured to another location. Local installs use the same code path, so tests cover the split everywhere.
- Our own bronze files go to the root that their match visibility selects. DuckLake-written files always go to the lake root, through its `data_path`. So every bronze table is written by v2's own writer, never by the DuckLake native writer, because a native file could mix public and private rows.
- **File granularity and root per kind of bronze** (author's call):
  - **Per-match artifacts** (events, tracking, lineups, rosters, match metadata, 360 frames): one file per match per table, in the root that the match visibility selects.
  - **Source-level listings** that cover many matches or entities (StatsBomb `competitions.json` and `matches/<competition>/<season>.json`, the Wyscout `players`, `teams` and `competitions` files, the pining match and player lists): one file per listing and per visibility. Public records go into a lake-root file, and private records into a restricted-root file. A record's visibility comes from its own flag where the source gives one (for example the per-record `visibility` of the pining players list), or else from its source's visibility. A listing with only public records has no restricted file.
- DuckDB `httpfs` cannot write to `hf://` (spike: `HuggingFaceFileSystem: DirectoryExists is not implemented!`). The spike wrote through `HfApi.batch_bucket_files` instead.
- HF implementation (owner decisions §29.4, design (a) of §10.5.1, and §29.9): the HF Job writes data files, raw files and the **catalog-control objects** through the S3 gateway. The catalog-control objects are the lease objects, the catalog versions, their manifests and `CURRENT`. They need conditional writes (`If-None-Match` to create, `If-Match` to replace `CURRENT`), which `HfApi` does not offer (§10.6). Other small objects (config-side objects, progress, skip records, run results) go through `HfApi`. Every HF reader reads through `hf://`; the Job reads `CURRENT` through the gateway when it needs its ETag. Neither the Space nor the Job mounts the bucket. The storage interface stays the only writer.
- **Missing root fails loudly.** If the configured lake root does not exist (for example an unmounted external drive), the run stops with an error. Only an explicit `init` command creates a new lake.

#### 10.5.1 DuckLake-native writes on HF (owner decision: design (a), §29.4)

dbt tables, link tables, match metadata, lineage and run history are written by DuckDB itself. DuckDB refuses to write `hf://` (§25.4). The spike proved only the registration of files that v2 uploaded with `HfApi`, and it ran dbt only on a local `data_path`. So the HF install needs a defined path for native writes. Two designs:

- **(a) Chosen by the owner: the HF S3 gateway.** The lake's `data_path` on HF is `s3://<bucket path>/data/` on the gateway at `s3.hf.co` (path style, region `us-east-1`). DuckDB `httpfs` then writes native files directly, dbt works unchanged, and v2's own files are written to the same S3 paths. Reasons:
  - One write path for every file, with no sync step.
  - S3 conditional writes (`If-None-Match`) are a candidate to close the lease race (§10.6, §26 item 2).
  - Any S3-compatible store works later with the same code.
  - Costs and risks: the gateway keys (HFAK) can only be created by hand in the HF web UI. Whether DuckDB writes, multipart uploads and conditional writes all work through the gateway is **not tested**. Whether a key can be limited to the two v2 buckets is **not known**. §26 item 15 verifies all of these.
  - **Registration scheme (author's call).** The Job writes each of v2's own files through the gateway (`s3://...`) into the bucket of its root (lake or restricted, §9), then registers it with `ducklake_add_data_files` under its `hf://buckets/<ns>/<bucket>/...` URI. This relies on both URIs naming the same object, which §26 item 15 verifies. Readers then need no S3 key. If the two URIs do not name the same object, the builder must stop and bring the evidence to the owner.
  - **Key holder (author's call, security).** Only the HF Job holds the gateway key. The public Space never holds it (§20). The owner creates the HF scheduled job once from the CLI, with the gateway key and the pining owner token as job secrets (§18). The Space then only changes, pauses, resumes and triggers that job, which needs no secret values.
  - **Key scope (stop rule).** If a gateway key cannot be limited to the two v2 buckets (lake and restricted, §9), the builder must report this to the owner before the first use of the key. The owner then decides whether an account-wide key in the Job's secrets is acceptable.
- **(b) Fallback, only with the owner's approval if (a) fails verification: a local mirror.** The writer's working copy uses a local `data_path` that mirrors the bucket's `data/` layout. At run start the writer downloads the native files that the current version names. Before each catalog commit it uploads every new native file through `HfApi` to the same relative path. Readers re-root with `override_data_path` (§10.4). This needs no new credentials and uses only proven parts, but it adds a sync step and a download at every run start. The total size of native files is measured in §26 item 4.

In both designs, data files reach storage before the catalog version that names them (§10.3).

**Absolute paths of v2's own files.** `ducklake_add_data_files` stores absolute paths (§10.3), so a moved lake root breaks them. A local lake moved to a new drive is an example. Author's call: a CLI `relocate` command rewrites the registered paths of v2's own files from the old root to the new one, in a new catalog version.

- **Mechanism.** DuckLake has no path-rewrite call. The catalog is a DuckDB file that holds the DuckLake metadata tables defined by the DuckLake 1.1 specification. `relocate` attaches a copy of the working catalog as a plain DuckDB database, with no DuckLake attach open on it, and updates the `path` column of `ducklake_data_file` (and of `ducklake_delete_file`, if it has rows) inside one transaction. It holds the lease.
- **Pinning.** The command checks that the catalog's DuckLake format version is `1.1` and refuses any other version. A DuckDB or DuckLake upgrade therefore fails loudly until the builder re-verifies the mechanism.
- **Guard.** The "Lake relocation" test (§23.6) moves a lake, runs `relocate` and reads every table by value. It runs again on every DuckDB upgrade (§26 item 6).

### 10.6 Single-writer lease (must)

- A lease object in storage holds the holder id, run id, start time, expiry and last heartbeat.
- A run takes the lease before it writes and renews it while it works. It checks the lease again just before the catalog commit.
- A run that cannot take the lease records "skipped: run in progress" and exits (guess 32). It has no lease, so it must not write the catalog. It writes a small object `runs/skipped/<run_id>.json` outside the catalog instead. The Ops page shows it at once. The next lease holder folds it into run history and then removes the object.
- HF Buckets through `HfApi` have no conditional write. The builder must choose a mechanism that closes the race (§26, item 2). If no safe mechanism exists, the builder must stop and bring the evidence to the owner.
- **Chosen mechanism (plan review round 1; HF routing by owner decision §29.9).** The lease is a chain of generation objects, each created only if absent, so exactly one taker wins each takeover. A stale holder is fenced three ways: catalog versions and manifests are created only if absent, `CURRENT` is replaced only by compare-and-swap, and the orphan sweep deletes only objects older than its own run's start (§10.3 step 7). On HF, these conditional writes go through the S3 gateway.
- **Amended (T3, owner-approved 2026-10-31, D5):** every lease acquisition stores the generation's run anchor (`runs/lease/<generation>.anchor`, write-once, names the generation and the run id) and then its acquisition marker (`runs/lease/<generation>.acquired`, write-once; its storage time is the generation's start) before it returns the lease. If either store fails, the generation is released and the caller writes nothing. `Lease._prune` deletes a marker with its generation and never deletes an anchor; the sweep, a restore and a relocate remove anchors (§10.3 step 7). A writer confirms the lease at each write step (each module, match and listing entry, each visibility move, and before resolution and dbt) when its last passing `assert_held` is older than one heartbeat.
- **Amended (T6, owner-approved 2026-10-31; `docs/specs/2026-10-31-v2-t6-hf-single-writer-design.md` D1):** on HF the gateway's conditional writes are not atomic under concurrency (2026-10-31 evidence), so the fence of record is a lease ledger: a private HF dataset repository per lake (`<lake bucket>-control`, Job variable `LAKEHOUSE_CONTROL_REPO`) holding `lease.json`; a generation is won only by an HF Hub commit with `parent_commit` set to the head the taker read. The current generation is the higher of the ledger's and the bucket's highest; a ledger generation without a bucket object is held for `LEASE_TTL` plus `TAKEOVER_GRACE` from its commit's server time; an unclear commit outcome is decided by reading the head; HTTP 429 or an unreadable ledger stops the acquisition loudly. The bucket generation object stays as a second fence, and the winner reads the ledger again before it returns the lease. `assert_held` reads the ledger. The heartbeat and the release stay bucket writes. A local lake is unchanged.

### 10.7 Snapshot retention (author's call)

Keep DuckLake snapshots for 8 days, then expire them and clean up old files. This gives a short recovery window at a small storage cost. Snapshot retention must stay longer than version retention (§10.8, 7 days), so the cleanup invariant of §10.3 step 7 holds. A config check refuses settings that break this rule.

### 10.8 Catalog safety and version retention (author's call)

HF Buckets are not versioned, so the published catalog versions (§10.4) are the lake's restore points. Retention, in the same units as §10.7:

- Keep every run-end version (the final catalog commit of each completed run) for 7 days.
- Keep the in-run versions of a run only until a run-end version supersedes them, for crash recovery: the run's own end, or for a crashed run the end of the next completed run. Then they are deleted. In-run versions are also never kept longer than 7 days, except the version that `CURRENT` names. This cap keeps the cleanup invariant of §10.3 step 7 true after a crash followed by a long pause.
- Delete other versions, with their manifests. Never delete the version that `CURRENT` names. Version retention (7 days) stays shorter than snapshot retention (8 days, §10.7), because cleanup may remove files that a version older than the snapshot retention names.
- **Amended (T3, owner-approved 2026-10-31, D4):** in-run versions are pruned during the run, after each in-run publish, and again after the run-end commit, each time only once their successor has been `CURRENT` for at least the grace G (`checkpoint.grace_minutes`, default 2 minutes), so a reader that opened a version a moment earlier keeps it. A crashed run's in-run versions go at the end of the next completed run. Version deletes never delete data files. A reader whose version object is gone before it attaches retries once on the new `CURRENT`, then shows `StaleCatalogError`.
- **Amended (T12, owner-approved 2026-11-01, D3):** the run-end merge (§10.3 step 6) replaces each non-bronze table's small data files with merged ones. It never deletes a file; the replaced files are still named by the snapshots of their window, so they leave only through the normal cleanup (§10.3 step 7) once those snapshots pass the snapshot retention. Because snapshot retention (8 days) stays longer than version retention (7 days), a merged-away file is never deleted while a kept version still names it. So the merge keeps every restore point and needs no change to the retention above.

A CLI command restores a kept version by pointing `CURRENT` to it, and the next run continues from that version. Rules:

- The restore takes the lease (§10.6). Without it, a running writer's next catalog commit would overwrite the restore.
- Every run builds its working copy from the version that `CURRENT` names. It never reuses a working copy left on disk by an earlier run.
- A restore drops every version newer than the restored one (the undone run's in-run and run-end versions) from the kept set. It writes a restore record with who, when, the restored version and the dropped versions, then deletes the dropped version objects. So after a restore, the undone run's files are named by no kept version.
- Cleanup deletes a file only when **no kept version** names it, never on the evidence of the newest version alone (§10.3 step 7). The next run's orphan sweep removes the undone run's files.
- Each version records its storage root. A restore to a version whose root differs from the current root (a version from before a `relocate`, §10.5.1) applies the same path rewrite to a copy of it and publishes that copy as a new version. So a restored lake never names files under an old root.
- A restore also gives every match its latest visibility, with its files in the root that this visibility selects, and keeps the restored version's content (§9, §29.7, §29.10, §29.11). Rules, so that the move records survive any number of restores:
  - Before it drops any version, the restore reads **every** move record from the version that `CURRENT` names (the newest catalog before the restore).
  - It writes all of them into the restored copy's move-record table, including those older than the restored version, before it publishes the copy.
  - A restore never removes a move record. A move record is pruned only after an event, never after a fixed time: when no kept version names its old key **and** DuckLake no longer tracks that key at all: no `ducklake_data_file` row, current or ended, and no `ducklake_files_scheduled_for_deletion` row, so its non-dry cleanup has removed it (§29.12). A key that has not yet reached the scheduled-deletion list is still tracked. A record of a raw file is pruned once the run-start delete has removed its lake-root object, or found that a public-channel delivery keeps it. Until then, the "not found" exemption of §9 still needs it. If no run happens for a long time, the record simply stays.

The restore test (§23.6) restores to the state before the last run. It checks that every file the restored version names exists, that the restore holds the lease, and that the next run starts from the restored version.

### 10.9 Resource caps (must)

Config values for DuckDB `threads` and `memory_limit` and for the pyarrow CPU count. Defaults: 4 threads and 3 GB (the values the spike used on the AI-PC). A run processes one heavy match at a time and avoids extra in-memory copies of large tables.

These caps do not cover the Python parse. One IDSSE positions file is 354.5 MB of XML with 3.2 M player rows, and its parse runs outside DuckDB's `memory_limit`. The builder must measure the peak RSS of one IDSSE parse and one SkillCorner parse (on the Mac Mini, not the AI-PC), record the budget in ADR-002, and add a benchmark that fails above it (§26 item 13).

## 11. Data model

### 11.1 Layers (owner decision: keep the medallion layers and dbt)

| Layer | Content | Written by |
|---|---|---|
| raw zone | the archive of raw source files (§13) | run |
| bronze | typed, complete, one canonical form per provider and artifact | run (our Parquet writer, DuckLake) |
| silver | dbt staging models per provider: views or thin models | run (dbt) |
| gold | conformed dimensions, link tables, canonical pointers, the cross-provider tracking view | run (dbt) |
| ops | run history, module runs, match steps, errors, unmapped fields | run |

Tracking frames are written to Parquet **once**. Silver and gold tracking models are views or thin models, never second copies (owner decision).

### 11.2 Bronze audit columns (must)

Every non-tracking bronze row carries `_ingested_at` (UTC timestamp), `_run_id` and `_source_hash` (the `content_sha256` of §13.1). It also carries the provider name, the canonical native ids and the surrogate keys (§14).

**Tracking tables are exempt** (author's call), because these values are constant per file and the measured layout (§25.1) did not contain them:

- Each tracking row carries only the keys it needs for queries and replaces: `match_key` and, in the players table, `player_key`.
- `_ingested_at`, `_run_id`, `_source_hash` and the provider name live once per file in the file registry of the match metadata table (§11.3).
- Native player ids live in a per-match roster table that maps each `player_key` to its native id, so nothing is lost.
- The benchmark of §23.4 re-measures the bytes with the final tracking schema. ADR-002 records the result next to §25.1.

### 11.3 Match metadata table

One row per match: keys, natural keys, visibility, chosen channel and the reason for it, last listed per source, coordinate-system reference, `has_360_frames` for StatsBomb matches (§29.13), and the location of every bronze file. The table is the pointer index into the Parquet lake.

### 11.4 Coordinate-system metadata (owner decision)

Coordinates are stored as the provider delivers them, with no unified frame. A coordinate-system table records, per provider and match: origin, units, pitch length and width, axis directions and the direction of play where the source states it. The builder must take each value from the provider's documentation or data, never from a guess. Known: IDSSE tracking (DFL_04_03) is centre-origin metres, and IDSSE events (DFL_03_02) are pitch-origin. A common centre-origin frame is a long-term aim, not cycle 1.

### 11.5 Shared tracking core (owner decision)

All tracking providers share the same column names, types and units for the core. Author's call for the core of the players table (amended to millimetres, §29.15: `x_mm`, `y_mm`, ball `x_mm`, `y_mm`, `z_mm`): `match_key`, `period`, `frame`, `player_key`, `x_cm`, `y_cm`, `is_visible` (or the provider's detection flag, mapped with one documented meaning). Frames table core: `match_key`, `period`, `frame`, `timestamp`, ball `x_cm`, `y_cm`, `z_cm` where the source has it, and ball detection. Provider-specific fields stay as extra typed columns.

### 11.6 Gold models

- `dim_competitions`, `dim_seasons`, `dim_teams`, `dim_players`, `dim_matches`.
- Link tables from entity resolution, with confidence, layer and method.
- The canonical pointer columns and the pointer move log (§14.3).
- The cross-provider tracking view (§16).
- dbt contracts are enforced on every gold model.

## 12. Completeness and exactness (owner decision)

The owner said: "the intent was always ... to preserve everything that gets ingested, so we never have to go back and reprocess to get fields we skipped the first time (if the provider adds new fields later, that is different)".

- **12.1** Every source field lands as its own typed column, wide and sparse. JSON or VARIANT columns are only for payloads whose shape varies.
- **12.2** Every adapter has a **field-inventory contract test**. It reads a real raw sample, lists every field and fails if any field has no bronze column. For StatsBomb and GS, the repository may hold synthetic fixtures only (§23.4), so the live tier is the proof of completeness for them. The live tier must pass for every adapter before the cycle closes.
- **12.3** At run time, an unmapped field fails that match loudly. The error goes to the run history and shows on the Ops page. Author's call: the run continues with the other matches and ends with a failed status and a non-zero exit code.
- **12.4** Every type transform has an exactness check, for example the integer-centimetre check and timestamp round trips.
- **12.5** Two forms of the same provider data (for example SkillCorner JSONL and pining `format_version` 2 Parquet) must produce identical canonical bronze. A test proves it on the same match.
- **12.6** v1 and IRIS parsers are never proof of completeness. Audit each one against the raw payload. The IRIS SkillCorner parser dropped `possession` (`player_id`, `group`), the 8 `image_corners_projection` values and the pre-match frames. v2 keeps them all.
- **12.7** IDSSE player names come from the DFL metadata XML (`FirstName`, `LastName`, `Shortname`), because bronze keeps every field. silly-kicks `MatchInfo` does not expose names. silly-kicks stays the parsing authority for IDSSE tracking.
- **12.8** Each bronze column is documented in the dbt `sources.yml`.
- **12.9** Additive schema evolution (audit T9, 2026-11-01). When a parser maps a new source field (12.3), the next write adds the column to a lake that already holds the table. It adds the column as nullable inside the write's transaction, so a failed write rolls the change back, and the rows written before read null. Both write paths do this: the register path (`ducklake_add_data_files`) and the encrypted-local path, which inserts by column name. A changed column type, or a table column that a fresh write no longer carries, is not additive: the write stops with `IntegrityError`, and a migration is needed. A visibility move re-registers copies of files that the table already holds, so it accepts a file that is older than an added column (`bronze/evolve.py`).

## 13. Raw archive (owner decision; replaces guess 19)

### 13.1 Hash identity (must)

The change signals of §7.1 are different kinds of value: git blob SHA-1 (for an LFS file, the SHA of the pointer), md5, LFS sha256, a Drive id with a day, and pining `updated_at`. Some are not hashes at all. So v2 separates two things:

- **`content_sha256`**: the SHA-256 of the **decoded** file bytes. Decoded means after removal of a transport wrapper (gzip, bz2) that the channel adds or that the file arrives in. A file with no wrapper is hashed as delivered. The raw zone, `_source_hash`, lineage comparison (§9) and no-op detection (§17.3) all use this one hash.
- **`change_signal`**: the channel's own value, stored in lineage per delivery. It decides only whether to download.

Lineage also stores `delivery_sha256` (the bytes as delivered) for audit. A test proves that the same file delivered by two channels, once with and once without a gzip wrapper, gets the same `content_sha256` (§23.6).

### 13.2 Archive rules

- An immutable raw zone. Each file is stored under its `content_sha256`, as the decoded bytes compressed with zstd. The original wrapper format is recorded in lineage.
- When a source changes a file, both versions stay.
- A raw file takes the visibility flag of its own delivery (§9). A private delivery's raw file lives only under `raw/` in the restricted root. A public delivery's raw file lives under `raw/` in the lake root. When a match becomes private, the raw files of its pining deliveries move to the restricted root; the lake-root object stays only if a public-channel delivery has the same content (§9, owner decision §29.12).
- Each install has an on/off switch. Default on for the HF install and the Mac Mini staging install. Default off on laptops, to save disk.
- The CLI can re-parse from the archive offline, for one match or for all, and the re-parse is incremental.
- Why: protection against parser bugs and against sources that vanish (GS Drive, SkillCorner LFS quota). Measured cost: SkillCorner 5.94 MB and IDSSE 36.64 MB per match at zstd 9 (§25.3).

## 14. Identity: the Kimball id layer (owner decision)

The owner said the v1 layer "eliminated any confusion about IDs overlapping between providers". v2 carries it over completely.

### 14.1 Surrogate keys (guess 46)

- `match_key`, `player_key`, `team_key`, `competition_key`, and new `season_key` (guess 49).
- Formula, unchanged from v1 (ADR-011): `xxhash64(concat_ws('|', provider, cast(native_id as string)))` with Spark's default seed 42, as a signed 64-bit BIGINT.
- NULL native id: the v1 player, team and competition macros return a NULL key. The v1 match macro has no NULL guard, and Spark `concat_ws` skips NULLs. v2 requires a non-null native match id and fails loudly without one, so the v1 match-key NULL case cannot occur. The other entities give a NULL key, as in v1. The parity fixture covers both cases.
- v2 computes keys in Python with `xxhash.xxh64_intdigest(f"{provider}|{native}".encode("utf-8"), seed=42)` converted to signed. Check (2026-10-26): `('idsse', 'J03WMX')` gives `4203055498971744044`, the v1 value in `scripts/create_indexes.py`. The builder must keep this as a parity test with several known v1 values per entity. Get the reference values from v1 (the Lakebase dims, or a one-off local `pyspark` run of `xxhash64`) and store them as a fixture.
- Author's call: one Python key generator computes every key at bronze write time. dbt reads key columns and never hashes. This keeps one generator, as ADR-018 requires.
- Because the keys equal v1 keys, v1 and v2 dimensions can be compared row by row during the side-by-side period.

### 14.2 Provider names and channels (guess 47)

- The hash uses the v1 provider names: `statsbomb`, `wyscout`, `idsse`, `skillcorner`, `gradientsports`.
- The channel never enters the hash. A match has one key whatever channel delivered it.

### 14.3 Natural keys and the canonical pointer

- Every dimension keeps `(provider, native_*_id)` as attributes for lineage and debugging.
- Facts and bronze store only per-provider keys. These never change.
- The canonical pointer (`canonical_player_key`, and the same for teams, competitions and matches) lives on the dimension only. It points to the cluster member from the highest-ranked provider. It may move when the cluster changes. Every move is logged with old value, new value, run id and reason (owner decision; replaces guess 41).
- What comes from v1 and what is new: the pointer on the dimension and the rank order come from v1 (`dim_players.sql:230-262`). v1 recomputes the pointer on every build and keeps **no** move log. The move log is new work in v2.
- Provider rank: v1 uses SB > WS > IDSSE > Metrica (`dim_players.sql:245`). Metrica is not in v2 (§3). Author's call: v2 uses `statsbomb` > `wyscout` > `idsse` > `skillcorner` > `gradientsports`. Note: the IRIS `PROVIDER_RANK` puts SkillCorner before IDSSE. v2 follows v1.
- The legacy `canonical_player_id` string is dropped. It exists only for v1 consumers (guess 50).

### 14.4 Native id formats (guess 48)

- Carry over `src/shared/identifiers.py`: one generator per id format per provider, with format-contract tests (ADR-018).
- Bronze stores the canonical native id directly. Staging needs no fix-ups such as the old `idsse_` prefix.
- Add a native season id generator per provider for `season_key`. **Grain** (author's call): `season_key` identifies a **competition-season**, the same grain as a tournament (§8.1). Reason: a StatsBomb `season_id` is a label that several competitions share. In `competitions.json`, 22 of 48 season ids are shared: id 281 covers five competitions in 2023/24, and id 90 covers La Liga and FA WSL 2020/21. A key on the bare season id would join unrelated tournaments, and bronze keys are permanent.
  - Native season id: `<native_competition_id>:<native_season_id>`, for example `9:281` for StatsBomb 1. Bundesliga 2023/24. The separator is `:`, because `|` is reserved for the key formula.
  - Providers without a season id use the season name as `<native_season_id>`. Example: Gradient Sports metadata gives only `season: "2022"`, so pining puts it in `season_name` and leaves `season_id` out (the author's recommendation to the pining session on 2026-10-26, relayed by the owner). IDSSE has both: `SeasonId="DFL-SEA-0001K6"` and `Season="2022/2023"` in the J03WPY metadata, so IDSSE uses its `SeasonId`. The builder defines each form in `identifiers.py`.
  - Correction from the J03WPY metadata: that match is `CompetitionId="DFL-COM-000002"`, 2. Bundesliga 2022/23. The IDSSE matches are therefore not all 1. Bundesliga. The player overlap with StatsBomb 1. Bundesliga 2023/24 (§15.4) is a player overlap across leagues and seasons. The builder lists the competition and season of all 7 matches when confirming the defaults (§8.2).
  - `dim_seasons` also keeps the plain season label (for example `2023/2024`), so analysts can group across competitions.
  - A format-contract test proves that StatsBomb season 281 under five competitions gives five different `season_key` values.
- Add the channel id mappings of §7.3, each with a test.

## 15. Entity resolution (owner decisions, guesses 39–45 and the review queue)

- **15.1 Base.** Reuse the IRIS resolver and read from v2 bronze:
  - `nway_resolution.py`: N-way union-find over all provider pairs, a same-source conflict guard and a deterministic canonical choice.
  - `entity_resolution.py`: three player layers. Layer 1 is name + date of birth + jersey + team (90 %). Layer 2 is name + date of birth with day/month swap detection (80 %). Layer 3 is name + position (75 %).
  - `team_resolution.py`: rapidfuzz, extended from StatsBomb ↔ Wyscout to every provider pair.
- **15.2 Entities.** Players, teams, competitions and matches. Match resolution finds the same real match across providers, for example GS World Cup 2022 and StatsBomb World Cup 2022.
- **15.3 Every run.** Resolution runs again on every ingestion run, because a new match can change clusters. Every link stores confidence, layer and method.
- **15.4 Quality gate** (owner decisions, §29.3).
  - **Truth set.** Hand-checked pairs from real overlaps. Known overlap: 59 IDSSE players match StatsBomb Bundesliga names over all 7 matches (IRIS gap-fix spec). SkillCorner (A-League) has no overlap with open sources.
  - **Storage** (author's call): the repository holds the truth set as ids only: `(provider_a, native_id_a, provider_b, native_id_b, label, labelled_by, labelled_at, evidence_ref)`. It holds no names, so the §23.4 licence rule is met. The quality test runs in the live tier and reads names from a live lake.
  - **Labeller:** the owner labels the truth set. It is a one-time job, not a task for every user. Author's estimate: about 200 player pairs, plus the team, competition and match pairs of the scopes below (about 50 teams, 10 competitions and 70 matches). These are estimates, not measurements.
  - **Investigation before labelling** (owner decision): when the truth set is due, the owner asks a Claude session to investigate every candidate pair as deeply as possible and to recommend a label with its evidence. The owner then confirms or rejects each recommendation in the review queue (§15.5). Rules for the investigation:
    - It may go further than the implemented resolver: for example dates of birth, nationality, club history, shirt numbers across matches and public reference sources.
    - It must be independent of the resolver. It must not take the resolver's own score or decision as evidence, or the gate would measure the resolver against itself.
    - Every recommendation records its evidence (`evidence_ref`). The label counts only after the owner confirms it, and `labelled_by` is the owner.
    - Any web research for it needs the owner's go-ahead in that session.
  - **Coverage per entity type.** Bands, margins and targets are needed for every entity that is resolved (§15.2), not only for players. Each entity type has its own truth-set part, with candidates across every layer and score band, and at least one scope that is labelled exhaustively (every member of that scope on both sides), so recall is measurable and not only precision. Author's call for the scopes:

    | Entity | Exhaustive scope |
    |---|---|
    | players | the 7 IDSSE match rosters against StatsBomb 1. Bundesliga 2023/24 |
    | teams | the teams of StatsBomb WC 2018 against Wyscout WC 2018, and the IDSSE clubs against StatsBomb 1. Bundesliga 2023/24 |
    | competitions | every competition of the default modules, across providers |
    | matches | StatsBomb WC 2018 against Wyscout WC 2018 (64 matches) |

    GS against StatsBomb WC 2022 is added when the owner switches GS on in the staging install.
  - **No measured margin, no automatic link.** Until an entity type has its truth-set part, its band limits and margin are not measured. Then every candidate of that type goes to the review queue, and "Accept recommended" (§15.6) accepts none of them. The Ops pages say why.
  - **When:** the plan contains a step where the builder stops and asks the owner to start the investigation and the labelling. The targets below become a failing gate once the labels exist.
  - **Targets** (owner decision): precision of at least 0.99 for links in the automatic band, and recall of at least 0.95 for all links after queue review. These targets do not depend on the measured baseline. They apply to each entity type separately.
  - **No-regression rule:** a separate test also fails if precision or recall falls below the last recorded value.
  - **splink trigger:** if the IRIS resolver cannot meet the targets after band tuning, the builder evaluates splink 5.0.0 against the same truth set and brings the result to the owner.
- **15.5 Review queue (owner decision).**
  - Three bands. A high score links automatically, a low score is rejected, and the middle goes to the queue. The band limits come from the truth set measurement, not from fixed numbers.
  - A link in the queue does not merge. The records stay separate until a person confirms.
  - The reviewer sees both records side by side (name, birth date, team, jersey, position, matches), the score with a plain-words meaning, and the actions confirm, reject and later. No raw ids.
  - Decisions are stored as data: "must link" or "cannot link", with who, when and why. Every later run applies them as hard rules. Decisions are config-side objects written by the operator side (§8.3).
  - Decisions export to a file and import from a file. Hand-checked decisions also grow the truth set.
- **15.6 Accept recommended (owner decision).** Demo users must be able to take the most likely links without reviewing each one.
  - The review queue has an "Accept recommended" action. It confirms the most likely candidate for every queued link, or for a filtered subset.
  - The install setting `auto_accept_recommended` does the same on every run. It is on by default on laptops and off by default on the HF install and the Mac Mini staging install.
  - "Recommended" means the top candidate, and only when it beats the runner-up by a clear margin. The margin comes from the truth-set measurement for that entity type (§15.4). Ties and close calls stay in the queue, even with the setting on.
  - Cannot-link decisions and the same-provider conflict guard still apply.
  - Each link records how it was accepted: hand-checked, or accepted as recommended with a batch id. One action undoes a whole batch.
  - The Ops pages show "N links accepted without review".
  - Links accepted as recommended never enter the truth set.
  - **Writers and flow** (single writer per object family, §8.3):
    - The setting `auto_accept_recommended` is config. The operator side writes it.
    - The "Accept recommended" button and the "Undo batch" button write **request objects** on the config side, under the config lock. They do not touch the catalog.
    - The run (the lease holder) is the only writer of link rows. During resolution it applies the setting, every pending accept request and every pending undo request. It writes the resulting link rows with their batch id, and it records each applied request id in a catalog table. It never writes the request objects.
    - A request with no applied record in the published catalog shows on the Ops page as "pending until the next run". An operator can start that run with "Run now".
  - **Undo holds the pairs.** An undo request also puts every pair of the undone batch on hold. A held pair stays in the queue until a person confirms or rejects it. Neither the setting nor a later accept request accepts it again. Without the hold, the next run would accept the same top candidates again, because nothing changed their scores.
  - **Order of application** in each run: (1) hand-checked decisions (must link, cannot link); (2) undo requests, which revert their batches and set the holds; (3) accept requests; (4) the `auto_accept_recommended` setting. Steps 3 and 4 skip held pairs and pairs without a measured margin.

## 16. Cross-provider tracking view (guess 44)

A thin dbt view over the per-provider tracking tables, joined to the resolved player and match keys. It uses the shared core columns (§11.5). It stores no data.

## 17. Incremental runs

### 17.1 Run model (owner decision)

- Ingestion never runs inside the web app.
- Local: a CLI command. HF: an HF Job, started by the schedule or by the "Run now" button. The Ops page shows progress and results.
- The HF Job runs the same CLI as a laptop.
- "Run now" on HF triggers the owner-created HF scheduled job at once, through the HF API with the Space token of §18. It does not create a new job, so the Space needs no secret values. "Run now" on a local install starts a detached CLI process.
- Author's call: a run writes a small progress object at each step, outside the catalog. The Ops page reads it for live progress, because the catalog changes only at catalog commits.

### 17.2 Steps of one run

1. Take the lease (§10.6). Read the saved config.
2. For each active module: list the source and read change signals. For each inactive pining module that holds ingested matches: list it for visibility only (§9, §29.14).
3. Apply the filter. Compare with lineage to classify each match as new, changed, visibility changed or unchanged.
4. For new and changed matches: download, store in the raw zone (when it is on), parse, write bronze and register the files.
5. Apply visibility changes as row updates.
6. Run entity resolution with the stored review decisions.
7. Run dbt (silver and gold), with the hand-off of §10.3.
8. Expire, clean up and make the final catalog commit (§10.3 steps 6–8).

Author's call: a catalog commit after each match's bronze write (§10.3 step 5), so a crashed run loses at most one match of work. Resolution and dbt run once at the end, followed by the final catalog commit. The next run sees committed matches as unchanged and continues.

- **Amended (T3, owner-approved 2026-10-31; `docs/specs/2026-10-31-v2-t3-catalog-checkpoint-design.md` D1):** "a catalog commit after each match" becomes "a catalog publish by the checkpoint policy": a deferrable write publishes when `checkpoint.minutes` (T) have passed or `checkpoint.matches` (M) units are pending, and a barrier (a destructive storage action or a privacy change depends on it) publishes at once. Resolution and dbt still run once per run. A crashed run loses at most one checkpoint interval of work, which the next run redoes. Readers see progress per publish, not per match.

### 17.3 Change detection per source

The `change_signal` of §7.1 decides whether to download. The `content_sha256` of §13.1 decides whether to re-parse. For GS public, the day-level date is not enough, so the adapter always compares `content_sha256` with lineage after download. For pining, `updatedSince` is a polling hint only. The change signal is the per-artifact `artifact_meta.sha256` (§7.2): the adapter downloads an artifact only when that hash differs from the `delivery_sha256` in lineage (§13.1). Both hash the bytes as stored and delivered, so after a download the adapter checks that the SHA-256 of the received bytes equals `artifact_meta.sha256`, and fails the match loudly if it does not. The hash is taken on the raw byte stream, with any transparent `Content-Encoding` decoding of the HTTP client switched off (§7.2 already distrusts these headers). If an entry has no `artifact_meta`, the adapter downloads and compares `content_sha256`, as for GS. The re-parse decision still uses `content_sha256`.

### 17.4 Identity of runs (guess 35)

Every run, module run and match step has a stable id. Run history uses these ids, and a later trace can carry them as attributes. Run history records the trigger: schedule, button or CLI (guess 34).

### 17.5 Missed runs (guess 33)

A missed run is not replayed. At most one run starts on wake. The next run picks up all changes because the run is incremental.

## 18. Scheduling (owner decisions, guesses 27–34 and the recommendations)

- One schedule for all active modules, with no per-module interval.
- Off by default on every install.
- **HF:** one HF scheduled job runs ingestion. HF supports `@hourly` … `@annually` or a cron expression, and `--no-concurrency` (checked in `hf jobs scheduled run --help` on 2026-10-26). The job does not mount the bucket. It uses the HF storage implementation of §10.5.
  - **Setup (changes guess 29; confirmed by the owner, §29.5).** The owner creates the scheduled job once from the CLI (`hf jobs scheduled run ... --suspend`), passing the S3 gateway key and the pining owner token as job secrets. Reason: creating a job sends the secret values, and the public Space must never hold the gateway key (§10.5.1, §20).
  - **Ops page.** Saving the schedule changes, pauses or resumes that job (`reschedule`, `suspend`, `resume`). "Run now" triggers it (`trigger`). None of these needs a secret value. Whether `trigger` works on a paused job, and whether `reschedule` keeps the job's secrets, is verified in §26 item 15.
- **HF tokens:** the Space keeps a fine-grained token as a Space secret, with permission to manage jobs and to read and write the **lake** bucket only. It has no access to the **restricted** bucket (§9, §29.6). HF bucket write permission is probably per bucket, not per object prefix (not verified, §26 item 16). So the rule that the Space writes only config-side objects (§8.3) is **enforced by code**, through the storage interface, and not by the token scope. If HF offers prefix scoping, the builder uses it as a second guard. The Space holds no gateway key.
- **Pining token in the Space** (owner decision, §29.6): the Space holds a pining **list-only** token. It lists private entries for the preview (guess 17) but cannot fetch any artifact. The tier is a separate pining project (§7.2, §29.8). Until it exists, the Space holds only the public token, with the visible marker of §7.2. The owner token stays a job secret.
- **The HF Job** holds an HF token that can read and write both buckets, the S3 gateway key for both buckets, and the pining owner token, all as job secrets.
- **Local:** v2 registers native OS tasks itself with `schedule install`, `schedule uninstall` and `schedule status`. Windows uses Task Scheduler, macOS uses launchd and Linux uses a systemd timer, or cron where systemd is missing. Install is idempotent and needs no admin rights by default.
- **Always-on machines:** a system-level mode (LaunchDaemon, systemd system timer, Task Scheduler "run whether user is logged on or not") runs without a login. It needs admin rights: the owner runs the one install command with `sudo`, and the session never handles a password. The task always runs as the operator user, never as root (§20), and installing it never touches the lake. The Mac Mini staging install uses it (owner decision §29.11).
- The Ops page shows whether the schedule is on, the next run time and the last result. Locally, a button calls the same install.
- Every OS backend is tested on its own OS (§23.4).

## 19. Ops UI

### 19.1 Shell

New pages on the existing Taipy shell (owner decision). No v1 page carries over (owner decision). The AI/ML Workflows page comes later, and nothing in the shell may block it.

The shell in `hf_taipy_app/` is v1 code: its image is `FROM python:3.10-slim` and installs the v1 wheel (`hf_taipy_app/Dockerfile:1-9`). So v2 **copies** the shell patterns it needs (page template, styles, layout helpers) into `v2/src/lakehouse/ui/` and leaves `hf_taipy_app/` unchanged. The v2 HF Space has its own image (Dockerfile, Python 3.12) and its own install path, both under `v2/`.

### 19.2 Sign-in (owner decisions, guesses 17 and 18)

- Every Ops page requires a signed-in operator.
- **HF:** the Space is public and shows only the Ops pages. Sign-in uses HF OAuth (Space metadata `hf_oauth: true`) plus an allow-list of HF user names. The allow-list is a Space variable (author's call).
- **Laptop and Mac Mini:** a local operator account, created on first run. Author's call: one operator account, the password hashed with Argon2id.
- Sign-in lives in the web server layer, not in Taipy (Taipy guard 2). v1 has no user sign-in, only a bearer-token admin API (`hf_taipy_app/src/admin_api.py`), so this is new work.

### 19.3 Pages (guesses 22–26 approved)

Cycle 1 ships these pages render-only (Markdown over the services, the CLI is the operator-action path). **Every** functional action below — the active switch, "Run now", "Preview", "Save", the schedule save/pause/resume, and the Review-queue accept/undo/decide — is a cycle-2 deliverable (§29.17; cycle-2 owner decision B19 = (a)).

| Page | Content |
|---|---|
| Modules | All nine modules: active switch, filter summary, match count, last run, token status for pining (present or missing, never the value). "Run now" starts a background run and does not block the page. |
| Filter editor | The clauses of one module. "Preview" reads the live source and lists its tournaments with match counts and a clear mark on the selected ones. It never ingests. Where a count is not cheap, the page says "count not available" and why. Draft and saved filters side by side, with what a save would change ("+34 matches would be added · 0 would stop updating · 0 would resume updating"; a narrowed filter keeps the data, §8.1). For StatsBomb: the checkbox "Only matches with 360 freeze frames", and each tournament shows its exact 360 share, for example "64 matches · 64 with 360 (100 %)" (§29.13). |
| Runs | Run history and progress: matches added, changed, failed and skipped. Every error, including unmapped fields, with the match name. |
| Review queue | §15.5 and §15.6, including "Accept recommended", its undo, and the count of links accepted without review. |
| Schedule | On or off, the cron expression or a preset, the next run time and the last result. On a local install, install status of the OS task. |
| Settings | Storage root, raw archive switch, resource caps. |

### 19.4 Preview mock (from the brainstorm)

```
FILTER · StatsBomb + 360                [Preview live source]  [Save]  (unsaved draft)
 Clauses (OR):  1) La Liga — seasons: 2015/16, 2020/21    2) Bundesliga — all seasons (live)
 + add competition   + add single match
 Preview (fetched 14:02 from source):
   ☑ La Liga         2015/16  380 matches   2020/21  35 matches   ☐ 2016/17 …
   ☑ Bundesliga      2023/24   34 matches   (new seasons included automatically)
   ☐ Champions Lg    …
 Draft vs saved:  +34 matches would be added · 0 removed
```

The numbers in the mock are examples only.

### 19.5 UI rules (from `AGENTS.md`, must)

- Raw ids never reach the user. Join to names.
- Every value can be read without outside knowledge. Add help text where needed, for example for a resolution score.
- No silent substitution. Every fallback or missing value has a visible marker.
- Navigation labels name the goal, not the implementation.
- Every query that returns user-facing rows has a LIMIT.
- First page load within 3 s, cached interaction within 500 ms.

### 19.6 Taipy guards (owner decision)

taipy 4.1.1 is the latest stable release (2026-02-16, requires Python < 3.13). The last default-branch commit was on 2026-05-07, and the last pre-release was 4.2.0.dev9 on 2026-04-30. The repository is not archived. Guards:

1. Thin pages over the typed services layer that the CLI also uses.
2. Sign-in in the web server layer, not in Taipy.
3. Exact pin `taipy==4.1.1`, with `pip-audit` in CI. Carry over the v1 fix of ADR-083: `[tool.uv] override-dependencies = ["flask-socketio>=5.5.1"]` lifts the taipy-gui `flask-socketio<5.5` cap (the Flask 3.1.3 `_session` fix). A plain install of the cap fails with `ResolutionImpossible` or gets the incompatible version. The Space image installs the uv-locked set with `--no-deps`, as v1 does. The builder must re-check whether the override is still needed for the pinned versions, and must test the install path (lockfile and image) on all three operating systems.
4. Taipy goes into the quarterly external-research review. Exit trigger: a known security flaw in a Taipy dependency that Taipy does not fix, or a forced move off Python 3.12 with no Taipy support.

## 20. Security (must)

- No secrets in code, config files, logs or the Ops pages. Tokens come from environment variables or platform secrets.
- HTTPS only. Reject `http://` at the function level. `verify=True` on every request.
- Every HTTP call has explicit `(connect, read)` timeouts, default `(10, 30)`. Retry 429 and 5xx with exponential backoff, at most 3 retries. Pining 401 during token rotation follows §7.2.
- Validate every user-supplied identifier with a regex before it reaches SQL or a path. Use parameterised SQL.
- No `eval()`, `exec()`, `pickle.loads()` or `subprocess` with `shell=True`.
- Downloaded files are untrusted. Author's call: parse XML with `defusedxml`, set size limits on downloads and decompression, and refuse paths that leave the target folder.
- Sessions: `HttpOnly` cookies, `Secure` over HTTPS, `SameSite=Lax`, CSRF protection on every state-changing request.
- The HF Space token has the smallest permission set that works (§18).
- The S3 gateway key lives only in the HF Job's secrets. The public Space never holds it (§10.5.1). An unscoped key is a stop-and-report result (§26 item 15).
- The public Space can read neither the restricted bucket nor any pining artifact. It holds a pining list-only token, or only the public token until that tier exists (§7.2, §29.8), and an HF token without access to the restricted bucket (§18, §29.6). The services layer never queries a bronze table of a private match from the Space. It reads dimensions, link tables, ops tables and catalog statistics only.

## 21. Observability seam (owner decision: option b)

- Cycle 1 code calls the **OpenTelemetry API** (`opentelemetry-api`) directly for spans and metrics. Without an SDK it does nothing. The observability cycle adds only the SDK, the Collector and their config.
- All telemetry names and attributes are stable once chosen. The builder lists them in one module and in an ADR.
- Logs are JSON lines on stdout, with source name, row counts and timing. No `print`.
- Run history stays the data source for the Ops page. OTel adds an export later and does not replace it (guess 37).
- Each run keeps its dbt `run_results.json`, so the observability cycle can parse old runs (guess 38).
- Reference: `ROADMAP.md`, section "Observability Layer (OpenTelemetry)", and `TODO.md` item D7.

## 22. Installs and environments

### 22.1 Laptop install

- Default config (§8.2), an empty lake, nothing preloaded. Raw archive off by default.
- Works on Windows, macOS and Linux with Python 3.12.

### 22.2 HF install (owner decision: the owner's own install)

- A public Space with the Ops pages only. Two private HF Storage Buckets hold the lake: the lake bucket and the restricted bucket (§9, §29.6). HF Jobs is the only production writer (approved recommendation).
- HF Jobs pricing on 2026-10-26: `cpu-basic` (2 vCPU, 16 GB) at $0.01 per hour, `cpu-upgrade` (8 vCPU, 32 GB) at $0.03 per hour. The builder measures the run time and picks the flavour.
- The owner names the Space, bucket and job before deploy. Every outward action (creating the Space, bucket or scheduled job, or deploying) needs the owner's explicit yes at that time.

### 22.3 Mac Mini staging install (approved recommendation)

- Mac Mini M4 Pro, 24 GB RAM, 512 GB SSD plus a 1 TB dock SSD over USB, the APFS volume `Development` (`/Volumes/Development`). Everything of v2 on the Mac Mini runs on `Development`: the code, the virtual environment, the caches and the staging lake (owner instruction, 2026-10-26). The one exception is the system task's definition file in `/Library/LaunchDaemons/`, which macOS requires there (owner decision §29.11). It is the staging and test machine with its own lake. It runs full nightly ingestion of all public sources and benchmarks at production scale.
- History (2026-10-26): the dock SSD was the exFAT volume `Data`. exFAT has no journal, no hard links and no symlinks, and macOS writes `._` sidecar files on it. **Owner decisions:** the owner approved wiping it, and it was erased as one APFS volume named `Development` (GUID partition map). `AutomountDisksWithoutUserLogin` is set to true, so the volume mounts after a restart without a login. SSH from the AI-PC is key-only. A restart on 2026-10-26 showed the volume mounted about 10 s after boot, 1.4 s after the auto-login. The owner declined a separate proof of the no-login case; the missing-root rule (§10.5) makes a run fail loudly if the volume is ever absent.
- It uses the system-level schedule mode (§18).
- Before first use, the builder checks: whether a user session starts after a restart, whether `Development` mounts after a restart (done on 2026-10-26; the owner declined a separate proof of the no-login case, see above), the macOS version (26.2 on 2026-10-26), the file system (APFS) and free space.
- Which machine writes a lake is config only: the holder of the lease and the token. A later switch needs no code change.

## 23. Repository layout and conventions

### 23.1 Layout (owner decision)

```
v2/
  AGENTS.md            v2 class-1 rules (nested, auto-loaded in v2/)
  pyproject.toml       package "lakehouse", Python 3.12, own lockfile
  uv.lock
  src/lakehouse/       the package
  dbt/                 the v2 dbt project
  tests/
  docs/specs/          this document
  docs/adrs/           v2 ADR series from ADR-001
  docs/plans/
```

- v2 imports nothing from v1 `src/`. Code copied from v1 or IRIS is copied into v2 and audited (§24).
- Author's call: v2 is not a uv workspace member of the root project.
- v2 has its own ruff, pyright and pytest configs in `v2/pyproject.toml`. No root edit is planned. The v1 CI already scopes ruff and pyright by path (`python-ci.yml:84,87,92`), the root pytest `testpaths` (`pyproject.toml:374`) do not include `v2/`, and ruff uses the nearest config file for each file. If the builder finds that a root edit is still needed, it is the only allowed change to a v1 file, and the plan must declare it.
- The root pre-commit hooks run on every staged file, so they also check `v2/`. Examples are `check-added-large-files` and `detect-secrets` (`.pre-commit-config.yaml:29,43`). v2 must pass them without a root edit: fixtures stay small, and the documented public pining token carries a `detect-secrets` allowlist comment where it appears.
- v1 must stay green. The builder must run v1's full check suite after adding `v2/`. Some v1 gates scan `src/` and `scripts/` with `rglob` (for example `test_no_new_private_sk_imports.py:74`), and the full suite proves that nothing else reaches `v2/`.

### 23.2 `v2/AGENTS.md`

v2's own class-1 rules: Python 3.12, DuckDB and DuckLake, the storage write protocol, the key formula, completeness, the security rules and the commit rules. v1's Databricks, Lakebase and Spark rules do not apply in `v2/`. Follow the pattern of `hf_taipy_app/AGENTS.md`.

### 23.3 ADRs for cycle 1

At least: ADR-001 storage engine (DuckLake + own Parquet files), ADR-002 tuned layout, ADR-003 Kimball keys carried over (v1 ADR-011, ADR-018), ADR-004 canonical pointer rule, ADR-005 raw archive, ADR-006 visibility and channel precedence, ADR-007 single-writer lease and catalog safety, ADR-008 Taipy with guards, ADR-009 OTel API seam, ADR-010 resolution quality gate and review queue. Nygard format, as in v1.

### 23.4 Quality bar (must)

- ruff with the v1 rule set (E, W, F, I, N, UP, B, S, BLE, RUF), line length 120, `ruff format`.
- pyright. Author's call: `strict` mode for `src/lakehouse`, with documented exceptions for untyped third-party libraries.
- `import-linter` contracts for the directions in §6.
- pytest: unit tests, contract tests (field inventory, format contract, key parity, channel equivalence), an end-to-end test on a small local lake, and every named test of §23.6.
- pytest-benchmark on the hot paths (parse and write of one tracking match) with production-scale data: one full IDSSE match (3.2 M player rows) and one full SkillCorner match.
- `pip-audit` with no unexplained ignores.
- CI: a GitHub Actions workflow for `v2/` with a matrix of `windows-latest`, `macos-latest` and `ubuntu-latest` on Python 3.12. It runs only after the owner approves a push.
- Local proof on three operating systems before the cycle closes: Windows (AI-PC, light jobs only), macOS (Mac Mini) and Linux (WSL on the Media-PC or the DGX Spark).
- No Real Madrid or other private data in the repository.
- Author's call on fixtures: the repository holds synthetic fixtures that mirror each source's real field set. Small slices of real data are allowed only from sources with a clear open licence (MIT, CC BY), with attribution. StatsBomb (`NOASSERTION`) and GS (no written licence) get synthetic fixtures only.
- Author's call: an opt-in "live" test tier downloads real samples and runs the field-inventory and channel-equivalence tests against them. It runs before the cycle closes and on the Mac Mini staging install.
- Run the production-scale benchmarks on the Mac Mini, not on the AI-PC (C7).

### 23.5 Commits and reviews (owner rules)

One git commit for the whole cycle, with green checks, and only after the owner's explicit yes for that commit. No push without explicit approval. The spec, the plan and the implementation each get independent A/B reviews.

### 23.6 Named tests for boundary behaviour (must)

Each rule below has its own named test with the stated assertion. The plan may split a test into several, but must not drop an assertion.

| Rule (§) | Test asserts |
|---|---|
| Precedence is order-independent (§9) | For every order (public then pining, pining then public) and for an update of either copy, the kept copy, the visibility and the lineage reason are the same. |
| Visibility comes from the source flag (§4, §9) | With the owner token, a public-flagged pining entry stays public. With the public token, nothing private appears. No code path reads the token to set visibility. |
| Kept-copy visibility (§9, §29.2) | A public pining entry with extra fields gives a public match. A private pining entry with extra fields gives a private match. |
| Raw-file visibility (§9, §13.2) | A private delivery's raw file stays private when the public copy wins. When a match becomes private, the lake-root raw files of its pining deliveries move to the restricted root, except a raw object whose content a public-channel delivery also has; that object stays in the lake root, and no run-start delete removes it (§29.12). |
| Same content, same hash (§13.1) | One file delivered by two channels, with and without a gzip wrapper, gets one `content_sha256` and one raw-zone object. |
| Channel equivalence (§7.3, §12.5) | Each channel pair gives the same native ids, the same `content_sha256` where the files are equal, and identical canonical bronze. |
| No-op `updated_at` (§17.3) | A pining entry with a new `updated_at` and the same content causes no re-parse and no new bronze file. |
| Pining artifact hash (§17.3) | An entry whose `artifact_meta.sha256` equals lineage causes no download. A changed hash causes a download. Received bytes whose SHA-256 differs from `artifact_meta.sha256` fail the match loudly. An entry without `artifact_meta` falls back to download and `content_sha256`. |
| Stale save rejected (§8.3) | Two concurrent saves: one succeeds, and the other fails with a message that names the newer version. |
| Missing root fails (§10.5) | A run on a missing lake root stops with an error and creates nothing. |
| Empty filter (§8.1) | An active module with an empty filter downloads nothing, and the Ops services report a warning. |
| Crash loses at most one match (§17.2) | A run killed during match N keeps matches 1 to N-1 committed, and the next run re-does only match N. |
| Skip record (§10.6) | A run that cannot take the lease writes `runs/skipped/<run_id>.json`, does not touch the catalog, and the next run folds the record into run history. |
| Two writers (§10.6) | Two runs started together: exactly one writes, on local storage and on an HF Bucket. Two runs that take over one expired lease together: exactly one holds it. A stale holder can overwrite no published version and no `CURRENT` of the new holder. |
| Read during a run (§10.4) | The services layer reads module and run data while a run is between catalog commits, on Windows, macOS and Linux. |
| Reader across catalog commits (§10.4) | A reader that stays active during N catalog commits gets consistent results or fails loudly, locally and on HF. |
| dbt hand-off (§10.3) | dbt builds while the run is in progress, and the run continues after it. |
| Pointer move log (§14.3) | A cluster change that moves a canonical pointer writes one log row with old value, new value, run id and reason. |
| Season grain (§14.4) | StatsBomb season 281 under five competitions gives five different `season_key` values. |
| Key parity (§14.1) | Known v1 keys for every entity, including the NULL cases, equal the v2 keys. |
| Catalog restore (§10.8) | Restoring the version from before the last run takes the lease, drops the undone run's versions from the kept set and writes a restore record. The lake's files all exist, its values match that version, and the next run starts from it. The next orphan sweep removes the undone run's files and keeps every file that a kept version names. |
| IDSSE exactness (§10.2) | An IDSSE positions file with a coordinate of 3 or more decimals in its XML text fails, even when the silly-kicks output rounds it to 2. |
| Exactness (§10.2) | A text value with 3 decimals fails. A float32 value whose shortest text has 2 decimals passes. A float64 value with 3 decimals fails. |
| Accept recommended (§15.6) | Only candidates that beat the runner-up by the margin are accepted. A tie stays in the queue. A cannot-link decision blocks acceptance. Undo of a batch, applied by the next run, restores the state before it, and the run after that does not accept the held pairs again with the setting on. The order of application is the one in §15.6. An entity type without a measured margin gets no automatic link. |
| Operator save during a run batch (§8.3, §15.6) | An operator saves a review decision while a run applies an accept batch. Both survive. The run writes no config-side object, and the Ops side writes no catalog data. |
| Native writes on HF (§10.5.1) | On a real HF Job, dbt builds one contract-enforced table, the run inserts one run-history row, and the Job registers one tracking file under its `hf://` URI. A Space reader with only its scoped HF token then reads all three by value. |
| Cleanup guard (§10.3 step 7) | A file that a kept version names is never deleted: a DuckLake dry-run list that contains one stops the cleanup loudly. The orphan sweep deletes an unlisted object under `data/` that no kept manifest names, and never touches `raw/`, `catalog/`, `config/` or `runs/`. |
| Linear multi-run cleanup (§10.3, §10.7, §10.8) | A match is replaced, then runs continue across more than 8 days. Every run's cleanup succeeds without a guard stop, and the replaced file is deleted once it leaves both retentions. The orphan sweep never deletes a file that DuckLake still tracks, so DuckLake's own delete of the replaced file never meets "not found" (§29.11). |
| Sweep with all path forms (§10.3.1) | On a local install and on HF, with DuckLake-written relative files, `hf://`-registered files and an `s3://` data path: the sweep deletes no file that the `CURRENT` manifest names. A deliberately broken key mapping makes the sweep abort before any delete. A planned deletion above 10 % aborts without the explicit flag. |
| Canonical keys (§10.3.1) | Every mapping rule gives the expected key for Windows, macOS and Linux paths, `hf://` and `s3://` URIs, and relative native paths. A reference outside the lake root fails. |
| Restore across a relocation (§10.8) | Restoring a version from before a `relocate` gives a lake whose every table reads by value under the current root. |
| Restricted root placement (§9, §10.5) | Every bronze file of a private match and every raw file of a private delivery is in the restricted root, and none is in the lake root. Public ones are in the lake root. No bronze table has a DuckLake-written file. A mixed-visibility listing (a pining players list with public and private records) gives one lake-root file with only the public records and one restricted-root file with only the private records. No lake-root DuckLake-written file contains a row of a private match from a row-level table. |
| Row-level models are views (§9) | Reading the dbt manifest: every model over row-level bronze is materialised as a view; only dimension, link, lineage and ops models are tables. Adding a row-level table model makes the test fail. |
| Visibility change moves files (§9, §29.7) | A match that changes from private to public, and back, ends with its files in the right root and readable by value. After a public-to-private move, the lake-root copy is gone right after the move's catalog commit, and a move record exists. The next cleanup succeeds without a guard stop. A restore to a pre-move version gives a lake in which that match is private, with its files only in the restricted root, readable by value. Two successive restores to ever older pre-move versions both give that result, and the restored copies carry every move record. A crash between the move's commit and the delete leaves a copy that the next run deletes at its start. A match that goes public, then private, then public again with unchanged content keeps every file that `CURRENT` names through the next run start, cleanup and sweep, and a restore across both moves gives it its latest visibility (§29.10). A match that changes content and becomes private in the same run has no lake-root object right after that run's commit, and a restore to the version before that run gives a private match whose old content reads by value from the restricted root. A public-to-private old key that is still registered stops the run before any delete. A restore to the version before a public → private → public sequence names no deleted file, and the match reads by value from the lake root. A restored copy that would name a missing file is not published. An upload of a crashed run under the match's file-name prefix is deleted by the private move (§29.12). A match whose content is replaced in one run and that becomes private in a later run has no lake-root object right after the move's commit, including the older content file that a kept version still names; a restore to the version before the replace gives a private match whose older content reads by value from the restricted root only. A private match that becomes public and changes content in one run, followed by a sweep, can still be restored to the version before that run, and every file the restored version names exists and reads by value (§29.11). About 8 days later, cleanup accepts the already-deleted moved key and fails on any other missing key. A move record is not pruned before DuckLake's cleanup has removed its old key, even when no run happened for more than 8 days. |
| Crashed-run version cap (§10.8) | After a crash and no completed run for more than 8 days, the next run's cleanup succeeds without a guard stop. |
| Space without restricted access (§18, §20) | On HF, the Space's token cannot read a restricted-bucket object. Every Ops page still works, and no services-layer query touches a private match's bronze table. |
| Lake relocation (§10.5.1) | After `relocate` moves a lake to a new root, every table reads all its rows by value (not only `count(*)`, which catalog statistics can answer without a file read). |
| Truth-set purity (§15.4, §15.6) | A link accepted as recommended never appears in the truth set. A truth-set row without an owner `labelled_by` fails validation. |
| StatsBomb 360 option (§8.1, §29.13) | The preview's 360 share per tournament equals the count from the fixture's match list and `three-sixty/` files, rounded to a whole percent; a list that cannot be fetched shows "360 share not available" and its reason, never a number. With the option on, no file of a match without 360 data is downloaded. A match whose `match_status_360` disagrees with the `three-sixty/` listing shows a visible warning. A non-StatsBomb module refuses the option. Narrowing a filter keeps the excluded matches' data and stops their updates. An excluded match that turns private, and an ingested match of an inactive pining module that turns private, each get the private move with no download, and no lake-root object of the match remains (§29.14). |
| Pining list fields (§29.1) | For every ingested pining match, the list's competition and season equal the metadata artifact. An entry without them shows in the "competition not given by source" group. |

## 24. Reuse map

| From | What | How |
|---|---|---|
| IRIS handover `src/iris_lake/providers/` | `pining.py`, `statsbomb.py`, `wyscout.py` | copy and adapt to the channel adapter interface |
| IRIS handover `src/iris_lake/ingest/` | `skillcorner.py`, `idsse.py`, `statsbomb_360.py` | copy, then audit each against the raw payload (§12.6) |
| IRIS handover `src/iris_lake/analytics/` | `nway_resolution.py`, `entity_resolution.py`, `team_resolution.py` | copy; extend team resolution to all pairs |
| IRIS handover `dbt_project/` | staging, intermediate and mart models | re-dialect to DuckDB; `int_tracking_frames.sql` is the union pattern |
| IRIS handover `src/iris_lake/shared/access_tier.py`, `src/iris_lake/registry.py`, `src/iris_lake/pipeline.py` | visibility and registry patterns | reference |
| IRIS handover `storage_lab.py` | the two-method byte measurement | reference for benchmarks |
| v1 `src/shared/identifiers.py` + `src/tests/test_format_contract.py` | native id generators and contract tests | copy; add season ids and channel mappings |
| v1 `dbt_project/macros/generate_*_key.sql`, ADR-011, ADR-018 | the key formula | reproduce in Python (§14.1) |
| v1 `dbt_project/models/marts/dim_*.sql`, `int_player_xref.sql`, `int_team_xref.sql` | dimension and canonical pointer logic | port to DuckDB |
| v1 `src/ingestion/` | `wyscout.py` (figshare URLs), `idsse.py` (coordinate notes), `gradientsports_*.py`, `skillcorner_*.py`, `statsbomb*.py` | reference for source quirks |
| v1 `hf_taipy_app/` | page template, styles, `admin_api.py` blueprint pattern, the ADR-083 dependency override | copy into `v2/src/lakehouse/ui/`; new pages only; `hf_taipy_app/` stays unchanged (§19.1) |

The IRIS handover is at `C:\Users\Karsten\AppData\Local\Temp\claude\D--Development-karstenskyt--luxury-lakehouse-skunk\7c2a8737-2bc5-4d79-9198-78a3ac16b9d6\scratchpad\handover\iris-lakehouse-handover\`. That is a temporary folder. The builder must copy what it needs into `v2/` early in the cycle, and must ask the owner for the zip again if the folder is gone.

## 25. Evidence (storage spike, 2026-10-26)

Environment: Python 3.12.14, duckdb 2.0.0, pyarrow 26.0.0, deltalake 1.6.6, pyiceberg 0.12.0, pylance 13.0.0, dbt-core 1.12.6 + dbt-duckdb 1.11.0, silly-kicks 4.139.1, on the AI-PC with 4 threads and a 3 GB DuckDB limit. Data: SkillCorner 1899585 (894,982 player rows, 60,530 frames) and 1925299 (1,054,724 rows); IDSSE J03WPY (3,216,642 player rows, 146,211 frames). Two measurement methods (sum of column-chunk compressed sizes, and file size on disk) agreed within 0.66 % for every Parquet variant.

### 25.1 Bytes per match

| Layout | SkillCorner MB | IDSSE MB |
|---|---:|---:|
| IRIS COLUMNAR bronze (handover §9) | 24.8 | 557.8 |
| Parquet as is, snappy | 3.99 | 77.79 |
| Parquet as is, zstd 3 | 3.58 | 50.03 |
| **v2 layout** (§10.2) | **1.89** (players 1.29 + frames 0.60) | **9.07** (players 8.04 + frames 1.03) |

Projection for 1,000 matches: SkillCorner about 1.9 GB, IDSSE about 9.1 GB. The v2 SkillCorner layout includes the fields that IRIS dropped.

What moved the bytes: player-major sort without delta encoding grew `frame` from 0.17 MB to 1.64 MB. Integer centimetres with delta encoding cut SkillCorner x from 1.53 MB to 0.64 MB and IDSSE frame from 7.09 MB to near 0.

### 25.2 Engines (SkillCorner players table, same sorted integer-centimetre data)

| Engine | MB per match | 2 matches after replace + maintenance | Frame-window read |
|---|---:|---:|---:|
| pyarrow tuned Parquet, own manifest | 1.29 | 2.71 | 4.3 ms |
| **DuckLake + our tuned files** | **1.29** | **2.72** | 14.1 ms |
| DuckLake native writer (unpartitioned / partitioned) | 4.54 / 3.45 | 8.21 / 7.55 | 34 / 42 ms |
| Delta (delta-rs, zstd) | 4.59 | 9.97 | 17.4 ms |
| Iceberg (pyiceberg + SQLite catalog, zstd) | 4.63 | 14.66 (no expire run) | 18.2 ms |
| DuckDB native file | 5.51 | 16.51 (vacuum does not shrink) | 1.4 ms |
| Lance | 8.32 | 17.98 | 13.8 ms |

DuckDB's Parquet writer picks dictionary encoding for these integers. With `parquet_version v2, dictionary_size_limit 1` it gives 1.73–1.79 MB. DuckLake does not expose that option. dbt-duckdb 1.11.0 on DuckDB 2.0.0 with a DuckLake attach built a view, a test and a contract-enforced table: `PASS=3 WARN=0 ERROR=0`.

### 25.3 Raw sizes per match

| Source | Raw | zstd 3 | zstd 9 |
|---|---:|---:|---:|
| SkillCorner tracking JSONL | 81.6 MB | 7.64 MB | 5.94 MB |
| IDSSE positions XML | 354.5 MB | 54.33 MB | 36.64 MB |

Measured in the spike session on 2026-10-26 on the raw files of SkillCorner 1899585 and IDSSE J03WPY. Recorded in `SPIKE-FINDINGS.md` §5.

### 25.4 HF Storage Bucket spike (private bucket, deleted afterwards)

| Step | Result |
|---|---|
| DuckDB `COPY ... TO 'hf://buckets/...'` | refused: `Not implemented Error: HuggingFaceFileSystem: DirectoryExists is not implemented!` |
| Upload a tuned file (1.29 MB) with `HfApi` | first 18.5 s (cold Xet session), then 0.4–0.9 s |
| `ducklake_add_data_files` on 2 remote `hf://` files | 1.5 s, row count correct (1,949,706) |
| Catalog upload (4.3 MB, then 6.0 MB) | 0.6–0.7 s; Xet sent only 2.2 MB and 0.9 MB of new data |
| Fresh reader attaches the remote catalog read-only over `hf://` | works with no download (1.2–1.3 s) |
| Reader full count / frame-window query | 2.9–3.4 s first count; window median 43–79 ms |
| Replace, expire, cleanup dry run | lists exactly the orphan file; deleted with `HfApi`; then the non-dry cleanup succeeds |
| Bucket after replace | exactly 3 objects: catalog + 2 current data files (8.5 MB) |

The catalog grew from 4.3 MB to 6.0 MB after one replace. Catalog upkeep is a verification item (§26).

### 25.5 DuckLake path probe (after review round 2, local only)

`SPIKE-FINDINGS.md` §6, duckdb 2.0.0 on Windows: `ducklake_add_data_files` stores absolute paths and refuses a relative argument. DuckLake-written files are stored relative to `data_path`. A 5-row native write was inlined in the catalog with no file. A copied catalog attaches under a new root with `(read_only, data_path '<new>/', override_data_path true)`: the native table read all 200,000 rows from the new root, while the absolute-registered file failed with `IO Error: Cannot open file`. `count(*)` on that table still returned 3 from catalog statistics, so a count does not prove that files are reachable.

### 25.6 Version facts (2026-10-26)

duckdb 2.0.0 LTS (2.0.1 planned for 2026-11-18), DuckLake 1.1, taipy 4.1.1, pyarrow 26.0.0, huggingface_hub 2.3.0, kloppy 3.19.1, splink 5.0.0, rapidfuzz 3.14.6, silly-kicks 4.139.1, xxhash 4.0.2. PyPI has macOS arm64 wheels for Python 3.12 for duckdb 2.0.0 and pyarrow 26.0.0. taipy, silly-kicks, huggingface_hub, dbt-duckdb and polars install as pure Python.

## 26. Verifications the builder must do in cycle 1

Each item is part of cycle 1, not a later task. If an item fails, the builder must stop, report the evidence and let the owner decide.

1. **Bucket access from the Space and the Job.** The Space writes config-side objects through `HfApi` and reads through `hf://` with its scoped token. The Job writes data files, raw files and the catalog-control objects through the S3 gateway (item 15, §29.9), writes other small objects through `HfApi`, and reads through `hf://` (§10.5, §18). No mount is used.
2. **Single-writer lease.** A mechanism that closes the race between two writers on an HF Bucket. The chosen mechanism (§10.6, §29.9) needs the gateway to honour `If-None-Match` on single-part and multipart creates and `If-Match` on a replace, and an `hf://` read to see a gateway write at once. Prove it with the two-writer test (§23.6), including two takers of one expired lease. HF Jobs `--no-concurrency` is added as defence in depth.
   - **Result (2026-10-31, T3 Task 12 and T6):** the gateway honours `If-None-Match` and `If-Match` for sequential requests only: two processes both won one generation, 6-way creates had up to 6 winners, a 6-way `If-Match` race up to 4 successes, and `If-None-Match` on `CompleteMultipartUpload` was ignored. The fence of record moved to the T6 lease ledger (§10.6); conditional writes are single-part only; the six-taker race test (`tests/hf/test_lease_race_hf_live.py`) was red on the old fence (8 of 10 repeats) and proves the ledger.
3. **Catalog publication (§10.4).**
   - A catalog copied after `CHECKPOINT` and detach attaches as a valid DuckLake catalog with the same snapshots.
   - A reader attaches a versioned `hf://` path read-only.
   - A reader that stays active during N catalog commits gets consistent results, locally and on HF (no torn read of a replaced object).
   - The local read-during-run test passes on Windows, macOS and Linux. Reviewer B's lock probes ran on Windows only.
   - A reader re-roots DuckLake-written files with `override_data_path` on the storage it uses on HF (measured locally only, `SPIKE-FINDINGS.md` §6).
   - The cached interaction time of an Ops page on HF stays within the 500 ms budget of §19.5, with one attachment per version.
4. **Catalog at scale.** Catalog size and attach time at 1,000 matches across all tables, the effect of `CHECKPOINT` on growth after replaces, the storage used by the version retention of §10.8, and the total size of DuckLake-written (native) files and of inlined data.
   - **Closed (T3 scale test, `tests/bench/test_catalog_scale.py`, Mac Mini, 2026-10-31):** 1,000 re-keyed fixture matches of the five providers on an encrypted local lake with the production checkpoint settings. Catalog 10.8 MB at 1,000 matches, attach 0.03 s; 1,128 snapshots; native data 153 MB; no inlined data (the encrypted path sets `DATA_INLINING_ROW_LIMIT 0`). `CHECKPOINT` shrinks nothing, so the catalog rewrite of §10.3 step 5 replaces it: 16 rewrites during the ingest (14.4 to 10.8 MB, at most 150 ms each). 100 replaces grew the catalog by 1.0 MB (to 11.8 MB). The ingest made 21 publishes (bound 24) and kept at most 10 in-run versions; a burst of 100 public-to-private moves made 101 publishes and kept at most 60 versions inside the 2-minute grace (bound 67), 740 MB of version store until the next cleanup. A publish took 0.06–0.6 s while the lake grew and about 1.85 s on the 1,000-match lake, of which the bronze integrity guard took about 1.45 s; the run-end publish took 0.6–2.4 s.
5. **DuckDB extensions on macOS arm64.** `install ducklake; install httpfs` on the Mac Mini. An earlier URL check was not conclusive.
6. **DuckDB 2.0.x.** 2.0.0 is new. Author's call: move to 2.0.1 if it ships before the cycle ends, and re-run the storage tests.
7. **SkillCorner LFS bandwidth.** Measure LFS use per full download and protect against repeated downloads (the raw zone and `content_sha256`).
8. **GS Drive.** The virus-scan confirm page, download quotas, total size of the folder, and the weak change signal.
9. **HF Job flavour.** Run time and memory of a full public run on `cpu-basic`.
10. **HF OAuth with the Taipy web server.** A working sign-in flow and allow-list check on a Space.
11. **One Space replica.** Confirm that the v2 Space runs as one replica, because the config lock of §8.3 depends on it. If HF can run more than one replica, the builder must stop and bring the evidence to the owner.
12. **Coordinate precision.** The dtype of pining `format_version` 2 SkillCorner coordinates (owner-tier sample) and the decimal precision of GS coordinates. Run the §12.5 equivalence test on a real `format_version` 2 sample in the live tier.
13. **Parse memory.** Peak RSS of one IDSSE and one SkillCorner parse, measured on the Mac Mini (§10.9).
14. **Taipy install.** Taipy with Python 3.12 and the ADR-083 override on Windows, macOS and Linux, and in the v2 Space image (§19.6).
15. **Native writes and own files on HF (§10.5.1, §18).** On a real HF Job:
   - dbt builds one contract-enforced table, and the run inserts one run-history row.
   - The Job writes one tracking file through the S3 gateway and registers it under its `hf://` URI.
   - A Space reader, with only its scoped HF token, reads the dbt table, the run-history row **and the tracking table, by value** (not only `count(*)`).
   - The object written as `s3://...` through the gateway is the same object as its `hf://buckets/...` URI.
   - DuckDB writes, multipart uploads and conditional writes work through the gateway.
   - Whether a gateway key can be limited to the two v2 buckets. An unscoped key is reported to the owner before its first use.
   - `trigger` starts a paused scheduled job, and `reschedule` keeps the job's secrets.

   - The orphan sweep on HF, with all three path forms, deletes nothing that a kept manifest names (§23.6 "Sweep with all path forms").

   If design (a) or any of these points fails, the builder must stop and bring the evidence to the owner before changing to design (b).
16. **Token scopes on HF.**
   - Whether HF fine-grained tokens can limit bucket writes to an object prefix (§18).
   - Whether a scheduled job's secrets stay out of every Space-visible API response.
   - The Space's HF token can be limited to the lake bucket: a read of a restricted-bucket object with it fails. If HF cannot limit a token to one bucket, the builder must stop and bring the evidence to the owner, because §29.6 depends on it.
   - The pining list-only token lists private entries and gets 404 on every artifact.

## 27. Left to the builder

These are minor. A sensible builder can settle them without changing what gets built:

- Module, class and table names inside `src/lakehouse`, apart from the names fixed here.
- The exact config file format (YAML or JSON) and its schema tool.
- The CLI framework.
- The exact OTel span and metric names (listed once, then stable).
- Page layout details inside the rules of §19.
- The lease heartbeat interval and expiry.
- How preview results are cached.

## 28. Author's calls (for owner review)

| § | Call |
|---|---|
| 7.1 | IDSSE public source is HF `pysport/idsse-data`, not figshare. |
| 8.1 | An active module with an empty filter ingests nothing and shows a warning. |
| 8.2 | The concrete laptop default filters (round 2: StatsBomb WC 2018 added, La Liga 2020/21 removed). |
| 8.3 | Config and review decisions are versioned objects outside the catalog, with one writer per object family, an in-process lock on HF and an OS file lock locally. |
| 9 | The whole HF lake bucket is private. |
| 10.2 | One Parquet file per match per table, with the match key, a content version and a write id in the name (the write id is owner decision §29.10). |
| 10.2 | The exactness check runs on the source representation (shortest round-trip text for binary floats). |
| 10.3 | The dbt hand-off: detach the working copy, let dbt attach it, re-attach after. |
| 10.4 | Immutable catalog versions plus a `CURRENT` pointer; one cached attachment per version; `CURRENT` self-checksum on every read, size check on attach, full hash at publish and in `verify`; retried replace of `CURRENT` on Windows. |
| 10.5.1 | A `relocate` command for v2's own absolute file paths, by a pinned edit of the DuckLake 1.1 metadata tables. |
| 10.5.1 | v2's own files are written through the gateway and registered under their `hf://` URI; only the Job holds the gateway key. |
| 10.3 | Cleanup in three parts: kept set from per-version head-snapshot manifests, guarded DuckLake cleanup, orphan sweep fenced to `data/` with two fail-closed guards (the `CURRENT` manifest must be found in the listing; at most 10 % deleted without an explicit flag). |
| 10.3.1 | One canonical object key (root-relative POSIX path) for every stored file reference and every listing. |
| 10.2 | IDSSE exactness is checked on the XML attribute text, because silly-kicks rounds to 4 decimals. |
| 15.6 | Undo holds the batch's pairs; a fixed order of application. |
| 18 | The owner creates the HF scheduled job once from the CLI; the Space only changes, pauses, resumes and triggers it (changes guess 29; the owner confirmed it, §29.5). |
| 10.8 | Restore takes the lease; runs build their working copy from `CURRENT`; cleanup deletes only files that no kept version names; a restore across a `relocate` rewrites the restored copy. |
| 10.5 | On HF the Job writes data files, raw files and the catalog-control objects (lease, versions, manifests, `CURRENT`) through the S3 gateway, and other small objects through `HfApi`; every reader reads through `hf://`; no bucket mount (the gateway part is owner decision §29.9). |
| 10.6 | A lease-less run writes its skip record as an object outside the catalog. |
| 10.7 | Snapshot retention of 8 days, always longer than version retention. |
| 10.8 | Version retention: run-end versions for 7 days; in-run versions only until a run-end version supersedes them. |
| 18 | The Space's write limit to config-side objects is enforced by code, not by token scope. |
| 9, 10.5 | The restricted root's contents and boundary: all bronze files of private matches and raw files of private deliveries; dimensions and links stay in the lake root; every bronze table is written by v2's own writer; a visibility change moves files in one catalog commit; local installs also have two roots. |
| 9 | Row-level silver and gold models are views; only dimension, link, lineage and ops models are tables, enforced by a dbt manifest test. |
| 10.5 | Source-level listings: one file per listing and per visibility, with each record's own flag where the source gives one. |
| 10.8 | In-run versions are never kept longer than 7 days, except the one `CURRENT` names. |
| 7.2 | During development, until the pining list-only tier exists, the Space holds only the public token and the preview marks private pining tournaments as not listed. Cycle 1 may not close that way: the tier is a prerequisite (owner decision §29.8). |
| 9 | The crash-safe order of a move (commit with the move record, then delete); "not found" accepted only for moved keys; stale readers fail loudly and re-read `CURRENT`. |
| 10.8 | A restore carries every move record forward from the pre-restore `CURRENT`; move records are never removed by a restore, and are pruned only after DuckLake's cleanup removed their old key. |
| 9 | Every run starts by deleting lake-root objects that a public-to-private move record names, so a crash leftover lives only until the next run starts (with the guard of §29.10). |
| 11.2 | Tracking tables carry only `match_key` and `player_key` per row; audit values live once per file. |
| 11.5 | The shared tracking core columns. |
| 13.1 | One `content_sha256` over decoded bytes; the channel's change signal stays separate. |
| 14.4 | `season_key` at competition-season grain, native form `<competition>:<season>`. |
| 15.4 | The truth set is stored as ids only, with no names. |
| 15.4 | The first exhaustively labelled scope: the 7 IDSSE rosters against StatsBomb 1. Bundesliga 2023/24. |
| 12.3 | A run continues after a failed match and ends with a failed status. |
| 14.1 | One Python key generator at bronze write time; dbt never hashes. |
| 14.3 | Provider rank extended with `skillcorner` and `gradientsports` after `idsse`. |
| 17.1 | A run writes a progress object outside the catalog for live progress. |
| 15.4 | Truth-set scopes per entity type (players, teams, competitions, matches); no automatic link without a measured margin. |
| 15.6 | Accept and undo are request objects written by the operator side and applied by the run. |
| 17.2 | Catalog commit after each match; resolution and dbt once per run. |
| 19.2 | The allow-list as a Space variable; one local operator account with Argon2id. |
| 20 | `defusedxml`, download and decompression size limits, and path checks. |
| 23.1 | v2 is not a uv workspace member. |
| 23.4 | pyright `strict` for `src/lakehouse`. |
| 23.4 | Synthetic fixtures; real slices only from clearly licensed sources; an opt-in live test tier. |
| 26.5 | Move to DuckDB 2.0.1 if it ships during the cycle. |

## 29. Owner decisions from the reviews

29.1–29.3 were decided by the owner on 2026-10-26, after review round 1. 29.4 was decided after review round 2. 29.5 comes from review round 3 and was confirmed by the owner. 29.6 comes from review round 4 and was decided by the owner. 29.7 comes from review round 5 and was decided by the owner. 29.8 comes from review round 6 and was decided by the owner. 29.9 and 29.10 come from plan review round 2, 29.11 from plan review round 3, and 29.12 from plan review round 4; 29.13 comes from an owner request after plan review round 5, and 29.14 from plan review round 6. The owner decided all six.

### 29.1 Pining match lists have no competition and no season

The filter model (§8.1) and the preview (§19.3) group matches by competition and season. Pining entries carry only `id`, `date`, `home`, `away`, `provenance`, `source` and the artifact map (checked live on 2026-10-26, and in `src/canonical/models.py` of the producer repo). The match list has no competition filter: `?competition_id=` is reserved only on the **players** endpoint (`api-reference.md`, "List Players"), and `list_matches` ignores unknown parameters.

**Decided (owner, 2026-10-26):** the pining producer adds competition and season fields to `MatchEntry` and `matches.json`. The owner delegates that change to a dedicated pining session. It is a prerequisite for the pining part of the filter preview.

What v2 relies on, and what v2 does:

- **Fields** (in each provider's native vocabulary, on every entry of both tiers): `competition_id`, `competition_name`, `season_id`, `season_name`. The exact names are settled with the pining session and recorded in ADR-006.
- **Use:** filters and previews of pining modules group by these fields from one list call.
- **Bronze stays complete:** bronze still takes competition and season from each match's own metadata artifact. A contract test checks that the list fields equal the metadata values for every ingested match.
- **Missing field:** an entry without these fields appears in the preview under a visible "competition not given by source" group. It is never guessed and never silently dropped.
- **Before the change lands:** the builder must not fall back to one metadata request per match without the owner's approval. v1 reads pining competition and season from `match.json` (`dim_matches.sql:161-168`), so that path is known to work if the owner chooses it later.

### 29.2 Visibility of the kept copy (reviewer A, A-SPEC-01)

The owner's precedence rule says the pining copy is kept when it has extra fields and the match "can stay marked as private". The 7 public pining IDSSE entries are flagged `public`, so the literal text would mark public matches private. That contradicts "trust the source flag".

**Decided (owner, 2026-10-26).** The round-1 text had turned the owner's "private copy" into "pining copy". The owner confirmed the fix, which restores the original intent:

- The rule for which copy wins is unchanged.
- The match takes the flag of the copy that is kept. A private copy with extra fields keeps the match private. A public copy, from either channel, keeps it public.
- Each raw file keeps the flag of the delivery it came from.

§9, §13.2 and §23.6 are written this way.

### 29.3 Resolution quality targets and labelling (A-SPEC-10, B09)

The gate in round 1 had only the measured baseline as its bar, so it could never fail.

**Decided (owner, 2026-10-26):**

- Targets: precision of at least 0.99 in the automatic band, and recall of at least 0.95 after queue review, plus a separate no-regression rule (§15.4).
- The owner labels the truth set (about 200 pairs, author's estimate). Before that, a Claude session investigates every candidate pair, deeper than the implemented resolver, and recommends a label with evidence (§15.4).
- Demo users can accept recommended links in one step, and laptops do it by default (§15.6). Those links never enter the truth set.

### 29.4 How DuckLake-native writes reach the HF bucket (reviewers A-SPEC-21 and B17, round 2)

Both reviewers found the same blocking gap. Without a bucket mount, DuckDB has no writable path on HF for dbt tables, link tables, lineage and run history (§10.5.1).

**Decided (owner, 2026-10-26): design (a), the HF S3 gateway.** One write path for every file, dbt unchanged, a candidate for the lease's conditional write, and portable to any S3 store.

- The owner creates the gateway keys once in the HF web UI. They are stored as Space and Job secrets, never in code or config files.
- Its DuckDB compatibility is not tested yet (§26 item 15). If the verification fails, the builder stops and brings the evidence to the owner. Design (b), the local mirror of §10.5.1, is the documented fallback, but changing to it needs the owner's approval at that point.

### 29.5 Who creates the HF scheduled job (reviewer B23, round 3)

A public Space must not hold the S3 gateway key, and creating an HF job sends its secret values. So the author changed guess 29 (§18):

- The owner creates the scheduled job **once** from the CLI, with the gateway key and the pining owner token as job secrets.
- The Ops page then only changes, pauses, resumes and triggers that job ("Run now" = trigger). None of these needs a secret value.
- Before, the Ops page created the job itself.

**Confirmed by the owner on 2026-10-26.**

### 29.6 Which pining token the public Space holds (reviewers A-SPEC-33 and B29, round 4)

The filter preview of pining modules lists private tournaments for signed-in operators (guess 17). In round 3 the author gave the Space the pining owner token for that. Both reviewers found that this is the owner's decision: with the owner token, a compromised public Space could read every private artifact (for example the commercial StatsBomb 360 delivery), not only the match list. The spec keeps the gateway key out of the Space for the same reason (§10.5.1, §20). Options:

- **(i)** Accept the owner token in the Space, with that risk.
- **(ii)** The Space holds only the public token. The Job (which holds the owner token) writes a cached list of the private matches and tournaments, with counts but no artifacts, as a run-side object. The preview shows private entries from that cache with a visible "as of <time>" marker, because they are not live.
- **(iii)** Pining issues a list-only token tier: the list endpoints with private entries, but no artifact access. The Space holds that token, and the preview stays live. This needs a change in the pining producer repo.

**Decided (owner, 2026-10-26): (iii) plus a restricted-data bucket.**

- The Space holds a pining list-only token (§7.2, §18). The pining session adds that tier.
- Private data files and private raw files live in a second root, the restricted root, which on HF is a second private bucket that the Space's token cannot read (§9, §10.5). This closes the remaining gap: without it, a compromised Space could read private data that is already ingested.
- What stays readable by the Space: dimension and link tables with names and metadata of private matches, which the Ops pages and the review queue need, and the catalog's file paths, sizes, row counts and column statistics of restricted files (§9).

### 29.7 The old lake-root copy after a public-to-private move (reviewers A-SPEC-37 and B31, round 5)

When a match becomes private, its files move to the restricted root. But every kept catalog version from before the move still names the old lake-root copy, and cleanup must not delete a file that a kept version names (§10.8). So, as written, the copy stays in the Space-readable lake bucket for up to 7 days. That is a gap in the §29.6 boundary. Options:

- **(a) Recommended: delete at once, and keep restore points working.** The move deletes the lake-root copy right after its catalog commit and writes a move record (match, old key, new key, time). A restore to a pre-move version re-applies every later move record to the restored copy (new path, private flag) before it publishes it, with the same mechanism as a restore across a `relocate` (§10.8). The kept-set check skips keys that a move record marks as moved. No exposure window, and every restore point still works.
- **(b) Accept a visible window of up to 7 days.** The Ops page lists the matches whose old public copy still exists, with the date it will go.
- **(c) Delete at once, and block restores** to pre-move versions for that match.

**Decided (owner, 2026-10-26): option (a).** §9, §10.8 and §23.6 are written this way.

### 29.8 The pining list-only tier and the close of cycle 1 (reviewer B35 and A's note, round 6)

§29.6 says the Space holds a pining list-only token, so the preview of private pining tournaments stays live. The pining draft spec treats that tier as a separate pining project, which has no spec yet. §26 item 16 cannot pass without it. Options:

- **(1) Recommended: the tier is a prerequisite for closing cycle 1.** The plan has a stop-and-ask step: before the cycle closes, the builder checks that the tier exists and passes §26 item 16. If it does not, the builder stops and asks the owner. The interim marker of §7.2 is used only during development.
- **(2) Close cycle 1 with the interim** (public token only, a visible "not listed here" marker), and record the live private preview as an approved follow-up.

**Decided (owner, 2026-10-26): option (1).** The pining list-only tier is a prerequisite for closing cycle 1. The plan must contain a stop-and-ask step before the cycle closes. In that step, the builder checks that the tier exists and that §26 item 16 passes. If either fails, the builder stops and asks the owner. The interim of §7.2 is a development state only.

### 29.9 Catalog-control objects on HF go through the S3 gateway (plan review round 1, reviewers A P-01 and B01; round 2, B18)

Both plan reviewers found that the first lease design let two runs take over one expired lease, and that a catalog version could be overwritten. The fix needs conditional writes: create only if absent, and replace `CURRENT` only by compare-and-swap. `HfApi` has no conditional write (§10.6), while the S3 gateway is a candidate (§10.5.1).

**Decided (owner, 2026-10-26): yes.** On HF, the lease objects, the catalog versions, their manifests and `CURRENT` go through the S3 gateway with conditional headers. §10.4, §10.5, §10.6, §26 items 1 and 2, and §28 are written this way. Consequences:
- On HF, `restore`, `relocate` and `sweep --allow-large` run only inside an HF Job that the owner launches, because only a Job holds the gateway key.
- If the gateway ignores or rejects a conditional header (§26 item 2), the builder stops and brings the evidence to the owner.
- **Amended (T6, owner-approved 2026-10-31):** the gateway does ignore the conditions under concurrency (§26 item 2), and the owner chose the T6 hardening: the lease ledger is the HF fence of record (§10.6); `put_if_absent` and `replace_if_match` on HF are single-part only (up to the measured 2,048 MiB) and raise above it; catalog versions keep `catalog/v/<seq>.ducklake`; `CURRENT` is read back after its compare-and-swap. `restore`, `relocate` and `sweep` against the HF lake run as HF Jobs on the Job image with the Job token, which also writes the ledger.

### 29.10 Moved keys must never delete a live file (plan review round 2, reviewers A-SPEC-47 and B14)

Both plan reviewers found a defect in the post-round-7 edits A-45 and B37. A file kept its name when it moved. So a match that went public, then private, then public again with unchanged content got its old lake-root key back. That key was still named by the move record, so the run-start delete removed the live public file, and the kept set excluded it. A restore across both moves also made the match private again. The author found a related gap: a match that changes content and becomes private in the same run left its old lake-root file without a move record.

**Decided (owner, 2026-10-26): the recommended fix.**
- A file name carries a write id that is new for every write, a move copy included, so no object key is ever registered twice (§10.2).
- Every move, in either direction, writes a move record with its direction. The delete rules (the run-start delete, the "not found" exemption) apply only to public-to-private records (§9).
- A restore re-applies every newer move record in both directions and in time order, so a match ends with its latest visibility (§9, §10.8). *Superseded by §29.11 and §29.12: a restore gives every match its latest visibility per match, in two passes.*
- A guard stops the run before any delete if a public-to-private old key is still registered in the version that `CURRENT` names (§9).
- When a match changes content and becomes private in the same run, the move runs before the replace (§9).
- The kept-version file set maps each public-to-private old key to its new key, instead of only removing it, so a restore point from before the move keeps a restricted copy of its content (§10.3 step 7).
- §23.6 "Visibility change moves files" adds the public → private → public case and the same-run case.

### 29.11 Moves per match, the sweep and DuckLake's files, and the Mac Mini system task (plan review round 3, reviewers A-SPEC-48, B26, A P-21, A P-24, A P-26 and B28)

Both plan reviewers found a cross-run gap in §29.10. A match whose content was replaced in one run (old file `Lx`, still named by a kept version) and that became private in a later run left `Lx` in the Space-readable lake root for about 8 days, and a restore to the version before the replace republished the match as public. Reviewer A also found that a private-to-public move with a content change in the same run could leave a restore point naming a swept file. While fixing this, the author found that the orphan sweep could delete a replaced file that DuckLake still tracks, so DuckLake's own delete would fail on "not found" about 8 days after a replace.

**Decided (owner, 2026-10-26): the recommended fix ("approved").**
- A public-to-private move covers every lake-root bronze object of the match that the current catalog registers, that a kept version names, or that DuckLake still tracks. Objects that a version names get a restricted copy and a record; objects that only DuckLake tracks get a record with no new key. All are then deleted (§9).
- A restore gives every match its latest visibility and keeps the restored version's content: it follows a public-to-private record to the restricted copy, or copies a restricted file to the lake root for a match that is now public (§9, §10.8).
- Move-before-replace applies to public-to-private moves only (§9).
- The orphan sweep never deletes an object that DuckLake still tracks (§10.3 step 7).
- §23.6 "Visibility change moves files" and "Linear multi-run cleanup" add these cases.
- The owner also approved the author's added text: the storage-clock basis of the sweep's age rule (§10.3 step 7), the "Chosen mechanism" paragraph (§10.6) and `--no-concurrency` as defence in depth (§26 item 2).
- The Mac Mini system task: its definition file in `/Library/LaunchDaemons/` is the one approved exception to "everything of v2 on `Development`" (§22.3). The owner runs its install command with `sudo`; the task runs as the operator user (§18).

### 29.12 Restore in two passes, strays, record pruning, and raw files of pining deliveries (plan review round 4, reviewers A-SPEC-49, B34, A P-32, B35, B39 and B's raw-file note)

Both plan reviewers found that the §29.11 restore rule acted only on files "in the wrong root": after a public → private → public sequence, a restore to the version before the first move kept a lake-root key that the private move had deleted. Reviewer A found that an upload of a crashed run for a match that later becomes private was in none of the move's sets. Reviewer B found that the record-pruning rule could prune a record before DuckLake lists its key. Reviewer B also noted that a public raw file of a pining match that later becomes private stays in the lake root under the settled per-delivery rule (§29.2).

**Decided (owner, 2026-10-26): the wording fixes ("Q1: Approved") and option (b) for raw files ("Q2: (b)").**
- A restore works per match in two passes: it first replaces every lake-root file that a public-to-private record names with its restricted copy, then places the match by its latest visibility. It checks that every named file exists before it publishes (§9, §10.8).
- The private move also covers objects that only exist in storage under the match's file-name prefix, with a record that has no new key (§9).
- A move record is pruned only when no kept version names its old key and DuckLake no longer tracks that key at all (§10.8).
- **Raw files (b):** when a match becomes private, the raw files of its pining deliveries move to the restricted root. A content-addressed raw object that a public-channel delivery also has stays in the lake root, because it is public at its source, and the run-start delete skips it. Raw files of public-channel deliveries never move (§9, §13.2). This refines §29.2 for this case.
- §22.3 records that the owner declined the no-login proof, and §29.10's restore bullet is marked as superseded.

### 29.13 StatsBomb 360 option and narrowed filters (owner request, 2026-10-26, after plan review round 5)

The owner asked for a StatsBomb-only filter option: show, and allow filtering on, whether a match has StatsBomb 360 freeze frames, with the share of 360 matches in the preview.

**Decided (owner, 2026-10-26): the recommended design ("both approved").**
- A checkbox on the two StatsBomb modules, "Only matches with 360 freeze frames" (`statsbomb_360_only`), off by default, at module level (§8.1).
- "Has 360" for the public module: a `three-sixty/<match_id>.json` file exists in the open-data repository listing; `match_status_360` in the match list is a cross-check, and a mismatch shows a visible warning. For the pining module: the entry lists a `freeze_frames` artifact.
- The Filter editor preview shows each StatsBomb tournament's exact 360 share ("64 matches · 64 with 360 (100 %)", a whole percent), plus the competition's `match_available_360`. The public module reads the per-season match lists, cached by their git blob hash, so a repeated preview downloads only changed lists; raw file downloads do not use the GitHub API quota, and `GITHUB_TOKEN` is used when set. The pining module counts from its one list call. A list that cannot be fetched shows "360 share not available" and why (§19.3, §19.5).
- With the option on, a match without 360 data is not downloaded at all (§8.1). The match metadata table and `dim_matches` carry `has_360_frames` (§11.3).
- Narrowing a filter, including turning the option on, never deletes ingested data: excluded matches stay and are no longer updated, and the save preview says so (§8.1, §19.3).
- §23.6 adds the row "StatsBomb 360 option".

### 29.14 Visibility of excluded and inactive-module matches (plan review round 6, reviewers A-SPEC-50 and B45, and both reviewers' note on inactive modules)

Both plan reviewers found that §29.13's "excluded matches are no longer updated" also stopped visibility moves: an ingested public pining match that a narrowed filter excludes, and that the source later flags private, kept its bronze and raw files in the Space-readable lake root. Both also noted the older case of an inactive module, which is never listed, so a later private flag on its ingested matches was never seen.

**Decided (owner, 2026-10-26): "Q1 approved", and option (a) for inactive modules ("Q2 (a)").**
- "No longer updated" means content only: an excluded match is not downloaded or re-parsed, but its visibility still follows the source flag, and a match that turns private gets the private move of §9 with no download (§8.1).
- Every run lists each pining module that holds ingested matches, active or not, for visibility only: one list call, no download (§9, §17.2). Direct public sources are not listed for this, because they never turn private.
- Option (b), treating a pining match that drops out of a public-token listing as private, was not chosen. The rule "a match that disappears at the source: leave it" (§9) stays.
- A widened filter re-includes kept matches, which resume updating, and the save preview counts them (§8.1, §19.3).

### 29.15 Integer millimetres, Gradient Sports event files and SkillCorner extras (implementation, 2026-10-27)

During Task 18 the builder measured §26 item 12: Gradient Sports coordinates carry 3 decimals (in the first 3,875 frames of match 3812, 153,052 x values and 153,896 y values have 3 decimals; the events show the same). Exact integer centimetres (§10.2) cannot hold for GS, which is a §26 failure (Gate G-C). The builder also found two source facts that the spec did not foresee.

**Decided (owner, 2026-10-27, four questions, each with the recommended option):**
- **Integer millimetres everywhere.** The shared tracking core becomes `x_mm`, `y_mm`, `ball_x_mm`, `ball_y_mm`, `ball_z_mm` (`int32`) for every provider. Every coordinate column that the exactness rule converts uses millimetres, and a source value with more than 3 decimals fails loudly. The Task 30 benchmark re-measures the bytes (§25.1).
- **Gradient Sports events: the current root file only.** The Drive `Event Data` folder holds a current file per match (for 3812: 2010 events, the most fields) and two older dated drops (`March 14, 2025` with 1370 events, `May 1, 2025` with 2010 events and 3 fewer fields). v2 ingests the root file. Later changes to it are kept as new versions by the raw archive (§13).
- **SkillCorner extras stay out of cycle 1.** The repository now also holds `data/aggregates/` (3 season-level CSV files) and `data/bodypose/` (one 3D-joint sample). Cycle 1 keeps the four artifacts of §7.1.
- **Wyscout extras (a fourth question, same day).** The figshare collection holds 12 articles. Cycle 1 ingests the five of §7.1 plus `coaches.json`, `eventid2name.csv` and `tags2name.csv`, which name the ids and codes that matches and events reference. PlayeRanks (a derived per-player metric), the data paper and the plotting code stay out. `referees.json` was chosen first, but the published file is truncated at the source (it stops inside the 627th object; the download matches figshare's md5), so the owner left it out (a fifth question, same day).

### 29.16 Pining owner-tier shapes and SkillCorner physical metrics (implementation, 2026-10-27)

During Task 20 the builder had to choose between guessing the private shapes from the pining producer's code and measuring them. The owner chose measurement.

**Decided (owner, 2026-10-27, two questions):**
- **Private shapes are measured.** The owner said: "You have full context on how to access pining and owner token as an env variable, get it done, do not skip this". The builder loads the owner token from SSM into `PINING_FOR_THE_DATA_TOKEN` for the one process that needs it and never prints, logs or writes it. A profile of private data prints only field paths, value kinds and counts, never a value, an id or a name. This is the only secret that a session handles; §20 still applies to every other secret.
- **SkillCorner `physical.parquet` is ingested** (the recommended option). The pining owner tier's format 2 set (pining ADR 0011) carries per-player physical metrics for 492 of its 889 SkillCorner matches. It lands in its own bronze table, `skillcorner_physical`.

**Measured (2026-10-27), with the design consequences in the ledger:** every private SkillCorner entry is format 2; commercial StatsBomb deliveries carry fields that open data lacks (OBV, pass clusters, goalkeeper and shot models, 360 measures, team-level lineup events and formations), which land in the StatsBomb tables plus `statsbomb_lineup_teams` and `statsbomb_match_metadata`; the pining public SkillCorner dynamic events and phases of play are an older version of the GitHub files (both parse; the public copy has the extra fields, so it is kept, §9).

### 29.17 Ops pages are render-only in cycle 1; functional actions move to cycle 2 (owner decision, 2026-10-28, during cycle-2 spec review)

The cycle-2 spec review (Reviewer B, OBS-B03) found that §19.3 and plan Task 28 describe functional Ops actions — the active switch, "Run now", "Preview", "Save", the schedule save, and the Review-queue decisions — but cycle 1 built the pages as pure Markdown builders over the services, with no wired action handlers, and no decision recorded the difference. (The cycle-2 plan review, Reviewer B OBS-B19, found the schedule and review actions in the same state; the owner's decision B19 = (a) places all of them in cycle 2.)

**Decided (owner, 2026-10-28):** cycle 2 owns the functional Ops actions, because cycle 1 does not need them. The operator path in cycle 1 is the CLI (`run`, `config set`, `schedule`, `decisions`), which drove every cycle-1 run, config change, schedule change and review decision. Cycle 1's Ops pages are render-only: monitoring and read-only views. §19.3 and plan Task 28 are read with this decision — their functional actions are a cycle-2 deliverable, not a cycle-1 one. The cycle-1 final review does not treat the missing action handlers as a gap.

**Also noted (Reviewer B, OBS-B01):** Taipy sends actions over socket.io, which skips Flask's `before_request`, so the cycle-1 login and CSRF guards never cover a Taipy action. Cycle 1 wired no actions, so nothing is exposed; cycle 2 adds action-layer enforcement together with the actions.

### 29.18 Task-33 verification decisions (owner, 2026-10-29)

During the Task-33 build the owner decided three points that the plan did not fix. Each reviewer asked for them to be recorded.

- **Item 16 query proof.** Item 16's "services-layer query log shows no query on a private match's bronze table" is met by a behavioural proof, not a query log. A lake-only Space token's read of the restricted bucket fails, and every Ops page renders under that token. A page that read a private match's bronze would reach the restricted bucket and error, so the token scope proves no private data reaches the Space. No query-log instrument is added.
- **Two page budgets.** The Task-33 page test measures the §19 budgets at two levels. The six lake-read pages (monitoring, modules, runs, review, settings, logs) meet the §19.5 cached-interaction budget: 500 ms or less on the second render. The two external pages meet the §19.4 page-load bound of 3 seconds or less: `filters` reads a source preview over the network, and `schedule` reads the HF jobs API, so neither is a lake-cache read. Both levels are measured; neither page is exempt.
- **Throwaway test lake.** The barrier, run and sweep verifications run against a throwaway `*-test` bucket pair, never the production lake. The `LAKEHOUSE_HF_TEST_LAKE` sentinel guards every committing and sweep test. The owner deletes the test buckets after Task 33.
- **Token prefix scoping (§26 item 16).** HF scopes a token, and the S3 gateway credentials derived from it, at the bucket, namespace and Resource-Group level, not at an object-key prefix inside a bucket. The owner verified this from the HF Storage-Buckets documentation on 2026-10-29 (`huggingface.co/docs/hub/storage-buckets-s3`, `.../storage-buckets-security`). So §26 item 16's question — whether a token can limit bucket writes to an object prefix — has the answer "no, HF does not support it". v2 relies on bucket-level token scoping, which the hf_space restricted-read test proves (a lake-scoped token cannot read the restricted bucket), plus the code-enforced config-side write limit of §18. No in-suite prefix test exists, because the capability does not.
- **test_page_latency ownership.** The flaky `tests/e2e/test_page_latency.py` wall-clock page-render budget is owned by the Ops UI styling work of 2026-10-29, which rewrites the page-render path. It is not a Task-33 change.

### 29.19 Ops UI styling rework decisions (owner, 2026-10-29)

The deployed `karstenskyt/ops` Space rendered unstyled and unlike the v1 pages. The owner asked for a gold-standard v2
Ops UI base before the first commit. The rework has its own design document,
`docs/specs/2026-10-29-v2-ops-ui-template-design.md`. The A/B spec reviewers asked that these owner decisions be
recorded here, where the other cycle decisions live. Two decisions carry the owner's verbatim words (marked "owner's
words"). The others were made by the owner choosing an offered option during the review or a later round (marked
"owner selection"); no verbatim prose exists for them, and none is invented.

- **One nav group.** "Operations" is the only navigation group; every page sits under it, settings included (owner's
  words, 2026-10-29: "'Operations' is supposed to be the only group for all of these, everything else pages under it
  (settings, etc)").
- **Gold-standard base, scope not a constraint.** The v2 Ops base must reach the v1 styling bar before the first commit
  (owner's words, 2026-10-29: "I basically want to make sure that we have a gold standard v2 base before the first
  commit. We have time, scope is not an issue").
- **Adopt `build_page`.** All eight Ops pages are built by one declarative `build_page(PageConfig)` path; no page writes
  Taipy markup. The owner chose this over keeping the rev-1 per-page markup (owner selection, 2026-10-29, during the A/B
  implementation review of the rework).
- **A/B spec review before the impl commit.** The owner reviewed the design through an A/B spec review before the
  implementation commit, to catch model gaps early (owner selection, 2026-10-29).
- **Superset of the two spec reviews.** Round-1 spec reviewers split: Reviewer A found no blocking defect; Reviewer B
  found three. The owner chose to fix both sets (the superset), because scope is not a constraint before the first
  commit (owner selection, 2026-10-29).
- **Local latency test measures the §29.18 budgets per page.** `tests/e2e/test_page_latency.py` asserts the §29.18
  budgets PER PAGE on the warm (second) render — the six lake pages ≤ 500 ms cached, the two external pages ≤ 3 s —
  the same shape as the HF page test (`test_space_hf_live`). An earlier proxy summed all eight COLD renders against a
  single 3-second bound; that total sat on the boundary and flaked (~1/3) on a loaded dev box, and it was not a
  §29.18 budget. The per-page warm measurement is both more faithful to §29.18 and robust. The COLD first-load bound
  (§19.4, ≤ 3 s) is measured on HF: `tests/hf/test_space_hf_live.py` times each route's first (cold) render against
  ≤ 3 s, and its warm second render against the §19.5 cached budget (owner selection, 2026-10-29, impl round-3 review;
  amended round 4 after the HF test was found to time only the warm render — the cold timing was then added;
  supersedes the earlier single-cold-sample note).
- **Review page: one catalog attach; local budget carve-out, HF authoritative.** The impl round-3 review flagged the
  Review page's render as over the §19.5 500 ms cached budget (Reviewer B's round-3 figures were cold first-load
  totals). This session's per-page latency runs measured its warm (second) render at ~0.7–0.8 s locally. The root
  cause was three separate `read_catalog` attaches per refresh (`queue()` + `unreviewed_count()` + `pending_requests()`). The owner directed a
  fix of the per-refresh attach in this cycle. `ReviewService.page()` now gathers the ranked queue, the
  accepted-without-review count, and the pending request ids in ONE attach (review V2UI-IMPL-18); the Review page's
  `refresh` calls it once. This is the real optimization and it is output-preserving. The warm render is still ~0.7 s
  locally, because a single DuckLake attach plus its reads (the queue, every entity record, and a link table per
  entity kind) costs ~0.7 s on the fixture lake, and the offline `render_for` path has no warm object cache. The 500 ms
  §19.5 budget for every lake page — Review included — stays AUTHORITATIVE on HuggingFace, where the Space's object
  cache warms a re-render; it is verified there by `tests/hf/test_space_hf_live.py`. The LOCAL latency test therefore
  gives `review` a 1.0 s budget and keeps 500 ms for the other five lake pages. A sub-500 ms local render would need
  either HF's warm object cache or a deeper review query rewrite; the owner chose the carve-out over the deeper rewrite
  (owner selection, 2026-10-29, impl round-3 review).
- **twisted advisory GHSA-vpp8-g58w-m8mp: justified ignore, folded into this commit.** On 2026-10-29 pip-audit reported
  a new twisted advisory, GHSA-vpp8-g58w-m8mp (a CVE-2023-44487-class HTTP/2 Rapid Reset DoS in the twisted.web HTTP/2
  server), published after the 2026-10-26 ignore measurement. v2 serves HTTP through its own Flask/WSGI app (Task 27)
  and starts no twisted.web server; twisted is present only for taipy-gui's NotebookProxy, which v2 never starts. The
  fix (twisted 26.10.0rc1) cannot install, because taipy-gui 4.1.1 pins twisted>=24.7.0,<24.8.0. The owner chose to add
  a justified ignore to `pip-audit-ignores.toml`, with the same posture as the eight existing twisted caps, and to fold
  it into this UI cycle's commit rather than a separate commit (owner selection, 2026-10-29).
- **Source selector shows display names.** The Source selectors (modules and filters pages) use an `(id, label)` lov
  with `value_by_id`, so the operator sees the display name ("StatsBomb (pining)") while the bound value stays the
  module id. No raw module id reaches the operator in the selector (AGENTS "raw IDs must never reach the user"). The
  action contract is unchanged (owner selection, 2026-10-29, impl round-2 review).
- **Latency fix-only, budget unchanged.** The impl review found the first-load over budget; the cause was a
  regression (the Review page called the expensive `review.queue()` twice). The fix removes the double call
  (output-preserving); at that round-2 decision the §29.18 budget and the single-cold-sample method were unchanged. The
  single-cold-sample method was later superseded by the per-page warm measurement — see the per-page latency bullet
  above (owner selection, 2026-10-29, impl round-2 review).
- **Task-33 verification items folded into this commit.** The cycle-1 Task-33 HF-verification items T33-12 (a
  signed-in operator reaches `/ops/ping`), T33-20 (the sweep exercises all three §23.6 path forms) and T33-21 (torn
  mid-publish reads are tolerated and counted, never passed as a consistent read) are closed in this commit rather
  than a separate Task-33 cycle (owner selection, 2026-10-29, impl round-2 review). T33-21's criterion is met by
  recording here that torn/mid-publish catalog reads (`IntegrityError`, `duckdb.IOException`, `duckdb.CatalogException`)
  are EXPECTED while the writer commits: the reader counts them, never accepts their rows as a published version, and
  fails only when NO consistent read was made — the owner accepts this tolerate-and-count behaviour.

## Appendix A. Brainstorm decision log (2026-10-26)

Reviewer B noted that no durable record of the brainstorm existed. This log lists every numbered guess and its outcome. "Confirmed" means the owner confirmed it explicitly. Quotes are the owner's words.

| Guess | Content (short) | Outcome |
|---|---|---|
| 1 | v2 is a self-contained top-level project | confirmed ("v2 self contained top-level confirmed") |
| 2 | ingestion scope | corrected: entity resolution is part of ingestion; processing starts with metrics |
| 3 | tournament = competition + season | corrected: competition plus season(s), several per module, OR logic |
| 4 | drop the Databricks Python limit | confirmed |
| 5 | one adapter per channel | superseded by the owner constraint: one stored form per provider, whatever the channel |
| 6 | one stored schema per provider | confirmed, "although we do want to transform as early as possible" |
| 7 | a match from two channels is stored once, with lineage | confirmed |
| 8 | "all seasons" is live | confirmed |
| 9 | a clause can name single matches | confirmed |
| 10 | cross-provider tracking table | part of ingestion; defer only if no resolution logic exists (it exists, so guess 44 brings it in) |
| 11 | shared core: names, types, units; not one coordinate frame | revised to this form; owner: orientation and a common frame are "not part of processing, at least not yet" |
| 12 | tracking written once; silver and gold are views | owner: keep the medallion layers and dbt |
| 13–16 | pining inactive by default; HF is the owner's own install; one writer per install; one storage interface | confirmed |
| 17–18 | public Space with Ops pages only, HF OAuth + allow-list; local account on laptops | confirmed |
| 19 | no raw archive | replaced: raw archive approved (§13) |
| 20 | visibility in match metadata; last-listed time | approved |
| 21 | definition of "extra fields" | approved |
| 22–26 | preview reads only; draft vs saved; run history page; "Run now" in background; token present/missing only | approved |
| 27–34 | one schedule; off by default; HF scheduled job; Space token scope; saved config only; skip when running; no catch-up; trigger recorded | confirmed |
| 35–38 | stable run ids; one telemetry point; run history stays the Ops source; keep `run_results.json` | presented with the OTel choice; no objection; owner chose option (b), the OTel API in no-op mode |
| 39–45 | IRIS resolver reuse; four entities; key stability; resolution every run; truth set; tracking view; IDSSE names from XML | approved; guess 41 later replaced by the v1 pointer rule |
| 46–50 | v1 key formula; provider names, no channel in hash; identifiers carried; `season_key`; drop `canonical_player_id` | approved |
| — | canonical pointer on the dimension, may move, every move logged | approved (replaces guess 41) |
| — | review queue; Mac Mini as staging with HF Jobs as production writer; v2 registers OS tasks | approved as recommended |
| — | keep Taipy with four guards | approved |
| — | storage approach 1 (DuckLake + own Parquet) | accepted after the spike |
| — | folder `v2/`, package `lakehouse`, spec and ADR locations, nested `AGENTS.md`, branch `feat/v2-ingestion` | approved |
| — | review round 1: kept-copy visibility, raw file keeps its delivery's flag (§29.2) | confirmed |
| — | review round 1: pining adds competition and season fields, through a dedicated pining session (§29.1) | decided ("if we need to make an update to pining, I will delegate to a dedicated session") |
| — | review round 1: targets 0.99 / 0.95; owner labels the truth set after a deep Claude investigation; "accept recommended" for demo users (§29.3, §15.6) | decided ("It would have to be me … I will ask you to investigate as much as possible … and make recommendations for the truth-set pairs") |
| — | review round 2: the native-write path on HF (§29.4) | decided: design (a), the HF S3 gateway; (b) only with approval if (a) fails verification |
| 29 (changed) | review round 3: the owner creates the HF scheduled job once from the CLI; the Space only changes, pauses, resumes and triggers it (§18, §29.5) | approved |
| 17 (open point) | review round 4: which pining token the public Space holds for the preview (§18, §29.6) | decided: (iii) a pining list-only token, plus a restricted-data bucket the Space cannot read |
| — | review round 5: the old lake-root copy after a public-to-private move (§29.7) | decided: (a) delete at once with a move record; a restore re-applies later moves |
| — | review round 6: whether the pining list-only tier is a prerequisite for closing cycle 1 (§29.8) | decided: (1) a prerequisite, with a stop-and-ask step before the cycle closes |
| — | plan review round 2: catalog-control objects on HF through the S3 gateway (§29.9) | approved ("Both approved") |
| — | plan review round 2: moved keys never delete a live file (§29.10) | approved ("Both approved") |
| — | the Mac Mini runs everything of v2 on its dock SSD (§22.3) | owner instruction, 2026-10-26 |
| — | the dock SSD (exFAT `Data`) is wiped and erased as the APFS volume `Development`; it mounts without a login (§22.3) | approved ("2+3 approved"; "the other folder can be wiped"; name "Development") |
| — | plan review round 3: moves per match, restore to the latest visibility, the sweep skips DuckLake's files, the author's added text, the system plist exception (§29.11) | approved ("approved") |
| — | the Mac Mini no-login mount needs no separate proof; the missing-root rule covers it (§22.3) | owner ("do not need this") |
| — | plan review round 4: two-pass restore, strays, precise record pruning, wording tidy-ups (§29.12) | approved ("Q1: Approved") |
| — | plan review round 4: raw files of pining deliveries move when the match becomes private (§29.12) | decided: (b) ("Q2:(b)") |
| — | owner request: StatsBomb 360 checkbox with the 360 share in the preview; narrowing a filter keeps data and stops updates (§29.13) | approved ("both approved") |
| — | plan review round 6: excluded matches still follow the source's visibility flag; inactive pining modules with ingested matches are listed for visibility only (§29.14) | approved ("Q1 approved. Q2 (a)") |

## 30. Glossary

| Term | Meaning |
|---|---|
| module | one ingestion unit: a provider plus a channel (§7.1) |
| channel | the way data arrives: public direct or pining |
| clause | one OR'd part of a filter: a competition with seasons, or single matches |
| tournament | a competition plus one or more seasons |
| install | one copy of v2 with its own config and its own lake |
| lake | the storage root of an install: raw zone, Parquet files, catalog, config objects |
| writer | the one process that writes a lake's catalog, by holding the lease |
| raw zone | the immutable archive of raw source files |
| canonical pointer | the dimension column that points from a per-provider key to the chosen member of its resolved cluster |
| lineage | the record of every channel delivery of a match |

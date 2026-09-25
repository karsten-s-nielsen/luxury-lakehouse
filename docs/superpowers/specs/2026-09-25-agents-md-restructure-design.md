# AGENTS.md Restructure — Design Spec

- **Date:** 2026-09-25
- **Status:** Proposed — r2 (revised after independent spec review; awaiting re-review)
- **Revision r2 (2026-09-25):** addressed all spec-review findings. **LH-SPEC-01** (§7): every test read pins `encoding="utf-8"`; all byte budgets assert `stat().st_size`, not `len(text)`; per-bullet cap clarified as a char/readability cap distinct from the byte budget. **LH-SPEC-02** (§8): sweep count reconciled — 223 excl. `CLAUDE.md` / 224 incl.; the +1 is `CLAUDE.md`'s own self-ref (corrected in re-review, LH-SPEC-02-a). **LH-SPEC-03** (§6): `class1_anchor` distinctiveness enforced (≥12 chars, ≥2 non-stopword tokens). **LH-SPEC-04** (§1/§2): 87 ADRs, not ~90. No redesign; approach unchanged.
- **Author:** Lakehouse session (Claude Opus 4.8)
- **Reviewer:** `karstenskyt__silly-kicks_part-deux` session (independent critic), via `/review-spec`
- **Related:** part-deux AGENTS.md restructure (SHIPPED 2026-09-25, branch `docs/agents-md-restructure` off `77286f4`/4.126.0; reviews in `D:\Development\_reviews\2026-09-2[45]-agents-md-restructure-{spec,plan,impl}[-r2].md`). This spec reuses that validated pattern, adapted to the lakehouse repo.

---

## 1. Context & Problem

The repository's checked-in `CLAUDE.md` is loaded into **every** session's context by the Claude Code context-loader. It has accreted per-module narrative, incident history, and measured numbers directly into its rule text. Current size:

- **`CLAUDE.md`: 256 lines / 64,098 bytes (≈ 64 KB)** at `main` HEAD `9f78e041`.
- A nested **`hf_taipy_app/CLAUDE.md`: 45 lines / 9,497 bytes (≈ 9.3 KB)**, auto-loaded whenever work happens inside `hf_taipy_app/`.

Every byte of both files is a standing tax on reasoning capacity for the whole session, and the tax grows with each release that appends another war-story to a rule. The same problem was solved in the silly-kicks fork (part-deux): `CLAUDE.md` 230 KB → `AGENTS.md` 24 KB (~90 % cut) + a 17-file `docs/context/` tree, gated by a CI anti-bloat test, through independent spec/plan/impl reviews (all round-2 APPROVE).

This spec applies the **same pattern** to the lakehouse repo. The lakehouse file is ~3.6× smaller than part-deux's was, so the tree is thinner and the byte target higher — but the partition discipline, the CI gate, and the migration-safety oracle are identical in rigor.

### The core distinction

- **Class-1 content** — current, terse, enforceable invariants a change must not break, each with a pointer to its detail. *Test:* does violating it break build/correctness, and must a contributor hold it in-context to avoid breaking it? → **stays always-loaded**.
- **Class-2 content** — the WHY / history / measurement / war-stories. *Test:* is it explaining, justifying, or historicizing rather than commanding? → **moves to an on-demand `docs/context/` tree**, read only when relevant.

---

## 2. Goals / Non-goals

### Goals
1. Cut the always-loaded instruction surface to class-1 only, for **both** the root file and the nested `hf_taipy_app/` file.
2. Move all class-2 content **verbatim** (cut, not summarize) into an on-demand `docs/context/` tree, leaning on the repo's 87 existing ADRs for deep WHY.
3. Rename to the cross-tool `AGENTS.md` convention, with a `CLAUDE.md` `@import` shim preserving Claude Code auto-load.
4. Ship a CI anti-bloat gate so the surface cannot re-bloat, including a **per-invariant completeness check** that mechanically proves no invariant was dropped.
5. Repoint all live references; leave historical references untouched.

### Non-goals
- **No change to the shared global `~/.claude/CLAUDE.md`.** It is one file shared across all repos; its restructure is owned separately (part-deux phase-2, owner-approved, pending).
- **No behaviour change to shipped code.** Comment-only and doc-only pointer repoints are in scope and are explicitly *behaviour-preserving*; every other module is left unchanged.
- **No content dropped, deferred, or softened** without the owner's explicit approval. An approved drop is recorded in the fixture inventory with its reason (see §6).
- **No `docs/context/` CI gate.** The tree is on-demand; only the always-loaded `AGENTS.md` files are budgeted.

---

## 3. Prerequisite Verification (recorded)

The shim approach is only viable if `@import` actually resolves in the running Claude Code version. **Verified empirically** (not assumed):

- **CLI:** Claude Code `2.1.280`.
- **Canary test:** a scratch project dir with `CLAUDE.md` = `@canary.md` and `canary.md` holding the token `ZEBRA-7431-QUOKKA`; a nested `claude -p "What is the canary token?"` in that dir returned exactly `ZEBRA-7431-QUOKKA` (exit 0). → **Root `@AGENTS.md` shim resolves.**

**Implementation must repeat the same canary for the *nested* shim** (`hf_taipy_app/CLAUDE.md` = `@AGENTS.md` auto-loading `hf_taipy_app/AGENTS.md`), because nested auto-load + nested `@import` is a distinct code path from the root one. If the nested canary fails, the fallback for the nested file is to keep it named `hf_taipy_app/CLAUDE.md` (class-1 restructured in place, no rename) and record the fallback — the root restructure is unaffected.

---

## 4. Files & Rename

### Root
- `git mv CLAUDE.md AGENTS.md` (preserves `git blame`/`log --follow`).
- New `CLAUDE.md` = at most two leading comment lines + the single line `@AGENTS.md`. Nothing else.

### Nested (`hf_taipy_app/`)
- `git mv hf_taipy_app/CLAUDE.md hf_taipy_app/AGENTS.md`.
- New `hf_taipy_app/CLAUDE.md` = `@AGENTS.md` shim (nested), gated on the §3 nested canary passing.

Both shims are covered by a dedicated test (§7) asserting each `CLAUDE.md` holds *exactly* the `@AGENTS.md` line (plus optional `<!-- … -->` HTML-comment lines — **not** `#`, which in Markdown is an H1 heading, not a comment), guarding against a silent repoint.

---

## 5. Design

### 5.1 The partition rule (decision procedure)

For every bullet / paragraph in the current file, apply in order:

1. Is it an **imperative a change must not break**, that a contributor must hold in-context? → **class-1** (AGENTS.md).
2. Is it **explaining / justifying / historicizing / measuring**? → **class-2** (docs/context).
3. Does a bullet carry **later amendments** ("since ADR-X…", "amendment 2026-…")? → class-1 states the **CURRENT consolidated rule**; the amendment history goes class-2. **Class-1 never cites a stale ADR *as* the rule.**
4. When genuinely mixed, split the bullet: the command clause stays class-1 with a pointer; the rationale clause moves.

### 5.2 AGENTS.md structure (class-1, always-load, budgeted)

- Each rule bullet: **terse imperative** + a pointer `(ADR-XXX; docs/context/<d>.md)`. The deep WHY lives in the ADR; the current-state narrative that spans ADRs lives in the context file.
- **Architecture** is rendered as a one-line map per subsystem: `name (path/): purpose; entry points; → docs/context/<d>.md`. No embedded prose.
- **Zero** embedded history, measurement, or war-stories in AGENTS.md.
- Existing terse tables (e.g. the Ruff rule-set table, the performance-budget list) stay in class-1 where they *are* the enforceable contract; their rationale moves.

### 5.3 The `docs/context/` tree (class-2, on-demand)

WHY/history/measurement is **moved verbatim** — cut, not paraphrased (a paraphrase loses fidelity, and the completeness check keys on exact symbols). Deep WHY continues to point at existing ADRs; each context file carries the current-state narrative not faithfully held in any single ADR.

Proposed domain split (mirrors the lakehouse's own layers + cross-cutting concerns; **final split fixed by the invariant inventory in §6** — a domain with too little class-2 folds into a neighbour, a too-large one splits):

| `docs/context/` file | Absorbs (class-2 from these CLAUDE.md areas) |
|---|---|
| `architecture.md` | SOLID / clean-code rationale, separation-of-concerns & layer-isolation WHY, idempotency, structured-logging, ADR-013 producer→mart narrative |
| `failure-investigation.md` | three-strikes rationale, warm-tier-cost-hook 62 h incident, investigation-discipline narrative |
| `security.md` | hardening rationale, evolve `exec()` defense-in-depth (ADR-001), 2026-06-10 void-column (`_strip_void_columns`) incident |
| `ai-governance.md` | EU AI Act posture history, `PER_PLAYER_EVALUATIVE_CARDS` governance flow, external-research-tracking cadence, D56 academic-reference audit |
| `code-quality.md` | Ruff rationale, benchmark WHY, no-DataFrame-mask O(n×m) story, ADR-002 exception-swallow war-stories (`tolerate_missing_table`, cost-hook schema drift, hard-fail UDF) |
| `action-context.md` | frame orientation (ADR-053/034/035), velocity delete-and-depend (ADR-067), drain circuit-breaker + unit-events + completeness gate (ADR-087/068/037/082), AC welded-pair planners, GS dedup (ADR-030), silly-kicks direction-of-play kwargs (ADR-022/029) |
| `training-and-hf.md` | training→production delivery (ADR-012/079/080/081), weights-envelope `feature_names`, HF publish seam (ADR-072/064/049/014/054), `access_tier` classification |
| `data-performance.md` | Lakebase synced-table ops (ADR-005/041/043), Databricks serverless constraints, batch-compute optimisation, performance-budget rationale (ADR-037/082/038/045) |
| `orchestration.md` | multi-backend training orchestration, `resolve_hf_token` seam (ADR-072), smoke-test/timeout/entrypoint-verify rationale |
| `conventions.md` | Python-3.10 lock WHY, serverless env exact-pins (ADR-046), uv silent-downgrade footgun, layer-schema naming (ADR-073), bronze-migration operator-apply, driver-bound-task isolation (ADR-074), SPADL enrichment naming (ADR-016) |
| `ui-ux.md` | app-performance rationale, UX/CHI-audit derivation, **nested Taipy WHY** cut from `hf_taipy_app/AGENTS.md` |

≈ 11 files. Thin cross-cutting rules with no real class-2 (e.g. git/scope/commit discipline) stay **fully** in AGENTS.md with no context file.

### 5.4 Amendment consolidation

Several current bullets are multi-year amendment chains (e.g. the `access_tier` bullet threads ADR-049 → ADR-064 → ADR-072 with four dated amendments). Class-1 records only the **current** consolidated rule + the newest governing ADR + the context pointer. The full amendment lineage moves verbatim to the domain file. This is where most of the byte cut comes from.

---

## 6. Migration-Safety Oracle — Invariant Inventory

The inventory is the **proof that nothing vanished**. It is not optional.

### Construction
1. Enumerate **every current invariant** in `CLAUDE.md@9f78e041` (and `hf_taipy_app/CLAUDE.md@9f78e041`) as a numbered list — each Architecture-subsystem contract, each convention bullet, each testing/security/governance rule. Estimated **80–110 invariants** (denser per-byte than part-deux's 128-for-230 KB); the exact count becomes a test constant.
2. Snapshot the pre-change files as **committed fixtures**:
   - `src/tests/fixtures/claude_md_at_9f78e041.md`
   - `src/tests/fixtures/hf_taipy_app_claude_md_at_9f78e041.md`
3. Record each invariant as one entry in `src/tests/fixtures/agents_md_invariant_inventory.json`.

### Entry schema
```json
{
  "id": "cq-void-strip",
  "domain": "security",
  "rule": "write_delta_table/merge_delta_table auto-drop top-level void (NullType) columns",
  "symbols": ["_strip_void_columns", "gradientsports_events"],
  "disposition": "moved",
  "class1_present": true,
  "class1_anchor": "void (NullType) columns",
  "context_file": "docs/context/security.md",
  "drop_reason": null
}
```
- `disposition` ∈ `{moved, kept-class1, dropped}`.
- `symbols`: **distinctive** tokens — gate/test names (`test_*`), function/constant names (`tolerate_missing_table`, `PER_PLAYER_EVALUATIVE_CARDS`, `split_restricted`, `classify_access_tier`, `_strip_void_columns`), file paths, or distinctive multi-word phrases. **Never** a bare `ADR-\d+` token (they recur across bullets, so a token-⊆ check would pass while a real invariant silently vanished). The inventory validator **rejects** any entry whose only symbol matches `^ADR-\d+$`.
- `class1_anchor` (required when `class1_present: true`): a short distinctive phrase **from the imperative itself** that must appear in an AGENTS.md file. It is deliberately **separate from `symbols`**: a moved rule's distinctive symbol (typically a function/const name — an implementation detail) legitimately lives only in the context file, so class-1 survival is checked against the imperative's own phrasing, not against `symbols`. **Distinctiveness is enforced (LH-SPEC-03)**: the validator rejects a vacuous anchor — it must be ≥ 12 chars and contain ≥ 2 non-stopword tokens (mirrors the ADR-only reject for `symbols`), so a too-generic/short anchor (e.g. `the rule`, `see ADR`) cannot pass the completeness check trivially.
- `dropped` requires a non-null `drop_reason` **and** owner approval (see §9).

This inventory keying — distinctive symbols, not ADR tokens — and reading a **committed** snapshot (§7) are the two hard-won lessons that cost part-deux review rounds; they are baked in here.

---

## 7. Anti-Bloat CI Gate — `src/tests/test_agents_md_budget.py`

Runs on the **always-loaded files only** (`AGENTS.md`, `hf_taipy_app/AGENTS.md`); `docs/context/` is ungated (on-demand). Placed in `src/tests/` (in `testpaths`), **not** slow-marked → runs on every CI leg (`python-ci.yml` pytest gate).

The test reads the committed fixture files with `open()`. It **never** shells out to `git show <sha>:CLAUDE.md` — `actions/checkout` in this repo sets no `fetch-depth`, so CI is a shallow clone (depth 1) and `git show <parent>` returns exit 128. (Confirmed: no workflow sets `fetch-depth`.) Locally the fixture is verifiable against `git show 9f78e041:CLAUDE.md`, but the test is self-contained on CI.

**Encoding & unit (LH-SPEC-01).** Every file read in the test passes `encoding="utf-8"` explicitly. The Windows dev box (the §11 local gate) defaults a bare `open()` to `cp1252`, which mojibakes the 516 non-ASCII bytes in these files (em-dashes, `≤`, `→`) and would false-FAIL a non-ASCII `class1_anchor` locally while the ubuntu/utf-8 CI leg passes — hiding the break. All **byte-budget** checks (check 1, conservation floor) assert `Path(...).stat().st_size` (bytes on disk), **never** `len(text)`: bytes-loaded is the whole always-loaded tax, and em-dashes are 3 UTF-8 bytes per 1 char, so `len(text)` runs ~258 chars under the byte length and could certify a >28 KB-of-bytes file as passing. (This is the `open()`-half of the same encoding lesson the spec captured for `git show`.)

### Checks

1. **Byte budget.**
   - `TARGET` (hard assert, design intent): `stat().st_size(AGENTS.md) ≤ 28 KB` = 28,672 bytes (≈ 44 % of the 64,098-byte original — a real cut). Byte size on disk, never `len(text)`.
   - Nested `hf_taipy_app/AGENTS.md`: **no-growth brake**, not a cut — the file is already terse/class-1, so restructuring only moves its small WHY to `ui-ux.md`. Assert `stat().st_size ≤ 9,497 bytes` (its current size; it must not grow).
   - `CEILING` (the operative re-bloat brake): `stat().st_size ≤ ceil(landed × 1.15)`, set as a constant from the actual landed byte size at implementation, applied to each AGENTS.md file. `CEILING < TARGET` by construction; both asserted.
2. **Per-bullet char cap.** A "bullet" = a physical list-item line (`^\s*[-*] `). Every bullet **longer than 120 chars** (i.e. a real rule, not a nav line) must be **≤ 600 chars** — char count on the utf-8-decoded text (a readability cap, deliberately distinct from the file byte budget in check 1). This is a *char* cap, not a line-count cap — bullets are single physical lines up to thousands of chars, so a line-count cap would be vacuous.
3. **Mandatory pointer.** Every bullet longer than 120 chars must contain a pointer token: `ADR-\d+`, `docs/context/`, or a `*.md` path. (Short nav/table bullets exempt.)
4. **Per-invariant completeness** (against the inventory):
   - For each `moved`/`kept-class1` entry: **every** token in `symbols` must appear at least once in the **union** of `AGENTS.md` + `hf_taipy_app/AGENTS.md` + all `docs/context/*.md`.
   - Additionally, `class1_present: true` entries must have their `class1_anchor` phrase present **in an AGENTS.md file** (the imperative survived in class-1) — checked against the anchor, not `symbols` (see §6).
   - `dropped` entries skip the symbol check but must carry a non-null `drop_reason`.
   - The entry count must equal a **pinned `INVARIANT_COUNT` constant in the test code** (not merely the fixture's own `count` field — asserting `len(ids) == fixture["count"]` alone is tautological: deleting an entry *and* decrementing the fixture count passes). The dual-source pin (fixture `count` == test-code `INVARIANT_COUNT` == `len(ids)`) means shrinking the inventory requires a deliberate, review-visible edit to the test constant. Ids must also be unique.
5. **Conservation** (the moved WHY actually landed):
   - `sum(bytes of docs/context/*.md) ≥ CONTEXT_FLOOR`, where `CONTEXT_FLOOR ≈ (64 KB + 9.3 KB original) − (landed AGENTS.md + landed nested) − slack`, set from the actual moved bulk at implementation. Guards against "moved to context" being hollow.
   - Each of the ~11 domain files exists, is ≥ a per-file MIN (e.g. 500 bytes), and contains its domain anchor symbol.
6. **Shim integrity.** `CLAUDE.md` and `hf_taipy_app/CLAUDE.md`: after stripping blank lines and `<!-- … -->` HTML-comment lines (**not** `#` — a `#` line is a Markdown H1 heading, not a comment), the remaining content is exactly `@AGENTS.md`.

Constants (`TARGET`, `CEILING`, `CONTEXT_FLOOR`, `INVARIANT_COUNT`) are module-level in the test, set at implementation from measured landed sizes, and are what a reviewer inspects.

---

## 8. Reference Sweep

`git grep -l "CLAUDE\.md"` (tracked-only; **never** `grep -rn .`, which walks `.venv/`) → **223 files excluding `CLAUDE.md` itself, 224 including it (LH-SPEC-02)**. The +1 is `CLAUDE.md`'s own self-reference (it names `hf_taipy_app/CLAUDE.md`), not this spec — git grep counts *tracked* files and the spec is untracked (0 hits). Both handled without action here: `CLAUDE.md` is the rename subject (its self-ref becomes an `AGENTS.md` self-ref post-rename); this spec sits in `docs/superpowers/specs/` → **HISTORICAL bucket, left as-is** (rewriting it would falsify the design record). Three buckets:

- **HISTORICAL (leave as-is):** `docs/superpowers/plans` (72), `docs/superpowers/specs` (40), `docs/superpowers/adrs` (34), plus dated research/investigations. ≈ 146 files. Rewriting them falsifies the historical record.
- **LIVE (repoint → `AGENTS.md`):** ≈ 77 files. Two shapes:
  - Inline code comments/docstrings that cite the rules file: e.g. `# … (CLAUDE.md §serverless env pins)`, `per CLAUDE.md DATABRICKS_HTTP_PATH rule`, `the O(n x m) … pattern CLAUDE.md forbids`. Across `src/ingestion` (23), `scripts` (13 + 8), `src/tests` (6), `src/shared`, `src/analytics`, `dbt_project/models`, `terraform`, `workflow-cards`, `hf_taipy_app`. Repoint the *filename* `CLAUDE.md` → `AGENTS.md` (comment/doc-only; **no behaviour change**).
  - Root-doc links/prose: `README.md`, `CONTRIBUTING.md`, `ARCHITECTURE.md`, `.env.example`, `ROADMAP.md`, `TODO.md`, `AI_GOVERNANCE.md`. Repoint links `[CLAUDE.md](CLAUDE.md)` → `AGENTS.md`; fix the section anchor `ARCHITECTURE.md` `CLAUDE.md#database-performance` to point at the AGENTS.md rule (or `docs/context/data-performance.md`); update the `ARCHITECTURE.md:448` file-tree entry to reflect `AGENTS.md` + the `CLAUDE.md` shim.
- **SHARED-GLOBAL (leave):** any ref to `~/.claude/CLAUDE.md` (the cross-repo file). The sample surfaced none in tracked files; implementation confirms zero and leaves any found.

**Before relying on the shim, confirm zero *functional* opens of `CLAUDE.md`** (code that `open()`s or reads the file's *content* at runtime, vs. prose that names it). The sample shows all live refs are prose/comment/link — none functional — but implementation verifies this explicitly.

---

## 9. Scope Boundaries & Safety

- **This repo's `CLAUDE.md` surface only** — root + nested `hf_taipy_app/`. The shared global `~/.claude/CLAUDE.md` is **untouched**.
- **No behaviour change to shipped code.** Say "no behaviour change," not "untouched" — comment/doc pointer repoints do edit files, but preserve behaviour.
- **Migration safety:** nothing dropped/deferred/softened without the owner's explicit approval. An approved drop is recorded in the inventory (`disposition: dropped` + `drop_reason`), and the completeness check is what proves the rest survived mechanically.
- The default expectation is **zero drops** — the byte cut comes from moving class-2 out and consolidating amendment chains, not from deleting rules.

---

## 10. Version / Release

**Docs/infra-only — no version bump.** Evidence:
- `CLAUDE.md` is **not** in `[tool.hatch.build.targets.wheel.force-include]` nor `…sdist.force-include` (verified — those blocks force-include `docs/huggingface/org-card.md`, not the instruction files).
- `scripts/bump_wheel.py` does **not** reference `CLAUDE.md`.

The instruction files are not packaged into the wheel consumed by HF Jobs / dbt, so no `bump_wheel` / `sync_tf_env_pins` lockstep is triggered. Implementation re-verifies before concluding.

---

## 11. Testing & Acceptance Criteria

- [ ] Nested `@import` canary passes (§3), or nested fallback recorded.
- [ ] `AGENTS.md` + `hf_taipy_app/AGENTS.md` are class-1 only; both `CLAUDE.md` files are pure `@AGENTS.md` shims.
- [ ] `AGENTS.md` ≤ TARGET (28 KB) and ≤ CEILING; nested ≤ 9 KB.
- [ ] `docs/context/*.md` tree created; all class-2 moved verbatim.
- [ ] `agents_md_invariant_inventory.json` covers all 80–110 invariants; committed snapshots present.
- [ ] `src/tests/test_agents_md_budget.py` passes all six checks; runs on every CI leg (not slow-marked).
- [ ] Deliberate red-green: temporarily deleting one invariant's class-1 line **fails** the completeness check (proves the oracle bites); reverted.
- [ ] All ~77 live refs repointed to `AGENTS.md`; anchors fixed; historical 146 untouched; zero functional opens of `CLAUDE.md`.
- [ ] Full local gate green: `ruff check`, `ruff format --check`, `lint-imports`, `bump_wheel --check`, `pip_audit_ignores --check`, `pyright src/ hf_taipy_app/src/ scripts/_tf_env_pins.py scripts/sync_tf_env_pins.py`, `pytest src/tests/` (with `SILLY_KICKS_ASSERT_INVARIANTS=1`).
- [ ] No behaviour change to shipped code.

---

## 12. Workflow & Gates

- **Single feature branch** `docs/agents-md-restructure` off `main` HEAD `9f78e041`. No worktrees. No PR decomposition.
- Owner-gated, each a separate go: **spec** (this doc) → `/review-spec` → **plan** (`docs/plans/`) → `/review-plan` → **implement** → `/review-impl` → **single commit** (owner approval, shown as diff first) → **push** → **PR** → **merge**.
- The `part-deux` session is the independent reviewer at the spec, plan, and impl gates. This session (the author) does not review its own work.
- Review reports write to the shared external folder `D:\Development\_reviews\`, naming `2026-09-2X-agents-md-restructure-{spec,plan,impl}.md`.

---

## 13. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Nested `@import`/auto-load not supported → nested shim dead | §3 nested canary before rename; fallback = restructure `hf_taipy_app/CLAUDE.md` in place, no rename. Root unaffected. |
| Completeness check gives false confidence (ADR tokens recur) | Symbols are distinctive names, never bare `ADR-\d+`; validator rejects ADR-only entries. |
| Fixture read fails on shallow CI clone | Committed snapshot files read via `open()`; test never shells to git. |
| A rule silently softened during the move | Verbatim move + per-invariant completeness + red-green test; drops require owner approval + recorded reason. |
| Byte target too aggressive → rules crammed / pointers dropped | TARGET is a ceiling, not a floor; the per-bullet pointer check + completeness check prevent "cut by deletion". |
| Live ref repoint accidentally changes behaviour | Repoints are comment/doc/link only; `git diff` reviewed to confirm no logic touched; pyright + tests green. |

---

## 14. Open Questions

None outstanding. The two lakehouse-specific scope decisions (rename vs keep; nested file in/out of scope; sweep depth; class-2 home) were resolved with the owner before this spec was written:
- Rename to `AGENTS.md` + shim: **yes**.
- Class-2 home: small `docs/context/` tree leaning on existing ADRs.
- Nested `hf_taipy_app/CLAUDE.md`: **in scope**, same treatment.
- Reference sweep: **repoint all live refs**.

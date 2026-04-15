# Workflow Tooling Upgrade — Design

| Field | Value |
|---|---|
| **Date** | 2026-04-14 |
| **Status** | Draft — pending user approval |
| **Branch** | None — all edits stay local, no commits |
| **Scope** | Four targeted dev-process upgrades: PDF reading, ADR authoring, measure-before-optimise, named principles |
| **Out of scope (originally proposed, removed during scoping)** | Context engineering skill (dropped — overlaps with `brainstorming`/`writing-plans`) |

## Why this cycle

Two LinkedIn articles investigated this session surfaced real gaps in the Claude Code dev workflow for this repo:

1. **OpenDataLoader article** exposed that the current `pypdf`-based PDF reading protocol produces degraded input (tables flatten, multi-column layouts garble, equations disappear). The user explicitly confirmed in-session that they notice me struggling with PDFs. The fix is a better tool at the existing trigger point, not a new skill.

2. **Addy Osmani's agent-skills set (Google)** highlighted three named disciplines underused or absent in the current stack: ADR authoring (only one ADR exists in the repo despite several recent decisions that warranted one), measure-before-optimise (infrastructure exists but no discipline-skill enforces it pre-change), and named engineering principle vocabulary (implicit in CLAUDE.md but without handles).

Four targeted changes land in three locations, each chosen for maximum trigger visibility and minimum maintenance surface.

## Goals

- Upgrade the PDF reading protocol so Claude receives Markdown output with preserved table and layout structure when reading academic papers.
- Add a trigger that prompts ADR authoring at the decision point, via an extension to the existing `mad-scientist-skills:final-review` skill, backed by a repo-local template and historical examples.
- Add a pre-change measurement discipline for perf-sensitive code via a new `mad-scientist-skills:measure-before-optimize` skill, designed as a peer to the existing retrospective `optimization-audit`.
- Introduce named engineering principle vocabulary (Shift Left, Chesterton's Fence, Hyrum's Law) to user-global `CLAUDE.md` for session-wide self-triggering.

## Non-goals

- No changes to lakehouse runtime code, ingestion pipelines, Spark jobs, Delta tables, or any `src/` Python module.
- No changes to `claude-plugins-official` (superpowers) — third-party, read-only.
- No changes to `mad-skills` plugin — wrong domain.
- No new standalone skill for "context engineering" — rejected during brainstorming as overlapping with existing `brainstorming` / `writing-plans` disciplines.
- No git commits on any repo. Changes stay local until the user explicitly approves committing.
- No cross-project generalisation of the ADR template in this cycle — template and historical examples stay repo-local; if the pattern proves useful, it can be promoted to a shared skill later.

---

## Item 1 — PDF reading upgrade (docling primary, pypdf fallback)

### Current state (verified)

- User-global `C:\Users\Karsten\.claude\CLAUDE.md` contains a `## Reading PDFs` section prescribing a `pypdf` one-liner invoked via `python -c "..."` with a `sys.stdout = io.TextIOWrapper(..., encoding='utf-8', errors='replace')` wrapper.
- Project-local memory at `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse-d32\memory\feedback_pdf_reading.md` duplicates the same pattern with additional prose (reason, how-to-apply).
- `pypdf` produces plain text extraction only. Tables flatten into linearised runs of whitespace-separated cells, multi-column layouts interleave, equations disappear, figure captions merge into body text.
- User confirmed mid-session: "you seem to struggle with pdf at times."
- Verified in this session: `uv run --with docling python -c "from docling.document_converter import DocumentConverter; print('OK')"` installs 103 packages in ~10s cold and imports cleanly. Pure Python — no Java, no JVM.

### Approach

Replace the primary PDF reading path with `docling` (IBM Research, pure Python, LLM-optimised Markdown output). Keep `pypdf` as an explicit fallback for plain-text-only PDFs.

Canonical Markdown extraction one-liner:

```bash
uv run --with docling python -c "
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
from docling.document_converter import DocumentConverter
doc = DocumentConverter().convert(r'<PATH>').document
print(doc.export_to_markdown())
"
```

Fallback condition for `pypdf`:
- The PDF is explicitly known to be plain-text-only (no tables, no multi-column, no equations).
- The user explicitly asks to use pypdf for a specific read.
- docling install fails in an unusual environment.

### File changes

| File | Change |
|---|---|
| `C:\Users\Karsten\.claude\CLAUDE.md` | Rewrite the `## Reading PDFs` section: docling primary with the one-liner above; pypdf as explicit fallback with its existing one-liner; short paragraph explaining when to use which. |
| `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse-d32\memory\feedback_pdf_reading.md` | Rewrite as a one-paragraph pointer: "See user-global CLAUDE.md `## Reading PDFs`. Primary: docling for tables and structured content. Fallback: pypdf for plain-text-only PDFs." |

### Risks

- **Cold install overhead.** First PDF read in a fresh uv env pulls ~103 packages in ~10 seconds. Warm reads are instant. Trade-off: ~10s one-time cost per fresh env per session in exchange for clean table and layout extraction. Acceptable.
- **Docling fails on scanned PDFs.** No embedded text layer → docling returns empty. Mitigation: the fallback language names this case explicitly; pypdf will also fail on scanned PDFs but with a clearer error. Neither tool replaces an OCR pipeline.
- **Table edge cases.** Heavily merged-cell or irregular-structure tables may not extract cleanly. Mitigation: the rule is "primary default," not "never use pypdf" — operator judgment still applies when output looks wrong.

### Verification

Next PDF handed to Claude should trigger the docling one-liner first. Output should contain Markdown tables (`| col | col |`) when the source PDF has tables. If the first attempt produces empty or corrupted output, the documented fallback path is to retry with pypdf.

---

## Item 2 — ADR writing hybrid

### Current state (verified)

- This repo has exactly one ADR: `docs/superpowers/adrs/ADR-001-evolve-code-execution.md`, written ad-hoc when setting up the evolve engine's exec sandbox (documented in CLAUDE.md security-hardening section).
- No `ADR-TEMPLATE.md` exists.
- No trigger mechanism for "did this change introduce an architectural decision worth documenting?"
- Five recent architectural decisions in the last ~30 days that could have been ADRs but weren't:
  1. EFPI algorithm reimplementation to avoid `unravelsports` dependency (Python 3.11+ conflict with Databricks Serverless Python 3.10 lock)
  2. Guard injection as a mandatory (no-default) `FilterResult` parameter in every `run_pipeline()`, enforced by `test_guard_conformance.py`
  3. `dbt-owners-{env}` group ownership model for `dev_silver` / `dev_gold` schemas (solves the "developer user vs. ingestion SP can't both rebuild" problem)
  4. `DATABRICKS_HTTP_PATH` double-slash convention (Git Bash MSYS workaround for single-slash path mangling)
  5. System-table access via definer's-rights views in `soccer_analytics.observability` (solves the "cannot grant system.* to SP" problem)
- These are captured as prose in CLAUDE.md and in various design specs, but lack the structured ADR format (context, decision, consequences, alternatives, date, status).

### Approach

Three coordinated pieces. **No new standalone skill.**

**2a. Extend `mad-scientist-skills:final-review` with an ADR sub-phase.**

Insert a new sub-phase 2.5 "Architectural Decision Review" between the existing Phase 2 (Code Quality Review) and Phase 3 (Documentation Review) in `SKILL.md`.

Sub-phase content (concise — target ≤25 lines):

- Scan the change for architectural decisions matching any of these patterns:
  - Introduces, removes, or replaces a cross-cutting dependency
  - Changes a schema ownership or grants model
  - Hard-codes a workaround for a platform constraint (DBR serverless, MSYS, etc.)
  - Introduces a naming, identifier, or path convention with downstream consumers
  - Reimplements an algorithm to avoid a dependency
  - Introduces a defense-in-depth control or security boundary
- For each decision matched, ask: "Is this documented in an ADR?"
- If no ADR exists: prompt the user to draft one using the repo-local template before commit. Draft inline if the user approves.
- If a stale ADR exists: update its Status field and Consequences section.

**2b. Add a repo-local ADR template at `docs/superpowers/adrs/ADR-TEMPLATE.md`.**

Standard Michael Nygard format plus the "Alternatives considered" section (non-standard but valuable for this repo because several ADRs boil down to "we chose option C over A, B, D"):

```markdown
# ADR-NNN: <Title>

| Field | Value |
|---|---|
| **Date** | YYYY-MM-DD |
| **Status** | Proposed / Accepted / Deprecated / Superseded by ADR-MMM |
| **Deciders** | <names> |

## Context

What problem are we solving? What constraints apply? What is the forcing function?

## Decision

What did we decide? One or two sentences, no hedging.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. ... | ... | ... | ... |
| B. ... | ... | ... | ... |
| C. ... (chosen) | ... | ... | — |

## Consequences

### Positive

- What gets better or becomes possible.

### Negative

- What gets worse, what debt we accept, what we lose.

### Neutral

- Side effects worth noting.

## Related

- Commits: <sha>, <sha>
- Specs: `docs/superpowers/specs/...`
- Issues / PRs: #NNN
- ADRs: supersedes ADR-XXX, superseded by ADR-YYY
```

**2c. Add a "When to write an ADR" section to this repo's `CLAUDE.md`.**

New section under a natural anchor (near "Architecture Principles" or "Project Conventions"). Content:

- One paragraph describing when ADRs are warranted (decisions that future maintainers would reasonably ask "why?" about).
- A bulleted list of the five historical examples above as concrete pattern-match anchors.
- A single-line pointer: "Template: `docs/superpowers/adrs/ADR-TEMPLATE.md`. Existing ADRs: `docs/superpowers/adrs/ADR-*.md`."

The historical examples are the key value-add here — they turn an abstract "write an ADR when it's architecturally significant" rule into pattern-matchable concrete prior art.

### File changes

| File | Change |
|---|---|
| `D:\Development\karstenskyt__mad-scientist-skills\plugins\mad-scientist-skills\skills\final-review\SKILL.md` | Insert new sub-phase 2.5 between current Phase 2 and Phase 3. Update the Phase 5 summary checklist to include an ADR row. |
| `D:\Development\karstenskyt__luxury-lakehouse-d32\docs\superpowers\adrs\ADR-TEMPLATE.md` | New file — Nygard-format template with Alternatives-considered section, as shown above. |
| `D:\Development\karstenskyt__luxury-lakehouse-d32\CLAUDE.md` | Add new section "When to write an ADR" with the five historical examples. |

### Risks

- **`final-review` bloat.** Adding a new sub-phase risks making the skill unwieldy. Mitigation: keep sub-phase 2.5 ≤25 lines, defer full guidance to the template and CLAUDE.md section.
- **False positives on the decision trigger.** The heuristic patterns will flag some changes that don't actually need an ADR. Mitigation: the check is a prompt, not a block. Operator judgment decides.
- **ADR sprawl over time.** The adrs/ folder could become noisy. Mitigation: each ADR has a Status field, a periodic review is deferred to a future cycle, and the template includes Superseded-by semantics.
- **Extension only helps when `final-review` runs.** If the user never invokes `final-review`, the trigger never fires. Mitigation: accepted — `final-review` is already the canonical pre-commit gate in this workflow, and the CLAUDE.md section serves as a passive trigger.

### Verification

1. Run `final-review` on any branch. Sub-phase 2.5 should appear and ask about ADR-worthy decisions.
2. Open `docs/superpowers/adrs/ADR-TEMPLATE.md` — should exist and be valid Markdown.
3. Search CLAUDE.md for "When to write an ADR" — should match, with the five historical examples visible.

---

## Item 3 — Measure-before-optimise skill

### Current state (verified)

- `mad-scientist-skills` currently has 8 skills: `architecture-audit`, `c4`, `cognitive-interface-audit`, `documentation-audit`, `final-review`, `observability-audit`, `optimization-audit`, `security-audit`.
- Seven are audits (retrospective), one is a diagram generator (`c4`), one is a pre-commit quality gate (`final-review`).
- `optimization-audit` is explicitly retrospective: "scan this codebase for performance anti-patterns, inefficient algorithms, N+1 queries, missing caching, concurrency issues." Fires *after* code exists.
- This repo has rich performance infrastructure already:
  - `docs/performance-baselines.md` — hand-maintained benchmark medians for 11 critical-path functions
  - `pytest-benchmark` wrappers on benchmarked functions in `src/tests/`
  - `CLAUDE.md` "Performance Budgets" section with explicit ≤5ms / ≤2ms / ≤1ms budgets
  - Memory entries about avoiding driver-bound loops and using `applyInPandas`
- What's MISSING: a discipline that fires *before* a perf-sensitive code change, captures a baseline, and enforces a delta comparison after the change.

### Approach

Add a new skill: `mad-scientist-skills:measure-before-optimize`.

**Skill frontmatter description** (designed to not collide with `optimization-audit`):

> Pre-change measurement gate for perf-sensitive functions. Use BEFORE modifying a function that has a `pytest-benchmark` test, appears in a performance baselines file, or is flagged as a hot path in CLAUDE.md. Captures a baseline median and p95, verifies the change does not regress beyond a configurable threshold, and records the delta. Peer skill to `optimization-audit` — this one is pre-change; that one is retrospective.

**Workflow (runs inside the skill):**

1. **Identify the measurement surface.**
   - Read the project's benchmarks file (default: `docs/performance-baselines.md`). Parse the table for function names.
   - Cross-reference with `pytest-benchmark` test names in `src/tests/test_*.py` via grep.
   - Build a set of "measured functions."

2. **Confirm the function being touched is in the measurement surface.**
   - If yes: proceed with the full flow.
   - If no: warn the user that the function is not currently measured and offer to add a benchmark. Do not block.

3. **Capture baseline.**
   - Run the matching `pytest-benchmark` test via `uv run pytest <test_path> --benchmark-only --benchmark-min-rounds=3 --benchmark-json=<scratch_path>`.
   - Write scratch file to `tempfile.gettempdir()` — NOT the project root — to avoid accidental commits.
   - Record median, p95, iteration count.

4. **Claude makes the planned change.** (Out of the skill's direct control — the skill yields to the main agent here and re-activates at verification time.)

5. **Re-run the benchmark.** Same command, same scratch file suffix.

6. **Compare and report.**

   ```
   ## Measure-before-optimize report

   Function: compute_pitch_control_at_points
   Budget: ≤5 ms
   Baseline median: 347 µs
   New median: 362 µs
   Delta: +4.3% (within threshold)
   Budget status: within budget (7.2% of 5 ms)
   ```

7. **If regression > threshold:** escalate to the user with the full delta and ask whether to proceed, revert, or investigate.

**Skill parameters (natural language, resolved inside the skill):**
- `baselines_file`: path to benchmarks markdown or JSON (default: `docs/performance-baselines.md`)
- `regression_threshold`: default 10%
- `budget_enforcement`: `warn` (default) or `block`

### Comparison to existing `optimization-audit`

| Attribute | `optimization-audit` | `measure-before-optimize` |
|---|---|---|
| Timing | Retrospective (after code exists) | Pre-change gate |
| Trigger | "Audit this codebase for perf issues" | "About to touch a measured function" |
| Output | Audit report with issues | Before/after delta, regression flag |
| Scope | Whole codebase | Single function / small change |
| Action | Recommends fixes | Gates the change |

These are designed as **peers**, not overlapping. `optimization-audit` finds problems; `measure-before-optimize` prevents new ones.

### File changes

| File | Change |
|---|---|
| `D:\Development\karstenskyt__mad-scientist-skills\plugins\mad-scientist-skills\skills\measure-before-optimize\SKILL.md` | New file — full skill definition with frontmatter, trigger description, workflow, parameters, comparison to `optimization-audit`, examples. |
| `D:\Development\karstenskyt__luxury-lakehouse-d32\CLAUDE.md` | Add a short paragraph under "Performance Budgets" pointing to the skill: "Before modifying any function listed above or any function with a `pytest-benchmark` wrapper, invoke `mad-scientist-skills:measure-before-optimize`." |

**No `templates/` subfolder for this skill** — the baseline file format is JSON from `pytest-benchmark`, which is well-documented and doesn't need a template. Keep the skill self-contained.

### Risks

- **Skill discoverability collision with `optimization-audit`.** Mitigation: distinct trigger language in the frontmatter description (see above — `optimization-audit` starts with "Comprehensive optimization audit," the new skill starts with "Pre-change measurement gate"). The Skill tool matches descriptions, so distinct framing is load-bearing.
- **False negatives.** The skill only fires if the function is in a known measurement surface. If the user adds a new hot-path function without adding it to `docs/performance-baselines.md`, the skill won't fire. Mitigation: Item 2a's sub-phase 2.5 in `final-review` can include "new hot-path function added without baseline" as a decision-type pattern. Known limitation, explicitly documented in the skill.
- **Benchmark run time.** Some benchmarks are slow. Mitigation: the skill uses `--benchmark-min-rounds=3` by default for fast checks and documents `--benchmark-min-rounds=10` for more stable measurements when needed.
- **Scratch file pollution.** The baseline JSON file could end up in the project root if not careful. Mitigation: skill writes to `tempfile.gettempdir()` (e.g. `%TEMP%` on Windows, `/tmp/` on Linux), never to the project root.
- **Regression threshold is a single number.** A 10% default doesn't capture budget-aware regressions (e.g. a function at 70% of budget can absorb a 20% regression without blowing the budget; a function at 95% cannot). Mitigation: the skill reports both delta-vs-baseline AND position-vs-budget in the comparison output, and the operator makes the final call. The threshold is a prompt, not a block.

### Verification

1. After the SKILL.md file is created, invoke `Skill mad-scientist-skills:measure-before-optimize` manually. The skill should load cleanly.
2. Trigger phrase test: describe a scenario "I'm about to modify `compute_pitch_control_at_points` to use a faster algorithm." The skill should activate and request a baseline capture.
3. Parameter resolution test: the skill should default `baselines_file` to `docs/performance-baselines.md` without explicit instruction.

---

## Item 4 — Named engineering principles glossary

### Current state (verified)

- User-global `C:\Users\Karsten\.claude\CLAUDE.md` has an "Engineering Standards" section (SOLID, clean code, security, type safety) but no named-principle vocabulary.
- CLAUDE.md and the superpowers skill set already apply these principles implicitly — "Failure Investigation Protocol" is Shift Left in spirit, "Investigate don't assume" is Chesterton's Fence in spirit, etc. — but without the named handles, Claude can't reference them in self-explanation or flag them as a failure mode ("I was about to violate Chesterton's Fence").
- The Osmani article proposes three principles as a minimum: Shift Left, Chesterton's Fence, Hyrum's Law.

### Approach

Add a new section to user-global `C:\Users\Karsten\.claude\CLAUDE.md` titled "Engineering Principles Glossary" with three entries. Each entry is 2–3 sentences: principle statement, practical application, optional concrete example.

**Draft content:**

```markdown
## Engineering Principles Glossary

Three named disciplines that shape how Claude approaches code changes in any project.
Use these as self-triggers and as explicit names when describing reasoning.

- **Shift Left.** Push quality checks (lint, type check, tests, security scan, benchmarks) as
  early as possible in the change cycle. In practice: run `ruff` + `pyright` + unit tests
  BEFORE declaring work complete, not after CI fails. Every check that catches an issue
  locally is a check that did not page a human on a shared pipeline.

- **Chesterton's Fence.** Never remove a piece of code, config, guard, or convention you
  do not fully understand. Find out WHY it exists first — `git log`, `git blame`, ADRs,
  surrounding comments, related tests. Removing an unfamiliar control because it seems
  redundant is how production outages start. The fence was put there for a reason; prove
  the reason no longer applies before you take it down.

- **Hyrum's Law.** With sufficient users, every observable behavior of your system will
  be depended on by somebody. In practice: changing a return type, a log format, an error
  message, a schema field, a file path, or a function name is an API break even if the
  "public API" technically did not change. When touching a widely-read output format, ask
  "who else is consuming this?" before assuming it is safe.
```

**Total section: ~20 lines.** No code, no skill, no plugin changes.

### File changes

| File | Change |
|---|---|
| `C:\Users\Karsten\.claude\CLAUDE.md` | Add new section "Engineering Principles Glossary" after the existing "Engineering Standards" section. |

### Risks

- **Vocabulary drift across projects.** Different codebases use the same principle names with slightly different scopes. Mitigation: the glossary is user-global, not project-local, so the definitions are stable across all projects and the rule is "these are my working definitions."
- **Cognitive-load noise.** Three more concepts in CLAUDE.md adds reading surface at session start. Mitigation: short section, clearly delimited, high signal per word. Total cost ≤20 lines.
- **Abstraction without concrete anchoring.** A principle without a concrete example is hard to pattern-match against. Mitigation: each entry includes a "In practice:" clause with a concrete action.

### Verification

After the edit, next session start should load the glossary as part of CLAUDE.md. Confirmation test: in a future session, explicitly ask Claude to "explain why you're not deleting that guard function" — the response should reference Chesterton's Fence by name.

---

## Cross-cutting concerns

### Distribution map

| Change | Lives in | Travels to |
|---|---|---|
| #1 PDF upgrade | User-global + project memory | Every Claude Code session, any project |
| #2a ADR hybrid (skill extension) | `karstenskyt__mad-scientist-skills` | Every project using the plugin |
| #2b ADR template | This repo | This repo only |
| #2c CLAUDE.md "When to write an ADR" | This repo | This repo only |
| #3 measure-before-optimize skill | `karstenskyt__mad-scientist-skills` | Every project using the plugin |
| #3 CLAUDE.md pointer | This repo | This repo only |
| #4 Principles glossary | User-global | Every Claude Code session, any project |

Three repos touched total:
1. `D:\Development\karstenskyt__luxury-lakehouse-d32` (this repo) — 3 files
2. `D:\Development\karstenskyt__mad-scientist-skills` (sibling) — 2 files
3. `C:\Users\Karsten\.claude\` (user home) — 2 files

### File surface (total)

| # | Location | File | Kind |
|---|---|---|---|
| 1 | user-global | `C:\Users\Karsten\.claude\CLAUDE.md` | Edit — `## Reading PDFs` rewrite + new `## Engineering Principles Glossary` section |
| 2 | project memory | `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse-d32\memory\feedback_pdf_reading.md` | Edit — simplify to pointer |
| 3 | mad-scientist-skills | `D:\Development\karstenskyt__mad-scientist-skills\plugins\mad-scientist-skills\skills\final-review\SKILL.md` | Edit — insert sub-phase 2.5, update Phase 5 checklist |
| 4 | this repo | `docs\superpowers\adrs\ADR-TEMPLATE.md` | New file |
| 5 | this repo | `CLAUDE.md` | Edit — add "When to write an ADR" section AND the measure-before-optimize pointer (one combined edit) |
| 6 | mad-scientist-skills | `D:\Development\karstenskyt__mad-scientist-skills\plugins\mad-scientist-skills\skills\measure-before-optimize\SKILL.md` | New file |

**Total unique files: 6.** (CLAUDE.md in this repo receives two additions in one edit pass; user-global CLAUDE.md similarly receives two additions in one edit pass.)

### Commit discipline (per user instruction)

**No commits on any repo.** All edits are local and left uncommitted. The user reviews the file system changes before authorising any commit decision, separately per repo.

### Rollback

- All edits are local file changes on three repos. Rollback = `git restore <file>` on each edit in each repo before any commit occurs.
- No runtime state, no database changes, no network side effects.
- If a skill proves unwanted, the file can be deleted from `plugins/mad-scientist-skills/skills/<skill-name>/` and the plugin will no longer expose it. No stale state persists.

### Out of scope (explicit)

- No changes to `src/`, `dbt_project/`, `terraform/`, workflow cards, model cards, or `NOTICE`.
- No changes to any audit skill other than `final-review` (for Item 2a).
- No changes to the `c4` skill.
- No changes to the `mad-skills` plugin.
- No changes to `claude-plugins-official` (superpowers).
- No git commits.
- No PR creation.
- No skill removals.
- No context-engineering skill (explicitly rejected during brainstorming).

---

## Order of operations

Implementation order chosen to minimise risk of leaving partial state and to keep related edits together:

1. **Item 1 + Item 4 → one edit to user-global `CLAUDE.md`.** Rewrite `## Reading PDFs`, add `## Engineering Principles Glossary`. Single atomic edit. Lowest-risk location, highest immediate value.
2. **Item 1 → edit project memory `feedback_pdf_reading.md`.** Simplify to pointer. Standalone edit.
3. **Item 2b → create `docs/superpowers/adrs/ADR-TEMPLATE.md`.** New file. No dependencies.
4. **Item 2c + Item 3 pointer → one edit to this repo's `CLAUDE.md`.** Add "When to write an ADR" section and the measure-before-optimize pointer in a single atomic edit.
5. **Item 2a → edit `mad-scientist-skills:final-review:SKILL.md`.** Insert sub-phase 2.5.
6. **Item 3 → create `mad-scientist-skills:measure-before-optimize:SKILL.md`.** New file.

End state: 6 files touched, zero commits, ready for user review.

---

## Open questions (low-stakes)

1. **Item 2a placement: new sub-phase 2.5 vs. checkbox inside Phase 2?** Current plan: new sub-phase 2.5. Rationale: ADR review is distinct from code quality review, and separation makes the gate more discoverable. Defaulting to new sub-phase unless the user prefers otherwise.

2. **Item 3 templates/ folder: include or not?** Current plan: no templates/, baseline file format inlined as an example in SKILL.md. Keeps the skill self-contained. Can be added later if the example grows.

3. **Mad-scientist-skills version bump and CHANGELOG.** Current plan: update `CHANGELOG.md` at the repo root if it exists (verified: it does — `D:\Development\karstenskyt__mad-scientist-skills\CHANGELOG.md`). Add a new dated entry for this cycle noting the `final-review` extension and the new `measure-before-optimize` skill. No plugin version bump unless the CHANGELOG convention requires one.

These are decidable during implementation without blocking. Defaults above will be followed unless the user overrides.

# Workflow Tooling Upgrade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade Claude's PDF reading to preserve tables and layout; add ADR authoring trigger + template + historical examples; add a pre-change measurement discipline as a new mad-scientist-skills peer skill; introduce named engineering principle vocabulary. Seven tasks across three repos. Zero commits.

**Architecture:** Changes land in three repos by design:
- **User-global** (`C:\Users\Karsten\.claude\CLAUDE.md`) — cross-project concerns (PDF reading, principle vocabulary)
- **This repo** (`D:\Development\karstenskyt__luxury-lakehouse-d32`) — repo-specific concerns (ADR template, historical examples, measure-before-optimize pointer)
- **Mad-scientist-skills sibling repo** (`D:\Development\karstenskyt__mad-scientist-skills`) — cross-project plugin changes (`final-review` extension, new `measure-before-optimize` skill, CHANGELOG entry)

**Tech Stack:** Markdown files, Python (for YAML frontmatter validation), Bash, uv for docling verification. No runtime code changes, no tests to run, no CI to trigger.

**Constraints (from user instructions, verbatim):**
- **NO commits on any repo.** Each task ends with a "report to user" checkpoint, not a commit. User reviews file-system state before any commit decision.
- **Local changes to mad-scientist-skills are authorised** — tasks 4-7 can edit files in `D:\Development\karstenskyt__mad-scientist-skills\`.
- All changes must be reversible via `git restore` if rejected.

**Source spec:** `docs/superpowers/specs/2026-04-14-workflow-tooling-upgrade-design.md` (approved by user 2026-04-14)

---

## Task order rationale

The seven tasks are ordered to minimise the window in which a reference points to a not-yet-existing target:

1. User-global `CLAUDE.md` (PDF + principles) — no dependencies
2. Project memory `feedback_pdf_reading.md` — references Task 1
3. `ADR-TEMPLATE.md` (new file) — no dependencies
4. `final-review` SKILL.md extension — no dependencies (references the template abstractly, not by path)
5. `measure-before-optimize` SKILL.md (new file) — no dependencies
6. This repo's `CLAUDE.md` — references the template from Task 3 AND the skill from Task 5, so runs after both
7. `mad-scientist-skills` CHANGELOG entry — references the plugin changes from Tasks 4 and 5, so runs last

---

## Task 1: Update user-global CLAUDE.md — Reading PDFs + Engineering Principles Glossary

**Files:**
- Modify: `C:\Users\Karsten\.claude\CLAUDE.md`

**Rationale:** Two changes to the same file — rewrite the existing `## Reading PDFs` section (docling primary, pypdf fallback) AND append a new `## Engineering Principles Glossary` section. One atomic `Edit` call covers both.

### Steps

- [ ] **Step 1: Read the current file**

Run: `Read` tool on `C:\Users\Karsten\.claude\CLAUDE.md`

Purpose: Confirm exact current text of the "Reading PDFs" section (needed as `old_string` for the Edit call) and identify the anchor for inserting the new Engineering Principles Glossary section.

Expected: File loads cleanly, contains `## Reading PDFs` section with pypdf one-liner.

- [ ] **Step 2: Verify docling installs in ephemeral uv env**

Run:
```bash
uv run --with docling python -c "from docling.document_converter import DocumentConverter; print('docling import OK')"
```

Expected output: `docling import OK` (after any initial install output). This was verified once earlier in the session, but re-verify because the install is what the new protocol depends on.

- [ ] **Step 3: Make the atomic edit**

Use the `Edit` tool with `C:\Users\Karsten\.claude\CLAUDE.md`.

**`old_string`:** The exact full text of the current `## Reading PDFs` section, from the `## Reading PDFs` heading through (and including) the closing paragraph ending with "re-read the missing pages before proceeding." Include the blank line separators before the next `## UI Implementation` heading.

**`new_string`:** Both the rewritten `## Reading PDFs` section AND the appended `## Engineering Principles Glossary` section, as below:

```markdown
## Reading PDFs

The built-in Read tool cannot read PDFs on this machine (`pdftoppm` not installed). Use one of the two patterns below.

**Primary — docling (pure Python, LLM-optimised Markdown).** Use for any PDF that may contain tables, multi-column layouts, equations, or figure captions. Produces Markdown with preserved structure.

```bash
uv run --with docling python -c "
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
from docling.document_converter import DocumentConverter
doc = DocumentConverter().convert(r'<PATH>').document
print(doc.export_to_markdown())
"
```

First invocation in a fresh uv env pulls ~103 packages (~10 s cold). Warm reads are instant. No Java runtime, no JVM.

**Fallback — pypdf (plain text only).** Use when the PDF is explicitly known to be plain-text-only (no tables, no multi-column, no equations), or when docling fails for a non-OCR reason. Produces plain text — tables flatten, multi-column layouts interleave, equations disappear.

```bash
python -c "
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
from pypdf import PdfReader
r = PdfReader(r'<PATH>')
for i, p in enumerate(r.pages):
    print(f'--- PAGE {i+1} ---')
    print(p.extract_text())
"
```

**Both tools:** For long PDFs, batch by page range to avoid Bash output truncation. **Every page must be read.** Partial reads are unacceptable — if output is truncated, re-read the missing pages before proceeding. Neither tool does OCR — scanned PDFs with no embedded text layer will return empty from both.

## Engineering Principles Glossary

Three named disciplines that shape how Claude approaches code changes in any project. Use these as self-triggers and as explicit names when describing reasoning.

- **Shift Left.** Push quality checks (lint, type check, tests, security scan, benchmarks) as early as possible in the change cycle. In practice: run `ruff` + `pyright` + unit tests BEFORE declaring work complete, not after CI fails. Every check that catches an issue locally is a check that did not page a human on a shared pipeline.

- **Chesterton's Fence.** Never remove a piece of code, config, guard, or convention you do not fully understand. Find out WHY it exists first — `git log`, `git blame`, ADRs, surrounding comments, related tests. Removing an unfamiliar control because it seems redundant is how production outages start. The fence was put there for a reason; prove the reason no longer applies before you take it down.

- **Hyrum's Law.** With sufficient users, every observable behaviour of a system will be depended on by somebody. In practice: changing a return type, a log format, an error message, a schema field, a file path, or a function name is an API break even if the "public API" technically did not change. When touching a widely-read output format, ask "who else is consuming this?" before assuming it is safe.
```

- [ ] **Step 4: Report to user — checkpoint**

Report to the user:
> "Task 1 complete. User-global `~/.claude/CLAUDE.md` updated: `## Reading PDFs` rewritten with docling primary and pypdf fallback; new `## Engineering Principles Glossary` section appended. docling install verified. No commit. Proceed to Task 2?"

Wait for user acknowledgement before moving to Task 2.

---

## Task 2: Simplify project memory — feedback_pdf_reading.md

**Files:**
- Modify: `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse-d32\memory\feedback_pdf_reading.md`

**Rationale:** The existing memory file duplicates the pypdf instructions already in user-global CLAUDE.md. Since Task 1 made user-global CLAUDE.md the authoritative source for PDF reading, this file becomes a one-line pointer. Prevents drift between two copies of the same rule.

### Steps

- [ ] **Step 1: Read the current file**

Run: `Read` tool on `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse-d32\memory\feedback_pdf_reading.md`

Purpose: Confirm the current frontmatter and body so we preserve the frontmatter but replace the body.

Expected: File contains the frontmatter (`name`, `description`, `type: feedback`, `originSessionId`) and a long body explaining pypdf with UTF-8 wrapper.

- [ ] **Step 2: Rewrite the file with simplified body**

Use the `Write` tool to overwrite the file (we are keeping the frontmatter shape but updating both the description and the body — `Write` is cleaner than multiple `Edit` calls).

Full new content:

```markdown
---
name: PDF reading — see user-global CLAUDE.md
description: PDF reading protocol lives in user-global CLAUDE.md. Primary tool is docling (Markdown output, preserves tables). Fallback is pypdf (plain text only). This memory is a pointer to avoid drift.
type: feedback
originSessionId: 07defb3d-b6e7-4a92-a6ec-716af102ad7f
---

The canonical PDF reading protocol lives in user-global `C:\Users\Karsten\.claude\CLAUDE.md` under the `## Reading PDFs` section.

**Primary:** docling via `uv run --with docling` — produces Markdown with preserved tables and multi-column layout.

**Fallback:** pypdf via `uv run --with pypdf` — plain text only, use only when the PDF is explicitly known to be plain-text-only or when docling fails for a non-OCR reason.

**Why this memory exists:** Prevents drift between two copies of the same rule. User-global CLAUDE.md is the authoritative source; this memory file records the project-specific historical context (repeated PDF struggles, 2026-04 upgrade) without duplicating the protocol itself.
```

- [ ] **Step 3: Report to user — checkpoint**

Report to the user:
> "Task 2 complete. Project memory `feedback_pdf_reading.md` simplified to a pointer at the user-global protocol. No commit. Proceed to Task 3?"

Wait for user acknowledgement before moving to Task 3.

---

## Task 3: Create ADR template — docs/superpowers/adrs/ADR-TEMPLATE.md

**Files:**
- Create: `D:\Development\karstenskyt__luxury-lakehouse-d32\docs\superpowers\adrs\ADR-TEMPLATE.md`

**Rationale:** Michael Nygard-format ADR template with a non-standard "Alternatives considered" section. The alternatives table is load-bearing for this repo because several ADRs reduce to "we chose option C over A, B, D" and the rejection rationale matters for future maintainers.

### Steps

- [ ] **Step 1: Confirm the target directory exists**

Run: `Glob` with pattern `docs/superpowers/adrs/**/*.md`

Expected: One match — `docs\superpowers\adrs\ADR-001-evolve-code-execution.md`. Confirms the directory exists and contains the existing single ADR, so the new template file can land next to it.

- [ ] **Step 2: Write the template file**

Use the `Write` tool on `D:\Development\karstenskyt__luxury-lakehouse-d32\docs\superpowers\adrs\ADR-TEMPLATE.md`.

Full content:

````markdown
# ADR-NNN: <Title>

| Field | Value |
|---|---|
| **Date** | YYYY-MM-DD |
| **Status** | Proposed / Accepted / Deprecated / Superseded by ADR-MMM |
| **Deciders** | <names> |

## Context

What problem are we solving? What constraints apply? What is the forcing function? Keep this to 2–4 short paragraphs. Include concrete numbers where relevant (row counts, latency budgets, version locks, platform constraints).

## Decision

What did we decide? One or two sentences, no hedging. A future maintainer should be able to read this sentence in isolation and know what the decision was.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. <option> | <short list> | <short list> | <one line> |
| B. <option> | <short list> | <short list> | <one line> |
| C. <chosen> | <short list> | <short list> | — |

This section is the part future maintainers wonder about most. Be concrete about what you looked at and why you did not choose it. If option A was rejected because of a specific version constraint, name the version. If option B was rejected because of a benchmark, cite the benchmark.

## Consequences

### Positive

- What gets better or becomes possible.
- Concrete capabilities unlocked.

### Negative

- What gets worse, what debt we accept, what we lose.
- What future-us will need to maintain because of this choice.

### Neutral

- Side effects worth noting but not valenced.

## Related

- **Commits:** `<sha>`, `<sha>`
- **Specs:** `docs/superpowers/specs/YYYY-MM-DD-<name>-design.md`
- **Issues / PRs:** `#NNN`
- **ADRs:** supersedes `ADR-XXX`, superseded by `ADR-YYY`
- **External references:** links to library docs, platform release notes, incident postmortems

## Notes

Optional. Use this section for supporting evidence, benchmark output, experiment results, or anything else that does not fit the sections above but would help a maintainer understand the decision.
````

- [ ] **Step 3: Report to user — checkpoint**

Report to the user:
> "Task 3 complete. `docs/superpowers/adrs/ADR-TEMPLATE.md` created. Nygard format with Alternatives-considered table. No commit. Proceed to Task 4?"

Wait for user acknowledgement before moving to Task 4.

---

## Task 4: Extend mad-scientist-skills:final-review with Phase 2.5 Architectural Decision Review

**Files:**
- Modify: `D:\Development\karstenskyt__mad-scientist-skills\plugins\mad-scientist-skills\skills\final-review\SKILL.md`

**Rationale:** The final-review skill is already the canonical pre-commit quality gate in this workflow. Adding a new standalone `adr-writing` skill would dilute the plugin's tight audit/review thematic identity. Extending the existing gate preserves the identity and puts the trigger at the exact moment it needs to fire — right before commit.

### Steps

- [ ] **Step 1: Read the current SKILL.md**

Run: `Read` tool on `D:\Development\karstenskyt__mad-scientist-skills\plugins\mad-scientist-skills\skills\final-review\SKILL.md`

Purpose: Confirm the current phase structure (I read this earlier in the session — Phases 1-5 with specific content). Need exact `old_string` for inserting Phase 2.5 and updating Phase 5 checklist.

Expected: File has frontmatter (`name: final-review`, description), then phases 1-5 with headings `### Phase 1: Codebase Discovery`, `### Phase 2: Code Quality Review`, `### Phase 3: Documentation Review`, `### Phase 4: Architecture Diagram`, `### Phase 5: Verification Summary`.

- [ ] **Step 2: Insert Phase 2.5 between Phase 2 and Phase 3**

Use the `Edit` tool.

**`old_string`:** The last paragraph/line of Phase 2's content plus the `### Phase 3: Documentation Review` heading — enough context to make the insertion point unique.

Specifically, the boundary: from the end of Phase 2's severity table (`| **Low** | Track in backlog | Best practice deviation, minor polish |`) through the `### Phase 3: Documentation Review` heading.

**`new_string`:** The same boundary content with Phase 2.5 inserted in the middle:

```markdown
| **Low** | Track in backlog | Best practice deviation, minor polish |

### Phase 2.5: Architectural Decision Review

Scan the change for architectural decisions that future maintainers will reasonably ask "why?" about. Decisions matching any of these patterns are ADR-worthy:

- Introduces, removes, or replaces a cross-cutting dependency
- Changes a schema ownership or grants model
- Hard-codes a workaround for a platform constraint (Databricks Serverless, MSYS path handling, etc.)
- Introduces a naming, identifier, or path convention with downstream consumers
- Reimplements an algorithm to avoid a dependency
- Introduces a defense-in-depth control or security boundary

For each decision matched, ask: "Is this documented in an ADR?"

- **No ADR exists**: prompt the user to draft one using the project's ADR template (commonly `docs/adrs/ADR-TEMPLATE.md` or `docs/superpowers/adrs/ADR-TEMPLATE.md`) before commit. If the user approves, draft the ADR inline during final-review.
- **Stale ADR exists**: update the existing ADR's Status field and Consequences section to reflect the current change.
- **Current ADR exists**: confirm and move to Phase 3.

This sub-phase is a prompt, not a block. Operator judgment decides whether a decision rises to ADR-worthiness. The check is a decision inventory, not a gate.

### Phase 3: Documentation Review
```

- [ ] **Step 3: Update the Phase 5 verification summary checklist**

Use a second `Edit` call on the same file.

**`old_string`:** The current Phase 5 checklist template, specifically the `### Architecture Diagram` section through the `### Issues Found` heading:

```markdown
### Architecture Diagram
- [x] architecture.html generated/updated
- Levels included: Context, Container, Component

### Issues Found
```

**`new_string`:** Add an Architectural Decisions row between Architecture Diagram and Issues Found:

```markdown
### Architecture Diagram
- [x] architecture.html generated/updated
- Levels included: Context, Container, Component

### Architectural Decisions
- [x] Decision inventory scanned (Phase 2.5)
- [x] ADRs up to date / drafted where needed

### Issues Found
```

- [ ] **Step 4: Validate YAML frontmatter still parses**

Run:
```bash
uv run --with pyyaml python -c "
import re
with open(r'D:\Development\karstenskyt__mad-scientist-skills\plugins\mad-scientist-skills\skills\final-review\SKILL.md', encoding='utf-8') as f:
    text = f.read()
m = re.match(r'^---\n(.*?)\n---\n', text, re.DOTALL)
assert m, 'No frontmatter found'
import yaml
meta = yaml.safe_load(m.group(1))
assert 'name' in meta and 'description' in meta
print(f'OK: name={meta[\"name\"]}, description length={len(meta[\"description\"])} chars')
"
```

Expected output: `OK: name=final-review, description length=... chars`. Confirms the edit did not corrupt the YAML block. Fail fast if the YAML is malformed — a broken frontmatter would silently break skill loading.

- [ ] **Step 5: Report to user — checkpoint**

Report to the user:
> "Task 4 complete. `mad-scientist-skills:final-review` extended with Phase 2.5 Architectural Decision Review and Phase 5 checklist row. YAML frontmatter validated. No commit. Proceed to Task 5?"

Wait for user acknowledgement before moving to Task 5.

---

## Task 5: Create mad-scientist-skills:measure-before-optimize skill

**Files:**
- Create: `D:\Development\karstenskyt__mad-scientist-skills\plugins\mad-scientist-skills\skills\measure-before-optimize\SKILL.md`

**Rationale:** New peer skill to `optimization-audit`. Pre-change measurement gate, distinct in timing (pre vs. retrospective), trigger ("about to touch a measured function" vs. "audit this codebase"), scope (single function vs. whole codebase), and action (gate vs. recommend fixes). Extends mad-scientist-skills' identity from "retrospective audits" to "disciplined quality checks, pre- and post-change."

### Steps

- [ ] **Step 1: Confirm the target parent directory exists**

Run: `Bash` command `ls /d/Development/karstenskyt__mad-scientist-skills/plugins/mad-scientist-skills/skills/`

Expected: Lists 8 existing skill directories (architecture-audit, c4, cognitive-interface-audit, documentation-audit, final-review, observability-audit, optimization-audit, security-audit). Confirms the parent path is correct before Write creates the new subdirectory.

- [ ] **Step 2: Write the new SKILL.md file**

Use the `Write` tool on `D:\Development\karstenskyt__mad-scientist-skills\plugins\mad-scientist-skills\skills\measure-before-optimize\SKILL.md`.

**Important:** The `Write` tool will create the `measure-before-optimize/` subdirectory automatically if it does not exist.

Full content:

````markdown
---
name: measure-before-optimize
description: Pre-change measurement gate for perf-sensitive functions. Use BEFORE modifying any function that has a pytest-benchmark test, appears in a performance baselines file, or is flagged as a hot path in CLAUDE.md. Captures baseline median and p95, verifies the change does not regress beyond a configurable threshold, reports the delta. Peer skill to optimization-audit — this one is pre-change; that one is retrospective.
---

# Measure Before Optimize

A pre-change measurement discipline that captures a performance baseline, gates the change on a regression threshold, and reports the delta. Designed as a peer to `optimization-audit`: this skill is pre-change, that one is retrospective.

## When to use this skill

- Before modifying a function that has a `pytest-benchmark` test.
- Before modifying a function listed in the project's performance baselines file (commonly `docs/performance-baselines.md` or `docs/benchmarks.md`).
- Before modifying a function flagged as a hot path in `CLAUDE.md`, `CONTRIBUTING.md`, or a performance-related document.
- When the user says "optimize X", "speed up Y", "this function is slow", or similar performance-intent phrases.
- When a task touches tracking-scale data, Spark UDFs with strict memory budgets, or any code in a documented hot loop.

## What this skill is NOT for

- Retrospective performance audits — use `optimization-audit` instead.
- First-time benchmark creation — if no benchmark exists for the function being modified, warn the user and offer to add one, but do not block. This skill gates CHANGES to measured functions, not the creation of new ones.
- Micro-benchmarks of framework internals that you do not own.
- Production profiling — this skill runs local micro-benchmarks only, not production traces.

## Workflow

### Phase 1: Identify the measurement surface

Read the project's baselines file (default: `docs/performance-baselines.md`). Extract the table of benchmarked functions. If the file is a JSON baselines file, parse it directly. If neither exists, `grep` for `@pytest.mark.benchmark` or `benchmark(` invocations in `tests/` and `src/tests/`.

Build a set of "measured functions" — functions with known benchmarks. Cross-reference with the function being modified.

- **If the function is in the measurement surface**: proceed to Phase 2.
- **If the function is NOT in the measurement surface**: warn the user:
  > "The function `<name>` is not currently benchmarked. I can add a `pytest-benchmark` test before modifying it, or you can proceed without a baseline. Which?"
- **Do not block** — the user may have a good reason to proceed without a baseline.

### Phase 2: Capture baseline

Run the matching `pytest-benchmark` test before any code change:

```bash
uv run pytest <test_path>::<test_name> --benchmark-only --benchmark-min-rounds=3 --benchmark-json=<scratch_file_pre>
```

Write the scratch file to `tempfile.gettempdir()` (typically `%TEMP%` on Windows, `/tmp/` on Linux). **NEVER write to the project root** — the scratch file must not be accidentally committable.

Parse the JSON output. For each benchmark in `benchmarks[]`:
- `stats.mean` or `stats.median` (median is preferred — more robust to outliers)
- `stats.hd15iqr` or `stats.iqr_outliers` (p95 equivalent)
- `stats.rounds`
- `stats.ops`

Look up the function's budget from the project's CLAUDE.md or baselines file if available.

Report to the user:

```
Baseline captured — <function_name>
  median:   <value> µs
  p95:      <value> µs
  rounds:   <count>
  budget:   <budget> (from <source>)
  headroom: <pct>% of budget
```

### Phase 3: Yield to the main agent

Exit the skill at this point. The main agent makes the planned code change. The skill reactivates when the user (or main agent) indicates the change is complete and it is time to re-measure. The skill does NOT attempt to wrap or supervise the code change itself.

### Phase 4: Re-run the benchmark

Run the same benchmark command with a different scratch file suffix (e.g., `.post.json` instead of `.pre.json`):

```bash
uv run pytest <test_path>::<test_name> --benchmark-only --benchmark-min-rounds=3 --benchmark-json=<scratch_file_post>
```

### Phase 5: Compare and report

Calculate:
- `delta_median_pct = (new_median - baseline_median) / baseline_median * 100`
- `delta_p95_pct = (new_p95 - baseline_p95) / baseline_p95 * 100`

Report to the user:

```
## Measure-before-optimize report

Function: <function_name>
Budget:   <budget>

             baseline       new            delta
median       <v> µs         <v> µs         <±%>
p95          <v> µs         <v> µs         <±%>

Budget status:        <within / over> budget (<pct>% of budget)
Regression threshold: <threshold>% (default 10%)
Result:               <within threshold / EXCEEDS THRESHOLD / IMPROVEMENT>
```

**If delta exceeds the regression threshold (default 10%)**: escalate to the user with the full delta and ask whether to proceed, revert, or investigate. Do not silently accept a regression.

**If delta is negative (improvement)**: report the improvement explicitly and suggest updating the baselines file to reflect the new floor. Do not update the file automatically.

## Parameters

Resolved from natural language or default. The skill never asks for these — it uses defaults unless the user names a parameter explicitly.

| Parameter | Default | Description |
|---|---|---|
| `baselines_file` | `docs/performance-baselines.md` | Path to the project's baselines markdown or JSON |
| `regression_threshold` | `10%` | Percent regression that escalates to user prompt |
| `budget_enforcement` | `warn` | `warn` (report and ask) or `block` (halt execution) |
| `benchmark_rounds` | `3` | `pytest-benchmark --benchmark-min-rounds` — raise for more stable measurements |

## Comparison to optimization-audit

| Attribute | optimization-audit | measure-before-optimize |
|---|---|---|
| **Timing** | Retrospective (after code exists) | Pre-change gate |
| **Trigger** | "Audit this codebase for perf issues" | "About to touch a measured function" |
| **Output** | Audit report with prioritised issues | Before/after delta, regression flag |
| **Scope** | Whole codebase | Single function / small change |
| **Action** | Recommends fixes | Gates the change |

Both skills share the theme of "don't change perf-sensitive code on vibes." They are designed to be invoked independently and do not overlap.

## Example invocation

**User:** "I'm going to rewrite `compute_pitch_control_at_points` to use a batched NumPy approach instead of the per-player loop."

**Claude:** Invokes `measure-before-optimize`.

**Skill Phase 1:** Reads `docs/performance-baselines.md`. Finds `compute_pitch_control_at_points` with median 347 µs, p95 512 µs, budget ≤5 ms.

**Skill Phase 2:** Runs `uv run pytest src/tests/test_pitch_control_benchmark.py::test_pitch_control_batched --benchmark-only --benchmark-min-rounds=3 --benchmark-json=%TEMP%/pitch_control_pre.json`. Reports baseline:
```
Baseline captured — compute_pitch_control_at_points
  median:   347 µs
  p95:      512 µs
  rounds:   3
  budget:   ≤5 ms (from CLAUDE.md Performance Budgets)
  headroom: 93.1% of budget
```

**Skill Phase 3:** Exits. Main agent makes the change.

**Skill Phase 4 (after change):** Same command with `%TEMP%/pitch_control_post.json`.

**Skill Phase 5:** Reports:
```
## Measure-before-optimize report

Function: compute_pitch_control_at_points
Budget:   ≤5 ms

             baseline       new            delta
median       347 µs         362 µs         +4.3%
p95          512 µs         534 µs         +4.3%

Budget status:        within budget (7.2% of 5 ms)
Regression threshold: 10% (default)
Result:               within threshold
```

If the new median had been 402 µs (+15.8%), the skill would have escalated.

## Important rules

- **Never write scratch baseline files to the project root.** Always use `tempfile.gettempdir()`. Scratch files that end up committed are a workflow smell.
- **Report BOTH delta-vs-baseline AND position-vs-budget.** A function at 70% of budget can absorb a 20% regression without blowing budget; a function at 95% cannot. Both numbers are load-bearing for the operator's decision.
- **The threshold is a prompt, not a block.** Operator judgment decides whether a regression is acceptable. This skill surfaces the delta; the operator decides.
- **Do not attempt this skill if no benchmark exists.** Warn and exit — creating benchmarks is a separate workflow that deserves its own TDD pass.
- **Use `--benchmark-min-rounds=3` for fast checks, `--benchmark-min-rounds=10` for stable measurements.** The default is set for fast feedback; raise it when the delta is borderline and you need more confidence.
````

- [ ] **Step 3: Validate YAML frontmatter parses**

Run:
```bash
uv run --with pyyaml python -c "
import re
with open(r'D:\Development\karstenskyt__mad-scientist-skills\plugins\mad-scientist-skills\skills\measure-before-optimize\SKILL.md', encoding='utf-8') as f:
    text = f.read()
m = re.match(r'^---\n(.*?)\n---\n', text, re.DOTALL)
assert m, 'No frontmatter found'
import yaml
meta = yaml.safe_load(m.group(1))
assert meta['name'] == 'measure-before-optimize', f'name mismatch: {meta.get(\"name\")}'
assert 'description' in meta and len(meta['description']) > 50
print(f'OK: name={meta[\"name\"]}, description length={len(meta[\"description\"])} chars')
"
```

Expected output: `OK: name=measure-before-optimize, description length=... chars`. Confirms the new skill file has valid YAML frontmatter.

- [ ] **Step 4: Report to user — checkpoint**

Report to the user:
> "Task 5 complete. New skill `mad-scientist-skills:measure-before-optimize` created. YAML frontmatter validated. Skill is self-contained (no templates/ folder). No commit. Proceed to Task 6?"

Wait for user acknowledgement before moving to Task 6.

---

## Task 6: Update this repo's CLAUDE.md — When to write an ADR + measure-before-optimize pointer

**Files:**
- Modify: `D:\Development\karstenskyt__luxury-lakehouse-d32\CLAUDE.md`

**Rationale:** Two additions to the same file, one atomic `Edit` or two sequential `Edit` calls depending on anchor uniqueness. The "When to write an ADR" section goes after the `## AI Governance` section (ADRs and AI Governance are both repo-wide record-keeping disciplines). The measure-before-optimize pointer goes inside the `### Performance Budgets` subsection under `## Database Performance`.

### Steps

- [ ] **Step 1: Read the current file**

Run: `Read` tool on `D:\Development\karstenskyt__luxury-lakehouse-d32\CLAUDE.md`

Purpose: Confirm the exact text surrounding both insertion anchors. Need:
- End of `## AI Governance` section → start of `## Type Safety` section (for the ADR section insertion)
- End of `### Performance Budgets` subsection → start of `## App Performance` section (for the measure-before-optimize pointer)

Expected: File contains both section boundaries as described.

- [ ] **Step 2: Insert "When to write an ADR" section after AI Governance**

Use the `Edit` tool.

**`old_string`:** The last line of the `## AI Governance` section (ending with "...D56 academic-reference audit; the rule exists so that gap does not reopen.") plus the `## Type Safety` heading — enough context to be unique.

**`new_string`:** The same boundary with the new section inserted:

```markdown
...D56 academic-reference audit; the rule exists so that gap does not reopen.

## Architectural Decision Records (ADRs)

Significant architectural decisions — ones future maintainers will reasonably ask "why?" about — are documented in `docs/superpowers/adrs/` using the Michael Nygard format captured in `docs/superpowers/adrs/ADR-TEMPLATE.md`. The `mad-scientist-skills:final-review` skill Phase 2.5 scans for decisions that warrant an ADR and prompts for one before commit.

**When to write an ADR** — any of these patterns:

- Introduces, removes, or replaces a cross-cutting dependency (e.g., swapping a library for another, dropping a framework)
- Changes a schema ownership or grants model (e.g., `dbt-owners-{env}` group ownership; definer's-rights views for system-table access)
- Hard-codes a workaround for a platform constraint (e.g., `DATABRICKS_HTTP_PATH` double-slash for Git Bash MSYS; Python 3.10 lock for Databricks serverless)
- Introduces a naming, identifier, or path convention with downstream consumers (e.g., `frame_batch_id` synthetic keys for `applyInPandas` group sizing)
- Reimplements an algorithm to avoid a dependency (e.g., EFPI algorithm reimplementation to avoid `unravelsports` Python 3.11+ requirement)
- Introduces a defense-in-depth control or security boundary (e.g., evolve exec sandbox AST allowlist — ADR-001; SEC2 artifact hash verification)
- Makes a structural trade-off in the pipeline (e.g., guard injection as a mandatory no-default parameter in `run_pipeline()`, enforced by `test_guard_conformance.py`)

**When NOT to write an ADR:**

- Routine feature work that follows established patterns
- Bug fixes that do not change an architectural contract
- Documentation-only changes
- Refactoring that preserves behaviour and contracts

**Existing ADRs:** `docs/superpowers/adrs/ADR-*.md`. **Template:** `docs/superpowers/adrs/ADR-TEMPLATE.md`.

## Type Safety
```

- [ ] **Step 3: Append measure-before-optimize pointer to Performance Budgets**

Use the `Edit` tool a second time.

**`old_string`:** The last bullet of the `### Performance Budgets` section (currently ending with something like "Team shape frame (both teams): ≤2ms per frame for 22 players (benchmark baseline)") plus the `## App Performance` heading — enough context to be unique.

**`new_string`:** The same boundary with a new paragraph inserted after the existing bullets and before `## App Performance`:

```markdown
- **Team shape frame (both teams)**: ≤2ms per frame for 22 players (benchmark baseline)

**Before modifying any function listed above, any function with a `pytest-benchmark` wrapper, or any function flagged as a hot path in this document, invoke `mad-scientist-skills:measure-before-optimize`.** The skill captures a baseline, waits for the change, re-measures, and reports the delta against the budget and a configurable regression threshold (default 10%). Peer skill to `mad-scientist-skills:optimization-audit`: this one is pre-change, that one is retrospective. Do not optimise benchmarked code on vibes.

## App Performance
```

- [ ] **Step 4: Verify both additions landed**

Run: `Grep` with pattern `## Architectural Decision Records \(ADRs\)` on `D:\Development\karstenskyt__luxury-lakehouse-d32\CLAUDE.md`

Expected: One match. Confirms the ADR section was inserted.

Run: `Grep` with pattern `measure-before-optimize` on `D:\Development\karstenskyt__luxury-lakehouse-d32\CLAUDE.md`

Expected: At least one match. Confirms the pointer was inserted.

- [ ] **Step 5: Report to user — checkpoint**

Report to the user:
> "Task 6 complete. This repo's `CLAUDE.md` updated with `## Architectural Decision Records (ADRs)` section (including the five historical examples) and a `measure-before-optimize` pointer in `### Performance Budgets`. Both additions verified via Grep. No commit. Proceed to Task 7?"

Wait for user acknowledgement before moving to Task 7.

---

## Task 7: Update mad-scientist-skills CHANGELOG.md

**Files:**
- Modify: `D:\Development\karstenskyt__mad-scientist-skills\CHANGELOG.md`

**Rationale:** The mad-scientist-skills repo has a root-level CHANGELOG.md (verified earlier in this session via `ls`). Each plugin change should be recorded so future maintainers and downstream consumers can track when the `final-review` phase extension shipped and when `measure-before-optimize` was added.

### Steps

- [ ] **Step 1: Read the current CHANGELOG.md**

Run: `Read` tool on `D:\Development\karstenskyt__mad-scientist-skills\CHANGELOG.md`

Purpose: Understand the existing entry format and versioning convention. The CHANGELOG could be Keep-a-Changelog format, or a simpler date-based log. The format dictates whether we add a new version entry, a new dated entry, or an unreleased-section entry.

Expected: File exists and follows some conventional format. If it's Keep-a-Changelog, it will have an `## [Unreleased]` section at the top, or the latest release's version header.

- [ ] **Step 2: Insert new entry at the top of the changelog**

Use the `Edit` tool.

**`old_string`:** Depends on the current format — likely the top heading plus the first version section header (e.g., `# Changelog\n\n## [Unreleased]\n...` or `# Changelog\n\n## [1.x.x] - YYYY-MM-DD\n...`).

**`new_string`:** Insert a new entry above the existing entries. Content to add (adapt to match the file's actual convention observed in Step 1):

```markdown
## [Unreleased] — 2026-04-14

### Added

- **New skill: `measure-before-optimize`** — pre-change measurement gate for perf-sensitive functions. Captures a `pytest-benchmark` baseline, waits for the change, re-measures, reports delta against budget and a configurable regression threshold. Peer to `optimization-audit` — this skill is pre-change, the other is retrospective. See `plugins/mad-scientist-skills/skills/measure-before-optimize/SKILL.md`.

### Changed

- **`final-review` skill** — added Phase 2.5 "Architectural Decision Review" between Phase 2 (Code Quality Review) and Phase 3 (Documentation Review). Scans the change for decisions matching six patterns (cross-cutting dependency changes, schema ownership, platform workarounds, naming conventions, algorithm reimplementations, defense-in-depth controls) and prompts for an ADR when one is missing or stale. Updated Phase 5 verification summary checklist with an "Architectural Decisions" row.
```

If the existing changelog format differs (e.g., no `## [Unreleased]` section, or uses `### Added`/`### Changed`/`### Fixed` subsections differently), adapt the entry to match the file's convention while preserving the substantive content.

- [ ] **Step 3: Report to user — final checkpoint**

Report to the user:
> "Task 7 complete. `mad-scientist-skills/CHANGELOG.md` updated with entries for the `final-review` Phase 2.5 extension and the new `measure-before-optimize` skill. No commit. **All seven tasks complete.** End-state: six files touched across three repos (this one, mad-scientist-skills, user home). Zero commits. Ready for your review and commit decision per repo."

---

## End-state checklist (for verification after all tasks complete)

Before closing the plan, verify:

- [ ] `C:\Users\Karsten\.claude\CLAUDE.md` — `## Reading PDFs` rewritten AND `## Engineering Principles Glossary` appended
- [ ] `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse-d32\memory\feedback_pdf_reading.md` — simplified to pointer
- [ ] `D:\Development\karstenskyt__luxury-lakehouse-d32\docs\superpowers\adrs\ADR-TEMPLATE.md` — created
- [ ] `D:\Development\karstenskyt__luxury-lakehouse-d32\CLAUDE.md` — `## Architectural Decision Records (ADRs)` section added AND measure-before-optimize pointer added to `### Performance Budgets`
- [ ] `D:\Development\karstenskyt__mad-scientist-skills\plugins\mad-scientist-skills\skills\final-review\SKILL.md` — Phase 2.5 inserted, Phase 5 checklist updated
- [ ] `D:\Development\karstenskyt__mad-scientist-skills\plugins\mad-scientist-skills\skills\measure-before-optimize\SKILL.md` — created
- [ ] `D:\Development\karstenskyt__mad-scientist-skills\CHANGELOG.md` — entry for both plugin changes added
- [ ] Zero git commits on any of the three repos
- [ ] User can run `git status` on each repo and see only the expected modifications

## Rollback (if any task is rejected)

Each task is a single file touch (Task 1 and Task 6 are two edits to one file). Rollback per task:

| Task | Repo | Rollback |
|---|---|---|
| 1 | user home | `git restore C:\Users\Karsten\.claude\CLAUDE.md` — if that directory is under git. Otherwise, manually revert to the prior content (recovered from Task 1 Step 1 Read output). |
| 2 | user home | Same as Task 1 rollback — revert `feedback_pdf_reading.md` to its Task 2 Step 1 Read output. |
| 3 | this repo | `git restore docs/superpowers/adrs/ADR-TEMPLATE.md` or `rm` the file if it was not committed. |
| 4 | mad-scientist-skills | `git restore plugins/mad-scientist-skills/skills/final-review/SKILL.md` (from the mad-scientist-skills repo root). |
| 5 | mad-scientist-skills | `git restore plugins/mad-scientist-skills/skills/measure-before-optimize/SKILL.md` or `rm -rf` the new subdirectory. |
| 6 | this repo | `git restore CLAUDE.md`. |
| 7 | mad-scientist-skills | `git restore CHANGELOG.md`. |

**Important:** do not run `git restore` or `rm` during normal execution. These are rollback commands, used only if the user explicitly rejects a task after review.

---

## Open decisions (resolved during execution, not blocking)

From the spec's "Open questions" section — how to handle during execution:

1. **Item 2a placement (sub-phase 2.5 vs. checkbox in Phase 2)** — Defaulting to new sub-phase 2.5 as specified in Task 4.
2. **Item 3 templates/ folder** — Defaulting to no templates/ subfolder, keeping the skill self-contained (Task 5).
3. **CHANGELOG format** — Resolved in Task 7 Step 1 by reading the actual file first.

## Notes for the executing agent

- **CLAUDE.md (both files) is long.** Use `Edit` with specific enough anchors to match uniquely. Do NOT use `replace_all`. If the first Edit fails due to non-unique anchors, widen the context (more lines before/after).
- **PDF reading protocol change is live as soon as user-global CLAUDE.md is saved** — Task 1 does not require a session restart to take effect.
- **New skill file is discoverable as soon as it exists** — `mad-scientist-skills:measure-before-optimize` will show up in future skill lookups after Task 5.
- **No tests to run.** This is documentation and configuration work. The YAML frontmatter validation in Tasks 4 and 5 is the only programmatic check.
- **No CI to wait for.** No commits means no CI.
- **No `.gitignore` changes needed.** The scratch baseline files from `measure-before-optimize` are written to `tempfile.gettempdir()`, outside any project tree.

## Self-review checklist (run inline after writing this plan)

- [x] Every spec item has a task: Item 1 → Tasks 1 & 2; Item 2a → Task 4; Item 2b → Task 3; Item 2c → Task 6; Item 3 → Task 5 & Task 6 pointer; Item 4 → Task 1; CHANGELOG → Task 7.
- [x] No "TBD", "TODO", "implement later" placeholders found.
- [x] Type/naming consistency: `measure-before-optimize` (hyphenated) used consistently; `Phase 2.5 Architectural Decision Review` used consistently; `docs/superpowers/adrs/ADR-TEMPLATE.md` path used consistently.
- [x] Each task ends with a checkpoint (no commit), matching the user's NO-COMMIT constraint.
- [x] File paths are absolute (Windows-style) everywhere an agent would need them.
- [x] The seven-task order respects dependencies: Task 6 runs after Tasks 3 and 5 (references their artefacts); Task 7 runs last (references Tasks 4 and 5).

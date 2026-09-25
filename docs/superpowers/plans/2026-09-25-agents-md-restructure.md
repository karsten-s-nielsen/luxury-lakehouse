# AGENTS.md Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans or superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax for tracking. **Execution mode is the owner's call at execution start (LH-PLAN-05).** These tasks are tightly coupled — Tasks 3–5 all edit the overlapping AGENTS.md / docs/context surface and the gate only goes fully green after Task 5 — so inline `executing-plans` (batch with checkpoints) fits better than fresh-subagent-per-task; confirm with the owner. This whole cycle lands as ONE owner-approved commit (Task 8), so there are no per-task commits regardless of mode.

**Goal:** Cut the always-loaded instruction surface (`CLAUDE.md` + nested `hf_taipy_app/CLAUDE.md`) to class-1 invariants only, move the WHY/history/measurement verbatim into an on-demand `docs/context/` tree, rename to the cross-tool `AGENTS.md` convention behind `@import` shims, and ship a CI anti-bloat gate that mechanically proves no invariant was dropped.

**Architecture:** `git mv` each `CLAUDE.md` → `AGENTS.md` (class-1 only); each old `CLAUDE.md` becomes a pure `@AGENTS.md` shim. Class-2 content moves verbatim into ~11 `docs/context/*.md` domain files that lean on the repo's 87 existing ADRs for deep WHY. A committed pre-change snapshot + a per-invariant inventory JSON drive a pytest gate (`src/tests/test_agents_md_budget.py`) enforcing byte budget, per-bullet cap + pointer, per-invariant completeness (distinctive symbols, never bare `ADR-NNN`), conservation, and shim integrity. All live references repoint to `AGENTS.md`; historical references are left untouched.

**Tech Stack:** Python 3.10, pytest, stdlib only (`pathlib`, `json`, `re`, `math`), git, Markdown.

**Spec:** `docs/superpowers/specs/2026-09-25-agents-md-restructure-design.md` (Status: APPROVE r2). The plan argues from the spec; executors read both.

**Revision r2 (2026-09-25) — plan-review findings resolved.** **LH-PLAN-02** (BLOCKING): `test_shim_integrity` strips `<!--` HTML-comment lines, not `#` (a `#` line is a Markdown H1 heading); spec §4/§7 wording fixed to match. **LH-PLAN-01**: Task 8 splits commit / push / PR / merge into four separate owner-gated steps (spec §12). **LH-PLAN-03**: pinned `INVARIANT_COUNT` test-code constant + assertion (fixture-only shrink no longer passes); spec §7.4 wording fixed. **LH-PLAN-04**: Task 7 runs bare `uv run pytest` (CI `python-ci.yml:108`, testpaths incl. `hf_taipy_app/src`) + `uv run validate_workflow_cards workflow-cards/` (`:111`) — both verified against CI. **LH-PLAN-05**: execution mode is the owner's call; leaning inline `executing-plans` given the coupled single-commit surface. No redesign.

## Global Constraints

- **ONE commit for the whole cycle — no per-task commits.** Owner's standing rule overrides the writing-plans skill's per-task commit template. Build the entire green state, then land it as a single owner-approved commit at Task 8. No micro-commits, no commit-after-every-step.
- **Branch:** single feature branch `docs/agents-md-restructure` off `main` HEAD `9f78e041`. No git worktree. No PR decomposition.
- **No behaviour change to shipped code.** Only comment/doc pointer repoints touch non-doc files; they preserve behaviour. Say "no behaviour change", not "untouched".
- **Do NOT touch the shared global `~/.claude/CLAUDE.md`.** Repo files only.
- **Verbatim moves.** Class-2 is cut, not paraphrased — the completeness check keys on exact symbols; a paraphrase loses fidelity and fails the gate.
- **Zero drops by default.** Nothing dropped/deferred/softened without the owner's explicit approval; an approved drop is recorded in the inventory (`disposition: "dropped"` + `drop_reason`).
- **Encoding & unit:** every file read in the gate test passes `encoding="utf-8"`; every byte budget asserts `Path(...).stat().st_size`, never `len(text)` (Windows cp1252 mojibake + em-dash = 3 UTF-8 bytes/char — spec LH-SPEC-01).
- **No version bump** (instruction files are not wheel-packaged — re-verified in Task 7).
- **Code quality:** Python 3.10, 120-char line limit, ruff rule sets E/W/F/I/N/UP/B/S/BLE/RUF. `src/tests/**` already carries the `S101,BLE001` per-file ignore.

---

### Task 1: Snapshot fixtures + build the invariant inventory (the migration-safety oracle)

**Files:**
- Create: `src/tests/fixtures/claude_md_at_9f78e041.md` (verbatim copy of `CLAUDE.md` at the branch point)
- Create: `src/tests/fixtures/hf_taipy_app_claude_md_at_9f78e041.md` (verbatim copy of nested file)
- Create: `src/tests/fixtures/agents_md_invariant_inventory.json`

**Interfaces:**
- Produces: the inventory JSON consumed by every check in Task 2. Top-level shape: `{"generated_from": "<sha>", "count": <int>, "invariants": [<entry>, …]}`.

- [ ] **Step 1: Cut the branch (execution start — owner-gated).**

```bash
git checkout main && git rev-parse HEAD          # confirm 9f78e041
git checkout -b docs/agents-md-restructure
```

- [ ] **Step 2: Snapshot the two pre-change files as committed fixtures.**

```bash
mkdir -p src/tests/fixtures
git show 9f78e041:CLAUDE.md > src/tests/fixtures/claude_md_at_9f78e041.md
git show 9f78e041:hf_taipy_app/CLAUDE.md > src/tests/fixtures/hf_taipy_app_claude_md_at_9f78e041.md
```
These are read via `open()` by the gate — never `git show` at test time (CI is a shallow clone → `git show <parent>` exits 128).

- [ ] **Step 3: Enumerate every invariant into the inventory.**

Procedure — walk each `##`/`###` section and each `-`/table-row rule in `src/tests/fixtures/claude_md_at_9f78e041.md`, then the nested fixture. One entry per **enforceable invariant** (a rule a change must not break). Estimated 80–110 entries. For each entry decide:
- `disposition`: `moved` (WHY goes to a context file, imperative stays class-1), `kept-class1` (terse rule wholly stays, no context file), or `dropped` (needs owner approval + `drop_reason`).
- `symbols`: distinctive tokens (gate/test/function/constant names, file paths, distinctive multi-word phrases). **Never** only a bare `ADR-\d+`.
- `class1_anchor` (when `class1_present: true`): a ≥12-char, ≥2-non-stopword-token phrase from the imperative that will appear in an AGENTS.md file.
- `context_file`: the `docs/context/<domain>.md` target (for `moved`).

Entry schema + representative samples (span the domains — populate the rest the same way):

```json
{
  "generated_from": "CLAUDE.md@9f78e041 + hf_taipy_app/CLAUDE.md@9f78e041",
  "count": 0,
  "invariants": [
    {
      "id": "git-commit-approval",
      "domain": "git-and-scope",
      "rule": "Never git commit/push/PR/merge without explicit per-action approval",
      "symbols": ["gh pr create", "gh pr merge", "separate, explicit approval"],
      "disposition": "kept-class1",
      "class1_present": true,
      "class1_anchor": "Never commit without explicit user approval",
      "context_file": null,
      "drop_reason": null
    },
    {
      "id": "sec-void-strip",
      "domain": "security",
      "rule": "write_delta_table/merge_delta_table auto-drop top-level void (NullType) columns",
      "symbols": ["_strip_void_columns", "gradientsports_events"],
      "disposition": "moved",
      "class1_present": true,
      "class1_anchor": "auto-drop top-level void (NullType) columns",
      "context_file": "docs/context/security.md",
      "drop_reason": null
    },
    {
      "id": "gov-per-player-cards",
      "domain": "ai-governance",
      "rule": "Adding/changing a per-player evaluative ML card updates AI_GOVERNANCE.md §5 + model card + governance YAML + re-run the test",
      "symbols": ["PER_PLAYER_EVALUATIVE_CARDS", "test_ai_governance_md.py", "SEC-AUDIT-v1.12.0 REG-01"],
      "disposition": "moved",
      "class1_present": true,
      "class1_anchor": "per-player evaluative ML system governance record",
      "context_file": "docs/context/ai-governance.md",
      "drop_reason": null
    },
    {
      "id": "cq-no-mask-in-loop",
      "domain": "code-quality",
      "rule": "Never boolean-mask filter a DataFrame inside a loop over tracking/event data (O(n×m))",
      "symbols": ["dict(iter(df.groupby", "O(n×m)", "3M+ rows"],
      "disposition": "moved",
      "class1_present": true,
      "class1_anchor": "No DataFrame boolean mask filtering inside loops",
      "context_file": "docs/context/code-quality.md",
      "drop_reason": null
    },
    {
      "id": "hf-publish-seam",
      "domain": "training-and-hf",
      "rule": "Publishers never call split_restricted/assert_no_private_leak directly — go through prepare_public_upload/upload_guarded",
      "symbols": ["prepare_public_upload", "upload_guarded", "GuardedFrame", "test_publisher_seam_conformance.py"],
      "disposition": "moved",
      "class1_present": true,
      "class1_anchor": "publishers never call split_restricted or assert_no_private_leak",
      "context_file": "docs/context/training-and-hf.md",
      "drop_reason": null
    },
    {
      "id": "perf-topandas-bounded",
      "domain": "data-performance",
      "rule": ".toPandas() calls must be DataFrame-API bounded or allowlisted",
      "symbols": ["test_topandas_boundedness.py", "_topandas_exemptions.yml"],
      "disposition": "moved",
      "class1_present": true,
      "class1_anchor": ".toPandas() calls must be bounded",
      "context_file": "docs/context/data-performance.md",
      "drop_reason": null
    },
    {
      "id": "ui-tp-prefix-ban",
      "domain": "ui-ux",
      "rule": "Taipy state prefixes: tp_ is banned",
      "symbols": ["page_template.py", "build_page", "PageConfig"],
      "disposition": "moved",
      "class1_present": true,
      "class1_anchor": "template-driven Taipy page architecture rules",
      "context_file": "docs/context/ui-ux.md",
      "drop_reason": null
    }
  ]
}
```

- [ ] **Step 4: Set `count` to the actual number of entries, and pin the same N into `INVARIANT_COUNT` in `src/tests/test_agents_md_budget.py` (Task 2).** Both the fixture `count` and the test-code constant must equal the enumerated total N — that dual-source pin is what makes a silent shrink (delete entry + decrement fixture count) fail the gate.

- [ ] **Step 5: Sanity-check the inventory parses and is schema-valid** (Task 2's `test_inventory_schema_valid` covers this once written; for now):

Run: `python -c "import json; d=json.load(open('src/tests/fixtures/agents_md_invariant_inventory.json', encoding='utf-8')); print(d['count'], len(d['invariants']))"`
Expected: two equal integers.

---

### Task 2: Write the anti-bloat gate test (fails red until the restructure lands)

**Files:**
- Create: `src/tests/test_agents_md_budget.py`

**Interfaces:**
- Consumes: `src/tests/fixtures/agents_md_invariant_inventory.json` (Task 1); the yet-to-exist `AGENTS.md`, `hf_taipy_app/AGENTS.md`, `docs/context/*.md`, and the two shims (Tasks 3–5).
- Produces: the six checks that gate the whole restructure.

- [ ] **Step 1: Write the test module verbatim.**

```python
"""Anti-bloat gate for the always-loaded AGENTS.md instruction surface.

Enforces the class-1 byte budget + per-invariant migration-safety oracle from
docs/superpowers/specs/2026-09-25-agents-md-restructure-design.md.
Runs on every CI leg (not slow-marked). Every read is utf-8; every byte
budget asserts stat().st_size, never len(text) (spec LH-SPEC-01).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]   # src/tests/ -> repo root
FIXTURES = Path(__file__).resolve().parent / "fixtures"

AGENTS_ROOT = REPO_ROOT / "AGENTS.md"
AGENTS_NESTED = REPO_ROOT / "hf_taipy_app" / "AGENTS.md"
SHIM_ROOT = REPO_ROOT / "CLAUDE.md"
SHIM_NESTED = REPO_ROOT / "hf_taipy_app" / "CLAUDE.md"
CONTEXT_DIR = REPO_ROOT / "docs" / "context"
INVENTORY = FIXTURES / "agents_md_invariant_inventory.json"

# --- budget constants (bytes) ---
TARGET_ROOT_BYTES = 28_672          # 28 KB design cut (spec §7)
NESTED_MAX_BYTES = 9_497            # no-growth brake (current nested size)
# CEILING starts == TARGET/NESTED_MAX (loosest valid brake). Task 7 tightens
# each to ceil(landed * 1.15) from the measured landed byte size.
CEILING_ROOT_BYTES = 28_672
CEILING_NESTED_BYTES = 9_497
CONTEXT_FLOOR_BYTES = 20_000        # Task 7 sets from actual moved bulk (spec §7.5)
INVARIANT_COUNT = 0                 # PINNED in test code to Task 1's enumerated total
                                    # (set in Task 1 Step 4). A fixture-only edit
                                    # (delete entry + decrement count) still fails
                                    # against this constant → no silent shrink.

BULLET_RE = re.compile(r"^\s*[-*] ")
POINTER_RE = re.compile(r"ADR-\d+|docs/context/|\S+\.(?:py|md|tf|sql|ya?ml|json|toml)\b")
ADR_ONLY_RE = re.compile(r"^ADR-\d+$")
ANCHOR_TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]+")
SUBSTANTIVE_BULLET_CHARS = 120
BULLET_CHAR_CAP = 600
ANCHOR_MIN_CHARS = 12
STOPWORDS = {
    "the", "a", "an", "of", "to", "and", "or", "see", "for", "in", "on",
    "is", "are", "be", "rule", "adr", "this", "that", "with", "via", "per",
}


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _agents_text() -> str:
    parts = [_read(AGENTS_ROOT)]
    if AGENTS_NESTED.exists():
        parts.append(_read(AGENTS_NESTED))
    return "\n".join(parts)


def _surface_text() -> str:
    parts = [_agents_text()]
    for f in sorted(CONTEXT_DIR.glob("*.md")):
        parts.append(_read(f))
    return "\n".join(parts)


def _load_inventory() -> dict:
    return json.loads(_read(INVENTORY))


def test_root_byte_budget():
    size = AGENTS_ROOT.stat().st_size
    assert size <= TARGET_ROOT_BYTES, f"AGENTS.md {size} B > TARGET {TARGET_ROOT_BYTES} B"
    assert size <= CEILING_ROOT_BYTES, f"AGENTS.md {size} B > CEILING {CEILING_ROOT_BYTES} B"
    assert CEILING_ROOT_BYTES <= TARGET_ROOT_BYTES, "CEILING must not exceed TARGET"


def test_nested_no_growth():
    size = AGENTS_NESTED.stat().st_size
    assert size <= NESTED_MAX_BYTES, f"nested AGENTS.md {size} B grew past {NESTED_MAX_BYTES} B"
    assert size <= CEILING_NESTED_BYTES


def test_bullet_char_cap_and_pointer():
    for label, path in (("root", AGENTS_ROOT), ("nested", AGENTS_NESTED)):
        for i, line in enumerate(_read(path).splitlines(), 1):
            if not BULLET_RE.match(line):
                continue
            n = len(line)                       # char count on decoded text
            if n <= SUBSTANTIVE_BULLET_CHARS:
                continue
            assert n <= BULLET_CHAR_CAP, f"{label}:{i} bullet {n} chars > {BULLET_CHAR_CAP}"
            assert POINTER_RE.search(line), f"{label}:{i} substantive bullet lacks ADR/docs/file pointer"


def test_shim_integrity():
    for shim in (SHIM_ROOT, SHIM_NESTED):
        lines = [ln for ln in _read(shim).splitlines()
                 if ln.strip() and not ln.lstrip().startswith("<!--")]
        assert lines == ["@AGENTS.md"], f"{shim} is not a pure @AGENTS.md shim: {lines}"


def test_inventory_schema_valid():
    inv = _load_inventory()
    ids = [e["id"] for e in inv["invariants"]]
    assert len(ids) == len(set(ids)), "duplicate invariant ids"
    assert len(ids) == inv["count"], f"count {inv['count']} != {len(ids)} entries"
    assert inv["count"] == INVARIANT_COUNT, (
        f"fixture count {inv['count']} != pinned INVARIANT_COUNT {INVARIANT_COUNT} "
        "(silent inventory shrink? edit the test constant deliberately)"
    )
    for e in inv["invariants"]:
        syms = e["symbols"]
        assert syms, f"{e['id']}: empty symbols"
        assert not all(ADR_ONLY_RE.match(s) for s in syms), f"{e['id']}: ADR-only symbols"
        assert e["disposition"] in {"moved", "kept-class1", "dropped"}
        if e["disposition"] == "dropped":
            assert e.get("drop_reason"), f"{e['id']}: dropped without drop_reason"
        if e.get("class1_present"):
            anchor = e.get("class1_anchor", "") or ""
            assert len(anchor) >= ANCHOR_MIN_CHARS, f"{e['id']}: anchor too short"
            toks = [t for t in ANCHOR_TOKEN_RE.findall(anchor.lower()) if t not in STOPWORDS]
            assert len(toks) >= 2, f"{e['id']}: anchor not distinctive: {anchor!r}"


def test_per_invariant_completeness():
    inv = _load_inventory()
    surface = _surface_text()
    agents = _agents_text()
    for e in inv["invariants"]:
        if e["disposition"] == "dropped":
            continue
        for s in e["symbols"]:
            assert s in surface, f"{e['id']}: symbol {s!r} vanished from AGENTS.md + docs/context"
        if e.get("class1_present"):
            assert e["class1_anchor"] in agents, \
                f"{e['id']}: class-1 anchor {e['class1_anchor']!r} not in an AGENTS.md file"
        if e["disposition"] == "moved":
            cf = REPO_ROOT / e["context_file"]
            assert cf.exists(), f"{e['id']}: context_file {e['context_file']} missing"


def test_context_conservation():
    files = sorted(CONTEXT_DIR.glob("*.md"))
    assert files, "no docs/context files"
    total = sum(f.stat().st_size for f in files)
    assert total >= CONTEXT_FLOOR_BYTES, f"context total {total} B < floor {CONTEXT_FLOOR_BYTES} B"
    for f in files:
        assert f.stat().st_size >= 500, f"{f.name} too small ({f.stat().st_size} B)"
```

- [ ] **Step 2: Run the gate; confirm it fails red (restructure not done yet).**

Run: `uv run pytest src/tests/test_agents_md_budget.py -v`
Expected: FAIL — `AGENTS.md` does not exist / shims not present / `docs/context` empty. This red state is correct; Tasks 3–5 turn it green. (`test_inventory_schema_valid` should already PASS from Task 1.)

---

### Task 3: Root rename + shim + class-1 `AGENTS.md`

**Files:**
- Rename: `CLAUDE.md` → `AGENTS.md`
- Create: `CLAUDE.md` (shim)
- Modify: `AGENTS.md` (rewrite to class-1 only)

**Interfaces:**
- Consumes: `src/tests/fixtures/claude_md_at_9f78e041.md` (source of truth for content to partition).
- Produces: `AGENTS.md` (class-1), `CLAUDE.md` shim. Makes `test_root_byte_budget`, `test_shim_integrity` (root leg), and the root half of `test_bullet_char_cap_and_pointer` passable.

- [ ] **Step 1: Rename preserving history.**

```bash
git mv CLAUDE.md AGENTS.md
```

- [ ] **Step 2: Rewrite `AGENTS.md` to class-1 only.** For each section, keep the terse imperative + a `(ADR-XXX; docs/context/<d>.md)` pointer; render Architecture as a one-line map per subsystem (`name (path/): purpose; entry points; → docs/context/<d>.md`); consolidate amendment chains to the CURRENT rule + newest ADR (history goes to the context file in Task 5). Every substantive bullet (>120 chars) carries a pointer and stays ≤600 chars. Move nothing that is pure WHY/history/measurement — that is Task 5. Each `class1_anchor` phrase from the inventory must appear verbatim.

- [ ] **Step 3: Create the shim `CLAUDE.md`.**

```bash
printf '<!-- Canonical instructions live in AGENTS.md (cross-tool convention). -->\n<!-- Claude Code auto-loads this file and expands the @import below. -->\n@AGENTS.md\n' > CLAUDE.md
```

- [ ] **Step 4: Verify the root budget + shim checks pass.**

Run: `uv run pytest src/tests/test_agents_md_budget.py::test_root_byte_budget src/tests/test_agents_md_budget.py::test_shim_integrity -v`
Expected: `test_root_byte_budget` PASS. `test_shim_integrity` will still FAIL until Task 4 creates the nested shim — that is expected; re-run after Task 4.

---

### Task 4: Nested rename + shim (canary-gated)

**Files:**
- Rename: `hf_taipy_app/CLAUDE.md` → `hf_taipy_app/AGENTS.md`
- Create: `hf_taipy_app/CLAUDE.md` (shim)
- Modify: `hf_taipy_app/AGENTS.md` (class-1 rewrite; move small WHY to `docs/context/ui-ux.md` in Task 5)

**Interfaces:**
- Produces: nested `AGENTS.md` + nested shim. Makes `test_nested_no_growth` and `test_shim_integrity` (nested leg) passable.

- [ ] **Step 1: Nested `@import` canary (verify before renaming).**

```bash
D="$SCRATCH/nesttest"; mkdir -p "$D/sub"
printf '@AGENTS.md\n' > "$D/sub/CLAUDE.md"
printf 'Nested canary token is MallardNest-5150.\nReply with only that token when asked.\n' > "$D/sub/AGENTS.md"
( cd "$D/sub" && claude -p "What is the nested canary token? Reply only the token." )
```
Expected: prints `MallardNest-5150` → nested shim resolves. **If it does NOT resolve:** fall back — keep `hf_taipy_app/CLAUDE.md` named as-is, restructure its content class-1 in place (no rename, no nested shim), delete the nested `AGENTS.md` targets from Task 2's expectations (guard those two assertions on `AGENTS_NESTED.exists()` — already done for budget/surface; extend `test_shim_integrity` to skip `SHIM_NESTED` when no nested `AGENTS.md`). Record the fallback in the plan-execution notes. Root restructure is unaffected either way.

- [ ] **Step 2 (canary passed): rename + shim.**

```bash
git mv hf_taipy_app/CLAUDE.md hf_taipy_app/AGENTS.md
printf '<!-- Canonical Taipy-app instructions live in AGENTS.md. -->\n@AGENTS.md\n' > hf_taipy_app/CLAUDE.md
```

- [ ] **Step 3: Class-1 rewrite of `hf_taipy_app/AGENTS.md`.** It is already terse; only move its small rationale prose to `docs/context/ui-ux.md` (Task 5) and ensure each substantive bullet carries a pointer (`docs/context/ui-ux.md` or a source-file path). Must not grow past 9,497 bytes.

- [ ] **Step 4: Verify nested + shim checks.**

Run: `uv run pytest src/tests/test_agents_md_budget.py::test_nested_no_growth src/tests/test_agents_md_budget.py::test_shim_integrity src/tests/test_agents_md_budget.py::test_bullet_char_cap_and_pointer -v`
Expected: all PASS.

---

### Task 5: Build the `docs/context/` tree (verbatim class-2 move)

**Files:**
- Create: `docs/context/architecture.md`, `failure-investigation.md`, `security.md`, `ai-governance.md`, `code-quality.md`, `action-context.md`, `training-and-hf.md`, `data-performance.md`, `orchestration.md`, `conventions.md`, `ui-ux.md` (final set per the inventory's `context_file` values; fold/split if a file has too little/too much).

**Interfaces:**
- Consumes: the fixture files (source of the verbatim WHY) + the inventory (`context_file` targets, `symbols` that must land).
- Produces: the on-demand tree. Makes `test_per_invariant_completeness` and `test_context_conservation` passable → whole gate green.

- [ ] **Step 1: For each domain file, move the WHY/history/measurement verbatim** from the fixture into that file, under headings matching the AGENTS.md rules. Cut, do not paraphrase — every inventory `symbol` for a `moved` entry must appear in its `context_file` (or somewhere in the surface). Each file starts with a one-line "what this covers" + a back-link to the governing ADR(s).

- [ ] **Step 2: Confirm every `moved` invariant's `context_file` exists and holds its symbols.**

Run: `uv run pytest src/tests/test_agents_md_budget.py::test_per_invariant_completeness src/tests/test_agents_md_budget.py::test_context_conservation -v`
Expected: PASS. If a symbol is reported vanished, it was paraphrased or missed — restore it verbatim.

- [ ] **Step 3: Full gate green.**

Run: `uv run pytest src/tests/test_agents_md_budget.py -v`
Expected: all 7 tests PASS.

---

### Task 6: Reference sweep — repoint all live refs

**Files:**
- Modify (doc/link): `README.md`, `CONTRIBUTING.md`, `ARCHITECTURE.md`, `.env.example`, `ROADMAP.md`, `TODO.md`, `AI_GOVERNANCE.md`
- Modify (comment/docstring only, no behaviour change): the ~23 `src/ingestion/*`, ~21 `scripts/*`, `src/tests/*`, `src/shared/*`, `src/analytics/*`, `dbt_project/models/*`, `terraform/*`, `workflow-cards/*`, `hf_taipy_app/*` files whose comments cite the rules file.
- Leave: `docs/superpowers/{plans,specs,adrs}/*` (146 historical), `docs/research`, `docs/investigations`, and any `~/.claude/CLAUDE.md` ref.

**Interfaces:**
- Produces: a tree where every live pointer names `AGENTS.md`; historical record untouched.

- [ ] **Step 1: List the live refs (tracked-only).**

```bash
git grep -l "CLAUDE\.md" -- . \
  ':(exclude)docs/superpowers/plans' ':(exclude)docs/superpowers/specs' \
  ':(exclude)docs/superpowers/adrs' ':(exclude)docs/research' ':(exclude)docs/investigations'
```

- [ ] **Step 2: Repoint prose/comment/link refs.** In each live file, replace the filename token `CLAUDE.md` → `AGENTS.md` (leaving the shim's own name where a file literally is the shim). For `ARCHITECTURE.md`: fix the section anchor `CLAUDE.md#database-performance` → point at the AGENTS.md rule or `docs/context/data-performance.md`, and update the `:448` file-tree entry to list `AGENTS.md` + the `CLAUDE.md` shim. For nested-path refs `hf_taipy_app/CLAUDE.md` → `hf_taipy_app/AGENTS.md` where the ref is to the rules content (leave where it refers to the shim).

- [ ] **Step 3: Confirm zero functional opens of `CLAUDE.md`.**

```bash
git grep -nE "open\(|read_text|Path\(.*CLAUDE" -- src scripts hf_taipy_app | grep -i "claude" || echo "no functional opens"
```
Expected: `no functional opens` (all refs are prose/comment/link).

- [ ] **Step 4: Confirm the historical bucket is untouched.**

```bash
git status --porcelain docs/superpowers/plans docs/superpowers/specs docs/superpowers/adrs
```
Expected: only `docs/superpowers/specs/2026-09-25-agents-md-restructure-design.md` and `.../plans/2026-09-25-agents-md-restructure.md` (this cycle's own new files) appear; no pre-existing historical file modified.

---

### Task 7: Full local gate + red-green proof + version re-check + CEILING tightening

**Files:**
- Modify: `src/tests/test_agents_md_budget.py` (set `CEILING_*` and `CONTEXT_FLOOR_BYTES` from measured landed sizes)

- [ ] **Step 1: Measure landed sizes and tighten the brakes.**

```bash
python -c "import os,math; \
r=os.path.getsize('AGENTS.md'); n=os.path.getsize('hf_taipy_app/AGENTS.md'); \
import glob; c=sum(os.path.getsize(f) for f in glob.glob('docs/context/*.md')); \
print('root',r,'ceil',math.ceil(r*1.15),'nested',n,'ceil',math.ceil(n*1.15),'ctx',c)"
```
Set `CEILING_ROOT_BYTES = ceil(root*1.15)`, `CEILING_NESTED_BYTES = ceil(nested*1.15)`, `CONTEXT_FLOOR_BYTES = round(ctx*0.9)` (10% slack below actual). Confirm `CEILING_ROOT_BYTES <= TARGET_ROOT_BYTES` (if landed root already exceeds 28 KB, the cut is insufficient — go back to Task 3/5 and move more to class-2, do not raise TARGET).

- [ ] **Step 2: Deliberate red-green — prove the oracle bites.**

Temporarily delete one `class1_anchor`'s line from `AGENTS.md`, run `uv run pytest src/tests/test_agents_md_budget.py::test_per_invariant_completeness -v` → expect FAIL naming that invariant. Restore the line; re-run → PASS. (Manual proof, nothing committed.)

- [ ] **Step 3: Re-verify no version bump.**

```bash
grep -n "CLAUDE\|AGENTS" pyproject.toml | grep -i include || echo "instruction files not wheel-packaged"
grep -in "CLAUDE\|AGENTS" scripts/bump_wheel.py || echo "bump_wheel unaffected"
```
Expected: both negative → no bump.

- [ ] **Step 4: Run the full local CI-equivalent gate.**

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run lint-imports
uv run python scripts/bump_wheel.py --check
uv run python scripts/pip_audit_ignores.py --check
uv run pyright src/ hf_taipy_app/src/ scripts/_tf_env_pins.py scripts/sync_tf_env_pins.py
uv run validate_workflow_cards workflow-cards/
SILLY_KICKS_ASSERT_INVARIANTS=1 uv run pytest
```
`uv run pytest` is **bare** (not `src/tests/`) to match CI (`python-ci.yml:108`): `testpaths = ["src/tests", "hf_taipy_app/src"]`, so the nested `hf_taipy_app/src` suite runs too — Task 4 renames a file there and Task 6 edits it, so a nested regression that greens on a `src/tests/`-only run would red on CI. `uv run validate_workflow_cards workflow-cards/` matches `python-ci.yml:111` — Task 6 edits `workflow-cards/*`, and CI validates them. Expected: all green. (These are long-running — run each with `run_in_background: true` and poll per the failure-investigation protocol.)

---

### Task 8: Commit gate (single, owner-approved) → push → PR

- [ ] **Step 1: Show the owner the complete change.**

```bash
git status
git diff --stat
```
Present the file list + stat. Wait for explicit approval for THIS commit. (Standing rule: none of "tests green"/"plan says commit" counts as approval.)

- [ ] **Step 2: On explicit "yes" — one commit.** Stage the whole coherent change (spec, plan, fixtures, gate test, `AGENTS.md` ×2, shims ×2, `docs/context/*`, sweep edits) and commit once:

```bash
git add -A
git commit   # message below
```
Message:
```
docs: restructure CLAUDE.md into class-1 AGENTS.md + docs/context tree

Cut the always-loaded instruction surface to class-1 invariants; move the
WHY/history/measurement verbatim into docs/context/*; rename to AGENTS.md
behind @import shims; add src/tests/test_agents_md_budget.py anti-bloat gate
(byte budget + per-bullet cap/pointer + per-invariant completeness oracle +
conservation + shim integrity). Repoint live references; historical left.
No behaviour change to shipped code. Spec + plan:
docs/superpowers/specs|plans/2026-09-25-agents-md-restructure*.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
```

- [ ] **Step 3: Push — separate owner go.** After a distinct explicit approval to push (the commit approval does not authorize it):

```bash
git push -u origin docs/agents-md-restructure
```

- [ ] **Step 4: Open the PR — separate owner go.** After a distinct explicit approval to open the PR:

```bash
gh pr create --fill --base main
```
PR body ends with: `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

- [ ] **Step 5: Merge — separate owner go.** After CI is green and a distinct explicit approval to merge:

```bash
gh pr merge --squash   # or per the owner's chosen merge strategy
```
Each of commit / push / PR / merge is its own owner-gated action (spec §12); none is implied by approval of a prior one.

---

## Self-Review

**1. Spec coverage.** §3 canary → Task 4 Step 1 (nested) + recorded root canary (spec §3); §4 rename+shim → Tasks 3,4; §5.2 class-1 → Tasks 3,4; §5.3 tree → Task 5; §5.4 amendment consolidation → Task 3 Step 2; §6 inventory oracle → Task 1; §7 gate (all six checks + encoding/byte unit) → Task 2 + Task 7 Step 1; §8 sweep (three buckets, anchors, functional-open check) → Task 6; §9 scope/safety → Global Constraints + Task 6 Step 4; §10 no version bump → Task 7 Step 3; §11 acceptance incl. red-green → Task 7 Steps 2,4; §12 workflow/gates → Task 1 Step 1 + Task 8. No gaps.

**2. Placeholder scan.** The inventory is populated by a stated enumeration procedure + schema + 7 worked samples spanning domains, then enforced by `test_inventory_schema_valid` + `test_per_invariant_completeness` — the machinery is concrete; the corpus population is the execution deliverable the gate checks. `CEILING_*`/`CONTEXT_FLOOR_BYTES` start at explicit valid values and are tightened in Task 7 Step 1 (not placeholders). No "TBD"/"handle edge cases"/"similar to Task N".

**3. Type consistency.** Test symbol names (`AGENTS_ROOT`, `_surface_text`, `_agents_text`, `POINTER_RE`, `ANCHOR_TOKEN_RE`, inventory keys `id/domain/rule/symbols/disposition/class1_present/class1_anchor/context_file/drop_reason/count`) are used identically across Tasks 1, 2, 5, 7. Fixture filenames identical across Tasks 1–2. Branch name identical across Tasks 1, 8.

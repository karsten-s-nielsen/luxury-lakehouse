# hf_sync Process Isolation + Driver Memory Observability — Implementation Plan (rev 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decouple the daily dbt build from an external service, stop `hf_sync` OOM-killing its driver, and make per-workflow driver memory observable platform-wide.

**Architecture:** Driver memory becomes a `LifecycleHook` (the port this repo already uses for cross-cutting workflow observability), so every `@workflow` gets it — including the tasks being split out. Two sub-operations leave the shared `hf_sync` process for their own Databricks tasks. `import_psxg_predictions` is already standalone-capable; `publish_spadl_vaep_hf` is **not**, and needs its `main()` given the watermark guard and hook registration that `hf_sync`'s factory was providing.

**Tech Stack:** Python 3.10, PySpark (Databricks serverless), Terraform, dbt seeds, pytest.

## rev 2 — what changed and why

Reviewed by a parallel session; **6 of 6 findings verified against source** and adopted. The material corrections:

| # | Finding | Effect on this plan |
|---|---|---|
| B1 | `publish_spadl_vaep_hf.main()` has **no watermark guard** — it all lives in `hf_sync`'s factory | rev 1 would have turned a watermark-gated publish into an **unconditional daily** 9.76M-row publish. New Task 6. |
| B2 | `main()` never calls `bootstrap_hooks` → **no `workflow_cost_live` row** once standalone | Same task. `import_psxg_predictions` *does* call it — the two splits are **not** symmetric. |
| B3 | `fct_action_values` is `intermediate_mart`; `hf_sync` has **no dbt edge at all** | Edge is `dbt_build_intermediate_marts`, not `output_marts`. Plus a **live staleness bug** — see below. |
| M4 | The probe belonged at the `LifecycleHook` seam, not hand-wired into `hf_sync`'s loop | rev 1 left the two split-out tasks — including the one that OOM-killed the driver — as the platform's only blind spot. |
| M5 | The adapters were `# pragma: no cover`, never executed anywhere | CI is `ubuntu-latest`; a Linux-gated adapter test now runs. A 1024× unit error would have made every logged number confident garbage. |
| M6 | `wf-import-psxg.yaml` is `execution.**import**`, `timeout: 900s`, `environment: analytics` | rev 1's Task 4 Step 6 was factually wrong and would have gone red on timeout parity. |

**Live bug this cycle fixes (name it in the ADR, do not let it pass as a side effect):** `hf_sync`'s `depends_on` is `{backfill_statsbomb_360, compute_elastic_sync, compute_spadl_vaep, resolve_players}` — **no dbt stage**. `fct_action_values` is built by `dbt_build_intermediate_marts`, a *sibling*. So `publish_spadl_vaep_hf` has been publishing a mart it has no ordering against.

**Nuance that makes B1 more urgent, not less:** the damage is currently bounded because `_make_watermark_op` calls `check_upstream_freshness` and **skips** when the mart hasn't changed — so today it lags ~a cycle rather than publishing arbitrarily. **That guard is exactly what rev 1 deleted.** B1 and B3 are one defect from two ends.

## Evidence this plan rests on

Measured. Do not re-litigate; do not optimise against these without re-measuring.

| Fact | Source |
|---|---|
| The **Python driver** was OOM-killed, not Spark | `exit code 137 (SIGKILL: Killed)`, run `49905842293930` attempt 1 |
| `publish_spadl_vaep_hf` **alone** peaks at **6.97 GB** of ~16 GB | diagnostic run `939215830803445` |
| `toPandas` is the only cost (+4.45 GB over a 2.52 GB baseline) | same |
| `prepare_public_upload` and `groupby`+`drop_columns` add **0 GB** | same |
| The 3 preceding sub-ops are **Spark-native** — no `.toPandas()` | source + no `_topandas_exemptions.yml` entries |
| `hf` env = `wheel + huggingface_hub==1.6.0` | job spec — the diagnostic env was representative |

**Chunking the read is NOT the fix.** Three designs were rejected on this evidence: chunk by `(data_source, competition_id)` (the read is 4.45 GB of a ~9 GB surplus); Spark→UC Volume (needs the SEC6 Spark-side guard for a non-problem); move to HF Jobs (`cpu-basic` is also 16 GB and also materialises via `pa.concat_tables(...).to_pandas()`).

**Known unknown, deliberately not guessed at:** which sub-operation consumed the remaining ~9 GB. Three theories were advanced and all three were wrong. Tasks 4–5 make the next real run answer it.

## Global Constraints

- **ONE feature branch, THREE commits, ONE PR.** `allow_merge_commit` was enabled on the repo for this cycle (it was `false`; squash-only would have collapsed every commit and destroyed the bisect boundaries). Merge with `gh pr merge --merge`, **not** `--squash`.
- Commit boundaries are deliberate, each independently green and independently revertable:
  - **Commit 1 = card/Terraform `environment` parity** — a gate plus a ~18-card documentation repair. Touches no runtime behaviour. Separated because "SEC7: decouple the dbt build" cannot honestly describe an 18-card sweep, and burying it there makes SEC7 illegible in `git log`.
  - **Commit 2 = SEC7** — clean topology move of a module that was *built* standalone-capable.
  - **Commit 3 = isolation + observability** — needs `main()` surgery first.
  Do not interleave.
- Each commit requires **separate explicit user approval**. Do not run `git commit` unprompted.
- **Commits 1 and 2 are bisect points, NOT deploy points.** The single wheel bump lands in Task 8 (commit 2), but commit 1 already edits `hf_sync.py` and adds a TF task. A `terraform apply` at the commit-1 boundary would run the new `import_psxg_predictions` task while the *deployed* wheel's `hf_sync` still lists it in `_SUB_OPERATIONS` — so the import runs **twice**, silently, because the entry point already exists. Never `terraform apply` between commits — only after commit 3.
- `src/shared/` is **stdlib-only** (import-linter enforced).
- `resource` and `/proc` are **Linux-only** — absent on the Windows dev box. Core logic tests inject fakes; the adapter test is `skipif(sys.platform == "win32")` and runs on CI (`ubuntu-latest`).
- `.importlinter` forbids `workflows → shared`, so `MemoryHook` lives in `src/ingestion/` and imports `shared.memory`. **Zero changes to `src/workflows/`** — `LifecycleHook` is a `Protocol` (structural typing).
- Line length 120; ruff `E,W,F,I,N,UP,B,S,BLE,RUF` zero violations; `pyright` basic, zero errors.
- One wheel bump for the branch, in Task 8: edit `pyproject.toml`, then `uv run python scripts/bump_wheel.py`. **Never hand-edit consumers.**
- Full gates before each commit: `ruff check`, `ruff format --check`, `pyright src/`, `pytest src/tests/` with `DATABRICKS_TOKEN` set (`scripts/mint_databricks_oauth.py`; 7 live dbt-macro tests fail without it). Never pass `-p no:benchmark` — it breaks 12 benchmark tests that need the fixture.

---

# COMMIT 1 — card/Terraform `environment` parity

### Task 1: Gate card↔Terraform `environment` drift (and repair every instance)

The timeout parity test exists because that drift class was *total* (21 of 42 cards). `environment` is the same field, with the same drift, and no gate.

**Scope, decided deliberately:** the gate stays in commit 1 AND its join is extended to orchestrated sub-operation cards, so it covers the two cards this cycle touches. That makes the failure set *larger*, not smaller — this task repairs **every** drifted card, not two.

> **Do NOT hardcode an expected drift list into this task, and do not trust an ad-hoc parser.**
> Two enumeration attempts have already been wrong. rev 1 assumed 2 drifted cards on the
> reasoning that the timeout drift was total — the precedent predicted the opposite and was
> right. A throwaway parser written to enumerate them then reported `hf_sync='dbt'`, which is
> false (the SDK reports `hf`), and mis-joined `wf-elastic-sync` → `ingest_idsse_events`.
> **Mirror the proven `_parse_tf_task_timeouts` depth-tracking exactly, then let the test
> itself produce the list.** The count is an output of this task, not an input.

**Files:**
- Modify: `src/tests/test_card_parity_with_terraform.py`

- [ ] **Step 1: Write the failing test**

Extend the TF parser to capture `environment_key` alongside the timeout, then add:

```python
def test_card_phase_environments_match_terraform() -> None:
    """A card's declared phase `environment` must equal its Terraform `environment_key`.

    Sibling of test_card_phase_timeouts_match_terraform, and it exists for the same
    reason: TERRAFORM IS THE RUNTIME TRUTH and the card is documentation about it.
    An operator reading `environment: analytics` will look for numba/xgboost/matplotlib
    that the task does not actually have.

    Found by review 2026-08-08. The drift is WIDE, not two cards: every orchestrated
    sub-operation of wf-hf-sync claims `analytics` while running under `hf`
    (wheel + huggingface_hub only), and multiple direct-task cards claim `analytics`
    against `default`/`embeddings`/`statsbomb`. The timeout field was gated after its
    drift proved total (21 of 42); this field was never gated at all.
    """
    tf = _parse_tf_task_environments()
    assert tf, "parsed ZERO task environments from main.tf — the parser is broken, not the cards"

    cards = {p.stem: _load_card(p) for p in _CARDS_DIR.glob("wf-*.yaml")}
    mismatches: list[str] = []
    unjoinable: list[str] = []
    checked = 0

    for task_key, (tf_env, tf_entry_point) in sorted(tf.items()):
        card_id = _DIRECT_TASK_ENTRY_POINT_TO_CARD.get(task_key)
        if card_id is None:
            continue
        if card_id not in cards:
            unjoinable.append(f"{card_id}: mapped from task_key={task_key} but no such card file")
            continue
        phases = _card_phases(cards[card_id])
        hits = [(n, p) for n, p in phases.items() if p.get("entry_point") in (task_key, tf_entry_point)]
        for phase_name, phase in hits:
            declared = phase.get("environment")
            if declared is None:
                continue  # environment is optional on a card; absent != wrong
            checked += 1
            if declared != tf_env:
                mismatches.append(f"{card_id}.{phase_name}: card={declared!r} terraform={tf_env!r}")

    # Orchestrated sub-operation cards have no TF task of their own; they run inside
    # their orchestrator's process and therefore ITS environment. Without this leg the
    # gate is blind to exactly the cards this cycle touches.
    orchestrator_env = {
        card_id: tf[task_key][0]
        for task_key, card_id in _DIRECT_TASK_ENTRY_POINT_TO_CARD.items()
        if task_key in tf
    }
    for card_id, card in sorted(cards.items()):
        for phase_name, phase in _card_phases(card).items():
            declared = phase.get("environment")
            orchestrator = phase.get("orchestrated_by")
            if declared is None or orchestrator is None:
                continue
            expected = orchestrator_env.get(orchestrator)
            if expected is None:
                unjoinable.append(f"{card_id}.{phase_name}: orchestrated_by={orchestrator!r} has no TF task")
                continue
            checked += 1
            if declared != expected:
                mismatches.append(
                    f"{card_id}.{phase_name}: card={declared!r} orchestrator {orchestrator}={expected!r}"
                )

    assert checked > 0, "joined ZERO card phases to TF environments — the join is broken"
    assert not unjoinable, "unjoinable cards:\n  " + "\n  ".join(unjoinable)
    assert not mismatches, "card/terraform environment drift:\n  " + "\n  ".join(mismatches)


def test_card_environments_are_defined_in_terraform() -> None:
    """A card may not name an environment that does not exist.

    Different defect from naming the WRONG one, so it gets its own assertion:
    `wf-action-context` declares `environment: spadl`, and the module defines only
    {analytics, dbt, default, embeddings, hf, lakebase, statsbomb}. A wrong name is
    misleading; a non-existent one is unresolvable, and no gate covered either.
    """
    # NOTE: this is the set of environments REFERENCED by a task, not the set DECLARED
    # in the module. They coincide today. If a declared-but-unreferenced environment is
    # ever added, a card naming it would false-positive here — parse the `environment {}`
    # blocks at that point rather than loosening this assertion.
    defined = {env for env, _ in _parse_tf_task_environments().values()}
    assert defined, "parsed ZERO environments from main.tf — the parser is broken"

    dangling: list[str] = []
    for path in sorted(_CARDS_DIR.glob("wf-*.yaml")):
        card = _load_card(path)
        for phase_name, phase in _card_phases(card).items():
            declared = phase.get("environment")
            if declared is not None and declared not in defined:
                dangling.append(f"{path.stem}.{phase_name}: environment={declared!r} not in {sorted(defined)}")

    assert not dangling, "cards naming undefined environments:\n  " + "\n  ".join(dangling)
```

- [ ] **Step 2: Run to verify it fails**

**Lint the new test code FIRST — before running it:**

```bash
uv run ruff check src/tests/test_card_parity_with_terraform.py
```

> This step is not ceremony. Rev 3 shipped this test with `unjoinable` used but never bound — `F821`, caught by ruff in seconds, but pytest would have raised `NameError` *before producing the work-list this whole step exists to produce*. A step whose value IS its output must have its code checked before the output is trusted. Do the same for every new test file in this plan.

Then run:

`uv run pytest src/tests/test_card_parity_with_terraform.py -k "environment" -q`
Expected: **FAIL — and the assertion message IS the work-list for Step 4.** Record it verbatim before changing anything.

> Sanity-check the parser against a known-true value before trusting the list: `hf_sync`'s
> `environment_key` is **`hf`** (verified via the Jobs API). If the parser reports anything
> else for `hf_sync`, it is mis-assigning keys across task boundaries — fix the parser, not
> the cards. Likewise `parsed ZERO` means the parser is broken, not that the cards are clean.

- [ ] **Step 3: Implement `_parse_tf_task_environments`**

Mirror `_parse_tf_task_timeouts` **exactly** — same depth-tracking, same "LAST match wins → innermost enclosing task" rule, same `in_task_depth` reset. Match `environment_key\s*=\s*"([^"]+)"`.

> Use `"([^"]+)"`, not `"([a-z_]+)"`. The restrictive form silently *skips* any key containing a digit or hyphen instead of failing, and with `assert checked > 0` as the only net a partially-skipping parser still passes.

Return `{task_key: (environment, entry_point)}`.

- [ ] **Step 4: Repair every drifted card**

Work through the Step 2 failure list. For each, the **Terraform value wins** — the card is documentation about the runtime, so the card changes, never the TF.

Two of these are cards this cycle also edits (`wf-publish-spadl-vaep`, `wf-import-psxg` — both claim `analytics`, both run under `hf` via `hf_sync`). Fix them here; Tasks 2 and 7 then only change `trigger`/`task_key`/`timeout`.

Also fix the dangling reference surfaced by `test_card_environments_are_defined_in_terraform`: `wf-action-context` declares `environment: spadl`, which no TF task defines. Resolve it to the environment its task actually uses — **look it up, do not assume** — and if the answer is that the card describes a task that no longer exists, say so in the commit message rather than silently picking a plausible value.

- [ ] **Step 5: Run the whole parity file**

Run: `uv run pytest src/tests/test_card_parity_with_terraform.py -q`
Expected: PASS, including the pre-existing timeout parity test (which must not regress).

- [ ] **Step 6: Full gates, then STOP for commit 1 approval**

```bash
export DATABRICKS_TOKEN="$(uv run --extra sdk python scripts/mint_databricks_oauth.py 2>/dev/null | tail -1)"
uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/
uv run pyright src/
uv run pytest src/tests/ -q
```
Commit message must name the SCOPE honestly: a new parity gate, a new undefined-environment gate, and ~18 card repairs — and record the orchestrated-leg finding (nine `wf-hf-sync` sub-operation cards told operators `analytics` for a task running on `wheel + huggingface_hub` only). **Do not commit without approval.**

---

# COMMIT 2 — SEC7: decouple the dbt build from hf_sync

### Task 2: Correct `wf-import-psxg.yaml` for standalone operation

**Files:**
- Modify: `workflow-cards/wf-import-psxg.yaml`

> **The phase is `execution.import`, NOT `execution.export`.** rev 1 said otherwise and was wrong. Read the file before editing.

- [ ] **Step 1: Edit the execution block**

```yaml
execution:
  import:
    trigger: scheduled
    runtime: databricks-workflow
    task_key: import_psxg_predictions
    entry_point: import_psxg_predictions
    module: ingestion.import_psxg_predictions
    distribution: driver-bound
    timeout: "600s"
    environment: hf
```

Changes: `trigger: orchestrated` → `scheduled`; drop `orchestrated_by: wf-hf-sync`; add `task_key`; **`timeout: "900s"` → `"600s"`** (must equal the TF block in Task 3 or `test_card_phase_timeouts_match_terraform` fails); `environment` already fixed in Task 1.

- [ ] **Step 2: Note the now-inert card dependency**

`wf-import-psxg.yaml` declares `depends_on: [wf-export-shots]`, and `export_shots` **stays inside `hf_sync`** — so after the split this import can run first. That is fine: `hf_sync.py:152-153` records that imports pull from *previous* HF Jobs runs, so it was never a within-run dependency. Add a comment saying so, or the next reader re-derives it:

```yaml
# ADR-074: lineage only, NOT runtime ordering. Imports pull from a PREVIOUS
# HF Jobs run (see hf_sync.py `_SUB_OPERATIONS` header), so this task
# deliberately has no Terraform depends_on — matching import_obso_results.
```

---

### Task 3: Split `import_psxg_predictions` into its own task

**Files:**
- Modify: `src/ingestion/hf_sync.py` (`_SUB_OPERATIONS`, `_VOLUME_PATHS`, header comment)
- Modify: `terraform/modules/workflows/main.tf`
- Modify: `workflow-cards/wf-hf-sync.yaml`
- Modify: `dbt_project/seeds/task_workflow_mapping.csv`
- Test: `src/tests/test_hf_sync.py`, `src/tests/test_card_parity_with_terraform.py`, `src/tests/test_workflow_dag_bronze_reads.py`

**Interfaces:**
- Consumes: entry point `import_psxg_predictions = "ingestion.import_psxg_predictions:main"` — **already exists at `pyproject.toml:200`. Do not add it again.**
- Produces: task key `import_psxg_predictions`; `_SUB_OPERATIONS` length **8**.

- [ ] **Step 1: Write the failing tests**

In `src/tests/test_hf_sync.py`:

```python
    def test_import_psxg_is_no_longer_an_hf_sync_sub_operation(self) -> None:
        """SEC7 / ADR-074 — the ONLY leg dbt needs must not sit behind nine publishers.

        Since ADR-073 hf_sync FAILS its task on any sub-op failure, leaving this here
        means an HF Hub outage blocks dbt_build_output_marts and its two dependents.
        """
        from ingestion.hf_sync import _SUB_OPERATIONS, _VOLUME_PATHS

        labels = [label for label, _ in _SUB_OPERATIONS]
        assert "ingestion.import_psxg_predictions" not in labels
        assert "ingestion.import_psxg_predictions" not in _VOLUME_PATHS

    def test_hf_sync_is_export_only(self) -> None:
        """No sub-operation may write a bronze table dbt reads — that coupling IS SEC7."""
        from ingestion.hf_sync import _SUB_OPERATIONS

        assert not [x for x, _ in _SUB_OPERATIONS if "import_" in x]
```

Update the count assertions to **8** (`test_sub_operations_count`, `test_calls_all_sub_operations`), with the ADR-074 line added to the existing changelog docstring.

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest src/tests/test_hf_sync.py -q`
Expected: FAIL — label still present, count still 9.

- [ ] **Step 3: Remove from `hf_sync`**

Delete from `_SUB_OPERATIONS`:

```python
    ("ingestion.import_psxg_predictions", _make_volume_op("ingestion.import_psxg_predictions")),
```

Delete from `_VOLUME_PATHS`:

```python
    "ingestion.import_psxg_predictions": "/Volumes/soccer_analytics/dev_gold/model_weights/psxg",
```

Extend the header comment above `_SUB_OPERATIONS` (it currently documents only the PR-Cycle-B split):

```python
# ADR-074 (2026-08-08) split `ingestion.import_psxg_predictions` out for the SAME
# reason PR-Cycle-B split import_obso_results: it writes bronze.psxg_predictions,
# which stg_psxg__predictions reads, so dbt_build_output_marts depended on hf_sync.
# Since ADR-073 hf_sync FAILS its task on any sub-op failure, that dependency let an
# HF Hub outage block the daily dbt build. hf_sync is now EXPORT-ONLY and gates nothing.
```

Also fix the module docstring at `hf_sync.py:4`, which still says "currently 9 sub-operations".

- [ ] **Step 4: Add the Terraform task**

Insert immediately after the `import_obso_results` block (alphabetical):

```hcl
  # ── Task: Import PSxG predictions from HF Hub ───────────────────────────
  # ADR-074 / SEC7: split out of hf_sync, mirroring PR-Cycle-B's split of
  # import_obso_results. Writes bronze.psxg_predictions, which
  # stg_psxg__predictions reads — so dbt_build_output_marts must depend on THIS,
  # not on a task that also runs eight HF Hub publishers.
  #
  # NO depends_on, matching import_obso_results: a pure HF Hub download with no
  # upstream in this run (imports pull from a PREVIOUS HF Jobs run).
  task {
    task_key        = "import_psxg_predictions"
    timeout_seconds = 600
    max_retries     = 1 # HF Hub download — transient failures benefit from retry

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "import_psxg_predictions"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
        "--volume-path", "/Volumes/soccer_analytics/dev_gold/model_weights/psxg",
      ]
    }

    environment_key = "hf"
  }
```

> `--volume-path` is `required=True` in that module's argparse — omitting it is a runtime failure, not a default.

- [ ] **Step 5: Repoint the dbt dependency**

In `dbt_build_output_marts`, replace `depends_on { task_key = "hf_sync" }` with, in alphabetical position:

```hcl
    depends_on { task_key = "import_psxg_predictions" }
```

Verified repo-wide: `main.tf:874` was the **only** dependency on `hf_sync`.

- [ ] **Step 6: Register the bronze-read edge**

In `src/tests/test_workflow_dag_bronze_reads.py`, add to `_BRONZE_READ_REQUIREMENTS`:

```python
    ("dbt_build_output_marts", "psxg_predictions", "import_psxg_predictions"),
```

Without this the new edge is unregistered and SEC7 can be silently reverted by deleting one TF line.

- [ ] **Step 7: Card + seed + parity map**

`workflow-cards/wf-hf-sync.yaml`: remove `- wf-import-psxg` from `execution.orchestration.sub_operations`.

`dbt_project/seeds/task_workflow_mapping.csv`, alphabetical:

```csv
import_psxg_predictions,wf-import-psxg
```

`src/tests/test_card_parity_with_terraform.py`: remove `"ingestion.import_psxg_predictions": "wf-import-psxg"` from the sub-operation map (~line 488); add to `_DIRECT_TASK_ENTRY_POINT_TO_CARD` (~line 314):

```python
    "import_psxg_predictions": "wf-import-psxg",
```

- [ ] **Step 8: Full gates, then STOP for commit 2 approval**

```bash
export DATABRICKS_TOKEN="$(uv run --extra sdk python scripts/mint_databricks_oauth.py 2>/dev/null | tail -1)"
uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/
uv run pyright src/
uv run pytest src/tests/ -q
```
Expected: all green. Present the diff; **do not commit without approval.**

---

# COMMIT 3 — driver isolation + memory observability

### Task 4: Hexagonal driver-memory probe in `src/shared/`

**Files:**
- Create: `src/shared/memory.py`
- Test: `src/tests/test_shared_memory.py`

**Interfaces:**
- Produces: `RssProbe = Callable[[], int | None]`; `peak_rss_bytes()`; `current_rss_bytes()`; frozen `MemorySample(label, peak_bytes, current_bytes, peak_delta_bytes)`; `sample_memory(label, previous_peak, *, peak_probe=peak_rss_bytes, current_probe=current_rss_bytes)`; `format_memory(sample)`.

- [ ] **Step 1: Write the failing tests**

```python
"""Tests for the stdlib-only driver-memory probe (ADR-074).

Core tests inject fake probes — `resource` and `/proc` do not exist on Windows.
The ADAPTER test is Linux-gated and DOES run on CI (ubuntu-latest): the whole
plan rests on `ru_maxrss * 1024`, and a wrong unit would make every logged
number wrong by 1024x while still looking plausible.
"""

from __future__ import annotations

import sys

import pytest

from shared.memory import MemorySample, current_rss_bytes, format_memory, peak_rss_bytes, sample_memory

_GB = 1024**3


def test_sample_computes_peak_delta_against_previous() -> None:
    s = sample_memory("publish_x", 2 * _GB, peak_probe=lambda: 7 * _GB, current_probe=lambda: 5 * _GB)
    assert (s.label, s.peak_bytes, s.current_bytes, s.peak_delta_bytes) == ("publish_x", 7 * _GB, 5 * _GB, 5 * _GB)


def test_first_sample_has_no_delta() -> None:
    s = sample_memory("first", None, peak_probe=lambda: 3 * _GB, current_probe=lambda: 3 * _GB)
    assert s.peak_delta_bytes is None


def test_unsupported_platform_degrades_to_none_not_crash() -> None:
    s = sample_memory("x", None, peak_probe=lambda: None, current_probe=lambda: None)
    assert (s.peak_bytes, s.current_bytes, s.peak_delta_bytes) == (None, None, None)
    assert "unavailable" in format_memory(s)


def test_delta_is_none_when_probe_unavailable_even_with_known_previous() -> None:
    s = sample_memory("x", 2 * _GB, peak_probe=lambda: None, current_probe=lambda: None)
    assert s.peak_delta_bytes is None


def test_format_reports_peak_resident_and_delta() -> None:
    s = sample_memory("op", 1 * _GB, peak_probe=lambda: 4 * _GB, current_probe=lambda: 3 * _GB)
    text = format_memory(s)
    assert "4.00 GB" in text and "3.00 GB" in text and "+3.00 GB" in text


def test_flat_peak_reads_as_zero_delta_not_a_drop() -> None:
    """ru_maxrss is a high-water mark: a light op shows +0.00 GB, never negative."""
    s = sample_memory("light", 8 * _GB, peak_probe=lambda: 8 * _GB, current_probe=lambda: 2 * _GB)
    assert s.peak_delta_bytes == 0
    assert "+0.00 GB" in format_memory(s)


@pytest.mark.skipif(sys.platform == "win32", reason="resource// proc are Linux-only; CI is ubuntu-latest")
def test_adapters_report_plausible_real_numbers() -> None:
    """The ONE number the whole plan depends on. A 1024x unit error dies here.

    Allocates ~256 MB and asserts resident RSS rises by roughly that much.
    Without this the adapters are never executed on any platform.
    """
    before = current_rss_bytes()
    assert before is not None and before > 8 * 1024**2, f"implausible baseline RSS: {before}"

    blob = bytearray(256 * 1024**2)
    try:
        after = current_rss_bytes()
        assert after is not None
        grew = after - before
        assert 128 * 1024**2 < grew < 512 * 1024**2, f"256MB alloc moved RSS by {grew} bytes — check units"
    finally:
        del blob

    peak = peak_rss_bytes()
    assert peak is not None
    assert peak >= after, "peak is a high-water mark; it cannot be below a resident reading"
    assert peak < 64 * _GB, f"implausible peak {peak} — units are probably wrong"
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest src/tests/test_shared_memory.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'shared.memory'`. On Windows the adapter test reports `skipped`; that is correct locally and it **will** run on CI.

- [ ] **Step 3: Implement**

```python
"""Driver-memory probe — stdlib only, hexagonal (ADR-074).

Pure core (`sample_memory`, `format_memory`) is separated from the OS adapters
(`peak_rss_bytes`, `current_rss_bytes`) so the logic is testable with injected
fakes. Both adapters return ``None`` where unsupported (Windows) and every
consumer must tolerate that.

WHY THIS EXISTS: hf_sync's driver was OOM-killed (exit 137) on 2026-08-07 while
running nine sub-operations in ONE process. The publisher that died measured
6.97 GB alone in a ~16 GB driver, and the three sub-ops preceding it are
Spark-native with no `.toPandas()` — so the consumer of the remaining memory is
UNIDENTIFIED. Three theories were advanced and all three were wrong. Rather than
guess a fourth time, every @workflow now reports memory (see
`ingestion.memory_hook.MemoryHook`).

READ THE TWO NUMBERS DIFFERENTLY:
  peak     -- high-water mark; NEVER falls. A delta means "this unit of work
              pushed the ceiling up by X".
  resident -- in memory right now. This is what reveals RETENTION: a workflow
              that ENDS with a high resident value left something behind.
"""

from __future__ import annotations

import dataclasses
import sys
from typing import Callable

RssProbe = Callable[[], "int | None"]

_BYTES_PER_GB = 1024**3


def peak_rss_bytes() -> int | None:
    """Peak RSS of this process, or None where unsupported (e.g. Windows)."""
    try:
        import resource
    except ImportError:
        return None
    raw = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    # ru_maxrss is KiB on Linux but BYTES on macOS. Databricks serverless is Linux;
    # branch anyway so this module does not lie on a developer's laptop.
    return raw if sys.platform == "darwin" else raw * 1024


def current_rss_bytes() -> int | None:
    """Currently-resident RSS, or None where unsupported."""
    try:
        with open("/proc/self/statm", encoding="ascii") as fh:
            fields = fh.read().split()
    except OSError:
        return None
    if len(fields) < 2:
        return None
    import os

    return int(fields[1]) * os.sysconf("SC_PAGE_SIZE")


@dataclasses.dataclass(frozen=True)
class MemorySample:
    """One observation of driver memory, taken around a named unit of work."""

    label: str
    peak_bytes: int | None
    current_bytes: int | None
    peak_delta_bytes: int | None


def sample_memory(
    label: str,
    previous_peak: int | None,
    *,
    peak_probe: RssProbe = peak_rss_bytes,
    current_probe: RssProbe = current_rss_bytes,
) -> MemorySample:
    """Observe memory for `label`, with the peak delta against `previous_peak`."""
    peak = peak_probe()
    current = current_probe()
    delta = peak - previous_peak if (peak is not None and previous_peak is not None) else None
    return MemorySample(label=label, peak_bytes=peak, current_bytes=current, peak_delta_bytes=delta)


def _gb(value: int | None) -> str:
    return "unavailable" if value is None else f"{value / _BYTES_PER_GB:.2f} GB"


def format_memory(sample: MemorySample) -> str:
    """Render a sample for the structured log."""
    if sample.peak_bytes is None and sample.current_bytes is None:
        return f"driver memory unavailable on this platform (after {sample.label})"
    delta = "n/a" if sample.peak_delta_bytes is None else f"+{sample.peak_delta_bytes / _BYTES_PER_GB:.2f} GB"
    suffix = ""
    if sample.peak_bytes is not None and sample.current_bytes is not None and sample.peak_bytes < sample.current_bytes:
        # Physically impossible — the signature of a units mismatch between the two adapters.
        suffix = " [WARNING: peak < resident — probe units are inconsistent]"
    return f"peak={_gb(sample.peak_bytes)} (delta {delta}), resident={_gb(sample.current_bytes)}{suffix}"
```

- [ ] **Step 4: Run**

Run: `uv run pytest src/tests/test_shared_memory.py -q`
Expected: 6 passed, 1 skipped (Windows). CI runs all 7.

- [ ] **Step 5: Confirm the stdlib boundary**

Run: `uv run lint-imports`
Expected: PASS — `src/shared/` still imports nothing external.

---

### Task 5: `MemoryHook` — memory at the `LifecycleHook` seam

Wiring the probe into `hf_sync`'s loop would leave the two split-out tasks — including the one that OOM-killed the driver — as the platform's only blind spot. `LifecycleHook` is the port this repo already uses for exactly this.

**Files:**
- Create: `src/ingestion/memory_hook.py`
- Modify: `src/ingestion/bootstrap.py`
- Test: `src/tests/test_memory_hook.py`

**Interfaces:**
- Consumes: `shared.memory.{sample_memory, format_memory}`; `workflows.hooks.LifecycleHook` (Protocol — structural, no import of `workflows` needed for typing at runtime); `WorkflowContext`.
- Produces: `MemoryHook`, registered by `bootstrap_hooks`.

- [ ] **Step 1: Write the failing tests**

```python
"""Tests for MemoryHook (ADR-074) — memory observability at the LifecycleHook seam."""

from __future__ import annotations

from unittest.mock import MagicMock

from ingestion.memory_hook import MemoryHook

_GB = 1024**3


def _ctx(workflow_id: str = "wf-publish-spadl-vaep") -> MagicMock:
    ctx = MagicMock()
    ctx.workflow_id = workflow_id
    return ctx


def test_logs_memory_on_complete_with_delta_across_the_workflow() -> None:
    logger = MagicMock()
    probes = iter([2 * _GB, 7 * _GB])
    hook = MemoryHook(logger=logger, peak_probe=lambda: next(probes), current_probe=lambda: 5 * _GB)

    ctx = _ctx()
    hook.on_start(ctx)
    hook.on_complete(ctx, 9_756_155)

    logged = " ".join(str(c) for c in logger.info.call_args_list)
    assert "wf-publish-spadl-vaep" in logged
    assert "+5.00 GB" in logged


def test_logs_memory_on_error_too() -> None:
    """The failing workflow is the one whose memory matters most."""
    logger = MagicMock()
    probes = iter([2 * _GB, 15 * _GB])
    hook = MemoryHook(logger=logger, peak_probe=lambda: next(probes), current_probe=lambda: 15 * _GB)

    ctx = _ctx("wf-hf-sync")
    hook.on_start(ctx)
    hook.on_error(ctx, RuntimeError("boom"))

    logged = " ".join(str(c) for c in logger.error.call_args_list) + " ".join(
        str(c) for c in logger.info.call_args_list
    )
    assert "wf-hf-sync" in logged and "13.00 GB" in logged


def test_unsupported_platform_is_silent_not_noisy() -> None:
    """On Windows both probes return None; the hook must not emit a useless line per workflow."""
    logger = MagicMock()
    hook = MemoryHook(logger=logger, peak_probe=lambda: None, current_probe=lambda: None)
    ctx = _ctx()
    hook.on_start(ctx)
    hook.on_complete(ctx, 1)
    assert logger.info.call_count == 0


def test_on_skip_emits_nothing() -> None:
    """A skipped workflow consumed nothing; a line would be noise.

    NOTE THE ARITY: the runner dispatches `on_skip(ctx, reason)` — two args
    (`workflows/runner.py`: `_dispatch(active_hooks, "on_skip", ctx, str(exc))`).
    Calling it with one here would shape the test to a WRONG implementation and
    pass while production emitted a TypeError traceback per skip.
    """
    logger = MagicMock()
    hook = MemoryHook(logger=logger, peak_probe=lambda: 1 * _GB, current_probe=lambda: 1 * _GB)
    hook.on_skip(_ctx(), "No HF sync work")
    assert logger.info.call_count == 0


def test_registered_hooks_match_the_lifecycle_protocol() -> None:
    """Every registered hook's signatures must match LifecycleHook exactly.

    Structural typing gives NO compile-time check: nothing declares MemoryHook as a
    LifecycleHook, so pyright cannot flag an arity drift. `_dispatch` swallows the
    resulting TypeError but logs it at ERROR with a traceback — which would bury the
    per-workflow memory lines this cycle exists to read. This test is the price of
    the Protocol seam.
    """
    import inspect

    from ingestion.cost_hook import CostEstimateHook
    from workflows.hooks import LifecycleHook

    for hook_cls in (CostEstimateHook, MemoryHook):
        for name in ("on_start", "on_complete", "on_skip", "on_error"):
            expected = list(inspect.signature(getattr(LifecycleHook, name)).parameters)
            actual = list(inspect.signature(getattr(hook_cls, name)).parameters)
            # Compare ARITY, not names: _dispatch calls positionally, so a hook using
            # `context` instead of `ctx` violates nothing. A gate with false positives
            # is a gate that eventually gets deleted.
            assert len(actual) == len(expected), (
                f"{hook_cls.__name__}.{name} takes {len(actual)} params, protocol takes {len(expected)}: "
                f"{actual} vs {expected}"
            )


def test_hook_never_raises_into_the_workflow() -> None:
    """Observability must not be able to fail a pipeline.

    The probe raises ValueError — NOT OSError — deliberately: an earlier draft caught
    only OSError while its docstring promised "never", and the test raised OSError, so
    it agreed with the code instead of the claim.
    """
    logger = MagicMock()

    def _boom() -> int | None:
        raise ValueError("malformed /proc field")

    hook = MemoryHook(logger=logger, peak_probe=_boom, current_probe=_boom)
    ctx = _ctx()
    hook.on_start(ctx)
    hook.on_complete(ctx, 1)  # must not raise
    logger.error.assert_called()
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest src/tests/test_memory_hook.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'ingestion.memory_hook'`

- [ ] **Step 3: Implement**

```python
"""Driver-memory LifecycleHook (ADR-074).

Registered by `bootstrap_hooks`, so EVERY @workflow-decorated pipeline reports
driver memory — not just hf_sync's sub-operations. That coverage is the point:
the 2026-08-07 OOM (exit 137) is still unexplained, and the publisher being
split into its own task would otherwise become the platform's only blind spot
exactly where a `.toPandas()`-shaped leak is most likely to land.

Sibling of `ingestion.cost_hook.CostEstimateHook` — same port, same
registration site. `LifecycleHook` is a Protocol, so this lives in
`ingestion/` (it imports `shared.memory`) and `src/workflows/` is untouched;
`.importlinter` forbids workflows -> shared.

DELIBERATE: `on_skip` emits nothing. A skipped workflow consumed nothing, and
`hf_sync` already logs "Watermark skip: %s". Note this differs from a
loop-body probe, which would emit a line per skip.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.memory import RssProbe, current_rss_bytes, format_memory, peak_rss_bytes, sample_memory


class MemoryHook:
    """Report driver peak/resident memory around every workflow."""

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        peak_probe: RssProbe = peak_rss_bytes,
        current_probe: RssProbe = current_rss_bytes,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._peak_probe = peak_probe
        self._current_probe = current_probe
        self._start_peak: dict[str, int | None] = {}

    def _safe(self, probe: RssProbe) -> int | None:
        """Run a probe without ever propagating.

        `except Exception` is deliberate and BROAD: the docstring promises telemetry
        cannot fail the work it observes, and a narrow tuple would not keep that promise
        (an injected probe can raise anything; `current_rss_bytes` can raise ValueError
        on a malformed /proc field). ERROR level, never warning -- ADR-002: a
        warning-level swallow hid the cost-hook blocker for 62+ hours. `run_workflow`'s
        `_dispatch` also wraps hook calls, so this is belt-and-braces; it exists so the
        failure is ATTRIBUTED to the probe rather than surfacing as an anonymous hook error.
        """
        try:
            return probe()
        except Exception as exc:  # noqa: BLE001 — telemetry must not fail the observed work; see docstring
            self._logger.error("MemoryHook probe failed: %s", exc, exc_info=True)
            return None

    def on_start(self, ctx: Any) -> None:
        self._start_peak[str(ctx.workflow_id)] = self._safe(self._peak_probe)

    def _report(self, ctx: Any, outcome: str) -> None:
        wid = str(ctx.workflow_id)
        sample = sample_memory(
            wid,
            self._start_peak.pop(wid, None),
            peak_probe=lambda: self._safe(self._peak_probe),
            current_probe=lambda: self._safe(self._current_probe),
        )
        if sample.peak_bytes is None and sample.current_bytes is None:
            return  # unsupported platform -- one useless line per workflow helps nobody
        self._logger.info("driver memory %s %s: %s", outcome, wid, format_memory(sample))

    def on_complete(self, ctx: Any, row_count: int | None) -> None:
        _ = row_count
        self._report(ctx, "after")

    def on_error(self, ctx: Any, error: Exception) -> None:
        _ = error
        self._report(ctx, "at failure of")

    def on_skip(self, ctx: Any, reason: str) -> None:
        # ARITY IS LOAD-BEARING: runner.py dispatches `on_skip(ctx, str(exc))`. A one-arg
        # version raises TypeError on EVERY skip; _dispatch swallows it but logs ERROR with
        # a traceback -- burying the memory lines this hook exists to produce.
        _ = reason
        self._start_peak.pop(str(ctx.workflow_id), None)
```

- [ ] **Step 4: Register in `bootstrap_hooks`**

In `src/ingestion/bootstrap.py`, beside the existing `CostEstimateHook` registration:

```python
    from ingestion.memory_hook import MemoryHook

    register_hook(MemoryHook())
```

- [ ] **Step 5: Run**

Run: `uv run pytest src/tests/test_memory_hook.py src/tests/test_shared_memory.py -q`
Expected: PASS.

---

### Task 6: Make `publish_spadl_vaep_hf` standalone-capable (BLOCKERS 1 + 2)

`main()` is currently `configure_logging → parse_ingestion_args → get_spark_session → run_pipeline`. **No watermark guard, no `bootstrap_hooks`** — `hf_sync`'s factory supplied both. Promote it as-is and the most expensive driver operation in the platform runs **unconditionally every day** with **no cost row**.

**Files:**
- Modify: `src/ingestion/publish_spadl_vaep_hf.py` (`main` only — `run_pipeline` is untouched)
- Test: `src/tests/test_guard_conformance.py`

- [ ] **Step 1: Write the failing test**

In `src/tests/test_guard_conformance.py`, add to `TestWatermarkRecordAfterSuccess._STANDALONE_MODULES`:

```python
        # ADR-074: promoted out of hf_sync into its own task; main() must now do
        # what _make_watermark_op used to do for it.
        "ingestion.publish_spadl_vaep_hf",
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest src/tests/test_guard_conformance.py -k WatermarkRecordAfterSuccess -q`
Expected: FAIL — `main()` calls neither `check_upstream_freshness` nor `record_watermarks`.

- [ ] **Step 3: Implement, mirroring `ingestion.model_validation.main`**

```python
def main() -> None:
    """CLI entry point for the SPADL/VAEP publisher.

    ADR-074: this module was promoted out of hf_sync into its own Databricks task
    (it peaks at 6.97 GB and must not share a driver). hf_sync's
    `_make_watermark_op` factory previously supplied BOTH the watermark gate and
    the hook registration; standalone, main() must do it, or a 9.76M-row publish
    plus four HF Hub uploads runs unconditionally every day with no cost row.
    """
    configure_logging("publish_spadl_vaep_hf")
    args = parse_ingestion_args("Publish SPADL/VAEP action values to HF Hub")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks
    from ingestion.guards import check_upstream_freshness, record_watermarks, resolve_upstream_tables_from_card

    bootstrap_hooks(spark, args.catalog, args.schema)

    card_id = "wf-publish-spadl-vaep"
    upstream = resolve_upstream_tables_from_card(card_id, args.catalog, args.schema)
    freshness = check_upstream_freshness(spark, args.catalog, card_id, upstream)
    if freshness.count == 0:
        logger.info("Watermark skip: %s — no upstream changes", card_id)
        return

    run_pipeline(spark, args.catalog, args.schema, logger)

    record_watermarks(spark, args.catalog, card_id, upstream)
```

> The card pins `{catalog}.dev_gold.fct_action_values` (ADR-073), so the `--schema bronze` argument does not reach this resolution. Do not "simplify" by passing `args.schema` somewhere it changes the layer.

- [ ] **Step 4: Close the `bootstrap_hooks` gap with a gate, not a note**

`TestWatermarkRecordAfterSuccess` AST-checks only `record_watermarks`. Nothing checks `bootstrap_hooks` — which is why Blocker 2 (no `workflow_cost_live` row) was invisible. This cycle closes the environment, bronze-read and gold-read gates; leaving this one open makes it the odd one out, and `publish_xg_shots_hf` / `publish_freeze_frame_hf` are the next promotion candidates with the identical hole.

Add to `src/tests/test_guard_conformance.py`:

```python
def test_direct_task_modules_register_hooks_in_main() -> None:
    """Every module behind its own TF task must call bootstrap_hooks in main().

    A sub-operation inherits hook registration from its orchestrator's main(); a
    standalone task does not. publish_spadl_vaep_hf was promoted with a main() that
    called neither bootstrap_hooks nor the watermark guard — it would have published
    9.76M rows daily with no workflow_cost_live row, and no test would have gone red
    (ADR-074, Blocker 2).
    """
    import ast
    import importlib

    from tests.test_card_parity_with_terraform import (
        _CARDS_DIR,
        _DIRECT_TASK_ENTRY_POINT_TO_CARD,
        _card_phases,
        _load_card,
    )

    missing: list[str] = []
    for task_key, card_id in sorted(_DIRECT_TASK_ENTRY_POINT_TO_CARD.items()):
        # Resolve the MODULE via the card, not via the map key. The map is keyed by
        # TASK_KEY, not entry point (e.g. "dbt_build_input_marts" -> entry_point
        # "dbt_build"; "publish_spadl_vaep" -> entry_point "publish_spadl_vaep_hf"),
        # so deriving a module from the key would silently skip exactly the module
        # this gate exists for. Every card phase already carries `module:`.
        if card_id is None:
            continue  # intentional gap — pure orchestration, no card
        phases = _card_phases(_load_card(_CARDS_DIR / f"{card_id}.yaml"))
        modules = {p["module"] for p in phases.values() if p.get("module")}
        for module_name in sorted(modules):
            module_file = importlib.import_module(module_name).__file__
            if module_file is None:
                continue
            tree = ast.parse(Path(module_file).read_text(encoding="utf-8"))
            main_fn = next(
                (n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "main"),
                None,
            )
            if main_fn is None:
                continue
            calls = {n.func.id for n in ast.walk(main_fn) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
            if "bootstrap_hooks" not in calls:
                missing.append(f"{module_name}.main() ({task_key}) does not call bootstrap_hooks")

    assert not missing, "standalone task modules missing hook registration:\n  " + "\n  ".join(missing)
```

> **Expect three PRE-EXISTING failures**, verified before writing this task — `ingestion.staleness_monitor`, `ingestion.dbt_runner`, `ingestion.refresh_synced_tables` all have **zero** `bootstrap_hooks` occurrences.
>
> **Bring that list to the user; do not fix it.** Widening this cycle is a scope decision. And `dbt_runner` needs *why* before *fix*: three dbt tasks with no `CostEstimateHook` either means dbt builds have no `workflow_cost_live` rows (a finding worth its own item) or they register hooks by another path (a fence — Chesterton's). Adding the call to find out is the wrong order.
>
> To keep commit 1 green while the decision is pending, add an explicit `_PRE_EXISTING_HOOK_GAPS` allowlist naming those three with that reasoning, so the gate is live for new tasks without silently absorbing the old ones.

- [ ] **Step 5: Run**

Run: `uv run pytest src/tests/test_guard_conformance.py src/tests/test_hf_sync.py -q`
Expected: PASS.

---

### Task 7: Split `publish_spadl_vaep_hf` into its own task

**Files:**
- Modify: `src/ingestion/hf_sync.py`, `terraform/modules/workflows/main.tf`, `workflow-cards/wf-hf-sync.yaml`, `workflow-cards/wf-publish-spadl-vaep.yaml`, `dbt_project/seeds/task_workflow_mapping.csv`
- Test: `src/tests/test_hf_sync.py`, `src/tests/test_card_parity_with_terraform.py`, `src/tests/test_workflow_dag_gold_reads.py`

**Interfaces:**
- Consumes: entry point `publish_spadl_vaep_hf` — **already at `pyproject.toml:205`.**
- Produces: task key `publish_spadl_vaep`; `_SUB_OPERATIONS` length **7**.

- [ ] **Step 1: Write the failing tests**

```python
    def test_publish_spadl_vaep_is_no_longer_an_hf_sync_sub_operation(self) -> None:
        """ADR-074 — measured at 6.97 GB peak; it must not share a driver."""
        from ingestion.hf_sync import _SUB_OPERATIONS

        assert "ingestion.publish_spadl_vaep_hf" not in [label for label, _ in _SUB_OPERATIONS]
```

Update both count assertions **8 → 7**.

In `src/tests/test_workflow_dag_gold_reads.py`, add to `_GOLD_READ_REQUIREMENTS`:

```python
    ("publish_spadl_vaep", "fct_action_values", "dbt_build_intermediate_marts"),
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest src/tests/test_hf_sync.py src/tests/test_workflow_dag_gold_reads.py -q`
Expected: FAIL on both.

- [ ] **Step 3: Remove the sub-operation**

Delete from `_SUB_OPERATIONS`:

```python
    (
        "ingestion.publish_spadl_vaep_hf",
        _make_watermark_op("ingestion.publish_spadl_vaep_hf", "wf-publish-spadl-vaep"),
    ),
```

- [ ] **Step 4: Add the Terraform task**

Alphabetical position (after `preflight_*`, before `refresh_synced_tables`):

```hcl
  # ── Task: Publish SPADL/VAEP action values to HF Hub ───────────────────
  # ADR-074: split out of hf_sync, which ran nine sub-operations in ONE driver
  # process and was OOM-killed (exit 137, run 49905842293930). Measured at
  # 6.97 GB peak ALONE in a ~16 GB driver (diagnostic run 939215830803445):
  # safe on its own, fatal when sharing. A dedicated task = a fresh driver.
  #
  # depends_on dbt_build_intermediate_marts, NOT output_marts: fct_action_values
  # is tagged `intermediate_mart` and output_marts explicitly excludes
  # +tag:intermediate_mart. This edge is NEW -- hf_sync had no dbt dependency at
  # all, so this publisher was a SIBLING of the stage that builds its input and
  # has been publishing a mart it has no ordering against (bounded until now
  # only by the watermark gate). Registered in _GOLD_READ_REQUIREMENTS.
  task {
    task_key        = "publish_spadl_vaep"
    timeout_seconds = 1800
    max_retries     = 1 # HF Hub network calls — transient failures benefit from retry

    depends_on { task_key = "dbt_build_intermediate_marts" }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "publish_spadl_vaep_hf"

      parameters = [
        "--catalog", var.catalog_name,
        # This argument is IGNORED for layer resolution -- run_pipeline reads
        # DEFAULT_GOLD_SCHEMA (ADR-073) and the card pins dev_gold. It is passed
        # because parse_ingestion_args makes --schema required.
        "--schema", "bronze",
      ]
    }

    environment_key = "hf"
  }
```

- [ ] **Step 5: Cards, seed, parity map**

`wf-hf-sync.yaml`: remove `- wf-publish-spadl-vaep` from `sub_operations`.

`wf-publish-spadl-vaep.yaml` `execution.export`:

```yaml
  export:
    trigger: scheduled
    runtime: databricks-workflow
    task_key: publish_spadl_vaep
    entry_point: publish_spadl_vaep_hf
    module: ingestion.publish_spadl_vaep_hf
    distribution: driver-bound
    timeout: "1800s"
    environment: hf
```

`task_workflow_mapping.csv`, alphabetical:

```csv
publish_spadl_vaep,wf-publish-spadl-vaep
```

`test_card_parity_with_terraform.py`: move `wf-publish-spadl-vaep` from the sub-operation map into `_DIRECT_TASK_ENTRY_POINT_TO_CARD` as `"publish_spadl_vaep": "wf-publish-spadl-vaep"`.

- [ ] **Step 6: Fix the stale guard docstring**

`src/tests/test_layer_schema_conformance.py::test_hf_sync_gold_consumers_do_not_use_the_passed_schema` opens *"Every hf_sync sub-operation that reads gold…"* and names a module that is no longer one. The test itself is correct (it globs all of `src/ingestion/*.py`, so coverage survives) — only the prose is stale. Reword to "Every `src/ingestion` module that reads gold…".

- [ ] **Step 7: Run**

Run: `uv run pytest src/tests/ -q -k "hf_sync or card or guard or layer or gold_reads"`
Expected: PASS.

---

### Task 8: ADR-074, docs, wheel bump, gates, commit 2

- [ ] **Step 1: Write ADR-074**

`docs/superpowers/adrs/ADR-074-hf-sync-process-isolation-and-memory-observability.md`, per `ADR-TEMPLATE.md`. MUST contain:
- The Evidence table with run IDs.
- **The three rejected designs and why**, each killed by a number.
- **The staleness finding as a named defect** — `hf_sync` had no dbt edge; `fct_action_values` is built by a sibling stage; the watermark gate is all that bounded it; the new edge fixes it. Not a side effect.
- **The BOUNDARY of that fix, stated explicitly.** Per ADR-073's own layer table, `hf_sync` had five gold readers. After both splits it retains **four** — `export_shots_on_target`, `publish_xg_shots_hf`, `export_scoutgpt_training_data`, `prepare_360_training_data` — and **still has no dbt edge at all**. They remain siblings of the stages that build their inputs, bounded only by their own watermark gates, and none is registered in `_GOLD_READ_REQUIREMENTS`. Write this down: "staleness fixed" will otherwise be read as general, and in three months someone builds on a wrong premise.
- **The Task 3/Task 7 asymmetry**: `import_psxg_predictions` was built standalone-capable; `publish_spadl_vaep_hf` was not. That asymmetry is why the cycle is two commits.
- The **known unknown** (~9 GB consumer) and that `MemoryHook` exists to answer it — plus the honest limit: isolating this publisher fixes this publisher; a genuine leak resurfaces for whichever sub-op runs last.
- The deliberate `on_skip` silence.
- Amends ADR-073; closes TODO SEC7.

- [ ] **Step 2: CLAUDE.md rule**

```markdown
- **A driver-bound sub-operation gets its OWN task, and standalone-capability is not free** ([ADR-074](docs/superpowers/adrs/ADR-074-hf-sync-process-isolation-and-memory-observability.md)): `hf_sync` runs its sub-operations in ONE driver process, so their memory is cumulative — it was OOM-killed (exit 137) while the publisher that died measured only 6.97 GB of ~16 GB alone. Anything pulling a multi-million-row mart to the driver via `.toPandas()` belongs in its own Databricks task. **Before promoting a sub-operation to a task, check what the `hf_sync` factory was doing for it** — `_make_watermark_op` supplies the watermark gate AND `hf_sync.main()` supplies `bootstrap_hooks`; a `main()` lacking either turns a gated publish into an unconditional daily one with no `workflow_cost_live` row, and no test catches it (`_STANDALONE_MODULES` is a hardcoded list). `hf_sync` is now EXPORT-ONLY (`test_hf_sync_is_export_only`): a sub-operation writing a bronze table dbt reads couples the daily dbt build to an external service. Driver memory is reported for every `@workflow` by `ingestion.memory_hook.MemoryHook`; **`peak` is a high-water mark (delta = "this raised the ceiling"), `resident` is what reveals retention.**
```

- [ ] **Step 3: TODO.md**

Replace `**Last updated**`. Remove **SEC7** (closed). Add **SEC8**:

```markdown
| SEC8 | Identify the hf_sync driver-memory consumer | Wicked | ADR-074 (2026-08-08) | **The 2026-08-07 OOM (exit 137) is unexplained.** `publish_spadl_vaep_hf` measured 6.97 GB alone in a ~16 GB driver (run `939215830803445`), and the three sub-ops preceding it are Spark-native with no `.toPandas()` — so what consumed the remaining ~9 GB is UNKNOWN. Three theories were advanced and all three were wrong; **do not add a fourth without data.** `MemoryHook` now logs `driver memory after <workflow>: peak=… (delta …), resident=…` for EVERY `@workflow`. **Read the next real run and find the workflow whose delta is large or whose `resident` stays high.** ⚠️ **`wf-hf-sync`'s own line is the ENVELOPE over its sub-operations, not a peer of them** — its delta is the sum of its children's ceiling rises and will be the largest number every run, by construction. Compare sub-op lines to each other; use the parent only as a total. Isolating publish_spadl_vaep fixed that publisher; a genuine leak will resurface for whichever sub-op now runs last. |
| SEC9 | Give `hf_sync`'s four remaining gold readers a dbt edge | Wicked | ADR-074 boundary (2026-08-08) | ADR-074 fixed the staleness ordering for `publish_spadl_vaep` **only**. `hf_sync` still has **no dbt `depends_on` at all**, while retaining four gold readers — `export_shots_on_target`, `publish_xg_shots_hf`, `export_scoutgpt_training_data`, `prepare_360_training_data` — each a SIBLING of the stage that builds its input, bounded only by its own watermark gate. **Scope:** add `depends_on { task_key = "dbt_build_intermediate_marts" }` to `hf_sync` and register the four entries in `_GOLD_READ_REQUIREMENTS`. **Evidence it is actionable, not speculative:** registering `hf_sync` in `_GOLD_READ_REQUIREMENTS` *today* goes red immediately. |
```

- [ ] **Step 4: Wheel bump**

Edit `pyproject.toml` 0.5.90 → 0.5.91, then `uv run python scripts/bump_wheel.py`.
Expected: `Synced 30 file(s) to version 0.5.91.`

- [ ] **Step 5: Full gates**

```bash
export DATABRICKS_TOKEN="$(uv run --extra sdk python scripts/mint_databricks_oauth.py 2>/dev/null | tail -1)"
uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/
uv run pyright src/
uv run pytest src/tests/ -q
```

- [ ] **Step 6: Terraform plan**

Run: `cd terraform/environments/dev && terraform plan`
Expected: **2 tasks added** (`import_psxg_predictions`, `publish_spadl_vaep`); `dbt_build_output_marts.depends_on` changed; wheel path updated. **No task deletions.**

- [ ] **Step 7: STOP for commit 3 approval, then PR**

Merge with `gh pr merge <n> --merge` (NOT `--squash`) to preserve all three commits — `allow_merge_commit` was enabled for this cycle.

---

## Self-Review

**1. Review coverage.** B1+B2 → Task 6. B3 → Task 7 Step 4 + gold-read registration + ADR. M4 → Tasks 4–5 (hook seam; `hf_sync.py` is not edited for memory at all). M5 → Task 4 Step 1 adapter test + `peak < resident` warning + macOS branch. M6 → Tasks 1–2. Bronze-read guard → Task 3 Step 6. Two commits → structure. `hf_sync.py:4` prose → Task 3 Step 3. Guard docstring → Task 7 Step 6.

**2. Placeholder scan.** No "TBD"/"handle edge cases"/"similar to Task N". Every code step carries literal code.

**3. Type consistency.** `sample_memory(label, previous_peak, *, peak_probe, current_probe) -> MemorySample` defined in Task 4, consumed in Task 5 with that signature. `RssProbe` exported from `shared.memory` and imported by `memory_hook`. `MemorySample` fields used consistently. `_SUB_OPERATIONS`: 9 → **8** (Task 3, commit 1) → **7** (Task 7, commit 2) — each commit's suite is green at its own boundary, so rev 1's deliberate red assertion is gone.

**4. Ordering risk.** Commit 1 and commit 2 both edit `hf_sync.py`, `wf-hf-sync.yaml`, `task_workflow_mapping.csv`, and `test_card_parity_with_terraform.py`. Re-read each file before editing in commit 2 — do not apply from memory.

**5. Residual risk, stated.** If the unidentified consumer is a real leak rather than an artefact of accumulation, isolation moves the failure to whichever sub-op now runs last. `MemoryHook` is the detector, not the cure. SEC8 carries this.

**6. Commit structure (rev 3, decided with the user).** Task 1 is its OWN commit. It grew from "fix two cards" to a **~18-card repair** — all 9 `wf-hf-sync` sub-operation cards plus ≥8 direct-task cards plus the `spadl` dangling reference — and a commit titled "SEC7: decouple the dbt build" cannot honestly describe that. Three commits, each independently green.

**7. rev 3 changes (review rounds two and three).** Task 1 rescoped to option 3 — the gate's join now covers orchestrated sub-operation cards, so it catches this cycle's two cards *and* every other drifted one; the drift list is an OUTPUT of Step 2, never hardcoded (two enumeration attempts were already wrong, the second by a parser that reported `hf_sync='dbt'` against a verified `hf`). A second gate added for cards naming *undefined* environments (`wf-action-context` → `spadl`). `MemoryHook.on_skip` corrected to `(ctx, reason)` — the runner dispatches two args and `_dispatch` logs arity failures at ERROR **with a traceback**, which would bury the very lines SEC8 depends on; a signature-conformance test now gates the whole Protocol seam, which `pyright` cannot check because nothing declares `MemoryHook` as a `LifecycleHook`. `_safe` broadened to `except Exception` + `# noqa: BLE001` (the repo's documented preference) so the code matches its docstring, with the test raising `ValueError` to assert the *claim* rather than the implementation. `bootstrap_hooks` gate added (Task 6 Step 4). Gold-reader boundary stated in the ADR + SEC9 opened. Envelope caveat added to SEC8. Commit-1-is-not-a-deploy-point added to Global Constraints. Parser regex widened to `"([^"]+)"`; `cards[card_id]` guarded; `MemoryHook` style minors folded in. **Round three fixed three pieces of rev 3's own new code that could not execute:** `unjoinable` was used but never bound (`F821` — pytest would have raised `NameError` *before* producing the work-list that step exists to produce); the `bootstrap_hooks` gate called a resolver (`_entry_point_to_module`) that **does not exist** and keyed off `_DIRECT_TASK_ENTRY_POINT_TO_CARD` as though it mapped entry points when it maps **task_keys** — so it would have silently skipped `publish_spadl_vaep_hf`, the module it was written for; and `pathlib.Path` against a `from pathlib import Path` import. Root cause distinct from rounds 1–2: not enumeration by inference but **code written against an API never read**. The guard is now in the plan — `ruff check` every new test file BEFORE relying on its output.

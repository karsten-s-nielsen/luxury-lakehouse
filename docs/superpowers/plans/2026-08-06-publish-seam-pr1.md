# Publish Seam (PR-1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the HuggingFace leak-guard convention with a runtime-enforced seam, so every public publish provably passes the access-tier guard and an unguarded file cannot reach a public repo.

**Architecture:** A new adapter module `src/ingestion/hf_upload_seam.py` exposes `prepare_public_upload` (guard → split → drop `access_tier` → `GuardedFrame`) and `upload_guarded` (path-diff refusal → tier-derived repo privacy → `create_repo` → `upload_folder`). `GuardedFrame` records every path it writes into a shared `UploadReceipt`; `upload_guarded` refuses to upload a staging directory containing any file no receipt accounts for. All 15 publisher files migrate onto it. An AST test bans direct `HfApi` use in publisher modules, and one registry-derived test replaces six hand-maintained substring assertions.

**Tech Stack:** Python 3.10, pandas, pyarrow, `huggingface_hub`, pytest, ruff, pyright.

**Spec:** `docs/superpowers/specs/2026-08-06-statsbomb-commercial-360-containment-design.md` — PR-1 covers R-8, R-8a, R-9, R-10, R-11, R-12, R-13, R-20.

**Revision 2** incorporated plan review round 1 (12 findings, 2 blocking). Structural changes: the convention-assertion surgery moved **before** the migrations (was Task 12, now Task 4) so no task ever runs or commits against a red tree; `prepare_public_upload` hoists its column check above `split_restricted`, which raises a bare `KeyError` on a missing column; `upload_guarded` takes `GuardedFrame`s rather than receipts so repo privacy is **derived from the tier** instead of defaulting to public; and the thirteen commit gates collapsed to one at the end.

**Revision 3** incorporates the targeted review of rev 2's delta. Four changes alter outcomes:

1. **`GuardedFrame` was forgeable.** Rev 2 claimed there was "no way to substitute an arbitrary frame". False — it is a public dataclass, so both direct construction and `dataclasses.replace(g, frame=other)` produced a wrapper that never passed `prepare_public_upload`, and every gate stayed green (the path diff records the write, no HF symbol appears, and `upload_guarded` trusts the `tier` field it is handed). A constructor sentinel does **not** fix this: verified on Python 3.10, `dataclasses.replace` re-runs `__post_init__` *and carries unreplaced fields through*, so the sentinel rides along. Authorization now attaches to the **DataFrame object** on the receipt, and `write_parquet` refuses anything the seam did not produce.
2. **A seventh convention assertion exists** — `test_gradientsports_spadl.py:485` — in a file earlier drafts never opened. It would have fired at Task 9, exactly where the plan promises green.
3. **Task 11's premise was inverted.** `publish_shots_on_target_hf.py:179` already publishes to `data/shots_on_target.parquet`; it was never at the repo root. Rev 2's hardcoded `path_in_repo=""` would have *moved* the file and stranded the old copy — the precise break the task exists to prevent.
4. **One publisher legitimately needed `HfApi` post-migration.** `publish_shots_on_target_hf.py:187-205` sweeps stale `data/*.parquet` via `list_repo_files`/`delete_files` — housekeeping that exists because stale part-files poisoned a PSxG retrain on 2026-06-21. Rather than exempt it, the sweep becomes `delete_patterns=["**"]` on the `upload_guarded` call.

Also folded in: `assert_publishable_frame` extracted rather than duplicated; `ValueError` (not `TierMismatchError`) for empty `frames`, with `match=` on both refusal tests; an anti-drift test tying `restricted_repo_id` to the seam's suffix constant; the surviving `delete_patterns=["**"]` parity constraint restated in Tasks 9 and 10; and a dead `api: HfApi` parameter annotation removed.

## Execution notes (what actually differed)

Recorded during execution so the plan does not contradict the delivered code. Everything here was
verified at source against `4abe255a`, **not** the `42a449e6` both review rounds used — that commit
turned out not to be an ancestor of `main`, and `main` had moved 6 commits with a wheel-bump sweep
touching all 15 publisher files. Every cited line number still held.

1. **The `hf_publish` re-export was abandoned.** Ruff's isort exploded the `X as X` form into eight
   separate import statements. Publishers now import `ingestion.hf_upload_seam` directly, which is
   honest and makes the import-cycle question moot. Task 3 Step 5 is superseded.
2. **The retired-assertion count was higher than either review found.**
   `test_publish_shot_freeze_frames.py` and `test_publish_xg_shot_data_v3.py` each had **three**
   dying tests, not one — `test_splits_on_access_tier` and `test_drops_access_tier_before_upload`
   also go false, because both operations moved inside the seam.
3. **Names in the plan were wrong in two places.** football2vec's function is
   `select_publishable_tables`, not `build_publishable_tables`. `export_shots_on_target.py` has no
   SQL constant — the query is built by `_build_query(catalog, schema)`.
4. **Three more `delete_patterns=["data/*"]` no-ops found** beyond the one the plan knew about:
   `src/ingestion/publish_freeze_frame_hf.py`, `src/ingestion/publish_xg_shots_hf.py`,
   `scripts/publish_line_breaking_passes_hf.py`, and `scripts/publish_obso_pausa_inputs_hf.py`. All
   are matched relative to `path_in_repo="data"` and therefore match nothing. **All preserved
   byte-for-byte** — PR-1 changes no publish behaviour, and correcting them would start deleting
   repo content. Each carries a comment. They need their own decision.
5. **`obso_pausa` joins on `(provider, native_match_id)`, not `match_key`** — `bronze.idsse_events`
   carries the native string match id. It hardcodes `soccer_analytics.dev_gold.dim_matches`,
   matching the established sibling pattern (`publish_line_breaking_passes_hf.py:66`).
6. **`assert` is unusable in publishers** — `S101` (flake8-bandit) is enforced. The narrowing for
   `prepared.restricted is None` is an explicit `if ... raise RuntimeError(...)`.
7. **Four `["data/*"]` no-ops were CORRECTED, not preserved.** The initial pass preserved them as a
   behaviour-change concern; CLAUDE.md already mandates `["**"]`, so they were violations of a
   documented standard rather than an open decision. `scripts/publish_freeze_frame_hf.py` gained a
   sweep to match its twin. **This is the one behavioural change on the branch** — five repos begin
   deleting stale siblings on their next run. See ADR-072's amendment.
8. **ADR-072 written**, plus amendment pointers on ADR-049 and ADR-064 (both gave stale call-path
   guidance) and a CLAUDE.md correction. §10 of the spec required this; the first pass omitted it.
9. **`test_publisher_upload_contract.py` added** beyond the plan. The `delete_patterns` correction
   was covered by nothing — the parity test is parametrized over the six split publishers, and four
   of the five changed files were not in that set. `publish_shots_on_target_hf.publish_to_hf_hub`
   was extracted from `main()` so its staging is reachable by a test at all.
10. **`shots_on_target` kept `path_in_repo="data"`.** The object was already at
   `data/shots_on_target.parquet` (`:179`), so the layout is unchanged and no consumer breaks.

## Global Constraints

- Python `>=3.10,<3.11`. No 3.11+ syntax.
- Line length 120 max.
- Ruff rule sets enforced: `E, W, F, I, N, UP, B, S, BLE, RUF` (`pyproject.toml:205-216`). `BLE001` means no bare `except Exception:` without a line-level `# noqa: BLE001 — <reason>`.
- **`SLF` is NOT enabled, and `RUF100` (unused-noqa) IS.** The seam calls `receipt._authorize(...)` from `prepare_public_upload`, `groupby`, `drop_columns` and the Task 1 test helper. Do **not** add `# noqa: SLF001` to any of them — the rule is off, so the suppression would itself be a `RUF100` violation and fail the "no violations" step.
- Pyright basic mode must pass on `src/`.
- All public function signatures fully type-annotated.
- Imports ordered stdlib → third-party → first-party (isort).
- Pre-compile regex at module level, never inside a function body.
- Tests are hermetic. No Databricks credentials, no network, no real HF calls — `HfApi` is monkeypatched.
- Test-module file discovery uses the repo-root idiom `Path(__file__).resolve().parents[2]`, never CWD-relative `glob.glob`. `src/tests/conftest.py` does not chdir, so a CWD-relative glob returns `[]` when pytest is invoked from anywhere else — and a parametrized gate over `[]` collects zero cases and reports success.
- **One commit for this branch, at Task 15, with explicit user approval.** Intermediate tasks stage only. Never run `git commit` unprompted.
- This PR changes **no row's tier**. Publisher modes (`fail_closed` / `split` / `derived`) stay exactly as they are; mode conversion is PR-2b.

## File Structure

| File | Responsibility |
|---|---|
| `src/ingestion/hf_upload_seam.py` | **Create.** `UploadReceipt`, `GuardedFrame`, `PreparedUpload`, `prepare_public_upload`, `upload_guarded`, `UnguardedFileError`, `TierMismatchError`. The only module permitted to construct `HfApi`. |
| `src/tests/test_hf_upload_seam.py` | **Create.** Unit tests for the seam. |
| `src/tests/test_publisher_seam_conformance.py` | **Create.** AST ban + registry-derived conformance (replaces six substring assertions). |
| `src/ingestion/hf_publish.py` | **Modify.** Re-export the seam names. `upload_hf_readme` unchanged. |
| `src/ingestion/hf_leak_guard.py` | **Modify.** Extract `assert_publishable_frame` (registry membership + tier-column presence) from `assert_no_private_leak:53-56`, so the seam can run it before `split_restricted` without duplicating the checks. |
| 15 publisher files | **Modify.** Enumerated in Tasks 5–12. |
| `src/tests/test_hf_publish_parity.py` | **Modify.** Task 4 — retire `test_publisher_imports_shared_split_helpers` and `test_publisher_splits_on_access_tier_and_calls_leak_guard`. |
| `src/tests/test_gradientsports_hf_exclusion.py` | **Modify.** Task 4 — retire six assertions across two tests. |
| `src/tests/test_publish_shot_freeze_frames.py` | **Modify.** Task 4 — retire the assertion at `:97`. |
| `src/tests/test_publish_xg_shot_data_v3.py` | **Modify.** Task 4 — retire the assertion at `:90`. |
| `src/ingestion/export_shots_on_target.py` | **Modify.** Add `dm.access_tier` to the SELECT (R-12). |
| `scripts/publish_obso_pausa_inputs_hf.py` | **Modify.** Add a `dim_matches` join for `access_tier` (R-13). |
| `ROADMAP.md` | **Modify.** Correct the "zero-code switch" claim (R-20). |

---

### Task 1: `UploadReceipt` and `GuardedFrame`

**Files:**
- Create: `src/ingestion/hf_upload_seam.py`
- Test: `src/tests/test_hf_upload_seam.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `UnguardedFileError`, `TierMismatchError`, `UnauthorizedFrameError`; `UploadReceipt(publisher: str)` with `.record(path: Path) -> None`, `._authorize(frame) -> None`, `.is_authorized(frame) -> bool` and `.paths -> frozenset[Path]`; `GuardedFrame` (frozen, `eq=False`) with fields `frame: pd.DataFrame`, `tier: str`, `publisher: str`, `receipt: UploadReceipt`, and methods `write_parquet(path: Path) -> None`, `groupby(column: str) -> Iterator[tuple[Any, GuardedFrame]]`, `drop_columns(columns: list[str]) -> GuardedFrame`.

**Why `eq=False`:** `@dataclasses.dataclass(frozen=True)` defaults to `eq=True`, which synthesises an `__eq__` comparing every field. Comparing two DataFrames returns a DataFrame, and `bool()` on it raises *"The truth value of a DataFrame is ambiguous"*. `frozen and eq` also synthesises a `__hash__` over the fields, which raises `TypeError: unhashable type: 'DataFrame'`. pytest's assertion rewriting, set membership, and `in` checks all reach both.

- [ ] **Step 1: Write the failing test**

```python
# src/tests/test_hf_upload_seam.py
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from ingestion.hf_upload_seam import GuardedFrame, UploadReceipt


def _guarded(df: pd.DataFrame, publisher: str = "publish_action_context_hf") -> GuardedFrame:
    """Build a GuardedFrame the way the seam does — authorizing the frame on the receipt."""
    receipt = UploadReceipt(publisher)
    receipt._authorize(df)  # test stands in for prepare_public_upload
    return GuardedFrame(frame=df, tier="public", publisher=publisher, receipt=receipt)


def test_write_parquet_records_the_path_on_the_receipt(tmp_path: Path) -> None:
    g = _guarded(pd.DataFrame({"a": [1, 2]}))
    out = tmp_path / "data" / "x.parquet"
    g.write_parquet(out)
    assert out.exists()
    assert g.receipt.paths == frozenset({out.resolve()})


def test_groupby_children_share_the_parent_receipt(tmp_path: Path) -> None:
    g = _guarded(pd.DataFrame({"k": ["a", "b"], "v": [1, 2]}))
    for key, child in g.groupby("k"):
        child.write_parquet(tmp_path / f"{key}.parquet")
    assert len(g.receipt.paths) == 2


def test_drop_columns_preserves_the_receipt_and_tier(tmp_path: Path) -> None:
    g = _guarded(pd.DataFrame({"k": ["a"], "v": [1]}))
    child = g.drop_columns(["k"])
    assert child.receipt is g.receipt
    assert child.tier == "public"
    assert list(child.frame.columns) == ["v"]


def test_guarded_frames_are_usable_in_sets_and_comparisons() -> None:
    # frozen+eq would synthesise __eq__/__hash__ over a DataFrame field; both raise at runtime.
    g = _guarded(pd.DataFrame({"a": [1]}))
    assert g in {g}
    assert g == g


def test_directly_constructed_guarded_frame_refuses_to_write(tmp_path: Path) -> None:
    # Forgery route 1: the public constructor. Borrowing a real receipt does not help — the
    # authorization is on the FRAME object, which the seam never produced.
    real = _guarded(pd.DataFrame({"a": [1]}))
    forged = GuardedFrame(
        frame=pd.DataFrame({"a": [999]}), tier="public", publisher=real.publisher, receipt=real.receipt
    )
    with pytest.raises(UnauthorizedFrameError):
        forged.write_parquet(tmp_path / "forged.parquet")


def test_replace_substituted_frame_refuses_to_write(tmp_path: Path) -> None:
    # Forgery route 2: dataclasses.replace. frozen=True does not block it, and a constructor
    # sentinel would not either — replace carries unreplaced fields straight through.
    import dataclasses

    g = _guarded(pd.DataFrame({"a": [1]}))
    forged = dataclasses.replace(g, frame=pd.DataFrame({"a": [999]}))
    with pytest.raises(UnauthorizedFrameError):
        forged.write_parquet(tmp_path / "forged.parquet")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_hf_upload_seam.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ingestion.hf_upload_seam'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/ingestion/hf_upload_seam.py
"""The single door for publishing a public HuggingFace artifact (ADR-072).

Replaces the prior convention — "every publisher remembers to call ``assert_no_private_leak``" —
with a seam that proves it. ``prepare_public_upload`` performs guard -> split -> drop; the returned
``GuardedFrame`` records every path it writes; ``upload_guarded`` refuses to upload a staging
directory containing any file no receipt accounts for, and derives repo privacy from the frame's
tier rather than from a caller-supplied flag.

Why a receipt and not just a guard call: a publisher could guard one frame and stage a second,
unguarded one into the same directory. The path diff catches that at RUNTIME. The AST ban in
``src/tests/test_publisher_seam_conformance.py`` catches a bypass at lint time. Both, deliberately.
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

from huggingface_hub import HfApi

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


class UnguardedFileError(RuntimeError):
    """A staging directory contains a file that no ``GuardedFrame`` wrote."""


class TierMismatchError(RuntimeError):
    """A repo's privacy or naming does not match the tier of the frames being uploaded to it."""


class UnauthorizedFrameError(RuntimeError):
    """A ``GuardedFrame`` holds a DataFrame the seam never produced."""


class UploadReceipt:
    """Records paths written through a ``GuardedFrame``, and which frame objects are authorized.

    The authorization list is what makes ``GuardedFrame`` non-forgeable. ``GuardedFrame`` is a
    public dataclass, so both ``GuardedFrame(frame=arbitrary_df, ...)`` and
    ``dataclasses.replace(guarded, frame=arbitrary_df)`` produce a wrapper that never passed
    ``prepare_public_upload`` — and neither the path diff nor the AST ban would notice, because the
    forged wrapper writes through the normal path and touches no HF symbol. Authorization attaches
    to the **DataFrame object**, not to the receipt, precisely because a forger can borrow the real
    receipt but cannot fabricate a frame the seam itself created.
    """

    def __init__(self, publisher: str) -> None:
        self.publisher = publisher
        self._paths: set[Path] = set()
        # Strong references, never id(): a freed DataFrame's id can be reused by a later
        # allocation, which would authorize an arbitrary frame by coincidence. The frames are
        # alive for the duration of the publish anyway, so this costs one pointer each.
        self._authorized: list[pd.DataFrame] = []

    def record(self, path: Path) -> None:
        self._paths.add(path.resolve())

    def _authorize(self, frame: pd.DataFrame) -> None:
        """Register a frame the SEAM produced. Called only by ``prepare_public_upload`` and by
        ``GuardedFrame``'s own derivations — never by a publisher (AST-banned in Task 13)."""
        self._authorized.append(frame)

    def is_authorized(self, frame: pd.DataFrame) -> bool:
        return any(frame is f for f in self._authorized)

    @property
    def paths(self) -> frozenset[Path]:
        return frozenset(self._paths)


# eq=False: the synthesised __eq__/__hash__ would compare/hash a DataFrame field and raise.
@dataclasses.dataclass(frozen=True, eq=False)
class GuardedFrame:
    """A frame that has passed the access-tier guard and had ``access_tier`` dropped.

    A ``GuardedFrame`` can only write a frame the seam itself produced: ``prepare_public_upload``,
    ``groupby`` and ``drop_columns`` register their outputs on the receipt, and ``write_parquet``
    refuses anything else. Direct construction, or ``dataclasses.replace`` with a substituted
    frame, is therefore **inert** — and the AST ban in
    ``src/tests/test_publisher_seam_conformance.py`` makes the attempt visible at lint time.

    (``frozen=True`` alone does not close this: ``dataclasses.replace`` re-runs ``__post_init__``
    but carries every unreplaced field through, so a constructor sentinel would block direct
    construction and still permit frame substitution. Verified empirically on Python 3.10.)

    Derivations return children sharing the SAME receipt, so a partitioned write stays fully
    accounted for.
    """

    frame: pd.DataFrame
    tier: str
    publisher: str
    receipt: UploadReceipt

    def write_parquet(self, path: Path) -> None:
        if not self.receipt.is_authorized(self.frame):
            raise UnauthorizedFrameError(
                f"{self.publisher}: GuardedFrame holds a frame the seam did not produce — it was "
                f"constructed directly or substituted via dataclasses.replace. Obtain frames from "
                f"prepare_public_upload / groupby / drop_columns only (ADR-072)."
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        self.frame.to_parquet(path, index=False, engine="pyarrow")
        self.receipt.record(path)
        logger.info("seam: %s wrote %d rows -> %s", self.publisher, len(self.frame), path)

    def groupby(self, column: str) -> Iterator[tuple[Any, GuardedFrame]]:
        for key, sub in self.frame.groupby(column):
            self.receipt._authorize(sub)  # same-module private; the seam owns authorization
            yield key, dataclasses.replace(self, frame=sub)

    def drop_columns(self, columns: list[str]) -> GuardedFrame:
        child = self.frame.drop(columns=columns, errors="ignore")
        self.receipt._authorize(child)  # same-module private; the seam owns authorization
        return dataclasses.replace(self, frame=child)
```

**Perfect enforcement is not reachable in Python, and is not the goal.** The achievable goal — that a bypass requires a line which is both obviously wrong to a reviewer *and* fails a gate — is met by this runtime check plus Task 13's lint-time ban. Neither alone suffices: the check can be defeated by calling `receipt._authorize(df)`, which is exactly why that name joins the AST ban.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_hf_upload_seam.py -v`
Expected: 4 passed

- [ ] **Step 5: Lint and type check**

Run: `uv run ruff check src/ingestion/hf_upload_seam.py src/tests/test_hf_upload_seam.py && uv run pyright src/ingestion/hf_upload_seam.py`
Expected: no violations, 0 errors

- [ ] **Step 6: Stage**

```bash
git add src/ingestion/hf_upload_seam.py src/tests/test_hf_upload_seam.py
```
Do **not** commit. One commit gate at Task 15.

---

### Task 2: `prepare_public_upload` with the column check hoisted

**Files:**
- Modify: `src/ingestion/hf_upload_seam.py`
- Test: `src/tests/test_hf_upload_seam.py`

**Interfaces:**
- Consumes: `GuardedFrame`, `UploadReceipt` from Task 1; `split_restricted` from `ingestion.hf_publish`; `assert_no_private_leak`, `LeakDetectedError`, `PUBLISHER_REGISTRY` from `ingestion.hf_leak_guard`.
- Produces: `PreparedUpload` (frozen, `eq=False`) with `public: GuardedFrame` and `restricted: GuardedFrame | None`; `prepare_public_upload(df: pd.DataFrame, *, publisher: str, receipt: UploadReceipt | None = None) -> PreparedUpload`.

**The hoisted check is load-bearing.** `split_restricted` (`hf_publish.py:118`) subscripts the column directly — `is_public = (df[column] == AccessTier.PUBLIC.value)...` — with no presence check. If `prepare_public_upload` split first and guarded second, a missing `access_tier` on a **`split`** publisher would raise a bare `KeyError`, not `LeakDetectedError`. That reads as a bug rather than a security refusal, and football2vec's `except LeakDetectedError` degradation path would not catch it. The registry lookup and the column check therefore run **before any branch**.

**Extract the precondition rather than duplicating it.** `assert_no_private_leak` already performs both checks (`hf_leak_guard.py:53-56`). Re-implementing them in the seam would give two error strings for one invariant and would not pick up a future third precondition. Add to `src/ingestion/hf_leak_guard.py`:

```python
def assert_publishable_frame(df: pd.DataFrame, *, publisher: str) -> None:
    """Fail closed unless ``df`` is a frame this publisher is permitted to attempt to publish.

    Registry membership + tier-column presence — the preconditions of any tier decision. Extracted
    so ``ingestion.hf_upload_seam.prepare_public_upload`` can run them BEFORE ``split_restricted``
    (which subscripts the column directly and would otherwise raise a bare ``KeyError`` on the
    split path). ``assert_no_private_leak`` calls this too, keeping one owner for the question
    "what makes a frame publishable".
    """
    if publisher not in PUBLISHER_REGISTRY:
        raise LeakDetectedError(f"publisher {publisher!r} not in PUBLISHER_REGISTRY — add it (fail-closed)")
    if "access_tier" not in df.columns:
        raise LeakDetectedError(f"{publisher}: frame has no access_tier column — cannot prove it is public")
```

Replace the two inline checks at `hf_leak_guard.py:53-56` with a call to it. The existing tests at `test_hf_leak_guard.py:31-40` cover both branches and must still pass unchanged.

- [ ] **Step 1: Write the failing test**

```python
# append to src/tests/test_hf_upload_seam.py
from ingestion.hf_leak_guard import LeakDetectedError
from ingestion.hf_upload_seam import prepare_public_upload


def _tiered(tiers: list[str | None]) -> pd.DataFrame:
    return pd.DataFrame({"access_tier": tiers, "v": list(range(len(tiers)))})


def test_split_mode_returns_both_sides_without_access_tier() -> None:
    prepared = prepare_public_upload(_tiered(["public", "restricted"]), publisher="publish_psxg_shots_hf")
    assert len(prepared.public.frame) == 1
    assert prepared.restricted is not None and len(prepared.restricted.frame) == 1
    assert prepared.public.tier == "public" and prepared.restricted.tier == "restricted"
    assert "access_tier" not in prepared.public.frame.columns
    assert "access_tier" not in prepared.restricted.frame.columns


def test_split_mode_routes_null_tier_to_restricted() -> None:
    prepared = prepare_public_upload(_tiered(["public", None]), publisher="publish_psxg_shots_hf")
    assert len(prepared.public.frame) == 1
    assert prepared.restricted is not None and len(prepared.restricted.frame) == 1


def test_fail_closed_mode_has_no_restricted_side() -> None:
    prepared = prepare_public_upload(_tiered(["public", "public"]), publisher="publish_xg_shots_hf")
    assert prepared.restricted is None
    assert len(prepared.public.frame) == 2


def test_fail_closed_mode_raises_on_a_restricted_row() -> None:
    with pytest.raises(LeakDetectedError):
        prepare_public_upload(_tiered(["public", "restricted"]), publisher="publish_xg_shots_hf")


def test_unregistered_publisher_raises() -> None:
    with pytest.raises(LeakDetectedError):
        prepare_public_upload(_tiered(["public"]), publisher="publish_brand_new_hf")


@pytest.mark.parametrize("publisher", ["publish_xg_shots_hf", "publish_psxg_shots_hf"])
def test_missing_access_tier_column_raises_leak_error_in_every_mode(publisher: str) -> None:
    # split_restricted subscripts the column directly and would raise a bare KeyError for the
    # "split" publisher if the check were not hoisted above the branch.
    with pytest.raises(LeakDetectedError, match="access_tier"):
        prepare_public_upload(pd.DataFrame({"v": [1]}), publisher=publisher)


def test_shared_receipt_accumulates_across_prepares(tmp_path: Path) -> None:
    receipt = UploadReceipt("publish_football2vec_embeddings_hf")
    for name in ("per_match", "career"):
        prepared = prepare_public_upload(
            _tiered(["public"]), publisher="publish_football2vec_embeddings_hf", receipt=receipt
        )
        prepared.public.write_parquet(tmp_path / name / "data.parquet")
    assert len(receipt.paths) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_hf_upload_seam.py -v`
Expected: FAIL — `ImportError: cannot import name 'prepare_public_upload'`

- [ ] **Step 3: Write minimal implementation**

Append to `src/ingestion/hf_upload_seam.py`:

```python
_ACCESS_TIER_COLUMN = "access_tier"


@dataclasses.dataclass(frozen=True, eq=False)
class PreparedUpload:
    """Guard result. ``restricted`` is None for non-``split`` publishers."""

    public: GuardedFrame
    restricted: GuardedFrame | None


def prepare_public_upload(
    df: pd.DataFrame,
    *,
    publisher: str,
    receipt: UploadReceipt | None = None,
) -> PreparedUpload:
    """Guard -> split -> drop ``access_tier``, returning guarded frames ready to stage.

    Mode is read from ``PUBLISHER_REGISTRY`` — a property of the call, not of a docstring.

    Fail-closed preconditions run BEFORE any mode branch: an unregistered publisher and a frame
    with no ``access_tier`` column both raise ``LeakDetectedError`` in every mode. This ordering is
    deliberate — ``split_restricted`` subscripts the column directly, so splitting first would
    surface a missing column as a bare ``KeyError`` on the ``split`` path only.

    Pass ``receipt`` to accumulate several frames under one receipt (the football2vec
    per_match/career/season case) so a single ``upload_guarded`` can account for all of them.
    """
    from ingestion.hf_leak_guard import PUBLISHER_REGISTRY, assert_no_private_leak, assert_publishable_frame
    from ingestion.hf_publish import split_restricted

    assert_publishable_frame(df, publisher=publisher)
    shared = receipt if receipt is not None else UploadReceipt(publisher)

    def _guard(frame: pd.DataFrame, tier: str) -> GuardedFrame:
        stripped = frame.drop(columns=[_ACCESS_TIER_COLUMN], errors="ignore")
        shared._authorize(stripped)  # same-module private; the seam owns authorization
        return GuardedFrame(frame=stripped, tier=tier, publisher=publisher, receipt=shared)

    if PUBLISHER_REGISTRY[publisher] == "split":
        public_df, restricted_df = split_restricted(df, column=_ACCESS_TIER_COLUMN)
        assert_no_private_leak(public_df, publisher=publisher)
        return PreparedUpload(public=_guard(public_df, "public"), restricted=_guard(restricted_df, "restricted"))

    # "fail_closed" and "derived": the whole frame must already be public.
    assert_no_private_leak(df, publisher=publisher)
    return PreparedUpload(public=_guard(df, "public"), restricted=None)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_hf_upload_seam.py -v`
Expected: 12 passed

- [ ] **Step 5: Lint, type check, stage**

Run: `uv run ruff check src/ingestion/hf_upload_seam.py src/tests/test_hf_upload_seam.py && uv run pyright src/ingestion/hf_upload_seam.py`
Expected: no violations, 0 errors

```bash
git add src/ingestion/hf_upload_seam.py src/tests/test_hf_upload_seam.py
```

---

### Task 3: `upload_guarded` — path-diff refusal and tier-derived privacy

**Files:**
- Modify: `src/ingestion/hf_upload_seam.py`, `src/ingestion/hf_publish.py`
- Test: `src/tests/test_hf_upload_seam.py`

**Interfaces:**
- Consumes: `GuardedFrame`, `UploadReceipt`, `UnguardedFileError`, `TierMismatchError` from Task 1.
- Produces: `upload_guarded(staging_dir: Path, *, frames: list[GuardedFrame], repo_id: str, token: str, path_in_repo: str = "data", delete_patterns: list[str] | None = None, repo_type: str = "dataset") -> str`.

**No `private` parameter, by design.** The seam already knows the tier — `GuardedFrame.tier` is a field. A `private: bool = False` parameter would discard it and reintroduce exactly the shape the spec removes at R-6a (`stamp_access_tier(visibility: str | None = None)`): one omitted keyword away from publishing restricted rows to a public repo, with `create_repo(..., exist_ok=True)` ensuring nothing downstream notices. Taking `frames` instead of `receipts` lets privacy be **derived**, and lets the `-restricted` suffix convention (`restricted_repo_id`) be asserted for free.

- [ ] **Step 1: Write the failing test**

```python
# append to src/tests/test_hf_upload_seam.py
from ingestion.hf_upload_seam import TierMismatchError, UnguardedFileError, upload_guarded


class _FakeApi:
    def __init__(self, token: str | None = None) -> None:
        self.token = token
        self.created: list[tuple[str, bool]] = []
        self.uploaded: list[dict[str, object]] = []

    def create_repo(self, repo_id: str, **kw: object) -> None:
        self.created.append((repo_id, bool(kw.get("private", False))))

    def upload_folder(self, **kw: object) -> None:
        self.uploaded.append(kw)


@pytest.fixture()
def fake_api(monkeypatch: pytest.MonkeyPatch) -> _FakeApi:
    api = _FakeApi()
    monkeypatch.setattr("ingestion.hf_upload_seam.HfApi", lambda token=None: api)
    return api


def test_public_frames_create_a_public_repo(tmp_path: Path, fake_api: _FakeApi) -> None:
    prepared = prepare_public_upload(_tiered(["public"]), publisher="publish_xg_shots_hf")
    staging = tmp_path / "data"
    prepared.public.write_parquet(staging / "x.parquet")
    url = upload_guarded(staging, frames=[prepared.public], repo_id="org/repo", token="t")
    assert url == "https://huggingface.co/datasets/org/repo"
    assert fake_api.created == [("org/repo", False)]


def test_restricted_frames_create_a_private_repo_without_a_caller_flag(tmp_path: Path, fake_api: _FakeApi) -> None:
    prepared = prepare_public_upload(_tiered(["restricted"]), publisher="publish_psxg_shots_hf")
    assert prepared.restricted is not None
    staging = tmp_path / "data"
    prepared.restricted.write_parquet(staging / "x.parquet")
    upload_guarded(staging, frames=[prepared.restricted], repo_id="org/repo-restricted", token="t")
    assert fake_api.created == [("org/repo-restricted", True)]


def test_restricted_frames_refuse_a_non_restricted_repo_id(tmp_path: Path, fake_api: _FakeApi) -> None:
    prepared = prepare_public_upload(_tiered(["restricted"]), publisher="publish_psxg_shots_hf")
    assert prepared.restricted is not None
    staging = tmp_path / "data"
    prepared.restricted.write_parquet(staging / "x.parquet")
    with pytest.raises(TierMismatchError, match="-restricted"):
        upload_guarded(staging, frames=[prepared.restricted], repo_id="org/repo", token="t")
    assert fake_api.created == []


def test_public_frames_refuse_a_restricted_repo_id(tmp_path: Path, fake_api: _FakeApi) -> None:
    prepared = prepare_public_upload(_tiered(["public"]), publisher="publish_psxg_shots_hf")
    staging = tmp_path / "data"
    prepared.public.write_parquet(staging / "x.parquet")
    with pytest.raises(TierMismatchError):
        upload_guarded(staging, frames=[prepared.public], repo_id="org/repo-restricted", token="t")


def test_mixed_tier_frames_refuse(tmp_path: Path, fake_api: _FakeApi) -> None:
    prepared = prepare_public_upload(_tiered(["public", "restricted"]), publisher="publish_psxg_shots_hf")
    assert prepared.restricted is not None
    staging = tmp_path / "data"
    prepared.public.write_parquet(staging / "a.parquet")
    prepared.restricted.write_parquet(staging / "b.parquet")
    with pytest.raises(TierMismatchError, match="one tier"):
        upload_guarded(staging, frames=[prepared.public, prepared.restricted], repo_id="org/repo", token="t")


def test_upload_guarded_refuses_an_unrecorded_file(tmp_path: Path, fake_api: _FakeApi) -> None:
    prepared = prepare_public_upload(_tiered(["public"]), publisher="publish_xg_shots_hf")
    staging = tmp_path / "data"
    prepared.public.write_parquet(staging / "guarded.parquet")
    (staging / "smuggled.parquet").write_bytes(b"not guarded")
    with pytest.raises(UnguardedFileError, match="smuggled.parquet"):
        upload_guarded(staging, frames=[prepared.public], repo_id="org/repo", token="t")
    assert fake_api.uploaded == []


def test_upload_guarded_allows_an_empty_staging_dir(tmp_path: Path, fake_api: _FakeApi) -> None:
    # ADR-049 sweep-only publish: zero partitions is legitimate — delete_patterns clears stale data.
    prepared = prepare_public_upload(_tiered([]), publisher="publish_psxg_shots_hf")
    assert prepared.restricted is not None
    staging = tmp_path / "data"
    staging.mkdir(parents=True)
    upload_guarded(
        staging, frames=[prepared.restricted], repo_id="org/repo-restricted", token="t", delete_patterns=["**"]
    )
    assert fake_api.created == [("org/repo-restricted", True)]
    assert fake_api.uploaded[0]["delete_patterns"] == ["**"]


def test_upload_guarded_refuses_an_empty_frames_list(tmp_path: Path, fake_api: _FakeApi) -> None:
    # ValueError, not TierMismatchError — an empty list is a caller bug, and without match= this
    # test would pass even if the mixed-tier branch fired for the wrong reason.
    with pytest.raises(ValueError, match="at least one GuardedFrame"):
        upload_guarded(tmp_path, frames=[], repo_id="org/repo", token="t")


def test_restricted_repo_suffix_matches_the_shared_helper() -> None:
    # Anti-drift: the seam string-matches the ADR-049 suffix rather than importing
    # restricted_repo_id, because hf_upload_seam importing hf_publish at module level would
    # reintroduce the cycle the bottom-of-module re-export avoids. This test ties the two together
    # — same idiom as test_access_tier_visibility_consistency_allowlist.py for the dbt var.
    from ingestion.hf_publish import restricted_repo_id
    from ingestion.hf_upload_seam import _RESTRICTED_REPO_SUFFIX

    assert restricted_repo_id("org/x").endswith(_RESTRICTED_REPO_SUFFIX)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_hf_upload_seam.py -v`
Expected: FAIL — `ImportError: cannot import name 'upload_guarded'`

- [ ] **Step 3: Write minimal implementation**

```python
_RESTRICTED_REPO_SUFFIX = "-restricted"


def upload_guarded(
    staging_dir: Path,
    *,
    frames: list[GuardedFrame],
    repo_id: str,
    token: str,
    path_in_repo: str = "data",
    delete_patterns: list[str] | None = None,
    repo_type: str = "dataset",
) -> str:
    """Upload a staging directory, refusing any file no ``GuardedFrame`` recorded (R-8a).

    Repo privacy is DERIVED from the frames' tier — there is no caller-supplied ``private`` flag to
    forget. All frames must share one tier, and the repo id must match the ADR-049 naming
    convention for that tier.

    An empty staging directory is legitimate — the ADR-049 sweep-only publish uploads zero
    partitions so ``delete_patterns`` clears previously-restricted data. Emptiness is NOT an error;
    an *unaccounted* file is.

    ``delete_patterns`` are matched RELATIVE to ``path_in_repo``, so the only correct whole-path
    sweep is ``["**"]`` — a ``"data/"``-prefixed pattern silently matches nothing (ADR-049). No
    pattern can reach a file ABOVE ``path_in_repo``.
    """
    if not frames:
        # A caller bug, not a tier mismatch — reusing TierMismatchError would make the name lie.
        raise ValueError(f"upload_guarded requires at least one GuardedFrame (repo {repo_id!r})")
    tiers = {f.tier for f in frames}
    if len(tiers) != 1:
        raise TierMismatchError(f"all frames must share one tier, got {sorted(tiers)} for repo {repo_id!r}")
    tier = tiers.pop()
    publisher = frames[0].publisher
    private = tier == "restricted"
    if private and not repo_id.endswith(_RESTRICTED_REPO_SUFFIX):
        raise TierMismatchError(
            f"{publisher}: restricted frames target {repo_id!r}, which lacks the "
            f"{_RESTRICTED_REPO_SUFFIX!r} suffix (ADR-049 companion-repo convention)"
        )
    if not private and repo_id.endswith(_RESTRICTED_REPO_SUFFIX):
        raise TierMismatchError(f"{publisher}: public frames target the restricted companion {repo_id!r}")

    recorded = {p for f in frames for p in f.receipt.paths}
    actual = {p.resolve() for p in Path(staging_dir).rglob("*") if p.is_file()}
    unaccounted = sorted(str(p) for p in actual - recorded)
    if unaccounted:
        logger.error(
            "UPLOAD BLOCKED: %s staged %d file(s) no GuardedFrame recorded: %s",
            publisher,
            len(unaccounted),
            unaccounted,
        )
        raise UnguardedFileError(
            f"{publisher}: {len(unaccounted)} unguarded file(s) in staging dir — every file must be "
            f"written via GuardedFrame.write_parquet: {unaccounted}"
        )

    api = HfApi(token=token)
    api.create_repo(repo_id, exist_ok=True, repo_type=repo_type, token=token, private=private)
    api.upload_folder(
        folder_path=str(staging_dir),
        path_in_repo=path_in_repo,
        repo_id=repo_id,
        repo_type=repo_type,
        token=token,
        delete_patterns=delete_patterns,
    )
    logger.info("seam: %s uploaded %d file(s) to %s (tier=%s, private=%s)", publisher, len(actual), repo_id, tier, private)
    return f"https://huggingface.co/{repo_type}s/{repo_id}"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_hf_upload_seam.py -v`
Expected: 20 passed

- [ ] **Step 5: Re-export from `hf_publish`**

Append to the very end of `src/ingestion/hf_publish.py`:

```python
# Re-export the ADR-072 publish seam so publishers import guard + upload from one module.
# hf_upload_seam imports split_restricted from here FUNCTION-LOCALLY, so this bottom-of-module
# import cannot deadlock at load time.
from ingestion.hf_upload_seam import (  # noqa: E402  — deliberate bottom-of-module re-export
    GuardedFrame,
    PreparedUpload,
    TierMismatchError,
    UnguardedFileError,
    UploadReceipt,
    prepare_public_upload,
    upload_guarded,
)
```

Do **not** add an `__all__`. The module has none today, and an incomplete one — omitting `RESTRICTED_HF_PROVIDERS`, `get_hf_card_path`, `build_provider_configs`, `inject_frontmatter_configs`, all imported by publishers — would be a misleading API statement for no benefit.

- [ ] **Step 6: Run the seam and guard suites**

Run: `uv run pytest src/tests/test_hf_upload_seam.py src/tests/test_hf_leak_guard.py src/tests/test_hf_publish.py -v`
Expected: all pass

- [ ] **Step 7: Lint, type check, stage**

Run: `uv run ruff check src/ingestion/ && uv run pyright src/ingestion/hf_upload_seam.py src/ingestion/hf_publish.py`

```bash
git add src/ingestion/hf_upload_seam.py src/ingestion/hf_publish.py src/tests/test_hf_upload_seam.py
```

---

### Task 4: Retire the convention assertions **before** migrating anything

**Files:**
- Modify: `src/tests/test_hf_publish_parity.py`
- Modify: `src/tests/test_gradientsports_hf_exclusion.py`
- Modify: `src/tests/test_publish_shot_freeze_frames.py`
- Modify: `src/tests/test_publish_xg_shot_data_v3.py`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing.

**Why this task comes first.** Six surviving assertions encode the convention the seam replaces. They require each publisher's *source text* to import `split_restricted`, contain the literal `column="access_tier"`, and contain the literal `assert_no_private_leak` — all of which become false the moment a publisher migrates, because those are now implementation details **inside** `prepare_public_upload`. Requiring publishers to import a helper the seam calls for them is backwards. Retiring them first means no migration task ever runs against a red tree.

**This opens a deliberate coverage gap** between here and Task 13, where the AST gates replace them. The gap is one PR long and is closed before merge. The alternative — gates first — is impossible: they would fail on all 15 unmigrated files.

- [ ] **Step 1: Enumerate what to remove**

| File | Lines | Assertion |
|---|---|---|
| `test_hf_publish_parity.py` | `:321-328` | `test_publisher_imports_shared_split_helpers` — requires `{"RESTRICTED_HF_PROVIDERS", "restricted_repo_id", "split_restricted"}` |
| `test_hf_publish_parity.py` | `:347-359` | `test_publisher_splits_on_access_tier_and_calls_leak_guard` |
| `test_gradientsports_hf_exclusion.py` | `:103-105` | `assert _imports_split_restricted(source)` |
| `test_gradientsports_hf_exclusion.py` | `:106-109` | `assert 'column="access_tier"' in source` |
| `test_gradientsports_hf_exclusion.py` | `:110-113` | `assert "assert_no_private_leak" in source` |
| `test_gradientsports_hf_exclusion.py` | `:149-151` | the same three, for the `src/ingestion/publish_spadl_vaep_hf.py` twin |
| `test_publish_shot_freeze_frames.py` | `:97-99` | `assert "assert_no_private_leak(" in _source()` |
| `test_publish_xg_shot_data_v3.py` | `:90-92` | `assert "assert_no_private_leak(" in _source()` |
| `test_gradientsports_spadl.py` | `:485-488` | `assert "split_restricted" in script` — inside `TestHfLicenseGate.test_publish_spadl_vaep_gates_gs_via_restricted_split`. **Delete only this first assert.** The second (`:489-492`, `"!= 'gradientsports'" not in script`) stays true post-migration and guards the different invariant that the gate is never a SQL provider filter. This one lives in a file the earlier drafts did not touch, and it would have fired at Task 9 — exactly where the plan promises green. While the file is open, note that it reads its target via a CWD-relative `Path("scripts/...")`, the same fragility Task 13 avoids; fixing that is optional and out of scope. |

**Keep** `test_gradientsports_hf_exclusion.py:114-117` (`_EXCLUSION_RE` — no SQL provider filter) and `:128-142` (`test_no_publisher_restricts_by_data_source`). Both remain true after migration and guard a different invariant: that the restriction decision never keys on `data_source`. The seam guarantees the public frame is all-public; it does not guarantee the SQL pulled every provider.

**Also surviving, and it constrains the migration:** `test_hf_publish_parity.py:361-390`, `test_publisher_delete_patterns_sweep_whole_path_in_repo`, is AST-based on `delete_patterns` keyword arguments of **any** `Call` (`:373-383`), so `upload_guarded(delete_patterns=["**"])` satisfies it — it survives by design, not by luck. But it asserts `delete_patterns` is **non-empty and exactly `["**"]` for all six `_ADR049_SPLIT_PUBLISHER_CARDS`. Every split publisher must therefore keep passing it. Tasks 9 and 10 restate this; do not drop the argument when copying a template.

**One check is deliberately lost.** `test_publisher_imports_shared_split_helpers` also required `restricted_repo_id`, which kept the `-restricted` naming single-sourced. Deleting the function loses that. The replacement is stronger: `upload_guarded`'s tier assertion (Task 3) fires at publish time on the actual repo id rather than on source text, and Task 3's anti-drift test ties the seam's suffix constant to `restricted_repo_id`. Record the trade in the commit message so it reads as a decision, not an omission.

- [ ] **Step 2: Remove them**

Delete the two whole test functions in `test_hf_publish_parity.py`. In `test_gradientsports_hf_exclusion.py`, delete the listed assertions from within `test_split_publisher_uses_access_tier_split_and_leak_guard` and `test_src_ingestion_spadl_vaep_twin_splits_on_access_tier`, keeping the `_EXCLUSION_RE` assertion in the first. If a test body becomes empty, delete the function and any now-unused helper (`_imports_split_restricted`) — `grep -n "_imports_split_restricted" src/tests/` to confirm nothing else uses it.

- [ ] **Step 3: Run the full suite green**

Run: `uv run pytest src/tests/`
Expected: zero failures. Capture the exit code — do **not** pipe through `tail`, which masks it.

- [ ] **Step 4: Lint and stage**

Run: `uv run ruff check src/tests/`

```bash
git add src/tests/
```

---

### Task 5: Seam validation A — `publish_football2vec_embeddings_hf` (3 frames + degradation)

First of two **seam-validation** tasks. If the seam cannot express this publisher without contortion, stop and revise Tasks 1–3 before migrating anything else. Getting the seam shape wrong is the expensive mistake in this PR — 13 more files follow it.

**Files:**
- Modify: `scripts/publish_football2vec_embeddings_hf.py:183-232`
- Test: `src/tests/test_football2vec_public_only.py`

**Interfaces:**
- Consumes: the full seam.
- Produces: `build_publishable_tables` keeps its `(tables, withheld_reason)` shape; `tables` becomes `dict[str, GuardedFrame]`.

**Why first:** it guards three frames under a degradation policy — `:196-201` catches `LeakDetectedError` around career/season and returns a `withheld_reason` so the caller fails closed to per-match only. A single-frame port could not express this; the shared-`UploadReceipt` design exists because of it.

- [ ] **Step 1: Write the failing test**

```python
# append to src/tests/test_football2vec_public_only.py
def test_publishable_tables_are_guarded_frames_sharing_one_receipt() -> None:
    import publish_football2vec_embeddings_hf as pub

    tables, withheld = pub.build_publishable_tables(_per_match(["p1", "p2"]), _agg(["p1"]), _agg(["p1"]))
    assert withheld is None
    assert len({id(frame.receipt) for frame in tables.values()}) == 1, (
        "all three tables must share one receipt so a single upload_guarded accounts for them"
    )
    for frame in tables.values():
        assert "access_tier" not in frame.frame.columns
```

Use the `_per_match` / `_agg` helpers already in that module.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_football2vec_public_only.py -v`
Expected: FAIL — `AttributeError: 'DataFrame' object has no attribute 'receipt'`

- [ ] **Step 3: Rewrite `build_publishable_tables` (`:187-205`)**

```python
    receipt = UploadReceipt(PUBLISHER_NAME)

    # (a) input assertion FIRST — the leak guard runs inside prepare_public_upload, before any
    # computation derived from the frame. public_player_vocabulary is then read off the GUARDED
    # frame, so the vocabulary can only ever contain public players.
    prepared = prepare_public_upload(per_match_df, publisher=PUBLISHER_NAME, receipt=receipt)
    public_ids = public_player_vocabulary(prepared.public.frame)
    assert_output_vocabulary_subset(prepared.public.frame, public_ids=public_ids, table_label="per_match")
    tables: dict[str, GuardedFrame] = {"per_match": prepared.public}

    if career_df is None or season_df is None:
        return tables, "career/season aggregate unavailable (not provably public-recomputed)"

    try:
        staged: dict[str, GuardedFrame] = {}
        for label, agg_df in (("career", career_df), ("season", season_df)):
            guarded = prepare_public_upload(agg_df, publisher=PUBLISHER_NAME, receipt=receipt).public
            assert_output_vocabulary_subset(guarded.frame, public_ids=public_ids, table_label=label)  # (b) output
            staged[label] = guarded
    except LeakDetectedError as exc:
        return tables, f"career/season not provably public-recomputed: {exc}"

    tables.update(staged)
    return tables, None
```

Two deliberate changes beyond the mechanical swap: the guard now runs **before** `public_player_vocabulary` (it previously ran first at `:188` and the plan's revision 1 had moved it behind the vocabulary computation — a fail-closed check should not sit behind a computation derived from unguarded data); and the aggregates stage into a local `staged` dict merged only on full success, so a `season` failure cannot leave `career` in `tables`.

Update imports:

```python
from ingestion.hf_leak_guard import LeakDetectedError
from ingestion.hf_publish import GuardedFrame, UploadReceipt, prepare_public_upload, upload_guarded
```

Drop `assert_no_private_leak`; delete `_drop_access_tier` if unreferenced (`grep -n "_drop_access_tier" scripts/publish_football2vec_embeddings_hf.py`).

- [ ] **Step 4: Rewrite `publish_to_hf_hub` (`:208-232`)**

```python
def publish_to_hf_hub(tables: dict[str, GuardedFrame], hf_token: str) -> str:
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        for sub_dir, guarded in tables.items():
            guarded.write_parquet(staging_dir / sub_dir / "data.parquet")
        # Scope delete patterns to the subdirs being published (relative to path_in_repo per
        # ADR-049), so a fail-closed per-match-only publish never wipes a published career/season.
        return upload_guarded(
            staging_dir,
            frames=list(tables.values()),
            repo_id=DATASET_REPO,
            token=hf_token,
            delete_patterns=[f"{sub_dir}/**" for sub_dir in tables],
        )
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest src/tests/test_football2vec_public_only.py -v`
Expected: all pass, including the pre-existing degradation tests at `:133-168`

- [ ] **Step 6: Seam checkpoint**

Answer explicitly before continuing:
- Did expressing this publisher require any change to `GuardedFrame`, `PreparedUpload`, or `upload_guarded`?
- If yes: apply it, re-run Tasks 1–3's tests, and note it here.

- [ ] **Step 7: Lint and stage**

Run: `uv run ruff check scripts/publish_football2vec_embeddings_hf.py`

```bash
git add scripts/publish_football2vec_embeddings_hf.py src/tests/test_football2vec_public_only.py
```

---

### Task 6: Seam validation B — `publish_freeze_frame_hf` (folder staging + partitioning, both twins)

Second seam-validation task. This publisher partitions by `competition_id` into `competition_id=<v>/data.parquet` and drops the partition column from the file — the case `GuardedFrame.groupby` and `drop_columns` exist for.

**Files:**
- Modify: `scripts/publish_freeze_frame_hf.py:270-312`
- Modify: `src/ingestion/publish_freeze_frame_hf.py:129-148`
- Test: `src/tests/test_hf_upload_seam.py`

**Interfaces:**
- Consumes: the full seam. Produces: nothing new.

**Leave the hardcoded tier in place.** Both twins stamp `freeze_df["access_tier"] = classify_access_tier(provider="statsbomb", visibility=None).value` (`scripts:412`, `src:129`). Those are Finding 2 sites, but removing them is PR-2b's job — PR-1 changes no tier semantics, and removing them now would leave the frame with no `access_tier` column, so `prepare_public_upload` would refuse to run.

- [ ] **Step 1: Add the seam-validation test**

This is a **seam-shape assertion, not a red-first test** — `groupby` and `drop_columns` already exist from Task 1, so it passes on arrival. Its job is to prove the partitioned shape is expressible and stays expressible.

```python
# append to src/tests/test_hf_upload_seam.py
def test_partitioned_write_records_every_partition(tmp_path: Path, fake_api: _FakeApi) -> None:
    df = pd.DataFrame({"access_tier": ["public"] * 3, "competition_id": [11, 11, 43], "v": [1, 2, 3]})
    prepared = prepare_public_upload(df, publisher="publish_freeze_frame_hf")
    staging = tmp_path / "data"
    for comp_id, part in prepared.public.groupby("competition_id"):
        part.drop_columns(["competition_id"]).write_parquet(staging / f"competition_id={comp_id}" / "data.parquet")
    assert len(prepared.public.receipt.paths) == 2
    upload_guarded(staging, frames=[prepared.public], repo_id="org/ff", token="t")
    assert len(fake_api.uploaded) == 1
```

- [ ] **Step 2: Run it**

Run: `uv run pytest src/tests/test_hf_upload_seam.py::test_partitioned_write_records_every_partition -v`
Expected: PASS. A failure means `groupby` does not share the receipt — fix Task 1 before continuing.

- [ ] **Step 3: Rewrite `scripts/publish_freeze_frame_hf.py:270-312`**

Change the enclosing function to take `guarded: GuardedFrame` instead of `df: pd.DataFrame`, delete the `api = HfApi(token=hf_token)` / `api.create_repo(...)` block at `:270-279` (`upload_guarded` does both), and replace the body with:

```python
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)

        # One Parquet per competition_id. The partition column is dropped from the file because it
        # is encoded in the path.
        for comp_id, part in guarded.groupby("competition_id"):
            part.drop_columns(["competition_id"]).write_parquet(
                staging_dir / f"competition_id={comp_id}" / "data.parquet"
            )

        dataset_url = upload_guarded(
            staging_dir, frames=[guarded], repo_id=DATASET_REPO, token=hf_token
        )

    logger.info("Published dataset to %s", dataset_url)
    return dataset_url
```

- [ ] **Step 4: Update the caller at `:413`**

Replace `assert_no_private_leak(freeze_df, publisher="publish_freeze_frame_hf")` with `prepared = prepare_public_upload(freeze_df, publisher="publish_freeze_frame_hf")` and pass `prepared.public` to the publish function. Imports become `from ingestion.hf_publish import GuardedFrame, prepare_public_upload, upload_guarded`; drop `assert_no_private_leak` and any now-unused `HfApi`.

- [ ] **Step 5: Apply the identical change to the twin**

`src/ingestion/publish_freeze_frame_hf.py` — guard at `:130`, upload at `:148`. Leave the hardcoded tier at `:129`.

```bash
grep -n "upload_folder\|HfApi\|prepare_public_upload\|upload_guarded" scripts/publish_freeze_frame_hf.py src/ingestion/publish_freeze_frame_hf.py
```
Expected: no `upload_folder`, no `HfApi`; both files show `prepare_public_upload` and `upload_guarded`.

- [ ] **Step 6: Run tests**

Run: `uv run pytest src/tests/test_hf_upload_seam.py src/tests/test_hf_publish_parity.py -v`
Expected: all pass

- [ ] **Step 7: Second seam checkpoint**

Same two questions as Task 5 Step 6. **Both hard shapes are now expressed — if the seam survived unchanged, the remaining 13 files are mechanical.**

- [ ] **Step 8: Lint and stage**

Run: `uv run ruff check scripts/ src/ && uv run pyright src/`

```bash
git add scripts/publish_freeze_frame_hf.py src/ingestion/publish_freeze_frame_hf.py src/tests/test_hf_upload_seam.py
```

---

### Task 7: Migrate `publish_psxg_shots_hf` (two-repo split, flat per-provider)

**Files:** Modify `scripts/publish_psxg_shots_hf.py:120-160`, `:183-200`

**Interfaces:** Consumes the full seam. Produces nothing new.

- [ ] **Step 1: Rewrite the upload helper (`:132-160`)**

```python
def _publish_frame(guarded: GuardedFrame, repo_id: str, hf_token: str) -> str:
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)
        if guarded.frame.empty:
            # Sweep-only publish (ADR-049): zero partitions; delete_patterns removes any
            # previously-restricted partitions — the migration-to-public mechanic.
            logger.info("0 partitions for %s — sweep-only publish", repo_id)
        for source, sub in guarded.groupby("data_source"):
            # Flat per-provider files, KEEPING data_source so the card can declare one HF config
            # per provider (ADR-054).
            sub.write_parquet(staging_dir / f"{source}.parquet")
        return upload_guarded(
            staging_dir, frames=[guarded], repo_id=repo_id, token=hf_token, delete_patterns=["**"]
        )
```

The `private` parameter is gone — `upload_guarded` derives it from `guarded.tier` and asserts the `-restricted` suffix.

- [ ] **Step 2: Rewrite the `main()` split block (`:181-197`)**

```python
    prepared = prepare_public_upload(df, publisher="publish_psxg_shots_hf")
    assert prepared.restricted is not None  # "split" mode always yields both sides

    # Per-tier observability (spec C7): row counts per repo at INFO.
    logger.info(
        "Per-tier publish counts — public: %d rows %s; restricted: %d rows %s",
        len(prepared.public.frame),
        prepared.public.frame["data_source"].value_counts().to_dict(),
        len(prepared.restricted.frame),
        prepared.restricted.frame["data_source"].value_counts().to_dict(),
    )
```

Then `_publish_frame(prepared.public, DATASET_REPO, hf_token)` and `_publish_frame(prepared.restricted, RESTRICTED_DATASET_REPO, hf_token)`.

Imports become `from ingestion.hf_publish import GuardedFrame, prepare_public_upload, restricted_repo_id, upload_guarded`; drop `assert_no_private_leak` and `split_restricted`.

- [ ] **Step 3: Verify and stage**

```bash
uv run pytest src/tests/test_hf_publish.py src/tests/test_hf_publish_parity.py -v
grep -n "upload_folder\|upload_file\|create_commit\|HfApi" scripts/publish_psxg_shots_hf.py
git add scripts/publish_psxg_shots_hf.py
```
Expected: tests pass; grep returns nothing.

---

### Task 8: Migrate `publish_pitch_control_tracking_hf` and `publish_action_context_hf`

**Files:** Modify `scripts/publish_pitch_control_tracking_hf.py:161`, `:195`; `scripts/publish_action_context_hf.py:156`, `:190`

**Interfaces:** Consumes the full seam. Produces nothing new.

- [ ] **Step 1: Migrate `publish_pitch_control_tracking_hf.py`**

Delete the `split_restricted` call, replace `assert_no_private_leak(public_df, publisher="publish_pitch_control_tracking_hf")` with `prepared = prepare_public_upload(df, publisher="publish_pitch_control_tracking_hf")`, change the upload helper's DataFrame parameter to `guarded: GuardedFrame`, stage via `guarded.groupby(...)` / `sub.write_parquet(...)`, and replace `api.upload_folder(...)` with:

```python
        return upload_guarded(
            staging_dir, frames=[guarded], repo_id=repo_id, token=hf_token, delete_patterns=["**"]
        )
```

Drop the helper's `private` parameter and its call-site argument — the tier now decides.

- [ ] **Step 2: Migrate `publish_action_context_hf.py`**

Identical treatment. It also passes `config_providers=` to `upload_hf_readme` (ADR-054) — leave that call untouched; the card push is documentation, not data, and is outside the seam.

- [ ] **Step 3: Verify and stage**

```bash
uv run pytest src/tests/test_hf_publish.py src/tests/test_hf_publish_parity.py -v
grep -n "upload_folder\|upload_file\|create_commit\|HfApi" scripts/publish_pitch_control_tracking_hf.py scripts/publish_action_context_hf.py
git add scripts/publish_pitch_control_tracking_hf.py scripts/publish_action_context_hf.py
```

---

### Task 9: Migrate `publish_spadl_vaep_hf` and `publish_xg_shots_hf` (both twins each)

**Files:**
- `scripts/publish_spadl_vaep_hf.py:359`, `:457`
- `src/ingestion/publish_spadl_vaep_hf.py:101`, `:144`
- `scripts/publish_xg_shots_hf.py:365`, `:473`
- `src/ingestion/publish_xg_shots_hf.py:116`, `:98`

**Interfaces:** Consumes the full seam. Produces nothing new.

- [ ] **Step 1: Migrate all four**

Same substitution as Tasks 7–8. **`publish_xg_shots_hf` is `fail_closed`**, so `prepared.restricted` is `None` — do not attempt a restricted publish for it in this PR.

**`publish_spadl_vaep_hf` is a `_ADR049_SPLIT_PUBLISHER_CARDS` member, so its `upload_guarded` calls MUST pass `delete_patterns=["**"]`.** `test_hf_publish_parity.py:384` asserts the argument is present and `:386` that it is exactly `["**"]`. Task 6's freeze-frame example has no `delete_patterns` (it is `fail_closed` and legitimately does not sweep) — copying that template here drops the argument and fails the parity test.

- [ ] **Step 1a: Remove the dead `HfApi` annotation**

`src/ingestion/publish_spadl_vaep_hf.py:79` declares a parameter annotated `api: HfApi`. It is a parameter annotation, not a `Call` node, so Task 13's ban will not fire on it — but the parameter is unused after migration and would sit there implying a client the function no longer has. Remove the parameter and update its call sites.

- [ ] **Step 2: Confirm no twin is stranded**

```bash
grep -c "prepare_public_upload" scripts/publish_spadl_vaep_hf.py src/ingestion/publish_spadl_vaep_hf.py scripts/publish_xg_shots_hf.py src/ingestion/publish_xg_shots_hf.py
```
Expected: `1` for each of the four. A `0` is the exact Finding 2 failure mode.

- [ ] **Step 3: Verify and stage**

```bash
uv run pytest src/tests/test_gradientsports_hf_exclusion.py src/tests/test_hf_publish_parity.py -v
git add scripts/publish_spadl_vaep_hf.py src/ingestion/publish_spadl_vaep_hf.py scripts/publish_xg_shots_hf.py src/ingestion/publish_xg_shots_hf.py
```

---

### Task 10: Migrate `publish_xg_shot_data_v3_hf`, `publish_shot_freeze_frames_hf`, `publish_line_breaking_passes_hf`

**Files:** `scripts/publish_xg_shot_data_v3_hf.py:298`, `:382`; `scripts/publish_shot_freeze_frames_hf.py:315`, `:409`; `scripts/publish_line_breaking_passes_hf.py:135`, `:167`

**Interfaces:** Consumes the full seam. Produces nothing new.

- [ ] **Step 1: Migrate all three**

Same substitution. `publish_line_breaking_passes_hf` is `fail_closed`; the other two are `split`.

**`publish_xg_shot_data_v3_hf` and `publish_shot_freeze_frames_hf` are both `_ADR049_SPLIT_PUBLISHER_CARDS` members, so their `upload_guarded` calls MUST pass `delete_patterns=["**"]`** — `test_hf_publish_parity.py:384-390` requires the argument present and exactly `["**"]`. Between this task and Task 9 that covers three of the six split publishers; Tasks 7 and 8 cover the other three. Do not copy Task 6's freeze-frame call, which has no `delete_patterns` by design.

- [ ] **Step 2: Verify and stage**

```bash
uv run pytest src/tests/test_publish_xg_shot_data_v3.py src/tests/test_publish_shot_freeze_frames.py -v
git add scripts/publish_xg_shot_data_v3_hf.py scripts/publish_shot_freeze_frames_hf.py scripts/publish_line_breaking_passes_hf.py
```
Expected: all pass — the substring assertions that would have failed here were retired in Task 4.

---

### Task 11: `publish_shots_on_target_hf` — add `access_tier`, assert non-null, preserve the repo layout (R-12)

**Files:** Modify `src/ingestion/export_shots_on_target.py:118-136`, `scripts/publish_shots_on_target_hf.py:177`; test `src/tests/test_hf_upload_seam.py`

**Interfaces:** Consumes the full seam. Produces nothing new.

**Why this file is different:** it is the one publisher with **no guard call at all** and no `access_tier` column, and it uploads via `api.upload_file` rather than `upload_folder`. The `dim_matches` join already exists at `:132-133` — only the column is missing from the SELECT.

**This guard passes on today's data, and that is a dependency worth writing down.** `dbt_project/models/intermediate/int_unified_shots.sql` has exactly two provider legs — `'statsbomb'` (`:38`) and `'wyscout'` (`:74`) — both public-by-licence, so every row reaching `fct_shots` is `access_tier='public'`. The day a SkillCorner or Gradient Sports shot leg joins that mart, this publisher hard-fails on every run until it is converted to `split`. State that in the code comment.

- [ ] **Step 1: Write the failing test**

```python
# append to src/tests/test_hf_upload_seam.py
def test_shots_on_target_sql_selects_access_tier() -> None:
    from ingestion.export_shots_on_target import SHOTS_ON_TARGET_SQL

    assert "dm.access_tier" in SHOTS_ON_TARGET_SQL, (
        "R-12: the dim_matches join exists but access_tier was never selected, so the publisher "
        "had no tier column and could not be guarded."
    )
```

Confirm the constant's real name first: `grep -n "SQL" src/ingestion/export_shots_on_target.py`.

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest src/tests/test_hf_upload_seam.py::test_shots_on_target_sql_selects_access_tier -v`
Expected: FAIL — assertion error

- [ ] **Step 3: Add the column**

Add `dm.access_tier,` to the select list before `FROM` at `:131`. **Leave the join as `LEFT JOIN`** — `INNER` silently drops a shot whose match is missing from `dim_matches`, and silent withholding is the failure class this change exists to prevent.

- [ ] **Step 4: Add the loud non-null assertion**

In `scripts/publish_shots_on_target_hf.py`, after the DataFrame is retrieved and before `prepare_public_upload`:

```python
    unmatched = int(df["access_tier"].isna().sum())
    if unmatched:
        # LEFT JOIN on dim_matches: an unmatched match yields NULL, split_restricted fail-safes it
        # to restricted, and public data is silently withheld. Fail loud instead (R-12).
        raise RuntimeError(
            f"publish_shots_on_target_hf: {unmatched} shot rows have NULL access_tier "
            f"(match_key missing from dim_matches) — refusing to publish and silently withhold public data"
        )
```

- [ ] **Step 5: Replace `api.upload_file` AND the bespoke stale sweep**

**The object already lives at `data/shots_on_target.parquet`** — `scripts/publish_shots_on_target_hf.py:179` passes exactly that as `path_in_repo`. It was never at the repo root. Earlier drafts of this task assumed the opposite and hardcoded `path_in_repo=""`, which would have **moved** the file to the root and stranded the old one — the precise break the task exists to prevent. `path_in_repo="data"` reproduces the current path, and since that is the seam's default the argument can simply be omitted.

There is a second thing to absorb. `:187-205` does post-upload housekeeping the seam does not provide: `api.list_repo_files(...)` then `api.delete_files(...)` for any other `data/*.parquet`. Its comment records why — stale Spark part-files from the workflow-task publisher silently contaminated a PSxG retrain on 2026-06-21, because `load_shots` concatenates every `data/*.parquet`. **Task 13 bans `HfApi` as a name call, so after migration this publisher cannot construct the client that sweep needs.** Remove the need rather than exempting it:

```python
    prepared = prepare_public_upload(df, publisher="publish_shots_on_target_hf")
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        prepared.public.write_parquet(staging_dir / "shots_on_target.parquet")
        # Single-file canonical layout. delete_patterns are matched RELATIVE to path_in_repo, and
        # upload_folder prunes re-uploaded files from the delete set — so "**" sweeps every stale
        # data/*.parquet (the 2026-06-21 PSxG contamination class) while keeping the file just
        # written. Replaces the previous list_repo_files/delete_files pair, which needed a raw
        # HfApi client the seam no longer permits.
        url = upload_guarded(
            staging_dir,
            frames=[prepared.public],
            repo_id=DATASET_REPO,
            token=hf_token,
            delete_patterns=["**"],
        )
```

Delete `:187-205` entirely. This also converges the publisher onto the same idiom as the other six.

**Do not move the object out of `data/`.** Any consumer using a direct file URL breaks, and `delete_patterns` cannot reach above `path_in_repo`, so a stranded copy would be unreachable — present but invisible under a `data/*.parquet` card config.

`path_in_repo=""` is valid for the repo root should it ever be needed elsewhere (`huggingface_hub` 1.7.1 normalises `None` to `""`), but note that patterns would then match from the repo root, so `["**"]` would sweep the dataset card too.

- [ ] **Step 6: Verify and stage**

```bash
uv run pytest src/tests/test_hf_upload_seam.py -v
git add src/ingestion/export_shots_on_target.py scripts/publish_shots_on_target_hf.py src/tests/test_hf_upload_seam.py
```

---

### Task 12: `publish_obso_pausa_inputs_hf` — join `dim_matches` for `access_tier` (R-13)

**Files:** Modify `scripts/publish_obso_pausa_inputs_hf.py:60-80`, `:160`

**Interfaces:** Consumes the full seam. Produces nothing new.

**Why:** `prepare_public_upload` raises when `access_tier` is absent, so "no restricted rows" and "no tier column" are not interchangeable. This publisher reads `FROM soccer_analytics.bronze.idsse_events` (`:68`), which carries no tier column. It stays `fail_closed` — IDSSE is genuinely public-by-licence — but it must now *prove* that rather than assume it.

- [ ] **Step 1: Resolve the join key and the schema reference**

```bash
grep -n "match_key\|match_id\|catalog\|schema\|dev_gold\|soccer_analytics" scripts/publish_obso_pausa_inputs_hf.py
```

Use whatever catalog/schema resolution the script already performs. **Do not hardcode `soccer_analytics.dev_gold`** — pinning a *dev* gold schema into a publish path is a production hazard. If the script hardcodes `soccer_analytics.bronze` today, parameterise both in the same edit or follow the existing pattern consistently and note it.

If `idsse_events` exposes only a native match id, join on `dm.native_match_id` with `dm.provider = 'idsse'` rather than `dm.match_key`.

- [ ] **Step 2: Add the join and the column**

Add `dm.access_tier` to the SELECT and a `LEFT JOIN` to `dim_matches` on the key resolved in Step 1.

- [ ] **Step 3: Add the same loud non-null assertion as Task 11 Step 4**

Same rationale and shape, with `publish_obso_pausa_inputs_hf` in the message.

- [ ] **Step 4: Route the upload through the seam**

Replace `api.upload_folder(...)` at `:160` with `upload_guarded(staging_dir, frames=[prepared.public], repo_id=..., token=hf_token, ...)`, staging via `prepared.public.write_parquet(...)`.

- [ ] **Step 5: Verify and stage**

```bash
uv run pytest src/tests/ -k "hf" -v
git add scripts/publish_obso_pausa_inputs_hf.py
```

---

### Task 13: AST ban + registry-derived conformance gates (R-10, R-11)

**Files:** Create `src/tests/test_publisher_seam_conformance.py`

**Interfaces:** Consumes `PUBLISHER_REGISTRY` from `ingestion.hf_leak_guard`. Produces nothing importable.

- [ ] **Step 1: Write the test module**

```python
# src/tests/test_publisher_seam_conformance.py
"""ADR-072: the publish seam is the only door to a public HF repo.

Replaces six hand-maintained substring assertions retired in Task 4. A substring check passes on a
mention in a comment, on a call against the RESTRICTED frame, and on a call placed AFTER the
upload — it cannot fail for the right reason. These are AST-based and derived from
PUBLISHER_REGISTRY, so a new publisher is covered the day it is added.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from ingestion.hf_leak_guard import PUBLISHER_REGISTRY

# Repo-root anchored, never CWD-relative: src/tests/conftest.py does not chdir, so a glob.glob
# relative to CWD returns [] when pytest runs from anywhere else — and a parametrized gate over []
# collects zero cases and reports SUCCESS. Same idiom as test_hf_publish_parity.py:245.
_REPO_ROOT = Path(__file__).resolve().parents[2]

# Every publisher file on disk. The count is asserted so a short or empty discovery is loud.
_EXPECTED_PUBLISHER_FILE_COUNT = 15

# HfApi methods capable of writing bytes to a repo, plus HfApi construction itself. `upload_file`
# matters specifically: publish_shots_on_target_hf used it, so an `upload_folder`-only ban would
# have exempted the one publisher that had no guard at all.
_BANNED_ATTRIBUTE_CALLS: frozenset[str] = frozenset(
    {"upload_folder", "upload_file", "create_commit", "delete_files", "list_repo_files", "_authorize"}
)
# `GuardedFrame` construction and `dataclasses.replace` are the two forgery routes closed at
# runtime by the receipt's frame authorization (Task 1). Banning them here is lint-time depth: a
# publisher has no reason to construct or re-shape a GuardedFrame itself, and the attempt should be
# visible in review rather than only failing at publish time. `_authorize` above is the third route
# — the one that would defeat the runtime check — and is banned for the same reason.
#
# `replace` is matched precisely, NOT as a bare name: `dataclasses.replace(...)` is an ATTRIBUTE
# call, so adding "replace" to the attribute set would flag every `str.replace` / `df.replace` in
# the repo, and adding it to the name set would miss the dotted form entirely. `_is_dataclasses_replace`
# below matches only the two spellings that actually re-shape a frozen dataclass.
_BANNED_NAME_CALLS: frozenset[str] = frozenset({"HfApi", "GuardedFrame", "replace"})


def _publisher_files() -> list[Path]:
    paths = sorted((_REPO_ROOT / "scripts").glob("publish_*_hf.py")) + sorted(
        (_REPO_ROOT / "src" / "ingestion").glob("publish_*_hf.py")
    )
    return paths


def _call_names(tree: ast.AST) -> tuple[set[str], set[str]]:
    """(attribute-call names, bare-name call names) — e.g. ``api.upload_folder()`` vs ``HfApi()``."""
    attrs: set[str] = set()
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Attribute):
            attrs.add(node.func.attr)
        elif isinstance(node.func, ast.Name):
            names.add(node.func.id)
    return attrs, names


def _is_dataclasses_replace(node: ast.Call) -> bool:
    """``dataclasses.replace(...)`` (attribute form) or a bare ``replace(...)`` imported from it.

    The bare form is already covered by _BANNED_NAME_CALLS; this adds the dotted form without
    banning the attribute name ``replace`` outright, which would flag str.replace / df.replace.
    """
    func = node.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "replace"
        and isinstance(func.value, ast.Name)
        and func.value.id == "dataclasses"
    )


def test_publisher_discovery_finds_every_file() -> None:
    found = _publisher_files()
    assert len(found) == _EXPECTED_PUBLISHER_FILE_COUNT, (
        f"expected {_EXPECTED_PUBLISHER_FILE_COUNT} publisher files, found {len(found)}: "
        f"{[str(p) for p in found]}. A short discovery makes the gates below vacuous."
    )


@pytest.mark.parametrize("path", _publisher_files(), ids=lambda p: f"{p.parent.name}/{p.name}")
def test_publisher_does_not_bypass_the_seam(path: Path) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    attrs, names = _call_names(tree)
    banned = sorted((attrs & _BANNED_ATTRIBUTE_CALLS) | (names & _BANNED_NAME_CALLS))
    if any(isinstance(n, ast.Call) and _is_dataclasses_replace(n) for n in ast.walk(tree)):
        banned.append("dataclasses.replace")
    assert not banned, (
        f"{path.parent.name}/{path.name} bypasses the publish seam ({sorted(banned)}). Route uploads "
        f"through ingestion.hf_upload_seam.upload_guarded and obtain frames only from "
        f"prepare_public_upload / groupby / drop_columns — it is the only door (ADR-072). "
        f"The ADR-014 card push upload_hf_readme() is a bare function call and is unaffected; "
        f"get_token() is likewise not banned and is still needed."
    )


@pytest.mark.parametrize("path", _publisher_files(), ids=lambda p: f"{p.parent.name}/{p.name}")
def test_publisher_routes_through_the_seam(path: Path) -> None:
    attrs, names = _call_names(ast.parse(path.read_text(encoding="utf-8")))
    called = attrs | names
    assert "prepare_public_upload" in called, (
        f"{path.parent.name}/{path.name} never calls prepare_public_upload — its public frame is "
        f"unguarded (ADR-072)."
    )
    assert "upload_guarded" in called, f"{path.parent.name}/{path.name} never calls upload_guarded (ADR-072)."


def test_registry_and_disk_agree() -> None:
    basenames = {p.stem for p in _publisher_files()}
    assert not (set(PUBLISHER_REGISTRY) - basenames), (
        f"PUBLISHER_REGISTRY entries with no module on disk: {sorted(set(PUBLISHER_REGISTRY) - basenames)}"
    )
    assert not (basenames - set(PUBLISHER_REGISTRY)), (
        f"publishers missing from PUBLISHER_REGISTRY: {sorted(basenames - set(PUBLISHER_REGISTRY))}"
    )
```

There is deliberately **no exemption list**. `_publisher_files()` globs `publish_*_hf.py`, which matches neither `hf_upload_seam.py` nor `hf_publish.py`, so the seam module is outside the gate by construction rather than by an allowlist entry that would never execute.

**`test_publisher_discovery_finds_every_file` is what makes "outside by construction" safe** — widening the glob changes the count, which fails loudly with the full file list before either parametrized gate can misbehave. Add a comment saying so, or a future reader deletes the count assertion as redundant and turns both gates silently vacuous.

- [ ] **Step 2: Run**

Run: `uv run pytest src/tests/test_publisher_seam_conformance.py -v`
Expected: all pass, with 15 parametrized cases per gate. Any failure names the exact stranded file.

- [ ] **Step 3: Prove the gate fails for the right reason**

Temporarily add `HfApi(token="x").upload_file(path_or_fileobj="a", path_in_repo="b", repo_id="c")` to `scripts/publish_psxg_shots_hf.py`. Re-run; confirm `test_publisher_does_not_bypass_the_seam[scripts/publish_psxg_shots_hf.py]` fails naming both `HfApi` and `upload_file`. Revert.

Repeat with `dataclasses.replace(prepared.public, frame=df)` and confirm the failure names `dataclasses.replace`. Then confirm a benign `"a".replace("a", "b")` in the same file does **not** trip it — a gate that flags every string operation gets disabled within a week.

- [ ] **Step 4: Prove the discovery guard fails for the right reason**

Temporarily set `_EXPECTED_PUBLISHER_FILE_COUNT = 16`; confirm `test_publisher_discovery_finds_every_file` fails. Revert.

- [ ] **Step 5: Full suite, lint, type check, stage**

```bash
uv run pytest src/tests/
uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/
git add src/tests/test_publisher_seam_conformance.py
```
Expected: zero failures, no violations, 0 errors. Capture exit codes; do not pipe through `tail`.

---

### Task 14: Correct `ROADMAP.md` (R-20)

**Files:** Modify `ROADMAP.md:412-414`

**Interfaces:** none.

**Why in this PR:** the claim is an active hazard the moment StatsBomb credentials exist. Fixing it here means no window in which someone reads "zero-code switch" and acts on it.

- [ ] **Step 1: Replace the section**

```markdown
### What already works (and what does not)

StatsBomb's open-to-commercial **fetch** switch is zero-code: `statsbombpy` checks for
`SB_USERNAME`/`SB_PASSWORD` env vars and switches endpoints automatically.

**Containment is not.** Setting those variables today would pull paid data into the same bronze
tables under `data_source='statsbomb'`, where `dim_matches.sql` hardcodes `access_tier = 'public'`
and `statsbomb` sits on `PUBLIC_BY_LICENSE_PROVIDERS` — so the rows would publish to public
HuggingFace datasets with the leak guard reporting success. See
`docs/superpowers/specs/2026-08-06-statsbomb-commercial-360-containment-design.md`; the containment
work must land before any commercial credential is configured.
```

- [ ] **Step 2: Verify no other file repeats the claim, then stage**

```bash
grep -rn "zero-code\|no refactoring needed" --include=*.md .
git add ROADMAP.md
```
Expected: only the corrected passage.

---

### Task 15: Final verification and the single commit gate

**Files:** none modified.

- [ ] **Step 1: Full test suite**

Run: `uv run pytest src/tests/`
Expected: zero failures. Run the real command — not `-k` subsets, not `--collect-only`, and do not pipe through `tail`.

- [ ] **Step 2: Full lint, format, type check**

Run: `uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/`
Expected: no violations, 0 errors

- [ ] **Step 3: Confirm all 15 files migrated**

```bash
grep -L "prepare_public_upload" scripts/publish_*_hf.py src/ingestion/publish_*_hf.py
```
Expected: no output.

- [ ] **Step 4: Confirm no direct HF access survives outside the seam**

```bash
grep -rn "\.upload_folder(\|\.upload_file(\|\.create_commit(\|HfApi(" scripts/publish_*_hf.py src/ingestion/publish_*_hf.py
```
Expected: no output.

- [ ] **Step 5: Confirm no tier semantics changed**

```bash
grep -n "statsbomb" src/shared/access_tier.py dbt_project/dbt_project.yml
```
Expected: `statsbomb` still present in `PUBLIC_BY_LICENSE_PROVIDERS` and in the `public_by_license_providers` var. If gone, PR-2b has leaked into this PR.

- [ ] **Step 6: Confirm the seam is the only `HfApi` construction outside tests**

```bash
grep -rn "HfApi(" src/ingestion/ scripts/ | grep -v "hf_upload_seam.py"
```
Expected: only non-publisher modules (e.g. `artifact_deploy.py`, `manage_space.py`), never a `publish_*_hf.py`.

- [ ] **Step 6a: Confirm the conformance gates actually ran**

```bash
uv run pytest src/tests/test_publisher_seam_conformance.py -v
```
Expected: `test_publisher_discovery_finds_every_file` passes **and** each parametrized gate reports **15** cases. A skipped or zero-case run means Task 13 never took effect — the one way the whole PR can reach the commit gate looking green while enforcing nothing.

- [ ] **Step 7: Request commit and merge approval**

Present: files migrated, tests added, tests retired, and the two seam-checkpoint answers from Tasks 5 and 6.

Proposed message:

```
refactor(hf): route every publisher through a guarded publish seam (ADR-072)

Replaces the leak-guard convention with prepare_public_upload/upload_guarded.
GuardedFrame records every staged path; upload_guarded refuses any file no
receipt accounts for and derives repo privacy from the frame tier. Closes the
two publishers that were registered fail_closed but never called the guard.
```

**Ask the user before running `git commit`.** Per repo convention this branch carries one squash-merged commit; the spec and plan documents are committed with it.

---

## Self-Review

**Spec coverage:**

| Requirement | Task |
|---|---|
| R-8 (`prepare_public_upload` / `upload_guarded`) | 1, 2, 3 |
| R-8a (receipt path diff) | 3 |
| R-9 (migrate 15 files) | 5–12, verified in 15 Step 3 |
| R-10 (AST ban; 3 attribute calls + `HfApi` construction) | 13 |
| R-11 (registry-derived gate; retire the substring assertions) | 4 (retire), 13 (replace) |
| R-12 (`shots_on_target` access_tier + loud assertion) | 11 |
| R-13 (`obso_pausa` dim_matches join) | 12 |
| R-20 (ROADMAP correction) | 14 |

All 15 files across Tasks 5–12: `football2vec_embeddings` (5); `freeze_frame` ×2 (6); `psxg_shots` (7); `pitch_control_tracking`, `action_context` (8); `spadl_vaep` ×2, `xg_shots` ×2 (9); `xg_shot_data_v3`, `shot_freeze_frames`, `line_breaking_passes` (10); `shots_on_target` (11); `obso_pausa_inputs` (12). Count: 1+2+1+2+4+3+1+1 = **15**. ✓

**Ordering invariant:** Task 4 retires the convention assertions before the first migration, so no task after it runs or stages against a red tree. Task 13's gates are added only after all 15 files migrate, since they would fail on any unmigrated file. The coverage gap between Task 4 and Task 13 is intra-PR and stated.

**Deliberately out of scope (PR-2b):** publisher mode conversions `fail_closed` → `split`; removal of the hardcoded `classify_access_tier(provider="statsbomb", visibility=None)` at `scripts/publish_freeze_frame_hf.py:412` and `src/ingestion/publish_freeze_frame_hf.py:129`; creation of new `-restricted` companion repos. Task 6 states this so an implementer does not remove the tier stamps early and leave `prepare_public_upload` with no column to check.

**Type consistency:** `GuardedFrame`, `PreparedUpload`, `UploadReceipt`, `UnguardedFileError`, `TierMismatchError`, `UnauthorizedFrameError`, `prepare_public_upload`, `upload_guarded` are used with identical names and signatures throughout, plus `assert_publishable_frame` in `hf_leak_guard`. `upload_guarded` takes `frames: list[GuardedFrame]` at every call site — Tasks 5–12 all pass `frames=`, never `receipts=` or `private=`. `PreparedUpload.restricted` is `None` for every `fail_closed` / `derived` publisher; Tasks 9 and 10 note this for `xg_shots` and `line_breaking_passes`. `receipt._authorize` is called only in `prepare_public_upload` (Task 2), `GuardedFrame.groupby` / `drop_columns` (Task 1), and the Task 1 test helper — never in a publisher, which Task 13 enforces.

**Six publishers must pass `delete_patterns=["**"]`** to satisfy the surviving `test_publisher_delete_patterns_sweep_whole_path_in_repo`: `spadl_vaep` (Task 9), `xg_shot_data_v3` and `shot_freeze_frames` (Task 10), `psxg_shots` (Task 7), `pitch_control_tracking` and `action_context` (Task 8). `shots_on_target` also passes it, for the stale-sweep reason in Task 11. `freeze_frame`, `xg_shots`, `line_breaking_passes` and `obso_pausa_inputs` do not — they are not split publishers and do not sweep.

**One step carries a known-unknown** the implementer must resolve at the file rather than guess: the `idsse_events` → `dim_matches` join key plus catalog/schema resolution in Task 12. Task 11's two former unknowns are now resolved in the task text — the SQL constant is confirmed present and the current `path_in_repo` is `"data/shots_on_target.parquet"` at `:179`.

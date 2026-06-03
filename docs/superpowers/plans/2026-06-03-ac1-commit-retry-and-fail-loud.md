# Delta Concurrent-Commit Retry in `write_delta_table` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.
>
> **Commit policy (project hard rule):** every `git commit` requires explicit user approval at the moment + the `~/.claude-git-approval` sentinel. The "Commit" steps are the intended rhythm, NOT authorization — batch and ask. Default: one squash-merged PR.

**Goal:** Make concurrent AC-1 drain-worker writes to `bronze.spadl_action_context` survive Delta commit contention by retrying the concurrent-commit error class (Delta conflicts + the observed S3-400-at-commit) with fully-jittered backoff, inside the shared `ingestion.utils.write_delta_table`.

**Architecture:** One focused change in a shared util. Factor the commit into a pure helper `_commit_with_retry(commit_fn, …)` (callable injection → wiring is unit-testable without Spark) gated by a pure matcher `_is_concurrent_commit_error`. Single-writer callers never raise the conflict, so they're unaffected. **Drain fail-loud + bounded quarantine are a separate follow-up PR (spec §10).**

**Tech Stack:** Python 3.10, PySpark (Spark Connect serverless — *not installed locally*; tests mock it), Delta Lake, pytest, ruff/pyright. Spec: `docs/superpowers/specs/2026-06-03-ac1-commit-retry-and-fail-loud-design.md`.

---

## File Structure

- **Modify** `src/ingestion/utils.py` — `import random` + `Callable`; module-level markers/constants; `_is_concurrent_commit_error`; `_commit_with_retry`; wire into `write_delta_table`; pointer comment in `merge_delta_table`.
- **Tests** `src/tests/test_ingestion_utils.py` — matcher truth table + retry behavior + jitter bound.
- **Docs** `docs/superpowers/adrs/ADR-038-delta-concurrent-commit-retry.md` (new) + ADR-037 cross-link; `CLAUDE.md`; wheel bump.
- **No change** to `drain.py` / `action_context.py` / Terraform (fail-loud is the follow-up).

---

## Task 1: `_is_concurrent_commit_error` matcher (pure)

**Files:**
- Modify: `src/ingestion/utils.py`
- Test: `src/tests/test_ingestion_utils.py`

- [ ] **Step 1: Write the failing tests**

```python
# append to src/tests/test_ingestion_utils.py
from ingestion.utils import _is_concurrent_commit_error

# Real observed S3-400-at-commit message snippet (run 730644476818402, worker 0).
_S3_COMMIT_400 = (
    "(shaded.databricks.awssdk.com.amazonaws.services.s3.model.AmazonS3Exception) Bad Request; "
    "request: HEAD https://dbstorage-prod-1huff.s3.amazonaws.com uc/.../__unitystorage/catalogs/"
    ".../tables/.../_delta_log/00000000000000000035.json ... "
    "com.databricks.tahoe.store.EnhancedS3AFileSystem.nativeS3PutIfAbsent ... "
    "com.databricks.tahoe.store.MultiClusterLogStore.writeCommit ... Status Code: 400; "
    "Error Code: 400 Bad Request"
)


def test_matcher_true_on_delta_conflict_markers() -> None:
    for marker in (
        "ConcurrentAppendException",
        "ConcurrentDeleteReadException",
        "ConcurrentDeleteDeleteException",
        "ConcurrentModificationException",
        "ProtocolChangedException",
        "CommitFailedException",
    ):
        assert _is_concurrent_commit_error(RuntimeError(f"Job aborted: {marker}: files added")), marker


def test_matcher_true_on_s3_400_at_commit() -> None:
    assert _is_concurrent_commit_error(RuntimeError(_S3_COMMIT_400))


def test_matcher_false_on_unrelated_400() -> None:
    plain = "AmazonS3Exception Bad Request; request: GET s3://bucket/data/file.parquet Status Code: 400"
    assert not _is_concurrent_commit_error(RuntimeError(plain))


def test_matcher_false_on_delta_log_read_400() -> None:
    # a 4xx reading _delta_log (no commit-path marker) must NOT match
    read = "AmazonS3Exception Bad Request; HEAD .../_delta_log/00000000000000000010.json Status Code: 400"
    assert not _is_concurrent_commit_error(RuntimeError(read))


def test_matcher_true_on_412_precondition_at_commit() -> None:
    # canonical conditional-write conflict code; the broad "Status Code: 4" marker is DELIBERATE
    # (future-proof if Databricks "fixes" the 400 -> 409/412). This test documents that intent.
    msg = (
        "AmazonS3Exception Precondition Failed; HEAD .../_delta_log/00035.json "
        "com.databricks.tahoe.store.MultiClusterLogStore.writeCommit Status Code: 412"
    )
    assert _is_concurrent_commit_error(RuntimeError(msg))


def test_matcher_false_on_commit_path_4xx_without_delta_log() -> None:
    # pins the three-way AND: commit-path marker + 4xx but NO _delta_log -> False
    msg = "AmazonS3Exception Bad Request; MultiClusterLogStore.writeCommit Status Code: 400 (no log path)"
    assert not _is_concurrent_commit_error(RuntimeError(msg))


def test_matcher_false_on_other_errors() -> None:
    assert not _is_concurrent_commit_error(RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] cannot find table"))
    assert not _is_concurrent_commit_error(ValueError("boom"))
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest src/tests/test_ingestion_utils.py -k matcher -q`
Expected: FAIL — `cannot import name '_is_concurrent_commit_error'`.

- [ ] **Step 3: Implement matcher + markers + constants in `utils.py`**

Add to the stdlib imports (alphabetical: after `import os`, before `import re`):
```python
import random
```
Change the `collections.abc` import line to include `Callable`:
```python
from collections.abc import Callable, Iterable, Iterator
```

Add just above `def write_delta_table`:

```python
# Concurrent-commit contention on a shared Delta table (ADR-038): N workers race the same
# _delta_log version; one wins, the rest must retry. Delta's own retry does NOT catch the raw
# S3-400 the serverless coordinated-commit path raises (run 730644476818402), so we match it here.
# DRIFT RISK: this is stringly-typed on JVM messages; a DBR upgrade can change the format and
# silently stop matching. The exhaustion-ERROR log in _commit_with_retry surfaces that.
_COMMIT_CONFLICT_MARKERS: frozenset[str] = frozenset(
    {
        "ConcurrentAppendException",
        "ConcurrentDeleteReadException",
        "ConcurrentDeleteDeleteException",
        "ConcurrentModificationException",
        "ProtocolChangedException",
        "CommitFailedException",
    }
)
_S3_COMMIT_PATH_MARKERS: frozenset[str] = frozenset(
    {"putIfAbsent", "writeCommit", "MultiClusterLogStore", "S3CommitClient"}
)
_S3_4XX_MARKERS: frozenset[str] = frozenset({"Bad Request", "Status Code: 4", " 400"})

# Sized to the AC-1 for-each fan-out (concurrency 8): a worker can lose the _delta_log-version race
# more than once under 8-way contention, so allow > 8 attempts. An extra event-only retry costs
# ~seconds; exhausting costs a hard write failure -> over-provision.
_COMMIT_MAX_ATTEMPTS = 10
_COMMIT_BACKOFF_BASE_S = 0.5
_COMMIT_BACKOFF_CAP_S = 20.0


def _is_concurrent_commit_error(exc: BaseException) -> bool:
    """True if ``exc`` is a Delta concurrent-commit conflict that should be retried.

    Matches the typed Delta conflict family OR the serverless S3-400-at-commit signature
    (a 4xx whose message references the ``_delta_log`` commit path). Marker-based because
    Spark Connect returns these as stringly-typed SparkExceptions (ADR-002 §3 discipline).
    """
    msg = str(exc)
    if any(m in msg for m in _COMMIT_CONFLICT_MARKERS):
        return True
    return (
        "_delta_log" in msg
        and any(m in msg for m in _S3_COMMIT_PATH_MARKERS)
        and any(m in msg for m in _S3_4XX_MARKERS)
    )
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest src/tests/test_ingestion_utils.py -k matcher -q`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/utils.py src/tests/test_ingestion_utils.py
git commit -m "feat(delta): concurrent-commit error matcher for write retry"
```

---

## Task 2: `_commit_with_retry` helper + wire into `write_delta_table` + merge pointer

**Files:**
- Modify: `src/ingestion/utils.py`
- Test: `src/tests/test_ingestion_utils.py`

- [ ] **Step 1: Write the failing tests**

```python
# append to src/tests/test_ingestion_utils.py
import pytest

from ingestion.utils import _COMMIT_BACKOFF_BASE_S, _COMMIT_BACKOFF_CAP_S, _COMMIT_MAX_ATTEMPTS, _commit_with_retry


def test_commit_with_retry_succeeds_after_conflicts(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr("ingestion.utils.time.sleep", lambda *_: None)
    calls = {"n": 0}

    def _commit() -> None:
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("Job aborted due to ConcurrentAppendException: files added")

    _commit_with_retry(_commit, "cat.bronze.t", logger=None)
    assert calls["n"] == 3  # failed twice, succeeded on the third


def test_commit_with_retry_reraises_non_retryable(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr("ingestion.utils.time.sleep", lambda *_: None)
    calls = {"n": 0}

    def _commit() -> None:
        calls["n"] += 1
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        _commit_with_retry(_commit, "cat.bronze.t", logger=None)
    assert calls["n"] == 1  # not retried


def test_commit_with_retry_exhausts_then_raises(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr("ingestion.utils.time.sleep", lambda *_: None)
    calls = {"n": 0}

    def _commit() -> None:
        calls["n"] += 1
        raise RuntimeError("ConcurrentAppendException always")

    with pytest.raises(RuntimeError, match="ConcurrentAppendException"):
        _commit_with_retry(_commit, "cat.bronze.t", logger=None)
    assert calls["n"] == _COMMIT_MAX_ATTEMPTS


def test_commit_with_retry_jitter_is_bounded(monkeypatch) -> None:  # noqa: ANN001
    slept: list[float] = []
    monkeypatch.setattr("ingestion.utils.time.sleep", lambda s: slept.append(s))
    calls = {"n": 0}

    def _commit() -> None:
        calls["n"] += 1
        raise RuntimeError("ConcurrentAppendException")  # always retryable -> sleeps until exhaustion

    with pytest.raises(RuntimeError):
        _commit_with_retry(_commit, "cat.bronze.t", logger=None)
    # one sleep per non-final attempt, each within [0, min(cap, base*2^(n-1))]
    assert len(slept) == _COMMIT_MAX_ATTEMPTS - 1
    for attempt, s in enumerate(slept, start=1):
        ceiling = min(_COMMIT_BACKOFF_CAP_S, _COMMIT_BACKOFF_BASE_S * 2 ** (attempt - 1))
        assert 0.0 <= s <= ceiling, (attempt, s, ceiling)


def test_write_delta_table_routes_commit_through_retry(monkeypatch) -> None:  # noqa: ANN001
    """Protects the WIRING: write_delta_table must route its commit THROUGH _commit_with_retry,
    and the injected callable must actually call saveAsTable with the full table name. Catches a
    refactor that leaves saveAsTable beside (not inside) the helper — the unit-green/Databricks-red gap."""
    from unittest.mock import MagicMock

    import ingestion.utils as utils_mod

    monkeypatch.setattr(utils_mod, "add_audit_columns", lambda df: df)  # skip pyspark audit-col path
    writer = MagicMock()
    df = MagicMock()
    # replace_where path: df.write.format("delta").option(...).option(...).mode("overwrite") -> writer
    df.write.format.return_value.option.return_value.option.return_value.mode.return_value = writer

    captured: dict[str, object] = {}

    def _fake_retry(commit_fn, table, logger):  # noqa: ANN001, ANN202
        captured["table"] = table
        commit_fn()  # prove the injected lambda actually invokes saveAsTable

    monkeypatch.setattr(utils_mod, "_commit_with_retry", _fake_retry)

    rows = utils_mod.write_delta_table(
        df, "cat", "bronze", "spadl_action_context", replace_where="match_id = 'X'", row_count=7
    )
    assert rows == 7
    assert captured["table"] == "cat.bronze.spadl_action_context"
    writer.saveAsTable.assert_called_once_with("cat.bronze.spadl_action_context")
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest src/tests/test_ingestion_utils.py -k commit_with_retry -q`
Expected: FAIL — `cannot import name '_commit_with_retry'`.

- [ ] **Step 3: Implement `_commit_with_retry` + call it from `write_delta_table`**

Add just below `_is_concurrent_commit_error`:

```python
def _commit_with_retry(commit_fn: Callable[[], None], table: str, logger: logging.Logger | None) -> None:
    """Run ``commit_fn`` (a Delta write), retrying ONLY concurrent-commit conflicts.

    Full jitter is essential: N drain workers racing one table would re-collide on a fixed schedule;
    jitter de-synchronises them so they serialise across _delta_log versions. Non-conflict errors
    raise immediately; conflicts past the attempt cap raise too (with an ERROR hint, since either
    contention is severe or the matcher drifted -- see _is_concurrent_commit_error).
    """
    for attempt in range(1, _COMMIT_MAX_ATTEMPTS + 1):
        try:
            commit_fn()
            return
        except Exception as exc:  # noqa: BLE001 -- re-raised unless a retryable concurrent-commit conflict
            if not _is_concurrent_commit_error(exc):
                raise
            if attempt >= _COMMIT_MAX_ATTEMPTS:
                if logger is not None:
                    logger.error(
                        "write_delta_table exhausted %d concurrent-commit retries on %s -- contention "
                        "severe, OR _is_concurrent_commit_error may have drifted (DBR change); compare "
                        "against _S3_COMMIT_PATH_MARKERS",
                        _COMMIT_MAX_ATTEMPTS,
                        table,
                    )
                raise
            ceiling = min(_COMMIT_BACKOFF_CAP_S, _COMMIT_BACKOFF_BASE_S * 2 ** (attempt - 1))
            sleep_s = random.uniform(0, ceiling)  # noqa: S311 -- backoff jitter, not cryptographic
            if logger is not None:
                logger.warning(
                    "write_delta_table concurrent-commit retry %d/%d on %s after %.2fs (%s)",
                    attempt,
                    _COMMIT_MAX_ATTEMPTS,
                    table,
                    sleep_s,
                    type(exc).__name__,
                )
            time.sleep(sleep_s)
```

In `write_delta_table`, replace `writer.saveAsTable(full_table)` (~line 278) with:

```python
    _commit_with_retry(lambda: writer.saveAsTable(full_table), full_table, logger)
```

In `merge_delta_table` (just above its commit/merge execution), add a pointer comment:

```python
    # NOTE: single-writer today, so no concurrent-commit retry. If a concurrent merge writer is ever
    # added, wrap the commit in ingestion.utils._commit_with_retry (ADR-038) -- same S3-400 class.
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest src/tests/test_ingestion_utils.py -k "commit_with_retry or matcher or routes_commit" -q`
Expected: PASS (12 tests: 7 matcher + 4 retry + 1 wiring). Then `uv run ruff check src/ingestion/utils.py` clean; `uv run pyright src/ingestion/utils.py` 0 errors.

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/utils.py src/tests/test_ingestion_utils.py
git commit -m "feat(delta): retry write_delta_table on concurrent-commit conflict (jittered backoff)"
```

---

## Task 3: Governance — ADR-038 (new) + ADR-037 cross-link + CLAUDE.md + wheel bump

**Files:**
- Create: `docs/superpowers/adrs/ADR-038-delta-concurrent-commit-retry.md`
- Modify: `docs/superpowers/adrs/ADR-037-action-context-worker-drain-fanout.md` (one cross-link line)
- Modify: `CLAUDE.md`
- version: `scripts/bump_wheel.py`

- [ ] **Step 1: Write ADR-038** (use `docs/superpowers/adrs/ADR-TEMPLATE.md`)

Context: the confirmed contention finding (run `730644476818402`: 1/5 written, 4/5 identical S3-400
racing `_delta_log/00035.json`; Delta's own retry didn't catch the raw S3-400). **Be honest about the
mechanism (evidence-before-claim):** the spec evidences *what* happened (stack, version, 4/4 identical
400s) but not *why* a conflict surfaces as a malformed-request 400 rather than the canonical
409/412 — either (a) known UC coordinated-commit-under-contention behavior (retry is exactly right),
or (b) a transient `S3CommitClient` bug under concurrency (retry is a fine mitigation, but a
Databricks support case is warranted). Add one Context line: link Databricks coordinated-commits docs
**or** note that a support case was/should be filed. The matcher's broad `Status Code: 4` already
covers 409/412 if Databricks "fixes" the code — state that this is deliberate.

Decision: a gated, fully-jittered retry of the concurrent-commit error class (Delta conflict family +
S3-400-at-commit signature) in the shared `write_delta_table`, `_COMMIT_MAX_ATTEMPTS=10` sized to the
8-way fan-out; safe because AC-1 workers write disjoint `replaceWhere` partitions (idempotent under
retry).

**Alternatives considered (eliminate vs mitigate)** — `_delta_log` serialization is inherent to one
Delta table, so the only ways to *eliminate* (not mitigate) contention are: (a) serialize commits via
a single-committer / the queue, or (b) split into multiple tables. Retry is the pragmatic
*mitigation*; the dynamic-claim follow-up ([[project_ac1_drain_dynamic_claim_followup]]) is partial
elimination. State this so "why not fix it properly?" is answered in the doc.

Consequences (name the **repo-wide** scope): every `write_delta_table` caller now retries this class
and may add up to ~tens of seconds latency **only under genuine contention** (worst case ~91 s across
10 attempts — attempts 7–10 are pinned at the 20 s cap; well under the 1800 s per-game watchdog);
single-writer callers unaffected; matcher-drift risk + the exhaustion-ERROR self-diagnosis. Note that
`ProtocolChangedException`/`CommitFailedException` are included as **Delta's own retryable family** —
slightly wider than pure append/delete contention (a one-shot protocol change would burn up to 10
harmless retries before raising), so a reader isn't surprised. Related: ADR-037 (worker-drain that
exposed it); the fail-loud + bounded-quarantine follow-up (spec §10).

- [ ] **Step 2: Cross-link from ADR-037**

In `ADR-037-action-context-worker-drain-fanout.md` "Related" section, add:
"- **ADR-038** — Delta concurrent-commit retry in `write_delta_table`, the fix for the
concurrent-commit contention this worker-drain exposed (run 730644476818402)."

- [ ] **Step 3: Amend CLAUDE.md**

Under "Database Performance → Databricks (PySpark / Delta Lake)", add a bullet:
"`write_delta_table` retries the concurrent-commit conflict class (Delta `Concurrent*Exception` + the
serverless S3-400-at-`_delta_log`-commit signature) with fully-jittered backoff
(`_COMMIT_MAX_ATTEMPTS=10`, sized to the 8-way for-each fan-out), so multiple workers writing one
Delta table concurrently are safe (ADR-038). Single-writer callers are unaffected (they never raise
the conflict). The only behavior change for them is the rare added latency if they ever do contend."

- [ ] **Step 4: Bump the wheel**

Run: `uv run python scripts/bump_wheel.py` (NEVER edit `pyproject.toml version=` by hand).
Verify: `uv run python scripts/bump_wheel.py --check` → "All files consistent".

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/adrs/ADR-038-delta-concurrent-commit-retry.md \
        docs/superpowers/adrs/ADR-037-action-context-worker-drain-fanout.md CLAUDE.md
git add -A   # picks up ALL ~25 wheel-version files bump_wheel.py touched (avoids an inconsistent
             # hand-listed subset); review `git status` first to confirm only expected files staged
git commit -m "docs(delta): ADR-038 concurrent-commit retry + CLAUDE.md + wheel bump"
```

---

## Task 4: Full local gate

**Files:** none (verification)

- [ ] **Step 1: Run the full gate (mirror CI — full suite, not subsets; PR #334 lesson)**

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run pyright src/
uv run pytest
```
Expected: all clean / PASS.

- [ ] **Step 2: import-linter**

Run: `uv run lint-imports`
Expected: contracts kept (utils.py is in `ingestion`; no new cross-layer imports — only stdlib `random`/`time` + `Callable`).

- [ ] **Step 3: Stop for review.**

Serverless verification (manual, post-merge, after the wheel deploys): re-run `statsbomb` /
`max_units=5` → expect all 5 to write and the for-each GREEN (retry serialises the commits). This is
the contention-fix proof; it cannot run in offline CI (no local Spark).

---

## Self-Review (spec coverage)

| Spec section | Task |
|---|---|
| §5.1 matcher (Delta family + S3-400 signature, scoped) | 1 |
| §5.2 `_commit_with_retry` (jitter, attempts=10, exhaustion ERROR) | 2 |
| §5.4 `merge_delta_table` pointer | 2 |
| §6 testing (matcher truth table, retry counts, jitter bound) | 1, 2 |
| §9 governance (ADR-038, ADR-037 cross-link, CLAUDE.md, wheel) | 3 |
| full gate / import-linter | 4 |
| Decision: retry in shared util, gated | 2 |
| Decision: match Delta family + S3-400 signature | 1 |
| Decision: attempts sized to fan-out (10) | 1 |
| Decision: standalone ADR-038 | 3 |
| §10 fail-loud + quarantine = follow-up (NOT this PR) | n/a (out of scope) |

**Placeholder scan:** none — every code step has complete code. **Type consistency:**
`_is_concurrent_commit_error` / `_commit_with_retry` / `_COMMIT_*` constants used identically across
Tasks 1–2 and the tests. **No `drain.py` / `action_context.py` / Terraform changes** (fail-loud is
the follow-up — confirmed against spec §2/§10).

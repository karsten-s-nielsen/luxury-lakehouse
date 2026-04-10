# Evolve Error Feedback Artifacts — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire error tracebacks through OpenEvolve's existing artifact system so the LLM sees *why* a candidate failed, not just that it scored 0.

**Architecture:** Three files change. The target evaluator and remote worker capture tracebacks into a `_error_text` metrics key. The evaluator bridge strips it and wraps everything in `EvaluationResult` with error artifacts. OpenEvolve's prompt sampler renders artifacts into the LLM's next prompt automatically.

**Tech Stack:** Python 3.10, OpenEvolve (`EvaluationResult`), pytest, ruff, pyright

**Spec:** `docs/superpowers/specs/2026-04-09-evolve-error-feedback-design.md`

---

### Task 1: Target evaluator captures traceback on error

**Files:**
- Modify: `src/evolve/targets/scoutgpt/evaluator.py:277-279`

- [ ] **Step 1: Write the failing test**

Add to `src/tests/test_evolve_evaluator.py` at the end of the file:

```python
class TestErrorTextCapture:
    """Tests that _error_text is returned when train_and_evaluate fails."""

    def test_error_text_returned_on_backend_error(self, tmp_path: Path) -> None:
        """When backend returns _error_text, evaluator includes it in result."""
        candidate_path = _write_candidate(tmp_path, VALID_CONFIG)

        mock_backend = MagicMock()
        mock_backend.train.return_value = {
            "combined_score": 0.0,
            "error": 1.0,
            "_error_text": "RuntimeError: shape mismatch [2,3] vs [4,5]",
        }

        fitness = FitnessConfig(
            primary="spearman_rho",
            secondary="top1_accuracy",
            combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
        )
        evaluator = EvolveEvaluator(
            backend=mock_backend,
            target="scoutgpt",
            eval_config=EvalConfig(epochs=5, seed=42),
            fitness_config=fitness,
        )
        result = evaluator.evaluate(str(candidate_path))

        # Result should be EvaluationResult with artifact
        from openevolve.evaluation_result import EvaluationResult

        assert isinstance(result, EvaluationResult)
        assert "error" in result.artifacts
        assert "shape mismatch" in result.artifacts["error"]
        # _error_text must NOT appear in metrics
        assert "_error_text" not in result.metrics

    def test_no_artifact_on_success(self, tmp_path: Path) -> None:
        """Successful evaluation returns EvaluationResult with no artifacts."""
        candidate_path = _write_candidate(tmp_path, VALID_CONFIG)

        mock_backend = MagicMock()
        mock_backend.train.return_value = {
            "spearman_rho": 0.5,
            "top1_accuracy": 0.6,
        }

        fitness = FitnessConfig(
            primary="spearman_rho",
            secondary="top1_accuracy",
            combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
        )
        evaluator = EvolveEvaluator(
            backend=mock_backend,
            target="scoutgpt",
            eval_config=EvalConfig(epochs=5, seed=42),
            fitness_config=fitness,
        )
        result = evaluator.evaluate(str(candidate_path))

        from openevolve.evaluation_result import EvaluationResult

        assert isinstance(result, EvaluationResult)
        assert not result.has_artifacts()
        assert result.metrics["combined_score"] == pytest.approx(0.7 * 0.5 + 0.3 * 0.6)

    def test_error_text_stripped_before_combined_score(self, tmp_path: Path) -> None:
        """_error_text must not affect combined_score computation."""
        candidate_path = _write_candidate(tmp_path, VALID_CONFIG)

        mock_backend = MagicMock()
        mock_backend.train.return_value = {
            "spearman_rho": 0.3,
            "top1_accuracy": 0.5,
            "_error_text": "Warning: some issue occurred",
        }

        fitness = FitnessConfig(
            primary="spearman_rho",
            secondary="top1_accuracy",
            combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
        )
        evaluator = EvolveEvaluator(
            backend=mock_backend,
            target="scoutgpt",
            eval_config=EvalConfig(epochs=5, seed=42),
            fitness_config=fitness,
        )
        result = evaluator.evaluate(str(candidate_path))

        from openevolve.evaluation_result import EvaluationResult

        assert isinstance(result, EvaluationResult)
        # Score computed without _error_text interference
        assert result.metrics["combined_score"] == pytest.approx(0.7 * 0.3 + 0.3 * 0.5)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_evolve_evaluator.py::TestErrorTextCapture -v`
Expected: FAIL — `EvolveEvaluator.evaluate()` returns `dict`, not `EvaluationResult`

- [ ] **Step 3: Modify target evaluator to capture traceback**

In `src/evolve/targets/scoutgpt/evaluator.py`, add `import traceback` to the imports (line 8 area), then change the except block at line 277-279:

Current code:
```python
    except (torch.cuda.OutOfMemoryError, RuntimeError) as exc:
        _log.warning("Candidate failed (OOM or runtime error), returning score 0: %s", exc)
        metrics = {"combined_score": 0.0, "error": 1.0}
```

New code:
```python
    except (torch.cuda.OutOfMemoryError, RuntimeError) as exc:
        _log.warning("Candidate failed (OOM or runtime error), returning score 0: %s", exc)
        metrics = {"combined_score": 0.0, "error": 1.0, "_error_text": traceback.format_exc()}
```

- [ ] **Step 4: Modify EvolveEvaluator to return EvaluationResult**

In `src/evolve/evaluator.py`:

Add import at top (after existing imports):
```python
from openevolve.evaluation_result import EvaluationResult
```

Replace the `evaluate` method (lines 247-314) with:

```python
    def evaluate(self, program_path: str) -> EvaluationResult:
        """Evaluate a candidate program and return fitness metrics with optional error artifacts.

        On any failure (bad config, training error, missing metrics) returns
        ``EvaluationResult`` with zero scores and an error artifact containing
        the traceback so the LLM can learn from the failure.

        Args:
            program_path: Path to the ``.py`` file containing a ``config`` dict
                and optional ``custom_embed`` / ``custom_layers`` functions.

        Returns:
            ``EvaluationResult`` with metrics and optional error artifacts.
        """
        try:
            program = _load_program(program_path)
        except Exception:
            _log.exception("Failed to load program %s", program_path)
            return EvaluationResult(
                metrics={**fail_metrics(), **self._fail_score(), "reject_reason": "load_error"},
                artifacts={"error": traceback.format_exc()},
            )

        config = program.config

        # When custom_embed is present, conditioning_type is ignored (the custom
        # function replaces the built-in conditioning).  Override to a valid value
        # so search-space validation doesn't reject creative type names the LLM
        # may invent (e.g. "adaptive_gating").
        if program.has_custom_embed and "conditioning_type" in config:
            config = {**config, "conditioning_type": "additive"}

        if not validate_search_space(config):
            _log.warning("Program %s rejected: search space validation failed", program_path)
            return EvaluationResult(
                metrics={**fail_metrics(), **self._fail_score(), "reject_reason": "search_space"},
                artifacts={"error": f"Search space validation failed for {Path(program_path).name}"},
            )

        # Level 2 validation gate
        send_program_path: str | None = None
        if program.has_custom_embed or program.has_custom_layers:
            if self._validation_profile is None:
                _log.error("Level 2 program but no ValidationProfile configured")
                return EvaluationResult(
                    metrics={**fail_metrics(), **self._fail_score(), "reject_reason": "no_profile"},
                    artifacts={"error": "Level 2 program submitted but no ValidationProfile configured"},
                )
            source = Path(program_path).read_text()
            valid, reason = validate_program(
                source,
                self._validation_profile,
                code_evolution=self._code_evolution,
            )
            if not valid:
                _log.warning("Program %s rejected: %s", program_path, reason)
                return EvaluationResult(
                    metrics={**fail_metrics(), **self._fail_score(), "reject_reason": reason},
                    artifacts={"error": f"Code validation rejected: {reason}"},
                )
            send_program_path = program_path

        train_kwargs: dict[str, Any] = {
            "candidate_config": config,
            "target": self._target,
            "epochs": self._eval_config.epochs,
            "seed": self._eval_config.seed,
        }
        if send_program_path is not None:
            train_kwargs["program_path"] = send_program_path

        try:
            metrics = self._backend.train(**train_kwargs)
        except Exception:
            _log.exception("Backend training failed for %s", program_path)
            return EvaluationResult(
                metrics={**fail_metrics(), **self._fail_score(), "reject_reason": "backend_error"},
                artifacts={"error": traceback.format_exc()},
            )

        # Extract error text from backend (if any) before computing score
        error_text = metrics.pop("_error_text", None)

        combined = self._compute_combined_score(metrics)
        result_metrics = {**metrics, "combined_score": combined}

        if error_text:
            return EvaluationResult(metrics=result_metrics, artifacts={"error": str(error_text)})
        return EvaluationResult.from_dict(result_metrics)
```

Also add `import traceback` to the imports at the top of the file.

- [ ] **Step 5: Run the new tests**

Run: `uv run pytest src/tests/test_evolve_evaluator.py::TestErrorTextCapture -v`
Expected: All 3 tests PASS

- [ ] **Step 6: Run the full existing test suite to check for regressions**

Run: `uv run pytest src/tests/test_evolve_evaluator.py -v`
Expected: All tests PASS. Existing tests that check `result["combined_score"]` will still work because `EvaluationResult` supports `__getitem__` via `.metrics` — but if any fail, update them to use `result.metrics["combined_score"]`.

**If existing tests fail** because they index into the result dict: The `EvaluationResult` does NOT support `result["key"]` directly. Existing tests use `result["combined_score"]` etc. Since `evaluate()` now returns `EvaluationResult`, update the failing tests to use `result.metrics["key"]` or `result.to_dict()["key"]`. Check each failing test individually.

---

### Task 2: Remote worker captures traceback on error

**Files:**
- Modify: `src/evolve/remote_worker.py:67-77`
- Test: `src/tests/test_evolve_evaluator.py` (new test class)

- [ ] **Step 1: Write the failing test**

Add to `src/tests/test_evolve_evaluator.py`:

```python
class TestRemoteWorkerErrorCapture:
    """Tests that the remote worker outputs _error_text on failure."""

    def test_remote_worker_captures_traceback(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When train_and_evaluate raises, remote worker outputs JSON with _error_text."""
        import io

        # Write a dummy candidate config
        candidate = tmp_path / "candidate.json"
        candidate.write_text(json.dumps(VALID_CONFIG))

        # Mock the evaluator to raise
        def mock_train_and_evaluate(**kwargs: Any) -> dict[str, float]:
            msg = "CUDA out of memory"
            raise RuntimeError(msg)

        mock_module = MagicMock()
        mock_module.train_and_evaluate = mock_train_and_evaluate
        monkeypatch.setattr("importlib.import_module", lambda name: mock_module)

        # Capture stdout
        captured_stdout = io.StringIO()
        monkeypatch.setattr("sys.stdout", captured_stdout)
        monkeypatch.setattr("sys.argv", ["remote_worker", str(candidate), "cpu", "1", "42", "scoutgpt"])

        from evolve.remote_worker import main

        main()

        output = captured_stdout.getvalue().strip()
        result = json.loads(output)
        assert result["combined_score"] == 0.0
        assert result["error"] == 1.0
        assert "_error_text" in result
        assert "CUDA out of memory" in result["_error_text"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_evolve_evaluator.py::TestRemoteWorkerErrorCapture -v`
Expected: FAIL — remote worker currently lets the exception propagate and exits non-zero

- [ ] **Step 3: Modify remote worker to capture errors**

In `src/evolve/remote_worker.py`, add `import traceback` to the imports, then replace lines 67-77:

Current code:
```python
    _log.info("Running %s evaluator (device=%s, epochs=%d, seed=%d)", target, device, epochs, seed)
    target_module = importlib.import_module(f"evolve.targets.{target}.evaluator")
    metrics: dict[str, float] = target_module.train_and_evaluate(
        candidate_config=config,
        device=device,
        epochs=epochs,
        seed=seed,
        program_path=program_path,
    )

    # Single JSON line to stdout — the SSH caller parses this.
    print(json.dumps(metrics))
```

New code:
```python
    _log.info("Running %s evaluator (device=%s, epochs=%d, seed=%d)", target, device, epochs, seed)
    target_module = importlib.import_module(f"evolve.targets.{target}.evaluator")
    try:
        metrics: dict[str, Any] = target_module.train_and_evaluate(
            candidate_config=config,
            device=device,
            epochs=epochs,
            seed=seed,
            program_path=program_path,
        )
    except Exception:
        _log.exception("Remote worker evaluation failed")
        metrics = {"combined_score": 0.0, "error": 1.0, "_error_text": traceback.format_exc()}

    # Single JSON line to stdout — the SSH caller parses this.
    print(json.dumps(metrics))
```

Also update the type annotation import at the top — change `from typing import Any` (already present, just verify).

- [ ] **Step 4: Run all remote worker and error capture tests**

Run: `uv run pytest src/tests/test_evolve_evaluator.py::TestRemoteWorkerErrorCapture src/tests/test_evolve_evaluator.py::TestErrorTextCapture -v`
Expected: All PASS

---

### Task 3: Lint, type-check, and full test suite

**Files:**
- All modified files

- [ ] **Step 1: Run ruff check**

Run: `uv run ruff check src/evolve/evaluator.py src/evolve/targets/scoutgpt/evaluator.py src/evolve/remote_worker.py src/tests/test_evolve_evaluator.py`
Expected: No violations. Fix any that appear.

- [ ] **Step 2: Run ruff format check**

Run: `uv run ruff format --check src/evolve/evaluator.py src/evolve/targets/scoutgpt/evaluator.py src/evolve/remote_worker.py src/tests/test_evolve_evaluator.py`
Expected: No reformatting needed. If needed, run `uv run ruff format` on the files.

- [ ] **Step 3: Run pyright**

Run: `uv run pyright src/evolve/evaluator.py src/evolve/targets/scoutgpt/evaluator.py src/evolve/remote_worker.py`
Expected: 0 errors. The `EvaluationResult` return type and `traceback.format_exc()` calls should pass basic mode. If pyright flags the `EvaluationResult` import, add a `# type: ignore[import-untyped]` comment.

- [ ] **Step 4: Run full evolve test suite**

Run: `uv run pytest src/tests/test_evolve_evaluator.py src/tests/test_evolve_config.py src/tests/test_evolve_level2.py -v`
Expected: All PASS. If existing tests break on the `dict` → `EvaluationResult` return type change, update them to access `result.metrics["key"]` instead of `result["key"]`.

- [ ] **Step 5: Sync updated code to DGX Spark**

After all tests pass, sync the changed files to the remote:

```bash
scp -r src/evolve/* karsten@192.168.68.73:/home/karsten/Development/evolve-env/lib/python3.12/site-packages/evolve/
```

Verify with hash comparison on the changed files.

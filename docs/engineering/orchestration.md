# Orchestration Discipline

Rules and debugging patterns for scripts that dispatch training across multiple remote compute backends — the "orchestration layer" that sits above `src/evolve/backends/` and below domain-specific evaluators. Scope: every file under `scripts/evaluate_*.py` that uses `BackendPool` + `ThreadPoolExecutor` for parallel dispatch, and the backends themselves (`src/evolve/backends/remote_ssh.py`, `src/evolve/backends/local_cuda.py`, `src/evolve/backends/hf_jobs.py`).

These rules were derived from the EV2 Phase 1 cycle (2026-04-23), where a six-variant harvest on three backends produced five false silent-inf failures before the real root cause surfaced. The cycle cost roughly 4 hours of active debugging across 5 sequential re-fires (Phase 1a → 1e). Every rule below maps to a concrete failure mode from that debugging session — they're not speculative; they're sutures.

Short-form statement of each rule is in the project `CLAUDE.md` § Orchestration Discipline; the full rationale and failure-mode narrative live here.

## Rule 1 — HF tokens via `huggingface_hub.get_token()`

**Failure mode.** Non-interactive SSH sessions on Linux frequently have `HF_TOKEN` unset even when interactive sessions see it (`HF_TOKEN` typically lives in `~/.bashrc`, which is not sourced for `ssh host "cmd"`). A bare `os.environ.get("HF_TOKEN", "")` returns the empty string, which is then passed through as `token=""` to `hf_hub_download` or similar. `huggingface_hub` constructs an `Authorization: Bearer ` header with empty token, and `httpx` rejects this with `LocalProtocolError: Illegal header value b'Bearer '` — an exception class that is rarely in an evaluator's narrow `except` tuple, so it escapes the evaluator entirely.

**Fix.** Use `huggingface_hub.get_token()`, which cascades through:

1. `HF_TOKEN` env var
2. `HUGGING_FACE_HUB_TOKEN` env var
3. `~/.cache/huggingface/token` file (written by `hf auth login` or manual provisioning)
4. `~/.huggingface/token` (legacy path)
5. Returns `None`

Result: even if the non-interactive SSH env doesn't have the token, a file-cached token is picked up transparently. This matches `HfApi()`'s default resolution — a good indicator you're on the well-trodden path.

**Applies to.** Every evaluator, training script, and remote worker. Not just orchestrator code — the evaluator runs on the REMOTE, where the env is what the SSH session gave it.

**Code reference.** `src/evolve/targets/football2vec/evaluator.py:261,524` (fixed 2026-04-23). Same anti-pattern to sweep in: `src/evolve/targets/scoutgpt/evaluator.py:212`, `src/ingestion/utils.py:665`.

## Rule 2 — Smoke tests must exercise authentication, not just imports

**Failure mode.** A smoke test that checks `import X` for every required module and `torch.cuda.is_available()` passes when the venv is properly provisioned. But it says nothing about whether the remote can authenticate to HF Hub. An unauthenticated remote passes the import check, gets admitted to the pool, and then every dispatched variant silently burns in seconds returning `val_mlm=inf`.

**Fix.** The pre-dispatch smoke test must make an authenticated HF Hub call. `HfApi().whoami()` is cheap (~300 ms), authenticates via the full token-resolution chain (Rule 1), and returns the user name on success or raises `HfHubHTTPError` on 401. Use it as the final step of the smoke probe:

```python
hf_auth_probe = (
    "from huggingface_hub import HfApi; "
    '_name = HfApi().whoami().get("name", ""); '
    'assert _name, "hf_auth: HfApi().whoami() returned no user name or invalid token"'
)
```

**Code reference.** `scripts/evaluate_football2vec_l2_adversary_seeds.py::_smoke_test_remote` (Phase 1d 2026-04-23).

## Rule 3 — Post-deploy entrypoint verification is mandatory

**Failure mode.** The venv-only smoke test (Rule 2) tests the remote Python environment. But the dispatched worker imports code from the DEPLOYED SOURCE at `$REMOTE_DIR/src/` via `PYTHONPATH=./src`. A branch-specific change — a renamed module, a new transitive dep in `evolve/__init__.py`, a broken circular import — only surfaces when the deployed source is exercised. Pure venv smoke doesn't catch it.

**Fix.** After `_deploy_to_remote`, run a second probe that imports the exact entry chain the dispatched worker will use:

```bash
cd <REMOTE_DIR> && env PYTHONPATH=./src <VENV_PYTHON> -c 'from evolve.evaluator import EvolveEvaluator; from evolve.remote_worker import main'
```

This exercises `evolve/__init__.py` (which transitively imports `openevolve`), `evolve.evaluator` (which imports `openevolve.evaluation_result`), and `evolve.remote_worker`. If any of these fail — e.g., a new dep missing from the remote venv — the backend is skipped with a precise warning before any variant is wasted.

**Code reference.** `scripts/evaluate_football2vec_l2_adversary_seeds.py::_verify_remote_entrypoint` (Phase 1d 2026-04-23).

## Rule 4 — Per-backend timeout = measured per-epoch × max epochs × 2 safety

**Failure mode.** `RemoteSSHBackend(timeout=...)` defaults to `900` seconds (15 minutes) — tuned for fast backends where a single candidate runs ~5 min per epoch × ~3-4 epochs before early-stop. For a slower backend (DGX Spark GB10 at ~0.5× RTX 5070 Ti speed), a single candidate runs ~11 min per epoch × 16 epochs = 176 min. The 15-min default kills it mid-Epoch-1 at `elapsed ≈ 904s` with `val_mlm=inf`. The Phase 1b `attention_pool_head` loss on DGX Spark was exactly this mechanism.

**Fix.** Set `timeout_seconds` per host in the orchestrator's `_REMOTE_HOSTS` config based on the machine's measured epoch speed:

```python
timeout_seconds = measured_seconds_per_epoch * max_epochs * 2  # 2x safety factor
```

For the fast 5070 Ti: `330s/epoch × 30 epochs × 2 = 19800s` (round to `10800s` = 3h since early-stop usually fires around epoch 16). For the slow GB10: `660s/epoch × 30 epochs × 2 = 39600s` (round to `21600s` = 6h).

**Code reference.** `scripts/evaluate_football2vec_l2_adversary_seeds.py::_REMOTE_HOSTS.timeout_seconds` + `_build_pool` propagation to `RemoteSSHBackend(..., timeout=...)` (Phase 1c 2026-04-23).

## Rule 5 — Evaluator `except Exception`, not a narrow tuple

**Failure mode.** Evaluators that catch only specific exception classes miss the long tail of cross-library surprises:

- `httpx.HTTPError` and subclasses (`LocalProtocolError`, `TransportError`, `HTTPStatusError`) — from `huggingface_hub` HTTP calls
- `huggingface_hub.errors.HfHubHTTPError` — wrapped 4xx/5xx from HF Hub
- `OSError` — from the file system, the network stack, CUDA context init
- `KeyError`, `AttributeError`, `TypeError` — from `candidate_config` values that don't match the evaluator's expectations
- Any new exception class added by a library version bump

A narrow `except (OutOfMemoryError, RuntimeError, ValueError)` misses all of these. When any of them raises, it escapes the evaluator, crashes the remote worker with a traceback written to stderr... which the orchestrator may or may not capture and may or may not surface in logs. In Phase 1c, the remote worker exited so quickly after printing the startup log that the traceback never flushed to stderr; the orchestrator saw a clean exit with `val_mlm=inf` and moved on. Hours of silent-inf debugging followed.

**Fix.** Catch `Exception`. The cost is a few extra caught classes that in practice all result in `fail_metrics` anyway. The benefit is a guaranteed traceback in `_error_text` for post-mortem. If a specific exception class needs a specific recovery path, chain the handlers:

```python
try:
    ...
except SpecificError:
    do_specific_recovery()
except Exception as exc:
    _log.warning("Candidate failed: %s", exc)
    metrics["_error_text"] = traceback.format_exc()
```

**Note on `BLE001`.** `ruff`'s `BLE001` (flake8-blind-except) flags broad catches. If `BLE001` is enabled in the target path (check `pyproject.toml [tool.ruff]`), add `# noqa: BLE001 — <justification>` with a one-line reason. If `BLE001` is not in the enabled rule set for that path, no noqa is needed — ruff-0.x warns on unused noqa directives via `RUF100`.

**Code reference.** `src/evolve/targets/football2vec/evaluator.py:636` (broadened 2026-04-23).

## Rule 6 — Remote shell probes use double quotes internally, ASCII only

**Failure mode — single quotes.** A remote shell probe is a Python one-liner wrapped in a subprocess call:

```python
cmd = ["ssh", host, f"{venv_python} -c '{probe}' 2>&1"]
```

The remote bash receives `python -c '<probe>'`. Any `'` inside the probe terminates the outer single-quoted string. A probe containing `HfApi().whoami().get('name', '')` becomes:

```
python -c 'HfApi().whoami().get('name', '')'
         ^----quoted----^         ^-q-^   ^-q-^
```

Bash parses this as: `python`, `-c`, `HfApi().whoami().get(name` (adjacent quoted+unquoted), `,`, ` ` (unclear separator), `)` (unquoted), empty string, `2>&1`. Python receives garbage. The bash error message is typically truncated: `bash: -c: line 1: <first unbalanced string>`.

Python's `assert <cond>, 'msg'` with single quotes also breaks outer wrapping — but only triggers the bash parse error when `<cond>` is False (the assertion message is the lazy-evaluated arg; it's invisible when the assertion passes). So single-quoted assertion messages in probes can pass undetected for months, then break the first time the assertion trips.

**Failure mode — non-ASCII.** Em-dashes (`—` U+2014), smart quotes (`"` `"` U+201C/U+201D), and accented characters are multi-byte in UTF-8 and can be mangled by a remote locale that expects cp1252, Latin-1, or a C locale. The mangling can break shell parsing or produce `UnicodeDecodeError` in the subprocess reader thread (see the `cp1252 can't decode byte 0x8d` line in `phase1.log` from Phase 1a — a latent Windows-side issue triggered by Unicode slipping through).

**Fix.** In every Python probe string that will be wrapped in outer single quotes on the remote:

- Use double quotes for all string literals: `HfApi().whoami().get("name", "")`.
- Use plain ASCII for all message text: `-` not `—`, `"` for quotes around words.
- If a single quote is unavoidable (rare), use `'\''` escape sequence.

**Code reference.** `scripts/evaluate_football2vec_l2_adversary_seeds.py::_smoke_test_remote` + `_verify_remote_entrypoint` (Phase 1d 2026-04-23, after the first Phase 1d fire skipped both remotes with `bash: -c: line 1` parse errors).

## Rule 7 — Silent-inf metrics are always a bug, never "variant failed"

**Failure mode.** An orchestrator that emits `variant=X val_mlm=inf val_adv_acc=0.0 fitness=0.0 elapsed=4.2s` is describing TWO things: (a) the variant's metrics, and (b) a short elapsed time. The short elapsed time is the tell. Real variants take minutes to hours; anything under a minute is a fast-fail. The `inf` and `0.0` values are `fail_metrics()` — a sentinel returned when the evaluator caught an exception. Accepting this as "variant X didn't work, oh well" is how real problems hide in aggregate `results.json`.

**Fix.** Treat every silent-inf as an investigation target before re-firing:

1. **Read `_error_text` in the uploaded `metrics.json`.** If the evaluator's except clause populated it (Rule 5), the traceback is there. If not, the evaluator is still narrow — broaden it.
2. **Reproduce the call manually via SSH.** Same venv, same token state, same config; run `train_and_evaluate_stage2(...)` (or equivalent) directly on the remote. The unhandled exception surfaces in the local terminal.
3. **Check `_REMOTE_REQUIRED_IMPORTS`, entrypoint verify, and HF auth smoke** (Rules 1-3). A missing or stale piece in any of these can silently break dispatch.

Never accept `val_mlm=inf elapsed=<small>s` as a legitimate "this variant doesn't work" result. A variant that genuinely doesn't work (converges to a garbage loss, OOMs, NaN-loops) will STILL take epochs of real compute time before reporting inf. Small-elapsed inf is always a setup failure.

## Appendix A — Failure mode catalog (EV2 Phase 1, 2026-04-23)

For each failure, the debugging clock started when the orchestrator reported `variant=X val_mlm=inf fitness=0 elapsed=<small>s` and the user asked "what's wrong?".

| Phase | Failure | Debug time | Rule violated |
|---|---|---|---|
| 1a | `stdbuf: failed to run command 'PYTHONPATH=./src'` on Media-PC | ~30 min | (pre-existing, unrelated — fixed by `env PYTHONPATH=./src` pattern) |
| 1b | `ModuleNotFoundError: No module named 'openevolve'` on Media-PC | ~15 min | Rule 2 (smoke test didn't include openevolve in the import probe) |
| 1b | `Remote training timed out` on DGX Spark at 904s | ~30 min | Rule 4 (global 900s default too tight for GB10) |
| 1c | `httpx.LocalProtocolError: Illegal header value b'Bearer '` on Media-PC | ~90 min (hard debug) | Rule 1 (empty HF_TOKEN → `token=""`) + Rule 5 (narrow except swallowed the class) |
| 1c | `Invalid user token` on DGX Spark | ~5 min | (HF_TOKEN rotated between cycles; orthogonal to rules — a provisioning issue) |
| 1d take 1 | `bash: -c: line 1: <truncated>` both remotes skipped at smoke | ~10 min | Rule 6 (single quotes in `HfApi().whoami().get('name', '')`) |
| 1d take 2 | `val_mlm=inf elapsed=3-8s` on Media-PC + DGX Spark | ~60 min | Rule 1 (evaluator read `HF_TOKEN` from env directly, empty on non-interactive SSH; smoke test passed via file cache while evaluator did not) |

Total debug time across Phase 1a-1d: roughly 4 hours of active investigation spread across 5 sequential re-fires. Phase 1e (fix applied) started behaving correctly from Epoch 1 onwards.

## Appendix B — Reference smoke-test probe

The canonical probe at `scripts/evaluate_football2vec_l2_adversary_seeds.py::_smoke_test_remote` as of 2026-04-23. Use this as the pattern for future orchestrators:

```python
_REMOTE_REQUIRED_IMPORTS: tuple[str, ...] = (
    "torch",
    "safetensors",
    "huggingface_hub",
    "datasets",
    "pandas",
    "numpy",
    "pyarrow",
    "sklearn.model_selection",
    "scipy",
    "openevolve",  # Rule 1 / 2
)


def _smoke_test_remote(host: str, venv_python: str) -> tuple[bool, str]:
    import_probe = "; ".join(f"import {m}" for m in _REMOTE_REQUIRED_IMPORTS)
    # Rule 6: double quotes, ASCII only, no single quotes.
    hf_auth_probe = (
        "from huggingface_hub import HfApi; "
        '_name = HfApi().whoami().get("name", ""); '
        'assert _name, "hf_auth: HfApi().whoami() returned no user name or invalid token"'
    )
    # Check order: imports (cheapest to fix) -> CUDA (hw) -> HF auth (token).
    probe = (
        f"{import_probe}; "
        f'import torch; assert torch.cuda.is_available(), "cuda unavailable"; '
        f"{hf_auth_probe}"
    )
    cmd = ["ssh", "-o", "ConnectTimeout=10", host, f"{venv_python} -c '{probe}' 2>&1"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)  # noqa: S603
    except subprocess.TimeoutExpired:
        return False, "smoke test timed out"
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout).strip()
        first_issue = stderr.split("\n")[-1][:200]
        return False, first_issue
    return True, "ok"
```

## Appendix C — Enforcement tests

`src/tests/test_evolve_football2vec_l2.py` enforces:

- `_REMOTE_HOSTS` has entries for `media` + `spark` with valid `host`, `remote_dir`, `venv_python`, AND `timeout_seconds: int > 0` (Rule 4).
- `_REMOTE_REQUIRED_IMPORTS` includes `openevolve` + the core dep surface (Rule 2).
- `_build_pool`, `_deploy_to_remote`, `_smoke_test_remote`, `_verify_remote_entrypoint` are all callable (Rules 2, 3).

Future orchestrators should carry equivalent assertions for their host sets.

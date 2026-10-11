# Serverless-safe parquet read for the preflight canary (+ serverless-parity testing) — Design

## 1. Problem

R6 (run `755550720578960`, `preflight_tracking_marts --full --providers idsse`, wheel 0.5.121) **FAILED at 102 s**. The timeout fix worked (102 s vs the prior > 1240 s), but the fixture Probe C could not read its own bundled parquet:

```
RuntimeError: tracking-marts drain canary FAILED (fixture correctness):
Unable to find a usable engine; tried using: 'pyarrow', 'fastparquet'.
 - Pandas requires version '10.0.1' or newer of 'pyarrow' (version '8.0.0' currently installed).
At score_fixture_marts -> build_inputs -> ParquetXtSource.grid() -> pd.read_parquet
```

**Root cause (confirmed):** `analytics/action_context/local/parquet_sources.py` reads fixtures with `pd.read_parquet`. Installed pandas (≥2.1) refuses `read_parquet` when pyarrow < 10.0.1. The Databricks **serverless** `analytics` environment runs **pyarrow 8.0.0** — the base-runtime version; pyarrow is only a *transitive* dep (not pinned in `[project].dependencies` nor the env spec; the only explicit pins are the `training` extra `pyarrow>=14` and `uv.lock` 19.0.1), and the serverless base runtime's arrow is **not** lifted (it is coupled to Spark). So on serverless, `pd.read_parquet` has no usable engine.

**Why this recurred (the real failure class):** the `parquet_sources.py` adapters are the AC-1 *local* hexagon — only ever run locally / in CI under pyarrow ≥ 10 (uv.lock 19.0.1). The fixture Probe C is the FIRST path to drive them **on serverless**. Every fix in this chain (selector → aggregate → fixture) was validated against uv.lock's environment and failed on the serverless runtime. The old real-unit Probe C read bronze via **Spark**, never `pd.read_parquet` — which is why the class never bit before. **The failure class is `local-env ≠ serverless-env`; the gold-standard cure is to test against serverless's actual constraints, not uv.lock's.**

## 2. Design

### 2.1 Engine-agnostic read primitive, single-sourced (R1/R2)
Replace `pd.read_parquet(path)` with `pyarrow.parquet.read_table(path).to_pandas()` in the SHARED `analytics/action_context/local/parquet_sources.py` — one helper `_read_parquet(path)` used by **all FIVE** `pd.read_parquet` call sites: `ParquetFrameSource` reads TWICE (`:49` tracking + `:57` sb360), plus Actions `:65`, Xt `:82`, Metadata `:98`. The sb360 `:57` branch is swapped too (uniform adapter safety — not cherry-picked to the idsse canary path, which only hits `:49`/`:82`/`:65`/`:98`; a serverless statsbomb/sb360 read through this adapter would otherwise stay broken and untested). pyarrow 8 **reads** parquet fine; only pandas' `read_parquet` wrapper gates on the pyarrow version. In-repo precedent: `scoutgpt_training.py:276` already uses `pq.read_table`. Single-sourced so the canary, `_oracle`, and `run_action_context_local` all become serverless-safe at once — **the class is eliminated repo-wide, and the read path is identical in test and on serverless** (the split read path is exactly what hid this).

**Dtype-neutrality is a hard constraint.** `read_table().to_pandas()` MUST yield the SAME DataFrame (numpy-backed columns, same dtypes/values) as today's `pd.read_parquet` — both use the pyarrow engine and default to numpy-backed arrays (NOT `dtype_backend="pyarrow"` / ArrowExtensionArray). Proven by: (a) a read-equivalence test (§2.4 R-EQ), and (b) the existing `_oracle` transport-neutrality + mini-golden suites staying byte-identical (they read via these adapters). If any column comes back Arrow-backed, pin `to_pandas(types_mapper=None)`.

### 2.2 Do NOT lift serverless pyarrow (R3)
We deliberately keep serverless on pyarrow 8.0.0. Pinning pyarrow ≥ 10 into the `analytics` environment is higher-risk (arrow is coupled to the Spark runtime; serverless may reject or silently ignore the override, and it widens the env-pin blast radius). The engine-agnostic read removes the need. Rejected alternatives: `fastparquet` (new dep, different dtype handling → golden drift); `engine="pyarrow"` arg (pandas gates on the version regardless of the engine arg).

### 2.3 Audit ALL `pd.read_parquet` for serverless-reachability (R-AUDIT)
Per the sibling-audit discipline, enumerate **MECHANICALLY** (grep `pd\.read_parquet` across `src/` — do NOT trust a seed list; the first seed already undercounted 4-vs-5 and omitted a site) and classify each by the compute it runs on. Sites: `parquet_sources.py` (×5 — FIXED here, incl sb360 `:57`), `player_embeddings_v2.py` (×2), `skillcorner_events.py` (BytesIO), `tracking_frame_spill.py:147` (`read_built_frames`). For each non-fixed site, determine whether it can run on a serverless `environment_key` (vs HF Jobs / classic compute with modern pyarrow): `tracking_frame_spill.py:147` is likely safe (prod Stage-2 spill reads via Spark `built_frame_struct_type`; `:147` is the local/test read) but MUST be confirmed + recorded, not skipped. Any serverless-reachable site is folded into this fix (same primitive); a non-serverless site is recorded with its compute + why it is safe (surfaced, not silently deferred) and added to the R-GATE allowlist (§2.6).

### 2.4 Testing — the layer that ends fix-after-fix
- **R-GUARD (primary, env-independent):** a test that monkeypatches `pandas.read_parquet` to RAISE, then runs `score_fixture_marts()` (and each Source adapter) and asserts SUCCESS. Non-vacuous — it FAILS on the current `pd.read_parquet` code (watch-it-fail-first) and passes after the swap. Proves the preflight read path has **no dependency on pandas' parquet engine**, and fails CI if one is reintroduced. Catches the whole class without needing serverless.
- **R-EQ (neutrality):** on pyarrow ≥ 10 (local/CI), assert `_read_parquet(path)` returns a frame equal (`assert_frame_equal`, dtypes included) to `pd.read_parquet(path)` for a fixture file — proves the swap changes nothing where both engines work (so goldens cannot shift).
- **R-PARITY (serverless parity, the gap-closer):** extend `docker/pyspark-test/` with a leg pinned to **`pyarrow==8.0.0`** (serverless's version) that runs the fixture Probe C END-TO-END (read + all six scorers) — catching not just the read but any scorer-level pyarrow-8 incompatibility, PRE-merge. Standing requirement for canary/preflight changes.
- Existing `_oracle` transport-neutrality + AC mini-golden suites MUST stay byte-identical (they exercise the adapters).

### 2.5 Document the constraint (R-DOC)
Add an AGENTS.md invariant + a `docs/context` note: **serverless runs the base-runtime package versions (pyarrow 8.0.0), NOT uv.lock's — any code reachable from a serverless task must do parquet IO through the engine-agnostic primitive, never `pd.read_parquet`.** With its pointer, per the anti-bloat gate.

### 2.6 Mechanical enforcement gate (R-GATE) — the durable class-killer
Prose (R-DOC) + a path-local guard (R-GUARD) are necessary but NOT sufficient: the hand-audit already leaked 2 sites (4-vs-5 + the spill read), so a hand discipline will leak again. The repo enforces exactly this kind of invariant MECHANICALLY — `test_topandas_boundedness.py` (allowlisted `.toPandas()`), the `resolve_hf_token` AST gate. Add the same: a CI test that scans `src/` for `pd.read_parquet` (AST or line match) and FAILS unless every occurrence is in a justified ALLOWLIST (the confirmed non-serverless sites from R-AUDIT, each with a one-line reason — mirroring `_topandas_exemptions.yml`). After the fix, `parquet_sources.py` has ZERO `pd.read_parquet` (all → `_read_parquet`); the allowlist holds only the genuinely non-serverless readers. New code adding `pd.read_parquet` fails CI until it either uses the primitive or is justified on the allowlist — so "no pandas parquet engine on a serverless path" is non-regressable, not a convention.

## 3. Requirements

- **R1** Canary/adapters read parquet via `pyarrow.parquet.read_table().to_pandas()` (engine-agnostic; pyarrow 8 + 19).
- **R2** Single-sourced `_read_parquet` in `parquet_sources.py`; ALL FIVE call sites (incl FrameSource sb360 `:57`) + the canary reuse it; dtype-neutral (numpy-backed, byte-identical frames).
- **R3** serverless pyarrow NOT lifted; fix is engine-agnostic.
- **R-AUDIT** every `src/` parquet read enumerated MECHANICALLY (grep, not a seed) + classified by compute; serverless-reachable ones folded in; others recorded + allowlisted.
- **R-GATE** a CI test forbids `pd.read_parquet` in `src/` outside a justified allowlist (repo precedent: `test_topandas_boundedness` / `resolve_hf_token` AST). The non-regressable class-killer.
- **R-GUARD** monkeypatch-`pd.read_parquet`-raises → canary succeeds (fails on current code).
- **R-EQ** pyarrow-direct read == `pd.read_parquet` (frame-equal) where both work.
- **R-PARITY** Docker leg pinned `pyarrow==8.0.0` runs the fixture Probe C end-to-end.
- **R-DOC** AGENTS.md invariant + context note.
- **R7 (final proof, post-merge, operator)** re-run `preflight_tracking_marts --full --providers idsse` → SUCCESS, ≤ 600 s. Only then does the combined re-materialize resume at GATE 0.

## 4. Tests
Per §2.4 + §2.6: R-GUARD (env-independent class guard), R-EQ (neutrality at pyarrow ≥10), R-PARITY (Docker pyarrow-8 e2e, incl non-empty bar), R-GATE (mechanical `pd.read_parquet` ban + allowlist), plus the unchanged `_oracle`/mini-golden suites, and full `uv run pytest`. (R-EQ's byte-for-byte neutrality is proven only where pandas' engine works, pyarrow ≥10; under pyarrow 8 R-PARITY proves liveness at the non-empty bar. Accepted — the PyPI `pyarrow==8.0.0` parity pin is a proxy for the serverless build.)

## 5. Scope / commit / approval
- Branch `fix/serverless-safe-canary-parquet-read` off `origin/main` (= `a1b973d2`, #609 MERGED — `tracking_marts_canary` + `canary_fixture/` present on main, verified; so `score_fixture_marts`, R-GUARD/R-PARITY targets, and the R7 preflight all exist on the base branch); one coherent commit; spec + plan bundled.
- `git commit` / `push` / `gh pr create` / `gh pr merge` each a SEPARATE explicit approval; no job run until post-merge CI green (R7 is then its own approval).
- Wheel bump 0.5.121 → **0.5.122** (`bump_wheel.py` + uv.lock root).

## 6. ADR
Amend **ADR-087** (or a short note): the fixture Probe C read is engine-agnostic (serverless pyarrow 8); record the `local-env ≠ serverless-env` lesson + the serverless-parity test requirement.

## 7. Out of scope
- Lifting serverless pyarrow; Probe M; the drain; the combined re-materialize (resumes at GATE 0 after this ships + R7 PASS).

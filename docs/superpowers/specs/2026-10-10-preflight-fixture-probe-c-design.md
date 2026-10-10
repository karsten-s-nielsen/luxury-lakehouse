# Preflight Probe C on a BUNDLED FIXTURE unit — Design (ADR-087 amendment follow-up 2)

## 1. Problem

Even after the bounded canary (0.5.119) + the size-signals single-pass (0.5.120), the `--full` preflight STILL times out at 1200 s. The R5 measurement (2026-10-10, run `144485168052616`, `--providers idsse`) TIMED OUT at ~1240 s, and the driver stack trace PINS the cost:

```
tracking_marts_drain.py main_tracking_marts_preflight → run_canary(...)
canary.py run_canary → processor.process(u, dry_run=True)         # Probe C
tracking_marts_processor.py:300 process → int(scored_sdf.count())  # Stage-2 scoring, blocked ~14 min
```

**Probe C — a full two-stage score of ONE real idsse unit — is the wall (~one drain unit; pitch-control/defcon dominate).** Every tracking provider's "smallest" unit is still a dense half (idsse 1.73 M, skillcorner 665 K, GS 2.4 M; metrica excluded), so Probe C on ANY real unit ≈ dense-unit cost and cannot fit the 1200 s planning budget. A sub-unit slice is not an option (scorers are whole-unit — the global action→frame link; a 1% slice leaves ~99 % of actions unlinked → spurious failure or vacuous pass).

## 2. Design — Probe C on a bundled, COMPLETE, tiny fixture unit (owner decision 2026-10-10)

Replace the real-unit Probe C with a **bundled tiny COMPLETE fixture unit** scored through the full scorer set. A complete small unit (few frames + MATCHING few actions, self-consistent) exercises every scorer's real code path (schema, wiring, the `gk_decision is_actor` class) in seconds, corpus-independent. Trade-off (accepted): it drops live per-provider DATA-shape coverage — the runtime circuit-breaker (ADR-087) is the backstop for a live data defect a static fixture cannot surface.

### 2.1 Reuse the existing oracle infrastructure, lifted into the wheel
`src/tests/tracking_marts/_oracle.py` ALREADY builds a per-provider fixture unit (`build_inputs`, which wraps `analytics.action_context.unit_inputs.build_unit_inputs` via `analytics.action_context.local.parquet_sources`) and runs all six scorers (`score_all_marts`) — proven present + deterministic for every provider (`test_transport_oracle`). But `_oracle.py` + the fixtures live under `src/tests/` (NOT shipped in the wheel). Lift the reusable build+score into a WHEEL module **`ingestion.tracking_marts_canary`** — NOT `analytics`: the six scorer cores are `ingestion.*_writer`, and `analytics ⇏ ingestion` (import-linter; `canary.py`'s own docstring), so the module that both builds AND scores must live in `ingestion` (ingestion → analytics is allowed). Pure-pandas, no Spark. `_oracle.py` imports it (single source, no drift).

### 2.2 Ship ONE DEDICATED public-tier canary fixture in the wheel
Bundle ONE complete idsse canary fixture unit (frames + actions + meta + the xT grid) as wheel package-data (idsse = public access-tier; avoids shipping restricted skillcorner/GS data in the distributed wheel per ADR-064 / [no-rm-data]). It is a **DEDICATED, single-consumer** fixture (curated from `J03WMX_p1` idsse data to clear R-N's all-6-non-empty gate) living only at the wheel package path — its one copy IS the wheel package-data; nothing else reads it, so there is no duplicate to drift (SPEC-05 / PLAN-03). It is NOT a copy of any `src/tests/` fixture, and it does NOT replace them (PFC-SPEC-03): `_oracle.py` keeps its OWN multi-provider `src/tests/` transport fixtures (skillcorner/idsse/gradientsports/metrica, parametrized by `test_transport_oracle`/`test_build_spill`) — only the build+score CODE is single-sourced (§2.1), the DATA is not shared. The AC full-golden `J03WMX_p1` fixture is likewise untouched. Package path included by the wheel build (hatchling `force-include` / package-data).

### 2.3 Probe C = PURE-CORE fixture smoke (not the Spark dispatch)
Probe C runs the six **pure scorer cores** on the bundled fixture (the `_oracle.score_all_marts` path), asserting each mart runs AND produces **≥ 1 row** (§R3 — non-empty is the bar, not merely non-crash). Pure cores, no Spark — seconds. Rationale: the canary's purpose is a SYSTEMIC code/schema defect (the `gk_decision is_actor` crash hit every unit); the pure cores catch that. The Spark DISPATCH (mapInPandas + StructTypes) is covered elsewhere and is NOT re-smoked here: Probe M already runs a REAL Spark Stage-1 build on live data, and CI's `test_build_spill` / the Docker tests cover the dispatch. (Running the full Spark dispatch on the fixture was considered + rejected: it adds Spark overhead for coverage the two already provide.)

**What the fixture does NOT cover (SPEC-04, accurate):** a fixture is one idsse unit, so Probe C no longer exercises (i) live per-provider DATA shape, NOR (ii) the per-provider BUILD/convert path for the non-fixture providers (skillcorner/GS/metrica). (ii) is covered by the per-provider ingest/parse CI tests + `test_transport_oracle` (all providers) + Probe M's real build; (i) by the runtime circuit-breaker. Owner-accepted.

### 2.4 Probe M UNCHANGED
Probe M stays the REAL global-densest-window Stage-1 build-fit (the ADR-089 memory check — a fixture is too small to stress memory). It still needs `compute_tracking_size_signals`'s window counts to find the global densest window.

### 2.5 `compute_tracking_size_signals` loses R1 (unit counts)
With Probe C no longer selecting a smallest real unit, the per-unit R1 `unit_counts` are unused for tracking-marts. Drop them; keep ONLY `window_counts` (Probe M). Simplifies the aggregate to one `assign_core_window_ids` → `groupBy(_window_id).count()`.

### 2.6 AC drain — DECIDED (a), owner-ratified 2026-10-10
**(a) tracking-marts ONLY.** AC keeps its real smallest-unit Probe C (AC was never observed to time out; minimal scope). AC is a follow-up only if it ever times out. `run_canary` stays generic; the tracking-marts preflight calls the fixture Probe C, the AC preflight keeps `select_canary_probes` smallest-unit.

## 3. Requirements

- **R1** Lift the fixture build+score into the wheel module **`ingestion.tracking_marts_canary`** (NOT `analytics` — the module calls the `ingestion.*_writer` scorer cores and `analytics ⇏ ingestion`; ingestion → analytics is allowed); import-linter clean; `_oracle.py` imports it (single source).
- **R2** Ship ONE idsse complete fixture unit as wheel package-data (single-sourced per §2.2); the wheel build includes it; read via `importlib.resources`, NOT from `src/tests/`.
- **R3 (NON-EMPTY is the BAR, FIXTURE-SPEC-02)** Probe C asserts each of the 6 marts is produced AND has **≥ 1 row**. Non-crash alone is NOT sufficient — a no-op mart (0 rows, valid schema) would let a scorer defect ship past the preflight (the vacuous-green the canary exists to prevent). A mart that is legitimately empty on the fixture is a fixture-curation failure (R-N), not an accepted pass.
- **R4** `run_canary`: for tracking-marts, Probe C is the fixture smoke; Probe M unchanged. AC per §2.6 (a) — unchanged.
- **R5** `compute_tracking_size_signals` returns ONLY `window_counts` (drop `unit_counts`); callers updated.
- **R-N (non-vacuity, HARD gate + red-green)** the bundled fixture MUST yield ≥ 1 row for EVERY one of the 6 marts (verified at impl, T1.2 — a hard gate, not "prefer"): start from `J03WMX_p1` (full, 97 actions); if any mart no-ops, CURATE the fixture (add the needed shot/keeper/players/possession) until all 6 are non-empty. Plus red-green: a seeded raising scorer core makes Probe C RAISE.
- **R6 (measurement, post-merge, operator)** re-run `preflight_tracking_marts --full --providers idsse`; now = aggregate + Probe M (real window build) + fixture Probe C (seconds) → expect **≤ 600 s**. If still over, the residual is the aggregate/Probe-M build → raise `timeout_seconds` (data-gated).

## 4. Tests

- Wheel-module unit test: the lifted `canary_fixture` build+score runs all 6 marts on the bundled fixture (present + deterministic), mirroring `_oracle` — but reading the WHEEL-bundled fixture, proving the package-data path works offline.
- Red-green: a monkeypatched raising scorer → Probe C raises (non-vacuous).
- `_oracle.py` still green (now importing the lifted module — single-source parity).
- Wheel-packaging test: the fixture file resolves via `importlib.resources` from the installed package (not a `src/tests` path) — proves it ships.
- Full `uv run pytest` + the existing canary/preflight/tracking-marts suites green.

## 5. Scope / commit / approval

- One branch `fix/preflight-fixture-probe-c` off `origin/main` (`7606ffa0`); one coherent commit; no worktrees/micro-commits; spec + plan bundled.
- `git commit`/`push`/`gh pr create`/`gh pr merge` each a SEPARATE explicit approval; no job run until post-merge CI green (R6 is then its own approval).
- Wheel bump 0.5.120 → **0.5.121** (`bump_wheel.py`).

## 6. ADR

Append to **ADR-087**: Probe C on a real unit ≈ one drain unit (R5 stack-trace evidence, run `144485168052616`); replaced with a bundled complete-fixture pure-core smoke (code/schema coverage; circuit-breaker backs live data). Probe M unchanged. Record the §2.6 AC decision.

## 7. Out of scope

- Probe M, `--providers` scope, the enqueue, the drain. The combined re-materialize (resumes at GATE 0 after this ships + R6 PASS).
- (If §2.6 = a) the AC drain Probe C.

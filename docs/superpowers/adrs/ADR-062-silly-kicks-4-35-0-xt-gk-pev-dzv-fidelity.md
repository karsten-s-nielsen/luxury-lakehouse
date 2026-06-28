# ADR-062: silly-kicks 4.35.0 adoption — xT-GK PEV/DZV fidelity fix (re-materialize fct_action_context)

| Field | Value |
|---|---|
| **Date** | 2026-06-27 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen, Claude |

> **Amendment (2026-06-27, same day):** PR #409 originally shipped this adoption WITHOUT a wheel
> bump, on the mistaken reasoning that "the math is upstream-only, so no lakehouse code changed."
> That was wrong: the sentinel dance edits `exec_visibility._REQUIRED_SK_MIN`, which ships in the
> wheel and is the ADR-044 env-drift guard — leaving the wheel un-bumped deployed a stale `(4,34,0)`
> guard that would NOT fail-loud on a stale env. Per project discipline the lakehouse wheel is
> bumped on every change. Corrected here: wheel `0.5.52 → 0.5.53`; the Decision/Alternatives below
> reflect the wheel bump. No data impact (the cancelled-then-rerun recompute uses 4.35.0 either way).

## Context

silly-kicks 4.34.0 → 4.35.0 ships a single lakehouse-relevant change (upstream ADR-024
**amendment**, 2026-06-27): Jeffrey Eyestone — author of the xT-GK framework adopted in
[ADR-048](ADR-048-silly-kicks-4-22-0-xt-gk-adoption.md) — answered three open fidelity questions
(Q1–Q3), and two terms of the composite were re-derived to match his published method:

- **PEV (Q1+Q2):** the progressive-evaluation term now measures its forward gain on the
  **GK-revalued surface** `V_GK = xT ⊙ φ(z,d)` (convolved like `xT*`, `σ=0.8`), not raw `xT*`.
  Raw xT flatlines in keeper zones, so PEV was structurally ~0; revaluing the surface is the
  point. `PEV = ρ · max(0, V_GK*(z′) − V_GK*(z))` (rectified form unchanged). RAV remains the
  sole owner of the destination — no double-count.
- **DZV (Q3, Option A):** the defensive-zone term now uses Eyestone's published revaluation
  multiplier `M(z) = φ(z,d) · (1 − V_GK(z)/max V_GK)`, applied as the increment it confers on
  the origin possession value: `DZV = (M − 1) · V_GK(z)`, gated to the defensive third. This
  replaces the old additive `(v_def − xT_raw(z))` floor and is scale-reconciled to ~0.01/action
  (Jeff's ~0.009 La Liga anchor).
- **φ(z,d)** `= α·(1 − d/D_max)^(−β)` for `d < D_threshold`, else 1 (`α=2.1`, `β=0.8` canonical;
  `D_max=105`, `D_threshold=35` provisional). φ enters value via **PEV and DZV only** — `base`
  (`−xT*(z)`) and RAV (`xT*(z′)`) stay on raw `xT*`. Option B (origin-subtraction negative
  centering) is **unchanged**.

No completion-model, geometry, or provider-variant change. The composite shape is unchanged:
`xt_gk = T·(base + γ·PEV + RAV) + φ_scalar·DZV`.

## Decision

Adopt 4.35.0 everywhere (pyproject `[spadl]` floor + `uv.lock` + terraform `==` pins +
`submit_ac1_oneshot.py` mirror + the seven `_REQUIRED_SK_MIN` constants + the orchestrator
sentinel) per [ADR-046](ADR-046-serverless-env-exact-pins.md) lockstep, **bump the lakehouse wheel
`0.5.52 → 0.5.53`** (`bump_wheel.py` propagates to all consumers; CI rebuilds + redeploys on merge),
and **re-materialize the `xt_gk_*` columns of `fct_action_context`** for all tracking-backed matches.

**Wheel bumped 0.5.52 → 0.5.53.** The xT-GK *math* lives entirely in `compute_xt_gk` (silly-kicks),
delivered via the serverless `analytics` env's `==4.35.0` pin — so the AC enrichment call site
(`analytics.action_context.enrich._enrich_tracking_match` → `XtGkParams.for_philosophy(preset)`) is
unchanged (`XtGkParams.v_def`, removed upstream, was never passed; the new `dzv_alpha/beta/d_max`
carry canonical defaults) and the **AC schema is unchanged**. But the PR DOES change lakehouse code
that ships in the wheel — `exec_visibility._REQUIRED_SK_MIN=(4,35,0)`, the [ADR-044](ADR-044-executor-env-drift-guard.md)
executor env-drift guard. That floor only protects the recompute if it is actually **deployed**, so
the wheel MUST be rebuilt + redeployed (CI build + HF-Hub upload + `deploy_wheel.py` on merge; the
serverless env then references the new wheel filename). Per project discipline the lakehouse wheel
version is bumped on **every** change regardless — same-version redeploy is unreliable for cached
serverless envs. The recompute runs AFTER the new wheel (and the `==4.35.0` env) are deployed.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Pin-only bump, defer recompute | trivial PR | stale `xt_gk_pev/dzv/composite` values in the live mart contradict the adopted method | the values ARE the deliverable for the Eyestone report |
| B. Amend ADR-048 in place instead of a new ADR | one xT-GK record | buries a value-changing re-materialization inside a closed adoption record | each value-changing silly-kicks adoption gets its own ADR (050/052/055/056/057/058 precedent) |
| C. Pin-bump but skip the wheel rebuild | smaller PR | the `(4,35,0)` guard floor never deploys → the env-drift guard silently accepts a stale 4.34.0 env; plus same-version-redeploy cache staleness | the wheel is the deployed source of truth; the guard must ship to protect anything (this was the initial mistake — corrected in the amendment) |
| D. Pin-bump + wheel bump 0.5.53 + recompute (chosen) | guard floor actually deploys; disciplined cache-bust; one schema-stable re-materialization | CI wheel build/deploy + `terraform apply` must roll before the recompute | — |

## Consequences

### Positive
- `xt_gk_pev` lights up for short deep build-out (PEV measured on the revalued surface instead of
  a flatlined raw xT), and `xt_gk_dzv` follows Eyestone's published multiplier — both now match the
  framework the WC2022 cohort / Eyestone report is built against.

### Negative
- `xt_gk_pev`, `xt_gk_dzv`, and the `xt_gk` composite (incl. all five preset columns) **change
  value** in `fct_action_context` → a full tracking-provider AC re-materialization is mandatory
  before the report is re-run. `xt_gk_base`, `xt_gk_rav`, `xt_gk_pressure` are byte-identical.
- `terraform apply` (serverless `analytics` env) must roll **before** the recompute, else the AC
  job runs on 4.34.0 and the executor env-drift guard ([ADR-044](ADR-044-executor-env-drift-guard.md),
  `_REQUIRED_SK_MIN=(4,35,0)`) fails the run loud.

### Neutral
- `D_threshold=35`/`D_max=105` are upstream-provisional; a future re-tune is another value change
  + re-materialization (same posture as the ADR-048 preset values).
- Post-run, report DZV by-zone profile + PEV action-type pattern + the d=35 φ discontinuity to
  Eyestone per his explicit "confirm post-run" ask (handoff §Post-run verification).
- Forward (not this release): computing PEV on `V_GK` is the first half of Eyestone's
  receiver-pressure extension (a future `q` term).

## Related
- **ADRs:** [ADR-048](ADR-048-silly-kicks-4-22-0-xt-gk-adoption.md) (xT-GK adoption this refines),
  [ADR-046](ADR-046-serverless-env-exact-pins.md) (pin lockstep),
  [ADR-044](ADR-044-executor-env-drift-guard.md) (`_REQUIRED_SK_MIN`),
  [ADR-016](ADR-016-spadl-enrichment-stage-canonical-naming.md) (AC enrichment home),
  [ADR-013](ADR-013-ml-inference-outputs-dbt-mart.md) (global xT grid = xT-GK baseline)
- **Upstream:** silly-kicks ADR-024 amendment (2026-06-27, PR #140, tag `v4.35.0`); CHANGELOG 4.35.0
- **Migrations:** none — AC schema unchanged (value-only re-materialization)

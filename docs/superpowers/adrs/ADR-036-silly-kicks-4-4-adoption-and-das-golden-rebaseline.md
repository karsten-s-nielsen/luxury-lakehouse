# ADR-036: silly-kicks 4.4.0 adoption + DAS golden re-baseline (offside carrier-forwarding correctness fix)

| Field | Value |
|---|---|
| **Date** | 2026-06-02 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

Adopting silly-kicks 4.4.0 (floor `>=4.4.0,<5`) surfaced — via the `AC1_E2E` golden e2e — that **DAS output (`das_team` / `das_opponent` / `das_diff`) changed in silly-kicks 4.2.0 and had been live on `main` unvalidated since PR #328**. The AC-1 golden (`tests/fixtures/action_context/idsse/J03WMX_p1/golden.parquet`) was frozen under silly-kicks 4.0 (PR #320) and never regenerated through 4.1.1 (#327) or 4.2.0 (#328).

**Isolation (airtight):** the e2e (`run_work_unit` over the committed IDSSE `J03WMX p1` fixture, 97 enriched actions, identical lakehouse code) passes the golden under silly-kicks `4.0.0` and `4.1.1`, and fails under `4.2.0` and `4.3.0`/`4.4.0` with identical deltas (`das_diff` maxΔ 260.7) — so the cause is purely silly-kicks 4.1.1→4.2.0; the lakehouse code is innocent; `ghost_gk_*` is unaffected on every version (`das_*` only).

**Root cause + correctness verdict (silly-kicks session, evidence-backed):** 4.2.0's DAS **offside carrier-forwarding** (`derive_team_in_possession` emits `ball_carrier_player_id`, forwarded to `add_das` so accessible-space's offside check exempts the passer). Pre-fix (4.1.1, `player_in_possession_col=None`), the on-ball carrier — tracked ~0.6 m ahead of the ball — was mis-flagged offside and deleted ("treated like air"), and his vacated central space was absorbed by a neighbour, inflating DAS to implausible values. The fix exempts the passer, collapsing DAS to sane values. Confirmed a **correctness fix, not a regression**: carrier team always == `team_in_possession` (0 mismatched frames), carrier is the nearest-to-ball most-advanced attacker, and the change is **rare** (~0.9% of frame-rows). The lakehouse spot-check reproduced it exactly: **1 of 97 actions** changed on the fixture — a **cross** (textbook offside-relevant action), `das_diff` 306.9 → 46.3 (implausible → sane). The 4.0 golden therefore encoded the **pre-fix bug**.

silly-kicks 4.2.0's CHANGELOG claimed the carrier-forwarding was "value-neutral (zero AS/DAS change) on real data" — that claim was **false**; the test that "confirmed" it used a no-offside fixture, so it never exercised the passer-exemption path. The value change was never flagged to downstream consumers (Hyrum's Law), and it hid because the `AC1_E2E` golden e2e is gated out of CI.

## Decision

Adopt silly-kicks **4.4.0** (floor `silly-kicks[das,ghost-gk]>=4.4.0,<5`) across pyproject `[spadl]`, the Terraform analytics env, `submit_ac1_oneshot`, the 6 trainer `_REQUIRED_SK_MIN=(4,4,0)`, and the orchestrator-invariants sentinel; wheel 0.5.8 → 0.5.9 via `bump_wheel.py`. **Re-baseline the AC-1 golden** to 4.4.0 output (the corrected DAS). 4.4.0's only runtime change vs 4.3.0 is kloppy `convert_to_actions` auto-deriving `game_id` (SkillCorner-ingestion path, not AC-1) and the TF-24 calibration-objective fix (calibration-only; `infer_ball_carrier` defaults unchanged at `tolerance_m=3.0`) — so 4.4.0 AC-1 enrichment output is identical to 4.2.0/4.3.0 (verified: `ghost_gk_*` maxΔ 0, only the 1 DAS cross moved). 4.3.0 is skipped (superseded same day).

## Alternatives considered

| Option | Why rejected |
|---|---|
| Keep the 4.0 golden, loosen DAS tolerance | Masks a real value change; the golden would no longer be a faithful reference. The pre-fix values are *wrong*. |
| Treat the DAS change as a regression / pin silly-kicks <4.2.0 | Disproven — it's a correctness fix (carrier correctly resolved; implausible→sane). Pinning would keep the offside bug. |
| Regenerate the golden without isolating the cause | Would have baked in an unexplained change. The version-bisection (4.0/4.1.1 pass, 4.2.0+ fail) + spot-check (1 cross, implausible→sane) prove what and why first. |
| Adopt 4.3.0 | Superseded by 4.4.0 the same day; 4.4.0 is value-identical for AC-1. |

## Consequences

### Positive
- AC-1 DAS now reflects the corrected offside handling (on-ball carrier no longer deleted). The golden is a faithful 4.4.0 reference again; the e2e + `test_differential` pass.
- The latent `main`-vs-golden DAS inconsistency (live since #328) is closed, with the cause documented rather than papered over.

### Negative / risk
- **The DAS value change is now canonical.** Any downstream consumer of `das_team/das_opponent/das_diff` sees the corrected values on a `4.1.x → 4.2.0+` upgrade. AC-1 is not yet published to HF, so blast radius is internal; record here for downstream (Hyrum's Law).
- ADR-035's "value-equivalent" framing applied to ghost-GK only; DAS was not value-equivalent across 4.1.1→4.2.0. This ADR is the correction.

### Neutral / follow-up
- **CI gap:** the `AC1_E2E` golden e2e is gated out of CI, which is why this rode `main` uncaught. Follow-up: a lighter golden gate that runs in CI so a value shift on a tracking-enrichment column cannot hide again.
- **Process rule:** regenerate the golden whenever a silly-kicks bump touches a tracking enrichment, and run `AC1_E2E=1` before merging such a bump (the #328 miss).
- The ghost-GK perf wall (a full metrica game can't fit the 1800s serverless iteration budget) is unrelated to this ADR — tracked separately (FFT-KDE algorithmic lever).

## Related
- **ADRs:** extends `ADR-035` (silly-kicks 4.2.0 / vectorized ghost-GK — corrects its value-equivalence scope to ghost-GK-only), `ADR-029` (silly-kicks 4.0).
- **PRs:** silly-kicks 4.2.0 adoption #328 (where the DAS change rode in unvalidated); this PR (4.4.0 adoption + golden re-baseline).
- **Memory:** `project_ac1_das_changed_in_sk420`.
- **External:** silly-kicks 4.4.0 CHANGELOG (TF-24 carrier-objective fix; kloppy game_id auto-derive); silly-kicks session's DAS verdict (accessible-space `core.py:183-204` offside mechanism).

## Notes
Version-by-version DAS map (lakehouse e2e, IDSSE J03WMX p1): 4.0.0 PASS · 4.1.1 PASS · 4.2.0 FAIL · 4.3.0 FAIL · 4.4.0 FAIL-vs-old-golden / PASS-vs-rebaselined. ghost_gk_x/y maxΔ 0, ghost_gk_spread maxΔ 1.4e-12 (float noise) across the re-baseline. Reproduce: `AC1_E2E=1 uv run --with "silly-kicks==<ver>" pytest src/tests/action_context/test_e2e.py::test_e2e_reproduces_golden_and_is_dup_free`.

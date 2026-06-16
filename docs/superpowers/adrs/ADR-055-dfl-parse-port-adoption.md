# ADR-055: Adopt the silly-kicks DFL parse port (delete-and-depend) for IDSSE/Sportec

| Field | Value |
|---|---|
| **Date** | 2026-06-16 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

For IDSSE/Sportec (DFL XML), the lakehouse maintained its own hand-rolled, stdlib-`xml.etree`,
kloppy-free DFL parser (`ingestion.idsse._parse_positions_xml`/`_parse_events_xml`/`_parse_teams`/
`_parse_match_metadata` + helpers, ~885 lines), a bronze→native shaper
(`_bronze_idsse_to_sportec_input`, duplicated and drift-locked across `action_context/convert.py`
and `ingestion/tracking_context.py`), and an events adapter + direction derivers in
`ingestion/spadl_adapter.py`. The silly-kicks dev/calibration harness parsed the *same* DFL with
**kloppy** instead — a train/serve skew at the parser layer (ADR-031 / silly-kicks T3) that the
near-ball pitch-control degeneracy had been masking and that the kloppy-tracking-y inversion exposed.

silly-kicks 4.30.0 ships `silly_kicks.providers.sportec` (behind the `[parse-dfl]` extra) — a
**verbatim lift of the lakehouse's own parser+shaper**, pinned at lakehouse commit `0efac60`. It exists
so the harness and the lakehouse stop maintaining two divergent DFL parsers. At adoption time the
lakehouse `HEAD == 0efac60`, so the port is byte-identical to our committed parser; adopting it is an
**output-preserving refactor** (`bronze.idsse_*` unchanged → no recompute, no retrain).

The forcing function: with two parsers, any DFL-parser fix has to be made twice or the harness and
production drift; the kloppy-y incident is the concrete cost of that drift. The IDSSE parser is now
stable (silly-kicks finished its DFL work), so the release-coupling cost of depending on the port is
acceptable.

## Decision

Adopt the silly-kicks parse port for IDSSE/Sportec in **delete-and-depend** form: production ingest, the
action-context compute path, the tracking-context mart, and SPADL conversion call
`silly_kicks.providers.sportec` (`parse_dfl_*` / `shape_*_to_native` / `derive_idsse_home_team_start_left*`),
and the lakehouse's own DFL parser/shaper/adapter/derivers are **deleted**. Data-quality
(`_smooth_tracking`, `finalize_bronze_df`) stays consumer-side, and a committed **port-output golden**
(`test_dfl_parse_port_parity.py` + `fixtures/dfl_parse_port_golden/*`) is the live cross-repo contract
guarding the port at the pinned `0efac60` shape.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Don't adopt; keep our parser | Zero coupling; one-repo parser iteration | Permanent two-parser drift (the kloppy-y incident class); harness ≠ prod | Defeats the single-source goal T3 exists for |
| B. Keep-both + parity test (B-ii only) | No release-coupling; fast in-repo iteration | We still maintain ~1000 lines of duplicate parser forever | Carries the duplicate it set out to remove; only a stepping stone |
| C. kloppy-shim (parse via kloppy) | Smaller change | Leaves parse/smooth/velocity divergent; reintroduces the parser the lakehouse deliberately avoided | Moves the skew up a layer instead of removing it (ADR-031 §4.4) |
| D. **Delete-and-depend (chosen)** | One parser for harness + prod; ~1000 fewer lines to maintain; output-preserving | Release-coupling (parser fix ⇒ silly-kicks release); gives up the per-half ingest memory halving | — |

We sequenced through B (the parity test) first as a low-risk on-ramp and prerequisite, then deleted.
The parity test is the drift-eliminator under either model; deletion is what stops the dual maintenance.

## Consequences

### Positive

- **One DFL parser** for the silly-kicks calibration harness and lakehouse production — the train/serve
  parser skew that produced the kloppy-tracking-y inversion class is structurally gone.
- ~1000 lines of duplicate parser/shaper/adapter deleted from the lakehouse (incl. the drift-locked
  double-copy of `_bronze_idsse_to_sportec_input` and its `test_idsse_converter_no_drift` guard).
- Output-preserving at adoption (`HEAD == 0efac60`): three pre-swap equivalence probes (parse,
  ingest-bronze post-smooth, events-native) and the port-output golden all proved byte-identical →
  **no `bronze.idsse_*` change, no AC recompute, no retrain**.

### Negative

- **Release-coupling (C4 trade-off).** A future DFL-parser change now routes through a silly-kicks
  release → wheel bump → terraform `==` pin (ADR-046), instead of a one-repo lakehouse patch. Accepted
  because the IDSSE parser is stable.
- **Cross-repo bronze contract.** The port's `SportecTrackingBronze`/`SportecEventBronze` shapes are
  field-identical to `bronze.idsse_*`; evolving the bronze schema is now a coordinated change. Kept live
  only by the committed parity golden — if it goes RED, silly-kicks regressed the port at our pinned
  shape.
- **Per-half ingest memory.** `ingest_idsse` previously parsed+processed one half at a time to halve peak
  DataFrame memory. The port parses the whole positions XML at once, so peak memory is the full-match
  bronze (acceptable at IDSSE scale, ~282k rows × 30 cols, on the 16 GB driver). The per-period *write*
  loop is preserved.
- **Hard `[parse-dfl]` dependency.** What is an optional extra for general silly-kicks users is a
  required runtime dep for the lakehouse (pyproject + terraform serverless env, ADR-046 parity).

### Neutral

- The lakehouse keeps `_smooth_tracking`, `finalize_bronze_df`, the bronze-cols constants and their
  computation helpers, `_derive_velocities_savgol`, and the metrica/skillcorner/GS builders — only the
  IDSSE parse/shape/derive functions were lifted.
- The former cross-comparison parity test (lakehouse-parser == port) could not survive deletion (no
  lakehouse parser left to compare); it was converted to a port-output golden, regenerated with
  `CAPTURE_DFL_GOLDEN=1` only on an intentional, reviewed port revision.
- The Spark `ingest_idsse`/`ingest_idsse_events` wiring validates fully only on a live Databricks run;
  the parse + bronze equivalence is proven locally, the Spark write path is deploy-validated.

## Related

- **Specs:** `docs/superpowers/specs/2026-06-16-dfl-parse-port-design.md`
- **ADRs:** silly-kicks **ADR-031** (the upstream kloppy-tracking-y / parse-port decision, T3); lakehouse
  ADR-046 (serverless env exact pins), ADR-016 (SPADL enrichment stage), ADR-018 (cross-table format
  contracts), ADR-053 (AC frame orientation). Distinct from silly-kicks' own ADR numbering.
- **External references:** silly-kicks 4.30.0 `silly_kicks.providers.sportec` (`[parse-dfl]` extra).

## Notes

Adoption verification (all green, local): three byte-identical equivalence probes (parse-port bronze+native,
ingest post-smooth per-period, events-native shaper incl. plain-df input); mini-golden (IDSSE recompute
through `shape_tracking_to_native`); broad test batch; `ruff` + `pyright` (0 errors) across `src/`;
wheel-constants + terraform env-dep parity; zero dangling references to the deleted symbols. Delivered on
lakehouse wheel **0.5.42** with silly-kicks pinned `==4.30.0` + `[parse-dfl]`. NOTE: silly-kicks 4.31.0 is
a **breaking** column change (`pitch_control_at_ball` → `at_target`, ADR-032) and must not be pulled until
its AC+DEFCON migration — the lock + terraform pin hold at 4.30.0.

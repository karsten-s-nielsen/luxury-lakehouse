# ADR-NNN: <Title>

| Field | Value |
|---|---|
| **Date** | YYYY-MM-DD |
| **Status** | Proposed / Accepted / Deprecated / Superseded by ADR-MMM |
| **Deciders** | <names> |

## Context

What problem are we solving? What constraints apply? What is the forcing function? Keep this to 2–4 short paragraphs. Include concrete numbers where relevant (row counts, latency budgets, version locks, platform constraints).

## Decision

What did we decide? One or two sentences, no hedging. A future maintainer should be able to read this sentence in isolation and know what the decision was.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. <option> | <short list> | <short list> | <one line> |
| B. <option> | <short list> | <short list> | <one line> |
| C. <chosen> | <short list> | <short list> | — |

This section is the part future maintainers wonder about most. Be concrete about what you looked at and why you did not choose it. If option A was rejected because of a specific version constraint, name the version. If option B was rejected because of a benchmark, cite the benchmark.

## Consequences

### Positive

- What gets better or becomes possible.
- Concrete capabilities unlocked.

### Negative

- What gets worse, what debt we accept, what we lose.
- What future-us will need to maintain because of this choice.

### Neutral

- Side effects worth noting but not valenced.

## CLAUDE.md Amendment

Optional. Use this section only when the decision requires a documented exception to a project-wide rule in `CLAUDE.md` (e.g., an allowance of `exec()` in a specific module, a divergence from the security-hardening defaults, or a naming-convention exception). Quote the exact rule being amended and note the scope of the exception.

Example:
> CLAUDE.md "Security Hardening" prohibits `exec()`. This ADR carves out a scoped exception for `src/evolve/targets/*/evaluator.py` under the defense-in-depth policy documented below.

Omit this section if the decision does not require a CLAUDE.md amendment.

## Related

Include only the categories that apply to this decision. Omit categories that are not relevant.

- **Commits:** `<sha>`, `<sha>`
- **Specs:** `docs/superpowers/specs/YYYY-MM-DD-<name>-design.md`
- **Issues / PRs:** `#NNN`
- **ADRs:** supersedes `ADR-XXX`, superseded by `ADR-YYY`
- **External references:** links to library docs, platform release notes, incident postmortems

## Notes

Optional. Use this section for supporting evidence, benchmark output, experiment results, or anything else that does not fit the sections above but would help a maintainer understand the decision.

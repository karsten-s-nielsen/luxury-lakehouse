# ADR-001: Code Execution for Evolve Level 2

**Date:** 2026-04-07
**Status:** Proposed
**Design:** [Evolve Level 2 — Code Evolution](../specs/2026-04-07-evolve-level2-code-evolution-design.md)

## Context

CLAUDE.md prohibits `eval()`, `exec()`, and `pickle.loads()` as dangerous
builtins. Level 1 of the evolve engine complied by using `ast.literal_eval`
exclusively — LLM-generated programs contain only `config = {...}` dicts, never
executable code.

Level 2 requires executing LLM-generated PyTorch functions. The config-only
search space is exhausted: Stage 3 ran 114 iterations (early-stopped at
patience=40) producing 120 programs — all cross_attention hyperparameter
variants. Structural code changes (hybrid conditioning, novel attention patterns,
learned gating) cannot be expressed as config dicts. `exec()` is unavoidable.

## Decision

Allow `exec()` within the evolve engine under a defense-in-depth security policy
with two independent protection layers and an opt-in toggle.

### Layer 1 — AST Allowlist (Parse-Time)

A generic `code_validator.py` module validates LLM-generated code before any
execution. Whitelist approach — unlisted AST node types are rejected.

Parameterized by a target-provided `ValidationProfile` so different evolve
targets can define their own allowlists without modifying the validator.

Allowed: `torch.*`, `math.*`, allowlisted `self.*` attributes, arithmetic,
control flow, local variables, tensor indexing, comprehensions.

Rejected: imports, `eval`/`exec`/`compile`/`__import__`/`open`/`getattr`/
`setattr`, dunder attribute access, arbitrary attribute chains, f-strings.

### Layer 2 — Subprocess Isolation (Runtime)

Backend workers execute code with restricted globals:

```python
exec(source, {"torch": torch, "math": math, "__builtins__": {}})
```

`__builtins__: {}` strips all builtin functions from the execution namespace.
Remote backends (SSH, HF Jobs) add process-level isolation with timeouts and
kill-on-exit enforcement.

### Toggle — `code_evolution=False` Default

Level 2 is opt-in via `--code-evolution` CLI flag. When disabled (default),
programs containing `custom_embed` or `custom_layers` are rejected with a
warning. Level 1 runs never execute program files regardless of content.

### Scope

`exec()` is permitted ONLY in:

- `src/evolve/targets/scoutgpt/evaluator.py` — `train_and_evaluate()` when
  `program_path` is provided
- `src/evolve/remote_worker.py` — same code path on remote backends

It is NOT permitted anywhere else in the codebase. The CLAUDE.md rule remains
in force for all non-evolve code.

## Alternatives Considered

### RestrictedPython

Python sandboxing library that rewrites AST to inject guards. Rejected because
it fights PyTorch's dynamic dispatch: `__torch_function__` protocol, C extension
modules, and `torch.autograd` all rely on patterns RestrictedPython blocks or
rewrites incorrectly.

### Subprocess-Only (No AST Validation)

Rely solely on subprocess isolation (no network, restricted env, timeout) without
parse-time validation. Viable but single-layer — rejected in favor of
defense-in-depth.

### Declarative Layer DSL

A `layers = {...}` dict using string descriptors, parsed via `ast.literal_eval`
and instantiated by our code. Safe by construction but recreates the config-only
ceiling at a different level. The whole point of Level 2 is architectural
freedom. Rejected.

## Consequences

### Positive

- Unlocks structural code changes beyond the config search space ceiling.
- Defense-in-depth: two independent layers must both fail for unsafe code to
  execute.
- Opt-in toggle prevents accidental code execution in Level 1 runs.
- Generic `ValidationProfile` enables future evolve targets without modifying
  the validator.

### Negative

- `exec()` appears in the codebase, requiring a scoped CLAUDE.md exception.
- AST validator must be maintained as the torch API evolves (new submodules).
- Every new evolve target must provide a `ValidationProfile`.
- `local_cuda` backend has no subprocess boundary — AST validator is the sole
  protection. Acceptable for single-user research workstation; would need
  worker-side re-validation if the backend were exposed to untrusted users.

## CLAUDE.md Amendment

```markdown
> `exec()` is permitted in `src/evolve/` under the defense-in-depth policy
> documented in ADR-001: AST allowlist (parse-time) + restricted globals with
> `__builtins__: {}` (runtime) + subprocess isolation (backends). Gated by
> `code_evolution=True`. All other code must continue to avoid `exec()`/`eval()`.
```

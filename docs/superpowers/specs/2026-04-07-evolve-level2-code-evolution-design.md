# Evolve Level 2 — Code Evolution Design

**Date:** 2026-04-07
**Status:** Approved
**Depends on:** [Evolve Engine Design (Level 1)](2026-04-04-evolve-engine-design.md)
**ADR:** [ADR-001: Code Execution for Evolve Level 2](../adrs/ADR-001-evolve-code-execution.md)

---

## Problem

Level 1 (config-only evolution) has hit its ceiling. Stage 3 ran 114 iterations
(early-stopped at patience=40) producing 120 programs — all cross_attention
hyperparameter variants. Best result: combined_score 0.6622 (+18.8% over seed),
spearman_rho 0.5946 (+35.4%), 3.9M params (-41%). The config search space is
exhausted.

Structural code changes — hybrid conditioning, novel attention patterns, learned
gating — cannot be expressed as config dicts. Level 2 allows the LLM to generate
actual PyTorch functions that replace the model's conditioning mechanism.

## Solution Overview

Extend the evolve engine so that program files can include two optional functions
alongside the existing `config` dict:

1. **`custom_layers(hidden_dim) -> dict[str, nn.Module]`** — declares new
   `nn.Module` layers registered on the model before training.
2. **`custom_embed(self, ...) -> Tensor`** — replaces `ScoutGPTDecoder._embed()`
   via monkey-patch.

Security is defense-in-depth: an AST allowlist validates code at parse time
(belt), and backend workers execute code in isolated subprocesses with restricted
globals (suspenders). Level 2 is opt-in via `--code-evolution` CLI flag; the
default remains Level 1 behavior.

## Architecture: Approach C — Validator + Direct Exec on Backend

```
program.py
    |
    v
[Evaluator: ast.literal_eval(config)]  -- Level 1 path (unchanged)
    |
    v
[Evaluator: code_validator.validate(program, profile)]  -- Level 2 gate
    |
    v
[Backend.train(config, program_path)]  -- transport to worker
    |
    v
[Worker: exec(source, {"torch": torch, "math": math, "__builtins__": {}})]
    |
    v
[Worker: register custom_layers on model]
    |
    v
[Worker: monkey-patch model._embed = custom_embed]
    |
    v
[Worker: train_and_evaluate() -- training loop unchanged]
    |
    v
metrics (stdout JSON)
```

The evaluator validates (belt). The backend subprocess isolates (suspenders).
No new subprocess type. The existing `remote_worker.py` gains a `--program`
argument — the 80% sandbox claim from Level 1 design was exactly this insight.

### Alternatives Considered

- **Approach A (Monolithic Validator + Exec):** Single `code_validator.py` then
  `exec()` in the evaluator process. Rejected — if the AST validator has a gap,
  malicious code runs with full process privileges.
- **Approach B (Validator + Isolated Loader Subprocess):** Separate subprocess
  for `exec()` before dispatching to training backend. Rejected — marginal safety
  gain over Approach C (code execs in a subprocess either way) at the cost of a
  serialization boundary for `nn.Module` constructors.

## Program File Format

A Level 2 program is a superset of Level 1. Three top-level constructs, all
optional:

```python
# 1. Config dict (same as Level 1) -- always present
config = {
    "conditioning_type": "cross_attention",
    "hidden_dim": 192,
    "num_layers": 3,
    "num_heads": 6,
    "dropout": 0.15,
    "max_seq_len": 128,
    "spatial_mlp_dim": 64,
    "vaep_loss_weight": 0.32,
    "player_prediction_weight": 0.18,
    "learning_rate": 2e-4,
    "weight_decay": 0.01,
    "batch_size": 384,
}

# 2. Custom layers (Level 2) -- optional
def custom_layers(hidden_dim: int) -> dict[str, torch.nn.Module]:
    """Declare new nn.Module layers to register on the model."""
    return {
        "hybrid_gate": torch.nn.Sequential(
            torch.nn.Linear(hidden_dim, hidden_dim // 4),
            torch.nn.ReLU(),
            torch.nn.Linear(hidden_dim // 4, hidden_dim),
        ),
    }

# 3. Custom embed (Level 2) -- optional
def custom_embed(
    self,
    action_ids: torch.Tensor,
    start_x: torch.Tensor,
    start_y: torch.Tensor,
    end_x: torch.Tensor,
    end_y: torch.Tensor,
    result: torch.Tensor,
    time_delta: torch.Tensor,
    player_ids: torch.Tensor,
) -> torch.Tensor:
    """Replaces ScoutGPTDecoder._embed() for this candidate."""
    player_emb = self.player_embedding(player_ids)
    action_emb = (
        self.token_embedding(action_ids)
        + self.start_x_mlp(start_x.unsqueeze(-1))
        + self.start_y_mlp(start_y.unsqueeze(-1))
        + self.end_x_mlp(end_x.unsqueeze(-1))
        + self.end_y_mlp(end_y.unsqueeze(-1))
        + self.result_embedding(result)
        + self.time_delta_mlp(time_delta.unsqueeze(-1))
    )
    gate = torch.sigmoid(self.hybrid_gate(player_emb))
    attn_out, _ = self.player_cross_attn(
        query=action_emb, key=player_emb, value=player_emb
    )
    emb = gate * self.conditioning_norm(attn_out) + (1 - gate) * action_emb
    return self.embedding_dropout(emb)
```

### Rules

- `custom_embed` absent: Level 1 behavior (`conditioning_type` selects built-in).
- `custom_embed` present: `conditioning_type` in config is ignored; function is
  monkey-patched.
- `custom_layers` present: called with `hidden_dim` from config; returned dict
  entries registered on model via `register_module()`. Keys become valid `self`
  attributes for `custom_embed`.
- `custom_layers` without `custom_embed`: valid but pointless (warning logged).
- `custom_embed` without `custom_layers`: valid (recombines existing layers).
- `custom_layers` receives `hidden_dim` as its only argument.

### Validation Order

1. `ast.literal_eval` extracts `config` dict (existing Level 1 path).
2. AST-validate `custom_layers` if present; extract return dict keys as dynamic
   attribute names.
3. AST-validate `custom_embed` if present (using static + dynamic allowlist).
4. Any validation failure: candidate score 0, reason logged, backend never called.

## Security Model: AST Allowlist

New module: `src/evolve/code_validator.py`. Generic recursive `ast.NodeVisitor`
parameterized by a target-provided `ValidationProfile`.

### Allowed AST Nodes

| AST Node Type | Allowed | Examples |
|---|---|---|
| Calls | `torch.*`, `torch.nn.*`, `torch.nn.functional.*`, `math.*`, `self.<allowlisted>(...)` | `torch.sigmoid(x)`, `nn.Linear(64, 128)` |
| Attributes on `self` | Static tier (known model attrs) + dynamic tier (custom_layers keys) | `self.player_embedding`, `self.hybrid_gate` |
| Attributes on `torch`/`math` | Unrestricted within namespace | `torch.float32`, `math.pi` |
| Operations | All arithmetic, comparison, boolean, unary | `+`, `*`, `>`, `and`, `-x` |
| Control flow | `if`/`elif`/`else`, `for`, `while`, `return` | Loops for multi-head iteration |
| Variables | Local assignments, parameters, unpacking | `scale = torch.sigmoid(...)` |
| Literals | Numbers, strings, booleans, None, tuples, lists, dicts | `0.1`, `True`, `(3, 3)` |
| Subscripts/slices | Tensor indexing | `x[:, :, :hidden_dim]` |
| Comprehensions | List/dict/generator | `[f(x) for x in items]` |

### Rejected AST Nodes

| Category | Rejected | Reason |
|---|---|---|
| Imports | `Import`, `ImportFrom` | No module loading |
| Dangerous builtins | `eval`, `exec`, `compile`, `__import__`, `open`, `print`, `input`, `getattr`, `setattr`, `delattr`, `globals`, `locals`, `vars`, `dir`, `type`, `super` | Escape hatches |
| Dunder access | Any attribute starting with `__` | `__class__`, `__dict__`, `__bases__` |
| Arbitrary attribute chains | `foo.bar` where `foo` is not `self`, `torch`, `math`, or local | Prevents `os.system` etc. |
| String formatting | f-strings, `.format()` | Not needed, potential injection |

### ValidationProfile (Target-Provided)

```python
@dataclass(frozen=True)
class ValidationProfile:
    """Target-specific rules for the AST validator."""
    patch_method: str                    # "_embed"
    patch_signature: list[str]           # ["self", "action_ids", ...]
    return_shape: str                    # "(batch, seq_len, hidden_dim)"
    known_model_attrs: frozenset[str]    # static tier
    allowed_namespaces: frozenset[str]   # {"torch", "math"}
    layers_args: list[str]              # ["hidden_dim"]
    rejected_builtins: frozenset[str]   # {"eval", "exec", ...}
```

Each evolve target provides a `ValidationProfile`. The ScoutGPT profile defines:

```python
KNOWN_MODEL_ATTRS = frozenset({
    "player_embedding", "token_embedding", "result_embedding",
    "start_x_mlp", "start_y_mlp", "end_x_mlp", "end_y_mlp",
    "time_delta_mlp", "position_embedding", "embedding_dropout",
    "player_cross_attn", "conditioning_norm",
    "film_scale", "film_shift", "gate_linear",
})
```

The dynamic tier extends this set with keys from the `custom_layers()` return
dict, extracted via AST inspection of the return statement (no execution).

### Dispatch-Side Only

AST validation runs on the dispatch machine (evaluator process) before sending
the program to any backend. Invalid code never reaches a worker. Worker-side
re-validation is deferred until an untrusted backend (e.g., shared HF Jobs)
is added.

## Evaluator Bridge Changes

### New `Program` Dataclass

```python
@dataclass(frozen=True)
class Program:
    config: dict[str, Any]
    has_custom_embed: bool
    has_custom_layers: bool
    source_path: str
```

`_load_program(program_path)` replaces `_load_config_from_program()`. Extracts
config via `ast.literal_eval` (unchanged), detects function presence via AST
walk (no execution).

### ComputeBackend Protocol Extension

```python
class ComputeBackend(Protocol):
    def train(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
        program_path: str | None = None,  # new
    ) -> dict[str, float]: ...
```

## Backend Changes

### local_cuda

Passes `program_path` through to `train_and_evaluate()`. The target evaluator
handles exec + monkey-patch. No subprocess boundary — AST validator is the
protection layer. Acceptable for single-user research workstation.

### remote_ssh

Additionally scp's the `.py` file to remote. Adds `--program <remote_path>` to
`remote_worker.py` CLI. Worker handles exec in restricted globals + monkey-patch.
Subprocess isolation is the runtime backstop.

### hf_jobs (currently stub, fleshed out)

Job payload includes the `.py` file as uploaded artifact. PEP 723 worker script
receives program path, performs same restricted exec + monkey-patch. Same pattern
as remote_ssh with HF Jobs API transport. Validated with single-candidate test
run before use in evolution.

### BackendPool

No changes. Already forwards `**kwargs` — `program_path` flows through.

### Key Invariant

The target evaluator's `train_and_evaluate()` is the *only* code that touches
the program file at execution time. Backends are transport — they get the file
there and invoke the worker. They never exec the program themselves.

## Worker Exec Protocol

When `remote_worker.py` (or the target evaluator for local_cuda) receives a
program path:

```python
source = Path(program_path).read_text()
restricted_globals = {"torch": torch, "math": math, "__builtins__": {}}
exec(source, restricted_globals)

# Extract and register custom layers
if "custom_layers" in restricted_globals:
    layers_fn = restricted_globals["custom_layers"]
    layers = layers_fn(hidden_dim=config["hidden_dim"])
    for name, module in layers.items():
        model.register_module(name, module)

# Monkey-patch custom embed
if "custom_embed" in restricted_globals:
    import types
    model._embed = types.MethodType(restricted_globals["custom_embed"], model)
```

`__builtins__: {}` strips all builtin functions from the execution namespace.
Even if AST validation missed something, `open()`, `__import__()`, `print()`
etc. are unavailable at runtime.

## LLM Prompt Design

### New Prompt Directory

`src/evolve/targets/scoutgpt/prompts_l2/system_message.txt` — selected when
`code_evolution=True`. The existing Level 1 `prompts/system_message.txt` is
untouched.

### Prompt Structure

1. **Role framing** — expert deep learning researcher inventing novel
   conditioning mechanisms; may modify config AND write PyTorch code.
2. **Program format spec** — exact structure: `config`, optional
   `custom_layers`, optional `custom_embed`. Full signatures with types.
3. **Available `self` attributes table** — every attribute from
   `KNOWN_MODEL_ATTRS` with type and shape semantics.
4. **Allowed operations** — `torch.*`, `math.*`, arithmetic, control flow,
   local variables, tensor indexing. No imports, I/O, dunder, getattr/setattr.
5. **Hard rules:**
   - `custom_embed` must return `(batch, seq_len, hidden_dim)`
   - `custom_layers` must return `dict[str, nn.Module]`
   - New layers must use `hidden_dim` parameter (not hardcoded sizes)
   - Config-only candidates are valid
   - Keep `custom_embed` under 50 lines
6. **Seed examples** — 2-3 annotated examples at increasing complexity.
7. **Fitness reminder** — `0.7 * spearman_rho + 0.3 * top1_accuracy`.

## Config & CLI

### EvolveConfig Addition

```python
class EvolutionConfig(BaseModel):
    # ... existing fields ...
    code_evolution: bool = False  # Level 2 toggle
```

### CLI

```bash
uv run evolve-scoutgpt                      # Level 1 (default)
uv run evolve-scoutgpt --code-evolution      # Level 2
uv run evolve-scoutgpt --code-evolution --resume results/run_042
```

### Behavior by Toggle

- `code_evolution=False` (default): exact Level 1 behavior. Programs with
  `custom_embed` are rejected with a warning.
- `code_evolution=True`: AST validation enabled, Level 2 prompts selected,
  code candidates accepted.

### Seed Programs

One new seed: `seed_programs/hybrid_gated_attention.py` — demonstrates
`custom_layers` + `custom_embed` format. Existing 4 seeds remain as config-only
baselines in the population.

## Testing Strategy

### Layer 1 — AST Validator Unit Tests

`src/tests/test_code_validator.py` (~15-20 cases):

- Valid: config-only, `custom_embed` with existing attrs, `custom_layers` +
  `custom_embed`, complex multi-head reimplementation.
- Rejected: `import`, `__import__`, dunder access, `eval`/`exec`, `open`,
  unknown `self` attr, `getattr` escape, f-string injection, attribute chains
  outside allowlist, wrong signature, bad `custom_layers` return.

Fast — pure AST parsing, no GPU.

### Layer 2 — Integration Tests

`src/tests/test_evolve_level2.py`:

- Config-only through Level 2 evaluator (backward compat).
- `custom_embed`-only candidate (monkey-patch + training).
- `custom_embed` + `custom_layers` (registration + patch + training).
- Invalid program rejected before backend dispatch.
- Wrong return shape handled gracefully (score 0).
- Wrong `custom_layers` return type handled gracefully.

Small fixture dataset, 1 epoch, CPU.

### Layer 3 — Smoke Test (Manual)

~10 iteration evolution run with Level 2 prompts on local_cuda. Verify LLM
generates valid candidates, validator accepts/rejects correctly, mixed population
coexists, fitness scores are reasonable.

## CLAUDE.md Update

Scoped exception to the "no dangerous builtins" rule:

> `exec()` is permitted in `src/evolve/` under the defense-in-depth policy
> documented in [ADR-001](docs/superpowers/adrs/ADR-001-evolve-code-execution.md):
> AST allowlist (parse-time) + restricted globals with `__builtins__: {}`
> (runtime) + subprocess isolation (backends). Gated by `code_evolution=True`.
> All other code must continue to avoid `exec()`/`eval()`.

## File Inventory

| File | Action | Purpose |
|---|---|---|
| `src/evolve/code_validator.py` | New | Generic AST allowlist validator |
| `src/evolve/evaluator.py` | Modify | `_load_program()`, validation gate, `program_path` passthrough |
| `src/evolve/config.py` | Modify | `code_evolution: bool` field |
| `src/evolve/runner.py` | Modify | CLI flag, prompt directory selection |
| `src/evolve/remote_worker.py` | Modify | `--program` arg, restricted exec, monkey-patch |
| `src/evolve/backends/base.py` | Modify | `program_path` on Protocol |
| `src/evolve/backends/local_cuda.py` | Modify | Pass `program_path` to evaluator |
| `src/evolve/backends/remote_ssh.py` | Modify | scp program file, `--program` CLI arg |
| `src/evolve/backends/hf_jobs.py` | Modify | Upload program, worker receives path |
| `src/evolve/targets/scoutgpt/__init__.py` | Modify | Export `ValidationProfile` |
| `src/evolve/targets/scoutgpt/evaluator.py` | Modify | `program_path` param, exec + monkey-patch |
| `src/evolve/targets/scoutgpt/prompts_l2/system_message.txt` | New | Level 2 LLM prompt |
| `src/evolve/targets/scoutgpt/seed_programs/hybrid_gated_attention.py` | New | Level 2 seed example |
| `src/tests/test_code_validator.py` | New | AST validator unit tests |
| `src/tests/test_evolve_level2.py` | New | Integration tests |
| `CLAUDE.md` | Modify | Scoped exec exception |
| `docs/superpowers/adrs/ADR-001-evolve-code-execution.md` | New | Security ADR |

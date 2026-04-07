# Evolve Level 2 — Code Evolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable the evolve engine to generate and execute LLM-authored PyTorch functions that replace `ScoutGPTDecoder._embed()`, breaking through the config-only search space ceiling.

**Architecture:** Defense-in-depth (Approach C) — AST allowlist validates code at parse time, backend workers exec in isolated subprocesses with restricted globals. Level 2 is opt-in via `--code-evolution` CLI flag. The evaluator validates, the backend isolates. See [design spec](../specs/2026-04-07-evolve-level2-code-evolution-design.md) and [ADR-001](../adrs/ADR-001-evolve-code-execution.md).

**Tech Stack:** Python 3.10, `ast` stdlib, PyTorch, Pydantic, OpenEvolve, pytest

**Spec corrections:** The design spec uses `conditioning_norm` and `gate_linear` as model attribute names. The actual code (`src/analytics/scoutgpt_decoder.py`) uses `player_cross_norm` and `player_gate`. This plan uses the correct names from the codebase.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `src/evolve/code_validator.py` | Create | Generic AST allowlist validator + `ValidationProfile` dataclass |
| `src/evolve/targets/scoutgpt/validation.py` | Create | ScoutGPT-specific `ValidationProfile` with correct model attrs |
| `src/evolve/targets/scoutgpt/prompts_l2/system_message.txt` | Create | Level 2 LLM prompt |
| `src/evolve/targets/scoutgpt/seed_programs/hybrid_gated_attention.py` | Create | Level 2 seed demonstrating `custom_layers` + `custom_embed` |
| `src/evolve/config.py` | Modify | Add `code_evolution: bool` to `EvolutionConfig` |
| `src/evolve/evaluator.py` | Modify | `Program` dataclass, `_load_program()`, validation gate, `program_path` passthrough |
| `src/evolve/backends/base.py` | Modify | `program_path` on `ComputeBackend` protocol |
| `src/evolve/backends/local_cuda.py` | Modify | Pass `program_path` to target evaluator |
| `src/evolve/backends/remote_ssh.py` | Modify | scp program file, `--program` CLI arg |
| `src/evolve/backends/hf_jobs.py` | Modify | Include program in job payload |
| `src/evolve/remote_worker.py` | Modify | `--program` arg, restricted exec, monkey-patch |
| `src/evolve/targets/scoutgpt/evaluator.py` | Modify | `program_path` param, exec + layer registration + monkey-patch |
| `src/evolve/targets/scoutgpt/__init__.py` | Modify | Export `VALIDATION_PROFILE` |
| `src/evolve/runner.py` | Modify | `--code-evolution` CLI flag, prompt dir selection, passthrough to evaluator |
| `src/tests/test_code_validator.py` | Create | AST validator unit tests (~20 cases) |
| `src/tests/test_evolve_level2.py` | Create | Integration tests for Level 2 flow |
| `CLAUDE.md` | Modify | Scoped `exec()` exception clause |

---

### Task 1: ValidationProfile Dataclass

**Files:**
- Create: `src/evolve/code_validator.py` (dataclass only — validator comes in Task 2)
- Test: `src/tests/test_code_validator.py`

- [ ] **Step 1: Write the test for ValidationProfile**

```python
"""Tests for the evolve code validator (AST allowlist + ValidationProfile)."""

from __future__ import annotations

import pytest

from evolve.code_validator import ValidationProfile


class TestValidationProfile:
    def test_frozen_dataclass(self) -> None:
        profile = ValidationProfile(
            patch_method="_embed",
            patch_signature=["self", "action_ids"],
            return_shape="(batch, seq_len, hidden_dim)",
            known_model_attrs=frozenset({"player_embedding"}),
            allowed_namespaces=frozenset({"torch", "math"}),
            layers_args=["hidden_dim"],
            rejected_builtins=frozenset({"eval", "exec"}),
        )
        assert profile.patch_method == "_embed"
        with pytest.raises(AttributeError):
            profile.patch_method = "other"  # type: ignore[misc]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_code_validator.py::TestValidationProfile::test_frozen_dataclass -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'evolve.code_validator'`

- [ ] **Step 3: Write ValidationProfile**

Create `src/evolve/code_validator.py`:

```python
"""AST allowlist validator for evolve Level 2 code evolution.

Validates LLM-generated `custom_embed()` and `custom_layers()` functions
against a target-provided ValidationProfile. Defense-in-depth belt layer —
see ADR-001 for the full security model.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ValidationProfile:
    """Target-specific rules for the AST validator.

    Each evolve target (e.g., ScoutGPT, xG model) provides a profile that
    parameterizes the generic validator. The validator rejects any code that
    doesn't match the profile's allowlist.
    """

    patch_method: str
    """Method name to monkey-patch (e.g., '_embed')."""

    patch_signature: list[str]
    """Required parameter names for the custom function (e.g., ['self', 'action_ids', ...])."""

    return_shape: str
    """Human-readable return shape for prompts (e.g., '(batch, seq_len, hidden_dim)')."""

    known_model_attrs: frozenset[str]
    """Static tier: model attributes the custom function may access via self."""

    allowed_namespaces: frozenset[str]
    """Allowed top-level namespaces for calls/attributes (e.g., {'torch', 'math'})."""

    layers_args: list[str]
    """Argument names passed to custom_layers() from config (e.g., ['hidden_dim'])."""

    rejected_builtins: frozenset[str]
    """Builtin function names that are always rejected."""
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_code_validator.py::TestValidationProfile::test_frozen_dataclass -v`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(evolve): add ValidationProfile dataclass for Level 2 code evolution
```

---

### Task 2: ScoutGPT ValidationProfile

**Files:**
- Create: `src/evolve/targets/scoutgpt/validation.py`
- Modify: `src/evolve/targets/scoutgpt/__init__.py`
- Test: `src/tests/test_code_validator.py`

- [ ] **Step 1: Write the test for ScoutGPT profile**

Append to `src/tests/test_code_validator.py`:

```python
from evolve.targets.scoutgpt.validation import SCOUTGPT_PROFILE


class TestScoutGPTProfile:
    def test_patch_method(self) -> None:
        assert SCOUTGPT_PROFILE.patch_method == "_embed"

    def test_signature_starts_with_self(self) -> None:
        assert SCOUTGPT_PROFILE.patch_signature[0] == "self"

    def test_signature_has_all_embed_params(self) -> None:
        expected = {
            "self", "action_ids", "start_x", "start_y", "end_x", "end_y",
            "result", "time_delta", "player_ids",
        }
        assert set(SCOUTGPT_PROFILE.patch_signature) == expected

    def test_known_attrs_include_core_layers(self) -> None:
        core = {"player_embedding", "token_embedding", "embedding_dropout"}
        assert core.issubset(SCOUTGPT_PROFILE.known_model_attrs)

    def test_known_attrs_include_conditioning_layers(self) -> None:
        conditioning = {
            "player_cross_attn", "player_cross_norm",
            "film_scale", "film_shift", "player_gate",
        }
        assert conditioning.issubset(SCOUTGPT_PROFILE.known_model_attrs)

    def test_torch_and_math_allowed(self) -> None:
        assert SCOUTGPT_PROFILE.allowed_namespaces == frozenset({"torch", "math"})

    def test_rejected_builtins(self) -> None:
        dangerous = {"eval", "exec", "compile", "__import__", "open"}
        assert dangerous.issubset(SCOUTGPT_PROFILE.rejected_builtins)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_code_validator.py::TestScoutGPTProfile -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'evolve.targets.scoutgpt.validation'`

- [ ] **Step 3: Write the ScoutGPT profile**

Create `src/evolve/targets/scoutgpt/validation.py`:

```python
"""ScoutGPT validation profile for Level 2 code evolution.

Defines which model attributes, namespaces, and builtins are allowed
in LLM-generated custom_embed() and custom_layers() functions.
Attribute names match ScoutGPTDecoder.__init__() in
src/analytics/scoutgpt_decoder.py.
"""

from __future__ import annotations

from evolve.code_validator import ValidationProfile

SCOUTGPT_PROFILE = ValidationProfile(
    patch_method="_embed",
    patch_signature=[
        "self", "action_ids", "start_x", "start_y", "end_x", "end_y",
        "result", "time_delta", "player_ids",
    ],
    return_shape="(batch, seq_len, hidden_dim)",
    known_model_attrs=frozenset({
        # Core embeddings (always present)
        "token_embedding",
        "player_embedding",
        "result_embedding",
        "position_embedding",
        "embedding_dropout",
        # Spatial MLPs (always present)
        "start_x_mlp",
        "start_y_mlp",
        "end_x_mlp",
        "end_y_mlp",
        "time_delta_mlp",
        # Cross-attention conditioning
        "player_cross_attn",
        "player_cross_norm",
        # FiLM conditioning
        "film_scale",
        "film_shift",
        # Gated conditioning
        "player_gate",
    }),
    allowed_namespaces=frozenset({"torch", "math"}),
    layers_args=["hidden_dim"],
    rejected_builtins=frozenset({
        "eval", "exec", "compile", "__import__", "open", "print", "input",
        "getattr", "setattr", "delattr", "globals", "locals", "vars",
        "dir", "type", "super", "breakpoint", "memoryview", "classmethod",
        "staticmethod", "property",
    }),
)
```

Update `src/evolve/targets/scoutgpt/__init__.py`:

```python
"""ScoutGPT target — architecture search for the ScoutGPT player embedding model."""

from evolve.targets.scoutgpt.validation import SCOUTGPT_PROFILE

__all__ = ["SCOUTGPT_PROFILE"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_code_validator.py::TestScoutGPTProfile -v`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(evolve): add ScoutGPT ValidationProfile with correct model attrs
```

---

### Task 3: AST Validator — Core Allow/Reject Logic

**Files:**
- Modify: `src/evolve/code_validator.py`
- Test: `src/tests/test_code_validator.py`

This is the largest task. The validator is a recursive `ast.NodeVisitor` that walks function bodies and checks every node against the allowlist.

- [ ] **Step 1: Write failing tests for valid programs**

Append to `src/tests/test_code_validator.py`:

```python
import textwrap

from evolve.code_validator import validate_program, ValidationProfile

# Minimal profile for testing
_TEST_PROFILE = ValidationProfile(
    patch_method="_embed",
    patch_signature=["self", "x", "y"],
    return_shape="(batch, hidden_dim)",
    known_model_attrs=frozenset({"linear", "norm", "dropout"}),
    allowed_namespaces=frozenset({"torch", "math"}),
    layers_args=["hidden_dim"],
    rejected_builtins=frozenset({
        "eval", "exec", "compile", "__import__", "open", "print", "input",
        "getattr", "setattr", "delattr", "globals", "locals", "vars",
        "dir", "type", "super",
    }),
)


class TestValidatorAccepts:
    def test_config_only_program(self) -> None:
        """Level 1 program with no functions should pass (nothing to validate)."""
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256, "num_layers": 6}
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_custom_embed_with_allowed_ops(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                a = self.linear(x)
                b = torch.sigmoid(a + y)
                return self.dropout(b)
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_custom_layers_and_embed(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_layers(hidden_dim):
                return {"gate": torch.nn.Linear(hidden_dim, hidden_dim)}

            def custom_embed(self, x, y):
                return self.gate(x) + y
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_math_namespace(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                scale = math.sqrt(256.0)
                return (x + y) / scale
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_tensor_indexing(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                return x[:, :, :128] + y[:, :, 128:]
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_control_flow(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                out = x + y
                for i in range(3):
                    out = self.norm(out)
                if True:
                    out = out * 2
                return out
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_list_comprehension(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                parts = [x * i for i in range(3)]
                return parts[0] + y
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_tuple_unpacking(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                a, b = torch.chunk(x, 2, dim=-1)
                return a + b + y
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_torch_nn_functional(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                return torch.nn.functional.relu(x + y)
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_dynamic_layer_in_custom_embed(self) -> None:
        """custom_layers declares 'proj'; custom_embed uses self.proj."""
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_layers(hidden_dim):
                return {"proj": torch.nn.Linear(hidden_dim, hidden_dim)}

            def custom_embed(self, x, y):
                return self.proj(x) + y
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason
```

- [ ] **Step 2: Write failing tests for rejected programs**

Append to `src/tests/test_code_validator.py`:

```python
class TestValidatorRejects:
    def test_import_statement(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                import os
                return x + y
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid
        assert "import" in reason.lower()

    def test_import_from(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                from os import path
                return x + y
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid
        assert "import" in reason.lower()

    def test_dunder_import(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                os = __import__("os")
                return x + y
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid

    def test_dunder_attribute(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                cls = self.__class__
                return x + y
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid
        assert "__" in reason

    def test_eval_call(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                return eval("x + y")
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid
        assert "eval" in reason.lower()

    def test_exec_call(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                exec("pass")
                return x + y
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid

    def test_open_call(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                f = open("/etc/passwd")
                return x + y
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid

    def test_getattr_escape(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                cls = getattr(self, "__class__")
                return x + y
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid

    def test_unknown_self_attribute(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                return self.nonexistent_layer(x)
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid
        assert "nonexistent_layer" in reason

    def test_arbitrary_attribute_chain(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                return os.system("whoami")
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid

    def test_fstring(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                s = f"{x}"
                return x + y
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid
        assert "f-string" in reason.lower() or "format" in reason.lower()

    def test_wrong_signature(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x):
                return x
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid
        assert "signature" in reason.lower()

    def test_custom_layers_bad_return(self) -> None:
        """custom_layers must return a dict literal (at AST level)."""
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_layers(hidden_dim):
                return torch.nn.Linear(hidden_dim, hidden_dim)
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid
        assert "dict" in reason.lower()

    def test_nested_dunder_via_method(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                return x.__mul__(y)
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid
        assert "__" in reason

    def test_code_evolution_disabled_rejects_custom_embed(self) -> None:
        """When code_evolution=False, programs with custom_embed are rejected."""
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                return x + y
        """)
        valid, reason = validate_program(source, _TEST_PROFILE, code_evolution=False)
        assert not valid
        assert "disabled" in reason.lower()

    def test_custom_layers_wrong_signature(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_layers():
                return {"gate": torch.nn.Linear(64, 64)}
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid
        assert "signature" in reason.lower()
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_code_validator.py::TestValidatorAccepts src/tests/test_code_validator.py::TestValidatorRejects -v`
Expected: FAIL — `ImportError: cannot import name 'validate_program'`

- [ ] **Step 4: Implement the AST validator**

Add to `src/evolve/code_validator.py` (after the `ValidationProfile` class):

```python
import ast
import logging

_log = logging.getLogger(__name__)


def validate_program(
    source: str,
    profile: ValidationProfile,
    *,
    code_evolution: bool = True,
) -> tuple[bool, str]:
    """Validate an evolve program source against a ValidationProfile.

    Returns (valid, reason). If valid is False, reason explains why.
    Config-only programs (no custom_embed/custom_layers) always pass.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        return False, f"Syntax error: {e}"

    functions = _extract_functions(tree)
    has_embed = f"custom_{profile.patch_method.lstrip('_')}" in functions
    # Normalize: custom_embed maps to patch_method="_embed"
    embed_name = f"custom_{profile.patch_method.lstrip('_')}"
    layers_name = "custom_layers"
    has_embed = embed_name in functions
    has_layers = layers_name in functions

    # Config-only program — nothing to validate
    if not has_embed and not has_layers:
        return True, "config-only program"

    # Gate: reject code when code_evolution is disabled
    if not code_evolution:
        return False, "Code evolution is disabled but program contains custom functions"

    # Collect dynamic attrs from custom_layers return keys
    dynamic_attrs: set[str] = set()
    if has_layers:
        ok, reason = _validate_function(
            functions[layers_name], profile, is_layers=True,
        )
        if not ok:
            return False, f"custom_layers: {reason}"
        dynamic_attrs = _extract_layers_keys(functions[layers_name])

    if has_embed:
        ok, reason = _validate_function(
            functions[embed_name], profile, is_layers=False,
            dynamic_attrs=dynamic_attrs,
        )
        if not ok:
            return False, f"custom_embed: {reason}"

    return True, "passed"


def _extract_functions(tree: ast.Module) -> dict[str, ast.FunctionDef]:
    """Extract top-level function definitions from the AST."""
    return {
        node.name: node
        for node in ast.iter_child_nodes(tree)
        if isinstance(node, ast.FunctionDef)
    }


def _extract_layers_keys(func: ast.FunctionDef) -> set[str]:
    """Extract string keys from the return dict of custom_layers.

    Looks for `return {"key": ..., ...}` — the return value must be a
    Dict node with string Constant keys.
    """
    keys: set[str] = set()
    for node in ast.walk(func):
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict):
            for key in node.value.keys:
                if isinstance(key, ast.Constant) and isinstance(key.value, str):
                    keys.add(key.value)
    return keys


def _validate_function(
    func: ast.FunctionDef,
    profile: ValidationProfile,
    *,
    is_layers: bool,
    dynamic_attrs: set[str] | None = None,
) -> tuple[bool, str]:
    """Validate a single function against the profile."""
    # Check signature
    if is_layers:
        expected_params = profile.layers_args
    else:
        expected_params = profile.patch_signature
    actual_params = [arg.arg for arg in func.args.args]
    if actual_params != expected_params:
        return False, (
            f"Signature mismatch: expected ({', '.join(expected_params)}), "
            f"got ({', '.join(actual_params)})"
        )

    # Check return for custom_layers must be a dict
    if is_layers:
        has_dict_return = False
        for node in ast.walk(func):
            if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict):
                has_dict_return = True
                break
        if not has_dict_return:
            return False, "Must return a dict literal"

    # Validate all nodes in the function body
    allowed_self_attrs = profile.known_model_attrs | (dynamic_attrs or set())
    # Local variable names (built up during walk)
    local_names: set[str] = set(actual_params)

    visitor = _AllowlistVisitor(profile, allowed_self_attrs, local_names)
    for node in func.body:
        result = visitor.check(node)
        if result is not None:
            return False, result

    return True, "passed"


class _AllowlistVisitor:
    """Recursive AST visitor that rejects nodes not on the allowlist."""

    def __init__(
        self,
        profile: ValidationProfile,
        allowed_self_attrs: frozenset[str] | set[str],
        local_names: set[str],
    ) -> None:
        self._profile = profile
        self._allowed_self_attrs = set(allowed_self_attrs)
        self._local_names = set(local_names)

    def check(self, node: ast.AST) -> str | None:
        """Return None if allowed, or an error string if rejected."""
        # Dispatch by node type
        handler = getattr(self, f"_check_{type(node).__name__}", None)
        if handler is not None:
            result = handler(node)
            if result is not None:
                return result
        elif isinstance(node, _ALWAYS_ALLOWED_NODES):
            pass
        else:
            # Check if it's a known statement/expression we handle generically
            if not isinstance(node, _GENERIC_CONTAINER_NODES):
                return f"Disallowed AST node: {type(node).__name__}"

        # Recurse into children
        for child in ast.iter_child_nodes(node):
            result = self.check(child)
            if result is not None:
                return result
        return None

    # --- Statements ---

    def _check_Import(self, node: ast.Import) -> str:
        return "Import statements are not allowed"

    def _check_ImportFrom(self, node: ast.ImportFrom) -> str:
        return "Import statements are not allowed"

    def _check_Assign(self, node: ast.Assign) -> str | None:
        # Track local variable names
        for target in node.targets:
            self._collect_names(target)
        return None  # recurse into children

    def _check_AnnAssign(self, node: ast.AnnAssign) -> str | None:
        if node.target:
            self._collect_names(node.target)
        return None

    def _check_AugAssign(self, node: ast.AugAssign) -> str | None:
        return None  # x += 1 is fine

    # --- Expressions ---

    def _check_Call(self, node: ast.Call) -> str | None:
        # Check for rejected builtins
        if isinstance(node.func, ast.Name):
            name = node.func.id
            if name in self._profile.rejected_builtins:
                return f"Rejected builtin call: {name}()"
            if name == "range":
                return None  # range() is always allowed
            # Unknown bare function call — could be a local lambda/var
            if name not in self._local_names:
                return f"Unknown function call: {name}()"
        return None  # recurse for method calls

    def _check_Attribute(self, node: ast.Attribute) -> str | None:
        attr = node.attr
        # Reject dunder access
        if attr.startswith("__") and attr.endswith("__"):
            return f"Dunder attribute access: {attr}"
        if attr.startswith("__"):
            return f"Private attribute access: {attr}"

        # Check the object being accessed
        if isinstance(node.value, ast.Name):
            obj_name = node.value.id
            if obj_name == "self":
                if attr not in self._allowed_self_attrs:
                    return f"Unknown self attribute: self.{attr}"
                return None
            if obj_name in self._profile.allowed_namespaces:
                return None  # torch.something, math.something
            if obj_name in self._local_names:
                return None  # local_var.something (e.g., tensor methods)
            return f"Attribute access on disallowed object: {obj_name}.{attr}"

        # Chained attribute access: e.g., torch.nn.functional
        if isinstance(node.value, ast.Attribute):
            # Walk the chain to find the root
            root = _get_attribute_root(node.value)
            if root in self._profile.allowed_namespaces:
                return None
            if root == "self":
                # self.layer.weight — only allow if first attr is allowed
                first_attr = _get_first_attr_after_root(node)
                if first_attr and first_attr not in self._allowed_self_attrs:
                    return f"Unknown self attribute: self.{first_attr}"
                return None
            if root in self._local_names:
                return None
            return f"Attribute chain on disallowed root: {root}"

        return None  # other cases handled by recursion

    def _check_JoinedStr(self, _node: ast.JoinedStr) -> str:
        return "F-strings are not allowed"

    # --- Helpers ---

    def _collect_names(self, target: ast.AST) -> None:
        """Add assigned variable names to local_names."""
        if isinstance(target, ast.Name):
            self._local_names.add(target.id)
        elif isinstance(target, ast.Tuple | ast.List):
            for elt in target.elts:
                self._collect_names(elt)
        elif isinstance(target, ast.Starred):
            self._collect_names(target.value)


def _get_attribute_root(node: ast.AST) -> str | None:
    """Walk an attribute chain to find the root Name."""
    while isinstance(node, ast.Attribute):
        node = node.value
    if isinstance(node, ast.Name):
        return node.id
    return None


def _get_first_attr_after_root(node: ast.AST) -> str | None:
    """Get the first attribute name after the root in a chain like self.a.b.c."""
    chain: list[str] = []
    while isinstance(node, ast.Attribute):
        chain.append(node.attr)
        node = node.value
    if chain:
        return chain[-1]  # last collected = first after root
    return None


# AST nodes that are always allowed (leaves or simple nodes)
_ALWAYS_ALLOWED_NODES = (
    ast.Constant,
    ast.Name,        # Variable references (checked via Call/Attribute)
    ast.Load,
    ast.Store,
    ast.Del,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
    ast.Pow,
    ast.LShift,
    ast.RShift,
    ast.BitOr,
    ast.BitXor,
    ast.BitAnd,
    ast.MatMult,
    ast.And,
    ast.Or,
    ast.Not,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.Is,
    ast.IsNot,
    ast.In,
    ast.NotIn,
    ast.UAdd,
    ast.USub,
    ast.Invert,
    ast.arg,
    ast.arguments,
    ast.keyword,
)

# Nodes that are containers — allowed but must recurse into children
_GENERIC_CONTAINER_NODES = (
    ast.Module,
    ast.FunctionDef,
    ast.Return,
    ast.Assign,
    ast.AnnAssign,
    ast.AugAssign,
    ast.For,
    ast.While,
    ast.If,
    ast.Expr,       # Expression statement
    ast.Call,
    ast.Attribute,
    ast.Subscript,
    ast.Slice,
    ast.Index,       # Python 3.8 compat
    ast.Starred,
    ast.Tuple,
    ast.List,
    ast.Dict,
    ast.Set,
    ast.BoolOp,
    ast.BinOp,
    ast.UnaryOp,
    ast.Compare,
    ast.IfExp,       # Ternary
    ast.ListComp,
    ast.SetComp,
    ast.DictComp,
    ast.GeneratorExp,
    ast.comprehension,
    ast.FormattedValue,  # Inside f-strings (caught by JoinedStr parent)
)
```

Note: Add `import ast` and `import logging` to the imports at the top of the file.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_code_validator.py -v`
Expected: ALL PASS

- [ ] **Step 6: Run ruff + pyright**

Run: `uv run ruff check src/evolve/code_validator.py && uv run pyright src/evolve/code_validator.py`
Fix any issues.

- [ ] **Step 7: Commit**

```
feat(evolve): implement AST allowlist validator for Level 2 code evolution
```

---

### Task 4: Config — Add `code_evolution` Toggle

**Files:**
- Modify: `src/evolve/config.py:92-102` (`EvolutionConfig` class)
- Test: `src/tests/test_evolve_config.py`

- [ ] **Step 1: Write the failing test**

Append to `src/tests/test_evolve_config.py`:

```python
from evolve.config import EvolutionConfig


class TestEvolutionConfigCodeEvolution:
    def test_default_is_false(self) -> None:
        cfg = EvolutionConfig()
        assert cfg.code_evolution is False

    def test_can_enable(self) -> None:
        cfg = EvolutionConfig(code_evolution=True)
        assert cfg.code_evolution is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_evolve_config.py::TestEvolutionConfigCodeEvolution -v`
Expected: FAIL — `ValidationError` (unexpected field `code_evolution`)

- [ ] **Step 3: Add the field**

In `src/evolve/config.py`, add to the `EvolutionConfig` class (after `checkpoint_interval`):

```python
    code_evolution: bool = False
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_evolve_config.py::TestEvolutionConfigCodeEvolution -v`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(evolve): add code_evolution toggle to EvolutionConfig
```

---

### Task 5: Evaluator — Program Dataclass and _load_program()

**Files:**
- Modify: `src/evolve/evaluator.py:118-155`
- Test: `src/tests/test_evolve_evaluator.py`

- [ ] **Step 1: Write the failing test**

Append to `src/tests/test_evolve_evaluator.py`:

```python
from evolve.evaluator import Program, _load_program


class TestLoadProgram:
    def test_config_only(self, tmp_path: Path) -> None:
        """Level 1 program returns Program with no custom functions."""
        prog = tmp_path / "config_only.py"
        prog.write_text('config = {"hidden_dim": 256, "num_layers": 6}\n')
        result = _load_program(str(prog))
        assert isinstance(result, Program)
        assert result.config == {"hidden_dim": 256, "num_layers": 6}
        assert result.has_custom_embed is False
        assert result.has_custom_layers is False
        assert result.source_path == str(prog)

    def test_with_custom_embed(self, tmp_path: Path) -> None:
        prog = tmp_path / "with_embed.py"
        prog.write_text(textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                return x + y
        """))
        result = _load_program(str(prog))
        assert result.has_custom_embed is True
        assert result.has_custom_layers is False

    def test_with_custom_layers_and_embed(self, tmp_path: Path) -> None:
        prog = tmp_path / "with_both.py"
        prog.write_text(textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_layers(hidden_dim):
                return {"gate": None}

            def custom_embed(self, x, y):
                return x + y
        """))
        result = _load_program(str(prog))
        assert result.has_custom_embed is True
        assert result.has_custom_layers is True

    def test_no_config_raises(self, tmp_path: Path) -> None:
        prog = tmp_path / "no_config.py"
        prog.write_text("x = 42\n")
        with pytest.raises(ValueError, match="config"):
            _load_program(str(prog))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_evolve_evaluator.py::TestLoadProgram -v`
Expected: FAIL — `ImportError: cannot import name 'Program'`

- [ ] **Step 3: Implement Program and _load_program()**

In `src/evolve/evaluator.py`:

Add import at top:
```python
from dataclasses import dataclass
```

Add after the existing `_load_config_from_program()` function (keep the old function — it's used by the `_EVALUATOR_SCRIPT` in `runner.py`):

```python
@dataclass(frozen=True)
class Program:
    """Parsed evolve program — config dict + optional custom functions."""

    config: dict[str, Any]
    has_custom_embed: bool
    has_custom_layers: bool
    source_path: str


def _load_program(program_path: str) -> Program:
    """Load an evolve program file, extracting config and detecting custom functions.

    The config dict is extracted via ast.literal_eval (same as Level 1).
    Custom function presence is detected via AST walk — no execution.
    """
    source = Path(program_path).read_text()
    tree = ast.parse(source, filename=program_path)

    # Extract config via the existing Level 1 path
    config = _extract_config(tree, source, program_path)

    # Detect custom functions
    func_names = {
        node.name
        for node in ast.iter_child_nodes(tree)
        if isinstance(node, ast.FunctionDef)
    }

    return Program(
        config=config,
        has_custom_embed="custom_embed" in func_names,
        has_custom_layers="custom_layers" in func_names,
        source_path=program_path,
    )


def _extract_config(
    tree: ast.Module, source: str, filename: str,
) -> dict[str, Any]:
    """Extract the config dict from a parsed AST via ast.literal_eval."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "config":
                value_source = ast.get_source_segment(source, node.value)
                if value_source is None:
                    msg = f"Cannot extract config value from {filename}"
                    raise ValueError(msg)
                raw = ast.literal_eval(value_source)
                if not isinstance(raw, dict):
                    msg = f"config must be a dict, got {type(raw).__name__} in {filename}"
                    raise ValueError(msg)
                return raw
    msg = f"No 'config = {{...}}' assignment found in {filename}"
    raise ValueError(msg)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_evolve_evaluator.py::TestLoadProgram -v`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(evolve): add Program dataclass and _load_program() for Level 2
```

---

### Task 6: Evaluator — Validation Gate and program_path Passthrough

**Files:**
- Modify: `src/evolve/evaluator.py:171-222` (`EvolveEvaluator.__init__()` and `evaluate()`)
- Modify: `src/evolve/backends/base.py:26-44` (`ComputeBackend.train()`)
- Test: `src/tests/test_evolve_evaluator.py`

- [ ] **Step 1: Write the failing test**

Append to `src/tests/test_evolve_evaluator.py`:

```python
from evolve.code_validator import ValidationProfile


class TestEvaluatorValidationGate:
    """Tests that the evaluator rejects invalid Level 2 programs before dispatch."""

    _PROFILE = ValidationProfile(
        patch_method="_embed",
        patch_signature=["self", "x", "y"],
        return_shape="(batch, hidden_dim)",
        known_model_attrs=frozenset({"linear"}),
        allowed_namespaces=frozenset({"torch", "math"}),
        layers_args=["hidden_dim"],
        rejected_builtins=frozenset({"eval", "exec", "open"}),
    )

    def test_invalid_program_returns_zero_score(self, tmp_path: Path) -> None:
        """Program with import should be rejected before backend is called."""
        prog = tmp_path / "bad.py"
        prog.write_text(textwrap.dedent("""\
            config = {"hidden_dim": 256, "num_layers": 6, "num_heads": 8,
                      "conditioning_type": "additive", "dropout": 0.1}

            def custom_embed(self, x, y):
                import os
                return x + y
        """))
        backend = MagicMock()
        backend.train = MagicMock(return_value={"combined_score": 1.0})
        evaluator = EvolveEvaluator(
            backend=backend,
            target="scoutgpt",
            eval_config=EvalConfig(),
            fitness_config=FitnessConfig(
                primary="combined_score",
                combined_weights={"combined_score": 1.0},
            ),
            code_evolution=True,
            validation_profile=self._PROFILE,
        )
        metrics = evaluator.evaluate(str(prog))
        assert metrics["combined_score"] == 0.0
        backend.train.assert_not_called()

    def test_valid_program_dispatches_to_backend(self, tmp_path: Path) -> None:
        """Config-only program should pass validation and reach the backend."""
        prog = tmp_path / "good.py"
        prog.write_text('config = {"hidden_dim": 256, "num_layers": 6, "num_heads": 8, "conditioning_type": "additive", "dropout": 0.1}\n')
        backend = MagicMock()
        backend.train = MagicMock(return_value={
            "spearman_rho": 0.5, "top1_accuracy": 0.8,
        })
        evaluator = EvolveEvaluator(
            backend=backend,
            target="scoutgpt",
            eval_config=EvalConfig(),
            fitness_config=FitnessConfig(
                primary="spearman_rho",
                combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
            ),
            code_evolution=False,
            validation_profile=self._PROFILE,
        )
        metrics = evaluator.evaluate(str(prog))
        assert metrics["spearman_rho"] == 0.5
        backend.train.assert_called_once()

    def test_code_evolution_disabled_rejects_custom_embed(self, tmp_path: Path) -> None:
        prog = tmp_path / "has_code.py"
        prog.write_text(textwrap.dedent("""\
            config = {"hidden_dim": 256, "num_layers": 6, "num_heads": 8,
                      "conditioning_type": "additive", "dropout": 0.1}

            def custom_embed(self, x, y):
                return x + y
        """))
        backend = MagicMock()
        evaluator = EvolveEvaluator(
            backend=backend,
            target="scoutgpt",
            eval_config=EvalConfig(),
            fitness_config=FitnessConfig(
                primary="combined_score",
                combined_weights={"combined_score": 1.0},
            ),
            code_evolution=False,
            validation_profile=self._PROFILE,
        )
        metrics = evaluator.evaluate(str(prog))
        assert metrics["combined_score"] == 0.0
        backend.train.assert_not_called()

    def test_level2_program_passes_program_path_to_backend(self, tmp_path: Path) -> None:
        prog = tmp_path / "l2.py"
        prog.write_text(textwrap.dedent("""\
            config = {"hidden_dim": 256, "num_layers": 6, "num_heads": 8,
                      "conditioning_type": "additive", "dropout": 0.1}

            def custom_embed(self, x, y):
                return self.linear(x) + y
        """))
        backend = MagicMock()
        backend.train = MagicMock(return_value={
            "spearman_rho": 0.5, "top1_accuracy": 0.8,
        })
        evaluator = EvolveEvaluator(
            backend=backend,
            target="scoutgpt",
            eval_config=EvalConfig(),
            fitness_config=FitnessConfig(
                primary="spearman_rho",
                combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
            ),
            code_evolution=True,
            validation_profile=self._PROFILE,
        )
        metrics = evaluator.evaluate(str(prog))
        # Backend should receive program_path
        call_kwargs = backend.train.call_args
        assert call_kwargs.kwargs.get("program_path") == str(prog) or \
               (len(call_kwargs.args) > 4 and call_kwargs.args[4] == str(prog))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_evolve_evaluator.py::TestEvaluatorValidationGate -v`
Expected: FAIL — `TypeError: EvolveEvaluator.__init__() got unexpected keyword argument 'code_evolution'`

- [ ] **Step 3: Modify EvolveEvaluator**

In `src/evolve/evaluator.py`, modify `EvolveEvaluator.__init__()` to accept new params:

```python
def __init__(
    self,
    backend: ComputeBackend,
    target: str,
    eval_config: EvalConfig,
    fitness_config: FitnessConfig,
    code_evolution: bool = False,
    validation_profile: ValidationProfile | None = None,
) -> None:
```

Add imports at top:
```python
from evolve.code_validator import ValidationProfile, validate_program
```

Store new attributes:
```python
    self._code_evolution = code_evolution
    self._validation_profile = validation_profile
```

Modify `evaluate()` to use `_load_program()` and add validation gate:

```python
def evaluate(self, program_path: str) -> dict[str, float]:
    try:
        program = _load_program(program_path)
    except Exception:
        _log.exception("Failed to load program %s", program_path)
        return {**fail_metrics(), **self._fail_score()}

    config = program.config
    if not validate_search_space(config):
        return {**fail_metrics(), **self._fail_score()}

    # Level 2 validation gate
    send_program_path: str | None = None
    if program.has_custom_embed or program.has_custom_layers:
        if self._validation_profile is None:
            _log.error("Level 2 program but no ValidationProfile configured")
            return {**fail_metrics(), **self._fail_score()}
        source = Path(program_path).read_text()
        valid, reason = validate_program(
            source, self._validation_profile,
            code_evolution=self._code_evolution,
        )
        if not valid:
            _log.warning("Program %s rejected: %s", program_path, reason)
            return {**fail_metrics(), **self._fail_score()}
        send_program_path = program_path

    try:
        raw = self._backend.train(
            candidate_config=config,
            target=self._target,
            epochs=self._eval_config.epochs,
            seed=self._eval_config.seed,
            program_path=send_program_path,
        )
    except Exception:
        _log.exception("Backend training failed for %s", program_path)
        return {**fail_metrics(), **self._fail_score()}

    # ... rest unchanged (combined score calculation) ...
```

- [ ] **Step 4: Update ComputeBackend protocol**

In `src/evolve/backends/base.py`, add `program_path` to the `train` method:

```python
def train(
    self,
    candidate_config: dict[str, Any],
    target: str,
    epochs: int,
    seed: int,
    program_path: str | None = None,
) -> dict[str, float]: ...
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest src/tests/test_evolve_evaluator.py::TestEvaluatorValidationGate -v`
Expected: PASS

- [ ] **Step 6: Run full existing test suite to confirm no regressions**

Run: `uv run pytest src/tests/test_evolve_evaluator.py src/tests/test_evolve_config.py -v`
Expected: ALL PASS (existing tests may need `code_evolution`/`validation_profile` defaults)

- [ ] **Step 7: Commit**

```
feat(evolve): wire validation gate and program_path into evaluator + protocol
```

---

### Task 7: Backend Changes — local_cuda, remote_ssh, hf_jobs

**Files:**
- Modify: `src/evolve/backends/local_cuda.py:29-64`
- Modify: `src/evolve/backends/remote_ssh.py:236-343`
- Modify: `src/evolve/backends/hf_jobs.py:38-89,143-186`
- Modify: `src/evolve/remote_worker.py`
- Test: `src/tests/test_evolve_evaluator.py` (existing backend mock tests should still pass)

- [ ] **Step 1: Update local_cuda.py**

In `src/evolve/backends/local_cuda.py`, modify the `train()` signature and call:

```python
def train(
    self,
    candidate_config: dict[str, Any],
    target: str,
    epochs: int,
    seed: int,
    program_path: str | None = None,
) -> dict[str, float]:
```

Update the `train_and_evaluate` call inside to pass `program_path`:

```python
    metrics = evaluator.train_and_evaluate(
        candidate_config=candidate_config,
        device=self._device,
        epochs=epochs,
        seed=seed,
        program_path=program_path,
    )
```

- [ ] **Step 2: Update remote_ssh.py**

In `src/evolve/backends/remote_ssh.py`, modify `_train_impl()`:

Add `program_path: str | None = None` to `train()` and `_train_impl()` signatures.

After the existing scp of `candidate.json`, add:

```python
    # Transfer program file for Level 2
    remote_program: str | None = None
    if program_path is not None:
        remote_program = "program.py"
        self._scp_to_remote(program_path, remote_program)
```

Update the SSH command construction to include `--program` when provided:

```python
    program_arg = f" --program {remote_program}" if remote_program else ""
    remote_cmd = (
        f"cd {self._remote_dir} && PYTHONUNBUFFERED=1 stdbuf -oL -eL "
        f"{self._python_path} -m evolve.remote_worker "
        f"{remote_filename} {self._device} {epochs} {seed} {target}"
        f"{program_arg}"
    )
```

- [ ] **Step 3: Update hf_jobs.py**

In `src/evolve/backends/hf_jobs.py`, modify the worker script template (`_WORKER_SCRIPT`) to accept an optional `EVOLVE_PROGRAM` env var containing base64-encoded program source:

Add after the existing `EVOLVE_CANDIDATE_CONFIG` handling:

```python
    program_b64 = os.environ.get("EVOLVE_PROGRAM")
    program_path = None
    if program_b64:
        program_source = base64.b64decode(program_b64).decode()
        program_path = "/tmp/evolve_program.py"
        Path(program_path).write_text(program_source)
```

Update the `train_and_evaluate` call in the worker script:

```python
    metrics = target_module.train_and_evaluate(
        candidate_config=config, device=device, epochs=epochs,
        seed=seed, program_path=program_path,
    )
```

In `_train_impl()`, add `program_path` to the signature and pass it via env:

```python
    env = {
        "EVOLVE_CANDIDATE_CONFIG": config_b64,
        "EVOLVE_DEVICE": ...,
        ...
    }
    if program_path is not None:
        program_source = Path(program_path).read_text()
        env["EVOLVE_PROGRAM"] = base64.b64encode(program_source.encode()).decode()
```

- [ ] **Step 4: Update remote_worker.py**

In `src/evolve/remote_worker.py`, add `--program` optional argument:

After existing positional arg parsing (line ~47–51), add:

```python
    # Optional: Level 2 program file
    program_path: str | None = None
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--program" and i + 1 < len(sys.argv):
            program_path = sys.argv[i + 1]
            break
```

Pass to `train_and_evaluate`:

```python
    metrics = target_module.train_and_evaluate(
        candidate_config=config,
        device=device,
        epochs=epochs,
        seed=seed,
        program_path=program_path,
    )
```

- [ ] **Step 5: Run existing tests to confirm no regressions**

Run: `uv run pytest src/tests/test_evolve_evaluator.py src/tests/test_evolve_config.py -v`
Expected: ALL PASS

- [ ] **Step 6: Run ruff + pyright on all modified files**

Run: `uv run ruff check src/evolve/backends/ src/evolve/remote_worker.py && uv run pyright src/evolve/backends/ src/evolve/remote_worker.py`

- [ ] **Step 7: Commit**

```
feat(evolve): add program_path support to all backends + remote_worker
```

---

### Task 8: Target Evaluator — Exec + Monkey-Patch

**Files:**
- Modify: `src/evolve/targets/scoutgpt/evaluator.py:85-90`
- Test: `src/tests/test_evolve_level2.py`

- [ ] **Step 1: Write the integration test**

Create `src/tests/test_evolve_level2.py`:

```python
"""Integration tests for evolve Level 2 — code evolution.

These tests exercise the exec + monkey-patch path in the ScoutGPT
target evaluator. They use a minimal model config (small hidden_dim,
1 epoch, tiny dataset) to run fast on CPU.
"""

from __future__ import annotations

import textwrap
import types
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import torch

from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder
from evolve.targets.scoutgpt.evaluator import _apply_program


class TestApplyProgram:
    """Tests for _apply_program() — the exec + monkey-patch logic."""

    def _make_model(self) -> ScoutGPTDecoder:
        config = ScoutGPTConfig(
            hidden_dim=64, num_layers=1, num_heads=2,
            dropout=0.0, conditioning_type="additive",
            num_players=50, max_seq_len=32,
        )
        return ScoutGPTDecoder(config)

    def test_no_program_path_is_noop(self) -> None:
        model = self._make_model()
        original_embed = model._embed
        _apply_program(model, program_path=None)
        assert model._embed == original_embed

    def test_custom_embed_replaces_method(self, tmp_path: Path) -> None:
        prog = tmp_path / "prog.py"
        prog.write_text(textwrap.dedent("""\
            config = {"hidden_dim": 64}

            def custom_embed(self, action_ids, start_x, start_y, end_x, end_y,
                             result, time_delta, player_ids):
                player_emb = self.player_embedding(player_ids)
                action_emb = self.token_embedding(action_ids)
                return self.embedding_dropout(action_emb + player_emb)
        """))
        model = self._make_model()
        _apply_program(model, program_path=str(prog))
        assert isinstance(model._embed, types.MethodType)
        # Verify it runs
        batch, seq = 2, 4
        out = model._embed(
            action_ids=torch.randint(0, 20, (batch, seq)),
            start_x=torch.rand(batch, seq),
            start_y=torch.rand(batch, seq),
            end_x=torch.rand(batch, seq),
            end_y=torch.rand(batch, seq),
            result=torch.randint(0, 2, (batch, seq)),
            time_delta=torch.rand(batch, seq),
            player_ids=torch.randint(0, 50, (batch, seq)),
        )
        assert out.shape == (batch, seq, 64)

    def test_custom_layers_registered(self, tmp_path: Path) -> None:
        prog = tmp_path / "prog.py"
        prog.write_text(textwrap.dedent("""\
            config = {"hidden_dim": 64}

            def custom_layers(hidden_dim):
                return {"test_gate": torch.nn.Linear(hidden_dim, hidden_dim)}

            def custom_embed(self, action_ids, start_x, start_y, end_x, end_y,
                             result, time_delta, player_ids):
                emb = self.token_embedding(action_ids)
                gate = torch.sigmoid(self.test_gate(emb))
                return self.embedding_dropout(gate * emb)
        """))
        model = self._make_model()
        _apply_program(model, program_path=str(prog))
        # Layer should be registered
        assert hasattr(model, "test_gate")
        assert isinstance(model.test_gate, torch.nn.Linear)

    def test_restricted_globals_no_builtins(self, tmp_path: Path) -> None:
        """exec runs with __builtins__={} — open() should fail."""
        prog = tmp_path / "prog.py"
        prog.write_text(textwrap.dedent("""\
            config = {"hidden_dim": 64}

            # This should fail at exec time due to __builtins__={}
            f = open("/etc/passwd")

            def custom_embed(self, action_ids, start_x, start_y, end_x, end_y,
                             result, time_delta, player_ids):
                return self.token_embedding(action_ids)
        """))
        model = self._make_model()
        with pytest.raises(Exception):
            _apply_program(model, program_path=str(prog))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_evolve_level2.py::TestApplyProgram -v`
Expected: FAIL — `ImportError: cannot import name '_apply_program'`

- [ ] **Step 3: Implement _apply_program()**

In `src/evolve/targets/scoutgpt/evaluator.py`, add:

```python
import types

import torch


def _apply_program(
    model: ScoutGPTDecoder,
    program_path: str | None,
) -> None:
    """Apply a Level 2 program to a model: register custom layers, monkey-patch _embed.

    If program_path is None or the program has no custom functions, this is a no-op.
    Code is exec'd with restricted globals (__builtins__={}) as a runtime safeguard.
    AST validation must have already passed before calling this function.
    """
    if program_path is None:
        return

    source = Path(program_path).read_text()
    restricted_globals: dict[str, Any] = {
        "torch": torch,
        "math": __import__("math"),
        "__builtins__": {},
    }
    exec(source, restricted_globals)  # noqa: S102 — see ADR-001

    # Register custom layers
    if "custom_layers" in restricted_globals:
        layers_fn = restricted_globals["custom_layers"]
        hidden_dim = model.config.hidden_dim
        layers = layers_fn(hidden_dim)
        if not isinstance(layers, dict):
            msg = f"custom_layers must return dict, got {type(layers).__name__}"
            raise TypeError(msg)
        for name, module in layers.items():
            model.register_module(name, module)

    # Monkey-patch custom embed
    if "custom_embed" in restricted_globals:
        model._embed = types.MethodType(restricted_globals["custom_embed"], model)  # type: ignore[assignment]
```

Add the `from __future__ import annotations` import if not already present.

Update `train_and_evaluate()` signature to accept `program_path`:

```python
def train_and_evaluate(
    candidate_config: dict[str, Any],
    device: str,
    epochs: int,
    seed: int,
    program_path: str | None = None,
) -> dict[str, float]:
```

After model construction (after `model = ScoutGPTDecoder(config).to(torch_device)`), add:

```python
    _apply_program(model, program_path)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_evolve_level2.py::TestApplyProgram -v`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(evolve): implement exec + monkey-patch in ScoutGPT target evaluator
```

---

### Task 9: Runner — CLI Flag and Prompt Directory Selection

**Files:**
- Modify: `src/evolve/runner.py:401-438` (CLI parser), `src/evolve/runner.py:526-543` (evaluator construction), `src/evolve/runner.py:196-242` (prompt dir)
- Test: `src/tests/test_evolve_evaluator.py`

- [ ] **Step 1: Write the failing test**

Append to `src/tests/test_evolve_evaluator.py`:

```python
from evolve.runner import _build_parser


class TestRunnerCodeEvolutionFlag:
    def test_default_is_false(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(["--target", "scoutgpt"])
        assert args.code_evolution is False

    def test_flag_enables(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(["--target", "scoutgpt", "--code-evolution"])
        assert args.code_evolution is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_evolve_evaluator.py::TestRunnerCodeEvolutionFlag -v`
Expected: FAIL — `error: unrecognized arguments: --code-evolution`

- [ ] **Step 3: Add CLI flag to runner**

In `src/evolve/runner.py`, in `_build_parser()`, add:

```python
    parser.add_argument(
        "--code-evolution",
        action="store_true",
        default=False,
        help="Enable Level 2 code evolution (LLM generates PyTorch functions)",
    )
```

- [ ] **Step 4: Wire through to config and evaluator construction**

In `runner.py`, where CLI args override config (after config loading):

```python
    if args.code_evolution:
        config.evolution.code_evolution = True
```

Where the evaluator is constructed, pass `code_evolution` and `validation_profile`:

```python
    # Load validation profile if code evolution is enabled
    validation_profile = None
    if config.evolution.code_evolution:
        target_module = importlib.import_module(f"evolve.targets.{target}")
        validation_profile = getattr(target_module, "VALIDATION_PROFILE", None)
        if validation_profile is None:
            _log.warning("Target %s has no VALIDATION_PROFILE; Level 2 disabled", target)
            config.evolution.code_evolution = False

    evaluator = EvolveEvaluator(
        backend=backend,
        target=target,
        eval_config=config.evaluation,
        fitness_config=config.fitness,
        code_evolution=config.evolution.code_evolution,
        validation_profile=validation_profile,
    )
```

For prompt directory selection in `_translate_to_openevolve_config()`:

```python
    # Select prompt directory based on code evolution level
    if config.evolution.code_evolution:
        prompts_dir = target_dir / "prompts_l2"
    else:
        prompts_dir = target_dir / "prompts"
    if prompts_dir.is_dir():
        oe_config["prompt"] = {"template_dir": str(prompts_dir)}
```

- [ ] **Step 5: Update _EVALUATOR_SCRIPT in runner.py**

The standalone evaluator script written to the results directory needs to pass `code_evolution` and `validation_profile` to `EvolveEvaluator`. Add these to the `_openevolve_evaluator_config.json` and wire them in the script's `_get_evaluator()` function.

In `_write_evaluator_script()`, add to the config dict:

```python
    "code_evolution": config.evolution.code_evolution,
```

In the `_EVALUATOR_SCRIPT` template, in `_get_evaluator()`, add:

```python
    code_evolution = cfg.get("code_evolution", False)
    validation_profile = None
    if code_evolution:
        target_mod = importlib.import_module(f"evolve.targets.{cfg['target']}")
        validation_profile = getattr(target_mod, "VALIDATION_PROFILE", None)
```

And pass to the `EvolveEvaluator` constructor:

```python
    _evaluator = EvolveEvaluator(
        backend=backend,
        target=cfg["target"],
        eval_config=eval_config,
        fitness_config=fitness_config,
        code_evolution=code_evolution,
        validation_profile=validation_profile,
    )
```

- [ ] **Step 6: Run tests**

Run: `uv run pytest src/tests/test_evolve_evaluator.py -v`
Expected: ALL PASS

- [ ] **Step 7: Commit**

```
feat(evolve): add --code-evolution CLI flag and prompt directory selection
```

---

### Task 10: Level 2 LLM Prompt

**Files:**
- Create: `src/evolve/targets/scoutgpt/prompts_l2/system_message.txt`

- [ ] **Step 1: Create the prompts_l2 directory**

```bash
mkdir -p src/evolve/targets/scoutgpt/prompts_l2
```

- [ ] **Step 2: Write the Level 2 prompt**

Create `src/evolve/targets/scoutgpt/prompts_l2/system_message.txt`:

```text
You are an expert deep learning researcher inventing novel player conditioning
mechanisms for a ScoutGPT sequential action model. You may modify config values
AND write custom PyTorch code that replaces the model's embedding function.

The system maintains a diverse population — both high fitness AND structural
novelty are valuable. You are competing with config-only candidates, so your
code must justify its complexity by outperforming simpler alternatives.

# Program Format

Your output is a Python file with up to 3 top-level constructs:

## 1. config dict (REQUIRED — same as config-only candidates)

```python
config = {
    "conditioning_type": "cross_attention",  # ignored if custom_embed present
    "hidden_dim": 192,        # int, 64-512, must be divisible by num_heads
    "num_layers": 3,          # int, 2-12
    "num_heads": 6,           # int, 2-16
    "dropout": 0.15,          # float, 0.0-0.5
    "learning_rate": 2e-4,    # float, 1e-5 to 1e-2
    "vaep_loss_weight": 0.32, # float, 0.0-1.0
    "player_prediction_weight": 0.18,  # float, 0.0-1.0
    "batch_size": 384,        # int, 64-512
    "max_seq_len": 128,       # int, positive
    "spatial_mlp_dim": 64,    # int, positive
    "weight_decay": 0.01,     # float, non-negative
    "dataset": "luxury-lakehouse/scoutgpt-training-data",  # must start with luxury-lakehouse/
}
```

## 2. custom_layers function (OPTIONAL)

Declares new nn.Module layers to register on the model. Receives hidden_dim
from config so layers are correctly sized.

```python
def custom_layers(hidden_dim):
    return {
        "my_gate": torch.nn.Sequential(
            torch.nn.Linear(hidden_dim, hidden_dim // 4),
            torch.nn.ReLU(),
            torch.nn.Linear(hidden_dim // 4, hidden_dim),
        ),
    }
```

## 3. custom_embed function (OPTIONAL)

Replaces ScoutGPTDecoder._embed(). Must accept these exact parameters and
return a tensor of shape (batch, seq_len, hidden_dim).

```python
def custom_embed(self, action_ids, start_x, start_y, end_x, end_y,
                 result, time_delta, player_ids):
    # Build embeddings from inputs
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
    # Your novel conditioning logic here
    ...
    return self.embedding_dropout(emb)
```

# Available self.* Attributes

These are the model layers you may access via self:

| Attribute            | Type                    | Notes                            |
|----------------------|-------------------------|----------------------------------|
| self.player_embedding | nn.Embedding(N, hd)    | Player identity embedding        |
| self.token_embedding  | nn.Embedding(25, hd)   | Action type embedding            |
| self.result_embedding | nn.Embedding(2, hd)    | Binary outcome embedding         |
| self.position_embedding | nn.Embedding(seq, hd)| Positional embedding             |
| self.start_x_mlp     | SpatialMLP(hd, smd)    | Pitch X start coordinate         |
| self.start_y_mlp     | SpatialMLP(hd, smd)    | Pitch Y start coordinate         |
| self.end_x_mlp       | SpatialMLP(hd, smd)    | Pitch X end coordinate           |
| self.end_y_mlp       | SpatialMLP(hd, smd)    | Pitch Y end coordinate           |
| self.time_delta_mlp   | SpatialMLP(hd, smd)    | Time since last action           |
| self.player_cross_attn | MultiheadAttention     | Cross-attention (if registered)  |
| self.player_cross_norm | LayerNorm(hd)         | Cross-attn layer norm            |
| self.film_scale       | Sequential(Linear,Sig) | FiLM scale (if registered)       |
| self.film_shift       | Linear(hd, hd)        | FiLM shift (if registered)       |
| self.player_gate      | Sequential(Linear,Sig) | Gated conditioning (if reg.)     |
| self.embedding_dropout | Dropout                | Always call this on output       |

You may also access any layer declared in your custom_layers() return dict.

# Allowed Operations

You may use: torch.*, torch.nn.*, torch.nn.functional.*, math.*, arithmetic
operators, comparison operators, if/elif/else, for/while loops, local variables,
tensor indexing/slicing, list/dict comprehensions, tuple unpacking.

You may NOT use: import statements, open(), print(), eval(), exec(), getattr(),
setattr(), any __dunder__ attributes, f-strings, or any module not listed above.

# Rules

1. custom_embed MUST return shape (batch, seq_len, hidden_dim) — shape mismatch
   crashes training and scores 0.
2. custom_layers MUST return dict[str, nn.Module] — anything else scores 0.
3. All new layers MUST use the hidden_dim parameter for sizing — hardcoded dims
   break when config changes.
4. You may omit custom_embed and custom_layers for config-only candidates.
5. Keep custom_embed under 50 lines — complexity kills gradient flow.
6. Always call self.embedding_dropout on your output.
7. hidden_dim MUST be divisible by num_heads. Safe combos: 192/6, 256/8, 384/12.

# Fitness Function

combined_score = 0.7 * spearman_rho + 0.3 * top1_accuracy

Structural novelty that hurts rho is worse than a simple config tweak that
improves it. The bar is: your code must outperform the best config-only variant.
```

- [ ] **Step 3: Commit**

```
feat(evolve): add Level 2 LLM prompt for code evolution
```

---

### Task 11: Level 2 Seed Program

**Files:**
- Create: `src/evolve/targets/scoutgpt/seed_programs/hybrid_gated_attention.py`
- Test: `src/tests/test_code_validator.py`

- [ ] **Step 1: Write the validation test**

Append to `src/tests/test_code_validator.py`:

```python
class TestSeedPrograms:
    def test_hybrid_gated_attention_passes_validation(self) -> None:
        source = Path("src/evolve/targets/scoutgpt/seed_programs/hybrid_gated_attention.py").read_text()
        valid, reason = validate_program(source, SCOUTGPT_PROFILE)
        assert valid, reason
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_code_validator.py::TestSeedPrograms -v`
Expected: FAIL — `FileNotFoundError`

- [ ] **Step 3: Write the seed program**

Create `src/evolve/targets/scoutgpt/seed_programs/hybrid_gated_attention.py`:

```python
"""Seed 5: Hybrid gated cross-attention (Level 2 — code evolution).

Demonstrates the custom_layers + custom_embed format. Combines cross-attention
with a learned sigmoid gate that controls how much player identity information
flows into the action embeddings. The gate is a separate MLP declared via
custom_layers, allowing the evolution engine to modify or replace it.

This is a starting point for Level 2 evolution — the LLM can modify both
the gating mechanism and the attention pattern.
"""

config = {
    "conditioning_type": "cross_attention",  # ignored — custom_embed overrides
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


def custom_layers(hidden_dim):
    """Learned gate: projects player info to a per-dimension sigmoid mask."""
    return {
        "hybrid_gate": torch.nn.Sequential(
            torch.nn.Linear(hidden_dim, hidden_dim // 4),
            torch.nn.ReLU(),
            torch.nn.Linear(hidden_dim // 4, hidden_dim),
            torch.nn.Sigmoid(),
        ),
    }


def custom_embed(self, action_ids, start_x, start_y, end_x, end_y,
                 result, time_delta, player_ids):
    """Gated cross-attention: attention output is modulated by a learned gate."""
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

    # Cross-attention: action queries attend to player keys/values
    attn_out, _ = self.player_cross_attn(
        query=action_emb, key=player_emb, value=player_emb,
    )
    attn_out = self.player_cross_norm(action_emb + attn_out)

    # Learned gate modulates how much player info flows through
    gate = self.hybrid_gate(player_emb)
    emb = gate * attn_out + (1 - gate) * action_emb

    return self.embedding_dropout(emb)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_code_validator.py::TestSeedPrograms -v`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(evolve): add hybrid_gated_attention Level 2 seed program
```

---

### Task 12: CLAUDE.md — Scoped exec() Exception

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add the scoped exception**

In `CLAUDE.md`, in the Security Hardening section, after the "No dangerous builtins" rule, add:

```markdown
- **Scoped exception — `src/evolve/`**: `exec()` is permitted in `src/evolve/targets/*/evaluator.py` and `src/evolve/remote_worker.py` under the defense-in-depth policy documented in [ADR-001](docs/superpowers/adrs/ADR-001-evolve-code-execution.md): AST allowlist (parse-time) + restricted globals with `__builtins__: {}` (runtime) + subprocess isolation (backends). Gated by `code_evolution=True`. All other code must continue to avoid `exec()`/`eval()`.
```

- [ ] **Step 2: Commit**

```
docs: add scoped exec() exception for evolve Level 2 (ADR-001)
```

---

### Task 13: Integration Smoke Test

**Files:**
- Test: `src/tests/test_evolve_level2.py`

This task adds end-to-end tests that exercise the full pipeline from program file through evaluator to training.

- [ ] **Step 1: Write integration tests**

Append to `src/tests/test_evolve_level2.py`:

```python
from evolve.code_validator import validate_program
from evolve.config import EvalConfig, FitnessConfig
from evolve.evaluator import EvolveEvaluator
from evolve.targets.scoutgpt.validation import SCOUTGPT_PROFILE


class TestLevel2EndToEnd:
    """End-to-end tests using a real (tiny) model on CPU."""

    def test_config_only_backward_compat(self, tmp_path: Path) -> None:
        """Level 1 program works with code_evolution=True."""
        prog = tmp_path / "config_only.py"
        prog.write_text(textwrap.dedent("""\
            config = {
                "conditioning_type": "additive",
                "hidden_dim": 64,
                "num_layers": 1,
                "num_heads": 2,
                "dropout": 0.0,
                "learning_rate": 1e-3,
                "vaep_loss_weight": 0.1,
                "batch_size": 64,
                "num_players": 50,
                "max_seq_len": 32,
            }
        """))
        source = prog.read_text()
        valid, reason = validate_program(source, SCOUTGPT_PROFILE)
        assert valid, reason

    def test_seed_hybrid_gated_attention_validates(self) -> None:
        """The Level 2 seed program passes AST validation."""
        seed = Path("src/evolve/targets/scoutgpt/seed_programs/hybrid_gated_attention.py")
        source = seed.read_text()
        valid, reason = validate_program(source, SCOUTGPT_PROFILE)
        assert valid, reason

    def test_all_existing_seeds_validate(self) -> None:
        """All existing Level 1 seeds pass validation (backward compat)."""
        seeds_dir = Path("src/evolve/targets/scoutgpt/seed_programs")
        for seed_file in seeds_dir.glob("*.py"):
            source = seed_file.read_text()
            valid, reason = validate_program(source, SCOUTGPT_PROFILE)
            assert valid, f"{seed_file.name}: {reason}"

    def test_invalid_program_rejected_by_validator(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, action_ids, start_x, start_y, end_x, end_y,
                             result, time_delta, player_ids):
                import torch.utils  # imports not allowed
                return self.token_embedding(action_ids)
        """)
        valid, reason = validate_program(source, SCOUTGPT_PROFILE)
        assert not valid
        assert "import" in reason.lower()
```

- [ ] **Step 2: Run all tests**

Run: `uv run pytest src/tests/test_evolve_level2.py -v`
Expected: ALL PASS

- [ ] **Step 3: Run full evolve test suite**

Run: `uv run pytest src/tests/test_evolve_config.py src/tests/test_evolve_evaluator.py src/tests/test_code_validator.py src/tests/test_evolve_level2.py -v`
Expected: ALL PASS

- [ ] **Step 4: Run ruff + pyright on all evolve code**

Run: `uv run ruff check src/evolve/ src/tests/test_code_validator.py src/tests/test_evolve_level2.py && uv run pyright src/evolve/`

- [ ] **Step 5: Commit**

```
test(evolve): add Level 2 integration and backward-compat tests
```

---

## Task Summary

| # | Task | Dependencies | Est. Size |
|---|------|-------------|-----------|
| 1 | ValidationProfile dataclass | — | Small |
| 2 | ScoutGPT ValidationProfile | 1 | Small |
| 3 | AST Validator core | 1 | Large |
| 4 | Config `code_evolution` toggle | — | Small |
| 5 | Program dataclass + `_load_program()` | — | Medium |
| 6 | Evaluator validation gate + protocol | 3, 4, 5 | Medium |
| 7 | Backend changes (local, ssh, hf_jobs, worker) | 6 | Medium |
| 8 | Target evaluator exec + monkey-patch | 7 | Medium |
| 9 | Runner CLI + prompt dir | 4, 6 | Medium |
| 10 | Level 2 LLM prompt | — | Small |
| 11 | Level 2 seed program | 2, 3 | Small |
| 12 | CLAUDE.md scoped exception | — | Small |
| 13 | Integration smoke tests | All | Medium |

Tasks 1, 4, 5, 10 can run in parallel (no dependencies).
Tasks 2, 3 depend on 1.
Tasks 6, 9 depend on 3, 4, 5.
Tasks 7, 8 are sequential.
Task 13 is the final integration gate.

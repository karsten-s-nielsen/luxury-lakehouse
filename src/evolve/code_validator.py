"""AST allowlist validator for evolve Level 2 code evolution.

Validates LLM-generated `custom_embed()` and `custom_layers()` functions
against a target-provided ValidationProfile. Defense-in-depth belt layer —
see ADR-001 for the full security model.
"""

from __future__ import annotations

import ast
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


# ---------------------------------------------------------------------------
# AST node classification
# ---------------------------------------------------------------------------

# Leaf nodes and simple expressions — always allowed, no children to recurse.
_ALWAYS_ALLOWED_NODES: frozenset[type] = frozenset(
    {
        ast.Constant,
        ast.Name,
        ast.Load,
        ast.Store,
        ast.Del,
        # Operators
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
        # Unary operators
        ast.UAdd,
        ast.USub,
        ast.Not,
        ast.Invert,
        # Comparison operators
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
        # Boolean operators
        ast.And,
        ast.Or,
    }
)

# Container / statement nodes that need recursion into children.
_GENERIC_CONTAINER_NODES: frozenset[type] = frozenset(
    {
        # Expressions
        ast.BinOp,
        ast.UnaryOp,
        ast.BoolOp,
        ast.Compare,
        ast.IfExp,
        ast.Starred,
        # Subscript / indexing
        ast.Subscript,
        ast.Slice,
        ast.Index,  # type: ignore[attr-defined]  # Python 3.8 compat
        # Collections
        ast.Tuple,
        ast.List,
        ast.Set,
        ast.Dict,
        # Comprehensions
        ast.ListComp,
        ast.SetComp,
        ast.DictComp,
        ast.GeneratorExp,
        ast.comprehension,
        # Statements
        ast.Assign,
        ast.AugAssign,
        ast.AnnAssign,
        ast.Return,
        ast.Delete,
        ast.Pass,
        ast.Break,
        ast.Continue,
        ast.If,
        ast.For,
        ast.While,
        ast.With,
        ast.withitem,
        ast.Raise,
        ast.Try,
        ast.ExceptHandler,
        ast.Assert,
        # Expression wrapper
        ast.Expr,
        ast.keyword,
        ast.arg,
        ast.arguments,
        ast.FunctionDef,
    }
)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _extract_functions(tree: ast.Module) -> dict[str, ast.FunctionDef]:
    """Extract top-level FunctionDef nodes by name."""
    return {
        node.name: node
        for node in ast.iter_child_nodes(tree)
        if isinstance(node, ast.FunctionDef)
    }


def _extract_layers_keys(func: ast.FunctionDef) -> set[str]:
    """Extract string keys from the return Dict literal in custom_layers().

    Expects a ``return {...}`` statement where keys are string constants.
    Returns the set of key strings.
    """
    keys: set[str] = set()
    for node in ast.walk(func):
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict):
            for key in node.value.keys:
                if isinstance(key, ast.Constant) and isinstance(key.value, str):
                    keys.add(key.value)
    return keys


def _get_attribute_root(node: ast.Attribute) -> ast.Name | None:
    """Walk an ``a.b.c.d`` chain to find the root ``ast.Name``.

    Returns ``None`` if the chain root is not a simple ``Name`` node
    (e.g., a function call result).
    """
    current: ast.expr = node
    while isinstance(current, ast.Attribute):
        current = current.value
    if isinstance(current, ast.Name):
        return current
    return None


def _get_first_attr_after_root(node: ast.Attribute) -> str | None:
    """For ``self.a.b``, return ``"a"`` (the first attribute after the root Name).

    Walks the chain to find the attribute whose value is the root Name.
    """
    current: ast.expr = node
    prev_attr: str | None = None
    while isinstance(current, ast.Attribute):
        prev_attr = current.attr
        current = current.value
    return prev_attr


def _collect_assign_targets(target: ast.expr) -> list[str]:
    """Collect all Name ids from an assignment target (handles unpacking)."""
    names: list[str] = []
    if isinstance(target, ast.Name):
        names.append(target.id)
    elif isinstance(target, (ast.Tuple, ast.List)):
        for elt in target.elts:
            names.extend(_collect_assign_targets(elt))
    elif isinstance(target, ast.Starred):
        names.extend(_collect_assign_targets(target.value))
    return names


# ---------------------------------------------------------------------------
# Allowlist visitor
# ---------------------------------------------------------------------------


class _AllowlistVisitor:
    """Recursive AST visitor that validates every node in a function body.

    Maintains a set of local variable names to distinguish ``local.method()``
    (allowed) from ``os.system()`` (rejected).
    """

    def __init__(
        self,
        profile: ValidationProfile,
        dynamic_attrs: set[str],
        local_names: set[str],
    ) -> None:
        self.profile = profile
        self.allowed_self_attrs = profile.known_model_attrs | dynamic_attrs
        self.allowed_namespaces = profile.allowed_namespaces
        self.rejected_builtins = profile.rejected_builtins
        self.local_names = set(local_names)
        self.errors: list[str] = []

    def visit(self, node: ast.AST) -> None:
        """Visit a node and all its children."""
        node_type = type(node)

        # Leaf nodes — always allowed
        if node_type in _ALWAYS_ALLOWED_NODES:
            return

        # F-strings — always rejected
        if isinstance(node, ast.JoinedStr):
            self.errors.append("f-string/format expressions are not allowed")
            return

        # FormattedValue — part of f-string internals
        if isinstance(node, ast.FormattedValue):
            self.errors.append("f-string/format expressions are not allowed")
            return

        # Import — always rejected
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            self.errors.append("import statements are not allowed")
            return

        # Attribute access
        if isinstance(node, ast.Attribute):
            self._check_attribute(node)
            return

        # Call expressions
        if isinstance(node, ast.Call):
            self._check_call(node)
            return

        # Assignment — track local variable names, then recurse into value
        if isinstance(node, ast.Assign):
            for target in node.targets:
                self.local_names.update(_collect_assign_targets(target))
                self._visit_children_of(target)
            self.visit(node.value)
            return

        if isinstance(node, ast.AugAssign):
            self.local_names.update(_collect_assign_targets(node.target))
            self._visit_children_of(node.target)
            self.visit(node.value)
            return

        if isinstance(node, ast.AnnAssign):
            if node.target:
                self.local_names.update(_collect_assign_targets(node.target))
            if node.value:
                self.visit(node.value)
            return

        # For loops — collect loop variable names
        if isinstance(node, ast.For):
            self.local_names.update(_collect_assign_targets(node.target))
            self._visit_children(node)
            return

        # Comprehension — collect target names
        if isinstance(node, ast.comprehension):
            self.local_names.update(_collect_assign_targets(node.target))
            self._visit_children(node)
            return

        # FunctionDef — collect name as local, visit body
        if isinstance(node, ast.FunctionDef):
            self.local_names.add(node.name)
            # Add function params as local names
            for arg in node.args.args:
                self.local_names.add(arg.arg)
            self._visit_children(node)
            return

        # Generic container nodes — recurse
        if node_type in _GENERIC_CONTAINER_NODES:
            self._visit_children(node)
            return

        # Unknown node type — reject
        self.errors.append(f"Unsupported AST node type: {node_type.__name__}")

    def _visit_children(self, node: ast.AST) -> None:
        """Visit all child nodes."""
        for child in ast.iter_child_nodes(node):
            self.visit(child)

    def _visit_children_of(self, node: ast.AST) -> None:
        """Visit children of a node (used for targets where Name is allowed)."""
        for child in ast.iter_child_nodes(node):
            self.visit(child)

    def _check_attribute(self, node: ast.Attribute) -> None:
        """Validate attribute access (``a.b``, ``self.x``, ``torch.nn.Linear``)."""
        attr_name = node.attr

        # Dunder attributes — always rejected
        if attr_name.startswith("__"):
            self.errors.append(
                f"Dunder attribute '__{attr_name[2:]}' is not allowed"
            )
            return

        root = _get_attribute_root(node)
        if root is None:
            # Chain root is a non-Name (e.g., function call result) — allowed
            self._visit_children(node)
            return

        root_name = root.id

        # self.attr — must be in allowlist
        if root_name == "self":
            first_attr = _get_first_attr_after_root(node)
            if first_attr and first_attr not in self.allowed_self_attrs:
                self.errors.append(
                    f"Unknown self attribute '{first_attr}' — not in known_model_attrs or custom_layers"
                )
            return

        # Allowed namespaces (torch, math, etc.) — always OK (chained too)
        if root_name in self.allowed_namespaces:
            return

        # Local variables — allowed (e.g., tensor methods)
        if root_name in self.local_names:
            return

        # Unknown namespace — reject
        self.errors.append(
            f"Attribute access on unknown namespace '{root_name}' is not allowed"
        )

    def _check_call(self, node: ast.Call) -> None:
        """Validate function/method calls."""
        func = node.func

        # Attribute call (e.g., self.linear(x), torch.sigmoid(x))
        if isinstance(func, ast.Attribute):
            # Check dunder method calls
            if func.attr.startswith("__"):
                self.errors.append(
                    f"Dunder method '__{func.attr[2:]}' call is not allowed"
                )
                # Still visit arguments
                for arg in node.args:
                    self.visit(arg)
                for kw in node.keywords:
                    self.visit(kw)
                return

            # Validate the attribute chain
            self._check_attribute(func)
            # Visit arguments
            for arg in node.args:
                self.visit(arg)
            for kw in node.keywords:
                self.visit(kw)
            return

        # Simple name call (e.g., range(3), eval("x"))
        if isinstance(func, ast.Name):
            name = func.id
            # Rejected builtins
            if name in self.rejected_builtins:
                self.errors.append(f"Rejected builtin '{name}' is not allowed")
                return
            # range() is always allowed
            if name == "range":
                for arg in node.args:
                    self.visit(arg)
                for kw in node.keywords:
                    self.visit(kw)
                return
            # len() is always allowed
            if name == "len":
                for arg in node.args:
                    self.visit(arg)
                for kw in node.keywords:
                    self.visit(kw)
                return
            # int/float/bool/str/list/tuple/dict/set are allowed
            if name in {"int", "float", "bool", "str", "list", "tuple", "dict", "set", "max", "min", "sum", "abs",
                        "round", "sorted", "reversed", "enumerate", "zip", "map", "filter", "any", "all", "isinstance",
                        "hasattr", "id", "hash", "repr", "slice"}:
                for arg in node.args:
                    self.visit(arg)
                for kw in node.keywords:
                    self.visit(kw)
                return
            # Local function calls (defined in the same scope)
            if name in self.local_names:
                for arg in node.args:
                    self.visit(arg)
                for kw in node.keywords:
                    self.visit(kw)
                return
            # Allowed namespaces used as calls (unlikely but valid)
            if name in self.allowed_namespaces:
                for arg in node.args:
                    self.visit(arg)
                for kw in node.keywords:
                    self.visit(kw)
                return
            # Unknown bare function call — reject
            self.errors.append(
                f"Unknown function call '{name}' is not allowed"
            )
            return

        # Anything else (e.g., lambda calls) — visit children
        self._visit_children(node)


# ---------------------------------------------------------------------------
# Signature validation
# ---------------------------------------------------------------------------


def _validate_signature(
    func: ast.FunctionDef,
    expected_params: list[str],
    func_label: str,
) -> str | None:
    """Check that a function's parameter list matches expected names.

    Returns an error message or None if valid.
    """
    actual = [arg.arg for arg in func.args.args]
    if actual != expected_params:
        return (
            f"{func_label} signature mismatch: "
            f"expected ({', '.join(expected_params)}), "
            f"got ({', '.join(actual)})"
        )
    return None


def _validate_custom_layers(
    func: ast.FunctionDef,
    profile: ValidationProfile,
) -> tuple[set[str], str | None]:
    """Validate ``custom_layers()`` function.

    Returns ``(layer_keys, error_or_none)``. Layer keys are extracted
    even on error so the caller can use them for downstream validation.
    """
    # Signature check
    sig_err = _validate_signature(func, profile.layers_args, "custom_layers")
    if sig_err:
        return set(), sig_err

    # Must return a dict literal
    has_dict_return = False
    for node in ast.walk(func):
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict):
            has_dict_return = True
            break

    if not has_dict_return:
        return set(), "custom_layers must return a dict literal"

    keys = _extract_layers_keys(func)

    # Walk the body through the visitor (custom_layers can use torch.nn.*)
    local_names = {arg.arg for arg in func.args.args}
    visitor = _AllowlistVisitor(profile, set(), local_names)
    for stmt in func.body:
        visitor.visit(stmt)

    if visitor.errors:
        return keys, visitor.errors[0]

    return keys, None


def _validate_custom_embed(
    func: ast.FunctionDef,
    profile: ValidationProfile,
    dynamic_attrs: set[str],
) -> str | None:
    """Validate ``custom_embed()`` function.

    Returns an error message or None if valid.
    """
    # Signature check
    sig_err = _validate_signature(func, profile.patch_signature, "custom_embed")
    if sig_err:
        return sig_err

    # Walk the body through the visitor
    local_names = {arg.arg for arg in func.args.args}
    visitor = _AllowlistVisitor(profile, dynamic_attrs, local_names)
    for stmt in func.body:
        visitor.visit(stmt)

    if visitor.errors:
        return visitor.errors[0]

    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def validate_program(
    source: str,
    profile: ValidationProfile,
    *,
    code_evolution: bool = True,
) -> tuple[bool, str]:
    """Validate an evolve program source against a ValidationProfile.

    Parameters
    ----------
    source:
        Python source code of the candidate program.
    profile:
        Target-specific validation rules.
    code_evolution:
        Whether Level 2 code evolution is enabled. When ``False``,
        programs containing ``custom_embed`` or ``custom_layers``
        are rejected.

    Returns
    -------
    tuple[bool, str]:
        ``(True, reason)`` if valid, ``(False, reason)`` if rejected.
    """
    # Step 1: Parse
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return False, f"Syntax error: {exc}"

    # Step 2: Extract top-level functions
    functions = _extract_functions(tree)
    has_custom_embed = "custom_embed" in functions
    has_custom_layers = "custom_layers" in functions

    # Step 3: Config-only program (Level 1 backward compat)
    if not has_custom_embed and not has_custom_layers:
        return True, "config-only program"

    # Step 4: Code evolution disabled
    if not code_evolution:
        return False, "Code evolution is disabled — custom_embed/custom_layers not allowed"

    # Step 5: Validate custom_layers first (extracts dynamic attr names)
    dynamic_attrs: set[str] = set()
    if has_custom_layers:
        keys, err = _validate_custom_layers(functions["custom_layers"], profile)
        if err:
            return False, err
        dynamic_attrs = keys

    # Step 6: Validate custom_embed
    if has_custom_embed:
        err = _validate_custom_embed(
            functions["custom_embed"], profile, dynamic_attrs
        )
        if err:
            return False, err

    return True, "passed"

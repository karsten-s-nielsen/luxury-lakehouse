"""Football2Vec Level-2 adversary validation profile.

Defines which model attributes, namespaces, and builtins are allowed in
LLM-generated (or hand-written) seed programs for the stage-2 adversary
search. Each seed defines the ScoutGPT two-function pattern:

- ``custom_layers(hidden_dim, num_competitions)`` returns a dict of
  ``nn.Module`` submodules (keys become attribute names on the adversary
  wrapper module built by the evaluator). A ``"grl"`` key is required —
  the training loop patches ``adversary.grl.lambda_val`` per epoch.
- ``custom_embed(self, encoder_output, attention_mask)`` returns logits of
  shape ``(B, num_competitions)``. Despite the function name (retained for
  reuse with the existing code_validator.py that hardcodes this name), this
  function is the **adversary forward** — not a spatial embedding function.
  The evaluator's ``_apply_program_adversary`` wraps layers + forward into
  a dynamic ``nn.Module`` whose forward delegates here.

Defense-in-depth belt layer per ADR-001 — parse-time AST allowlist.
"""

from __future__ import annotations

from evolve.code_validator import ValidationProfile

FOOTBALL2VEC_ADVERSARY_PROFILE = ValidationProfile(
    patch_method="adversary",
    patch_signature=["self", "encoder_output", "attention_mask"],
    return_shape="(B, num_competitions) unnormalized logits",
    known_model_attrs=frozenset(),
    allowed_namespaces=frozenset(
        {
            "torch",
            "math",
            "GradientReversal",
            "MoERouter",
            "HyperLinear",
            "KANLayer",
            "AdaLNZero",
            "CrossLayer",
            "CompetitiveGate",
            "AdaptiveBandwidth",
            "RatioGate",
        }
    ),
    layers_args=["hidden_dim", "num_competitions"],
    rejected_builtins=frozenset(
        {
            "eval",
            "exec",
            "compile",
            "__import__",
            "open",
            "print",
            "input",
            "getattr",
            "hasattr",
            "setattr",
            "delattr",
            "globals",
            "locals",
            "vars",
            "dir",
            "type",
            "super",
            "breakpoint",
            "memoryview",
            "classmethod",
            "staticmethod",
            "property",
        }
    ),
)

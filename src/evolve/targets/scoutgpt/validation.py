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
        "self",
        "action_ids",
        "start_x",
        "start_y",
        "end_x",
        "end_y",
        "result",
        "time_delta",
        "player_ids",
    ],
    return_shape="(batch, seq_len, hidden_dim)",
    known_model_attrs=frozenset(
        {
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
        }
    ),
    allowed_namespaces=frozenset({"torch", "math"}),
    layers_args=["hidden_dim"],
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

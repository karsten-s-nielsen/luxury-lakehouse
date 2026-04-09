"""Tests for the evolve code validator (AST allowlist + ValidationProfile)."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from evolve.code_validator import ValidationProfile, validate_program
from evolve.targets.scoutgpt.validation import SCOUTGPT_PROFILE


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


# ---------------------------------------------------------------------------
# ScoutGPT ValidationProfile
# ---------------------------------------------------------------------------


class TestScoutGPTProfile:
    def test_patch_method(self) -> None:
        assert SCOUTGPT_PROFILE.patch_method == "_embed"

    def test_signature_starts_with_self(self) -> None:
        assert SCOUTGPT_PROFILE.patch_signature[0] == "self"

    def test_signature_has_all_embed_params(self) -> None:
        expected = {
            "self",
            "action_ids",
            "start_x",
            "start_y",
            "end_x",
            "end_y",
            "result",
            "time_delta",
            "player_ids",
        }
        assert set(SCOUTGPT_PROFILE.patch_signature) == expected

    def test_known_attrs_include_core_layers(self) -> None:
        core = {"player_embedding", "token_embedding", "embedding_dropout"}
        assert core.issubset(SCOUTGPT_PROFILE.known_model_attrs)

    def test_known_attrs_include_conditioning_layers(self) -> None:
        conditioning = {
            "player_cross_attn",
            "player_cross_norm",
            "film_scale",
            "film_shift",
            "player_gate",
        }
        assert conditioning.issubset(SCOUTGPT_PROFILE.known_model_attrs)

    def test_torch_and_math_allowed(self) -> None:
        assert {"torch", "math"}.issubset(SCOUTGPT_PROFILE.allowed_namespaces)

    def test_building_blocks_allowed(self) -> None:
        building_blocks = {
            "MoERouter",
            "HyperLinear",
            "KANLayer",
            "AdaLNZero",
            "CrossLayer",
            "CompetitiveGate",
            "GradientReversal",
            "AdaptiveBandwidth",
            "RatioGate",
        }
        assert building_blocks.issubset(SCOUTGPT_PROFILE.allowed_namespaces)

    def test_rejected_builtins(self) -> None:
        dangerous = {"eval", "exec", "compile", "__import__", "open"}
        assert dangerous.issubset(SCOUTGPT_PROFILE.rejected_builtins)


# ---------------------------------------------------------------------------
# AST Validator — Core Allow/Reject Logic
# ---------------------------------------------------------------------------

_TEST_PROFILE = ValidationProfile(
    patch_method="_embed",
    patch_signature=["self", "x", "y"],
    return_shape="(batch, hidden_dim)",
    known_model_attrs=frozenset({"linear", "norm", "dropout"}),
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
            "setattr",
            "delattr",
            "globals",
            "locals",
            "vars",
            "dir",
            "type",
            "super",
        }
    ),
)


class TestValidatorAccepts:
    """Programs that MUST pass validation."""

    def test_config_only_program(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_custom_embed_with_allowed_ops(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                h = self.linear(x)
                h = torch.sigmoid(h)
                return h + y * 0.5
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
                return x / scale
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_tensor_indexing(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                return x[:, :, :128]
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_control_flow(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                h = x
                for i in range(3):
                    h = self.linear(h)
                if True:
                    h = h + y
                return h
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_list_comprehension(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                parts = [x * i for i in range(3)]
                return parts[0]
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_tuple_unpacking(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                a, b = torch.chunk(x, 2, dim=-1)
                return a + b
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_torch_nn_functional(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                return torch.nn.functional.relu(x)
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason

    def test_dynamic_layer_in_custom_embed(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_layers(hidden_dim):
                return {"proj": torch.nn.Linear(hidden_dim, hidden_dim)}

            def custom_embed(self, x, y):
                return self.proj(x) + y
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert valid, reason


class TestValidatorRejects:
    """Programs that MUST fail validation."""

    def test_import_statement(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                import os
                return x
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid
        assert "import" in reason.lower()

    def test_import_from(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                from os import path
                return x
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid
        assert "import" in reason.lower()

    def test_dunder_import(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                __import__("os")
                return x
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid
        assert "__import__" in reason

    def test_dunder_attribute(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                c = self.__class__
                return x
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
                return x
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid
        assert "exec" in reason.lower()

    def test_open_call(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                f = open("/etc/passwd")
                return x
        """)
        valid, _reason = validate_program(source, _TEST_PROFILE)
        assert not valid

    def test_getattr_escape(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                return getattr(self, "__class__")
        """)
        valid, _reason = validate_program(source, _TEST_PROFILE)
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
                os.system("whoami")
                return x
        """)
        valid, _reason = validate_program(source, _TEST_PROFILE)
        assert not valid

    def test_fstring(self) -> None:
        source = textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                s = f"{x}"
                return x
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
                return {"gate": torch.nn.Linear(256, 256)}
        """)
        valid, reason = validate_program(source, _TEST_PROFILE)
        assert not valid
        assert "signature" in reason.lower()


class TestSeedPrograms:
    def test_hybrid_gated_attention_passes_validation(self) -> None:
        source = Path("src/evolve/targets/scoutgpt/seed_programs/hybrid_gated_attention.py").read_text()
        valid, reason = validate_program(source, SCOUTGPT_PROFILE)
        assert valid, reason

"""Integration tests for evolve Level 2 — code evolution.

These tests exercise the exec + monkey-patch path in the ScoutGPT
target evaluator. They use a minimal model config to run on CPU.
"""

from __future__ import annotations

import textwrap
import types
from pathlib import Path

import pytest

torch = pytest.importorskip("torch", reason="torch not installed in CI")

from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder  # noqa: E402
from evolve.code_validator import validate_program  # noqa: E402
from evolve.targets.scoutgpt.evaluator import _apply_program  # noqa: E402
from evolve.targets.scoutgpt.validation import SCOUTGPT_PROFILE  # noqa: E402


class TestApplyProgram:
    """Tests for _apply_program() — the exec + monkey-patch logic."""

    def _make_model(self) -> ScoutGPTDecoder:
        config = ScoutGPTConfig(
            hidden_dim=64,
            num_layers=1,
            num_heads=2,
            dropout=0.0,
            conditioning_type="additive",
            num_players=50,
            max_seq_len=32,
        )
        return ScoutGPTDecoder(config)

    def test_no_program_path_is_noop(self) -> None:
        model = self._make_model()
        original_embed = model._embed
        _apply_program(model, program_path=None)
        assert model._embed == original_embed

    def test_custom_embed_replaces_method(self, tmp_path: Path) -> None:
        prog = tmp_path / "prog.py"
        prog.write_text(
            textwrap.dedent("""\
            config = {"hidden_dim": 64}

            def custom_embed(self, action_ids, start_x, start_y, end_x, end_y,
                             result, time_delta, player_ids):
                player_emb = self.player_embedding(player_ids)
                action_emb = self.token_embedding(action_ids)
                return self.embedding_dropout(action_emb + player_emb)
        """)
        )
        model = self._make_model()
        _apply_program(model, program_path=str(prog))
        assert isinstance(model._embed, types.MethodType)
        # Verify it runs and produces correct shape
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
        prog.write_text(
            textwrap.dedent("""\
            config = {"hidden_dim": 64}

            def custom_layers(hidden_dim):
                return {"test_gate": torch.nn.Linear(hidden_dim, hidden_dim)}

            def custom_embed(self, action_ids, start_x, start_y, end_x, end_y,
                             result, time_delta, player_ids):
                emb = self.token_embedding(action_ids)
                gate = torch.sigmoid(self.test_gate(emb))
                return self.embedding_dropout(gate * emb)
        """)
        )
        model = self._make_model()
        _apply_program(model, program_path=str(prog))
        # Layer should be registered
        assert hasattr(model, "test_gate")
        assert isinstance(model.test_gate, torch.nn.Linear)

    def test_restricted_globals_no_builtins(self, tmp_path: Path) -> None:
        """exec runs with __builtins__={} -- open() at module level should fail."""
        prog = tmp_path / "prog.py"
        prog.write_text(
            textwrap.dedent("""\
            config = {"hidden_dim": 64}

            # This line runs at exec time -- should fail
            f = open("/etc/passwd")

            def custom_embed(self, action_ids, start_x, start_y, end_x, end_y,
                             result, time_delta, player_ids):
                return self.token_embedding(action_ids)
        """)
        )
        model = self._make_model()
        with pytest.raises(Exception):  # noqa: B017
            _apply_program(model, program_path=str(prog))

    def test_custom_layers_must_return_dict(self, tmp_path: Path) -> None:
        """custom_layers returning a non-dict raises TypeError."""
        prog = tmp_path / "prog.py"
        prog.write_text(
            textwrap.dedent("""\
            config = {"hidden_dim": 64}

            def custom_layers(hidden_dim):
                return [torch.nn.Linear(hidden_dim, hidden_dim)]
        """)
        )
        model = self._make_model()
        with pytest.raises(TypeError, match="custom_layers must return dict"):
            _apply_program(model, program_path=str(prog))

    def test_program_without_custom_functions_is_noop(self, tmp_path: Path) -> None:
        """A program with only config and no custom_embed/custom_layers is a no-op."""
        prog = tmp_path / "prog.py"
        prog.write_text(
            textwrap.dedent("""\
            config = {"hidden_dim": 64}
        """)
        )
        model = self._make_model()
        original_embed = model._embed
        _apply_program(model, program_path=str(prog))
        assert model._embed == original_embed


class TestLevel2EndToEnd:
    """End-to-end validation tests for Level 2 programs."""

    def test_config_only_backward_compat(self, tmp_path: Path) -> None:
        """Level 1 program works with code_evolution=True."""
        prog = tmp_path / "config_only.py"
        prog.write_text(
            textwrap.dedent("""\
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
        """)
        )
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
                import torch.utils
                return self.token_embedding(action_ids)
        """)
        valid, reason = validate_program(source, SCOUTGPT_PROFILE)
        assert not valid
        assert "import" in reason.lower()

    def test_hybrid_seed_runs_on_model(self) -> None:
        """The Level 2 seed can be applied to a real model and produces correct output."""
        config = ScoutGPTConfig(
            hidden_dim=192,
            num_layers=1,
            num_heads=6,
            dropout=0.0,
            conditioning_type="cross_attention",
            num_players=50,
            max_seq_len=32,
        )
        model = ScoutGPTDecoder(config)
        seed_path = str(Path("src/evolve/targets/scoutgpt/seed_programs/hybrid_gated_attention.py"))
        _apply_program(model, program_path=seed_path)

        # Verify the custom layer was registered
        assert hasattr(model, "hybrid_gate")

        # Verify it runs and produces correct shape
        batch, seq = 2, 8
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
        assert out.shape == (batch, seq, 192)

    def test_scoutgpt_exports_validation_profile(self) -> None:
        """Runner lookup for VALIDATION_PROFILE succeeds on ScoutGPT target."""
        import importlib

        mod = importlib.import_module("evolve.targets.scoutgpt")
        profile = getattr(mod, "VALIDATION_PROFILE", None)
        assert profile is not None, "ScoutGPT target must export VALIDATION_PROFILE"
        assert profile.patch_method == "_embed"

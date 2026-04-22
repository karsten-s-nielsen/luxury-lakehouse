"""Tests for EV2 Football2Vec L2 infrastructure — validator profile, seed loading,
stage-2 evaluator entry point."""

from __future__ import annotations


def test_f2v_adversary_validation_profile_exists():
    """Profile is importable and registered as the target's VALIDATION_PROFILE."""
    from evolve.targets.football2vec import VALIDATION_PROFILE
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE

    assert VALIDATION_PROFILE is FOOTBALL2VEC_ADVERSARY_PROFILE


def test_f2v_adversary_validation_profile_accepts_valid_seed():
    """A minimal valid seed using the two-function pattern passes AST validation."""
    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE

    source = """
def custom_layers(hidden_dim, num_competitions):
    return {
        "grl": GradientReversal(lambda_=1.0),
        "mlp": torch.nn.Sequential(
            torch.nn.Linear(hidden_dim, hidden_dim),
            torch.nn.GELU(),
            torch.nn.Linear(hidden_dim, num_competitions),
        ),
    }

def custom_embed(self, encoder_output, attention_mask):
    cls = encoder_output[:, 0]
    return self.mlp(self.grl(cls))
"""
    ok, reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert ok, f"expected valid, got: {reason}"


def test_f2v_adversary_validation_profile_rejects_os_system():
    """Seeds invoking os.system are rejected (AST allowlist defense)."""
    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE

    source = """
def custom_layers(hidden_dim, num_competitions):
    os.system("echo pwned")
    return {"grl": GradientReversal(lambda_=1.0),
            "classifier": torch.nn.Linear(hidden_dim, num_competitions)}

def custom_embed(self, encoder_output, attention_mask):
    return self.classifier(self.grl(encoder_output[:, 0]))
"""
    ok, _reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert not ok


def test_f2v_adversary_validation_profile_rejects_in_body_imports():
    """Seeds attempting imports INSIDE a validated function body are rejected.

    Note: the existing code_validator.py only walks function bodies (not module-level).
    Module-level imports are caught at runtime by ``exec(..., {"__builtins__": {}})``
    which makes ``__import__`` unavailable — a runtime defense-in-depth layer.
    """
    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE

    source = """
def custom_layers(hidden_dim, num_competitions):
    import subprocess  # inside function body — caught by AST visitor
    return {"grl": GradientReversal(lambda_=1.0),
            "classifier": torch.nn.Linear(hidden_dim, num_competitions)}

def custom_embed(self, encoder_output, attention_mask):
    return self.classifier(self.grl(encoder_output[:, 0]))
"""
    ok, _reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert not ok


def test_f2v_adversary_validation_profile_rejects_wrong_layers_signature():
    """Seed with wrong custom_layers parameter list is rejected."""
    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE

    source = """
def custom_layers(hidden_dim):
    return {"grl": GradientReversal(lambda_=1.0),
            "classifier": torch.nn.Linear(hidden_dim, 22)}

def custom_embed(self, encoder_output, attention_mask):
    return self.classifier(self.grl(encoder_output[:, 0]))
"""
    ok, _reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert not ok


def _load_seed_module(seed_filename: str):
    """Read, validate, and exec a seed file; return the restricted_globals dict."""
    from pathlib import Path

    import torch

    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE
    from evolve.targets.scoutgpt.building_blocks import (
        AdaLNZero,
        AdaptiveBandwidth,
        CompetitiveGate,
        CrossLayer,
        GradientReversal,
        HyperLinear,
        KANLayer,
        MoERouter,
        RatioGate,
    )

    path = Path("src/evolve/targets/football2vec/seed_programs_stage2") / seed_filename
    source = path.read_text(encoding="utf-8")

    ok, reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert ok, f"{seed_filename}: validation failed: {reason}"

    restricted_globals = {
        "torch": torch,
        "math": __import__("math"),
        "MoERouter": MoERouter,
        "HyperLinear": HyperLinear,
        "KANLayer": KANLayer,
        "AdaLNZero": AdaLNZero,
        "CrossLayer": CrossLayer,
        "CompetitiveGate": CompetitiveGate,
        "GradientReversal": GradientReversal,
        "AdaptiveBandwidth": AdaptiveBandwidth,
        "RatioGate": RatioGate,
        "__builtins__": {},
    }
    exec(source, restricted_globals)  # noqa: S102
    return restricted_globals


def _build_seed_adversary(seed_filename: str, hidden_dim: int, num_competitions: int):
    """Build a DynamicAdversary wrapper from the seed's custom_layers + custom_embed."""
    import torch.nn as nn

    restricted_globals = _load_seed_module(seed_filename)

    layers_fn = restricted_globals["custom_layers"]
    forward_fn = restricted_globals["custom_embed"]
    layers_dict = layers_fn(hidden_dim, num_competitions)
    assert "grl" in layers_dict, f"{seed_filename}: custom_layers dict must include 'grl' key"

    class _DynamicAdversary(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            for name, mod in layers_dict.items():
                self.register_module(name, mod)

        def forward(self, encoder_output, attention_mask):
            return forward_fn(self, encoder_output, attention_mask)

    return _DynamicAdversary()


def test_seed_deep_mlp_2layer_loads_and_forwards():
    """deep_mlp_2layer seed parses, validates, exec's, and forward produces correct shape."""
    import torch

    adv = _build_seed_adversary("deep_mlp_2layer.py", hidden_dim=192, num_competitions=22)
    # Input: per-token encoder output + extended mask (CLS included).
    x = torch.randn(4, 17, 192)  # CLS + 16 tokens
    mask = torch.ones(4, 17, dtype=torch.bool)
    logits = adv(x, mask)
    assert logits.shape == (4, 22), f"expected (4, 22), got {logits.shape}"
    assert hasattr(adv, "grl"), "adversary must expose .grl for per-epoch lambda injection"


def test_seed_deep_mlp_3layer_loads_and_forwards():
    import torch

    adv = _build_seed_adversary("deep_mlp_3layer.py", hidden_dim=192, num_competitions=22)
    assert adv(torch.randn(4, 17, 192), torch.ones(4, 17, dtype=torch.bool)).shape == (4, 22)


def test_seed_cross_attention_adversary_loads_and_forwards():
    """cross_attention_adversary: verify per-token processing with partial mask."""
    import torch

    adv = _build_seed_adversary("cross_attention_adversary.py", hidden_dim=192, num_competitions=22)
    x = torch.randn(4, 17, 192)
    # All valid positions.
    assert adv(x, torch.ones(4, 17, dtype=torch.bool)).shape == (4, 22)
    # Half-masked sequence.
    mask = torch.ones(4, 17, dtype=torch.bool)
    mask[:, 9:] = False
    assert adv(x, mask).shape == (4, 22)


def test_seed_attention_pool_head_loads_and_forwards():
    import torch

    adv = _build_seed_adversary("attention_pool_head.py", hidden_dim=192, num_competitions=22)
    assert adv(torch.randn(4, 17, 192), torch.ones(4, 17, dtype=torch.bool)).shape == (4, 22)


def test_seed_residual_mlp_loads_and_forwards():
    import torch

    adv = _build_seed_adversary("residual_mlp.py", hidden_dim=192, num_competitions=22)
    assert adv(torch.randn(4, 17, 192), torch.ones(4, 17, dtype=torch.bool)).shape == (4, 22)


def test_seed_dual_head_ensemble_loads_and_forwards():
    import torch

    adv = _build_seed_adversary("dual_head_ensemble.py", hidden_dim=192, num_competitions=22)
    assert adv(torch.randn(4, 17, 192), torch.ones(4, 17, dtype=torch.bool)).shape == (4, 22)


def test_apply_program_adversary_builds_dynamic_wrapper(tmp_path):
    """_apply_program_adversary execs a seed and returns a DynamicAdversary module
    with .grl attribute for per-epoch lambda injection and correct forward shape."""
    import torch

    from evolve.targets.football2vec.evaluator import _apply_program_adversary

    seed_path = tmp_path / "simple.py"
    seed_path.write_text(
        """
def custom_layers(hidden_dim, num_competitions):
    return {
        "grl": GradientReversal(lambda_=1.0),
        "fc": torch.nn.Linear(hidden_dim, num_competitions),
    }

def custom_embed(self, encoder_output, attention_mask):
    return self.fc(self.grl(encoder_output[:, 0]))
""",
        encoding="utf-8",
    )

    adv = _apply_program_adversary(str(seed_path), hidden_dim=64, num_competitions=4, device=torch.device("cpu"))
    assert isinstance(adv, torch.nn.Module)
    assert hasattr(adv, "grl")
    # lambda_val attribute exists (for per-epoch patching from the training loop).
    assert hasattr(adv.grl, "lambda_")
    out = adv(torch.randn(2, 8, 64), torch.ones(2, 8, dtype=torch.bool))
    assert out.shape == (2, 4)


def test_apply_program_adversary_rejects_missing_grl_key(tmp_path):
    """Seed's custom_layers must include 'grl' key for per-epoch lambda injection."""
    import pytest
    import torch

    from evolve.targets.football2vec.evaluator import _apply_program_adversary

    seed_path = tmp_path / "bad.py"
    seed_path.write_text(
        """
def custom_layers(hidden_dim, num_competitions):
    return {"fc": torch.nn.Linear(hidden_dim, num_competitions)}

def custom_embed(self, encoder_output, attention_mask):
    return self.fc(encoder_output[:, 0])
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="'grl'"):
        _apply_program_adversary(str(seed_path), hidden_dim=64, num_competitions=4, device=torch.device("cpu"))


def test_apply_program_adversary_rejects_missing_custom_embed(tmp_path):
    """Seed must define both custom_layers AND custom_embed."""
    import pytest
    import torch

    from evolve.targets.football2vec.evaluator import _apply_program_adversary

    seed_path = tmp_path / "bad.py"
    seed_path.write_text(
        """
def custom_layers(hidden_dim, num_competitions):
    return {"grl": GradientReversal(lambda_=1.0),
            "fc": torch.nn.Linear(hidden_dim, num_competitions)}
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="custom_embed"):
        _apply_program_adversary(str(seed_path), hidden_dim=64, num_competitions=4, device=torch.device("cpu"))


def test_orchestrator_script_imports_and_has_main():
    """The PEP 723 orchestrator script imports without heavy side effects and exposes
    the expected top-level variables + main() function."""
    import importlib.util
    from pathlib import Path

    path = Path("scripts/evaluate_football2vec_l2_adversary_seeds.py")
    assert path.exists(), f"orchestrator not at {path}"
    spec = importlib.util.spec_from_file_location("ev2_orchestrator", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert hasattr(module, "main")
    assert callable(module.main)
    assert hasattr(module, "_VARIANTS")

    # Baseline + 6 seeds.
    variants = module._VARIANTS
    assert len(variants) == 7
    variant_names = {v[0] for v in variants}
    expected = {
        "linear",
        "deep_mlp_2layer",
        "deep_mlp_3layer",
        "cross_attention_adversary",
        "attention_pool_head",
        "residual_mlp",
        "dual_head_ensemble",
    }
    assert variant_names == expected

    # Fitness weights match the spec.
    assert module._FITNESS_W_MLM == 0.4
    assert module._FITNESS_W_DEBIAS == 0.6
    assert module._EPOCHS == 30
    assert module._SEED == 42

    # Dispatch target name matches the alias module.
    assert module._TARGET == "football2vec_stage2"
    # Remote hosts are spec'd for Media-PC + DGX Spark (both absolute venv python paths).
    assert set(module._REMOTE_HOSTS) == {"media", "spark"}
    for alias in ("media", "spark"):
        entry = module._REMOTE_HOSTS[alias]
        assert entry["host"].count("@") == 1
        assert entry["remote_dir"].startswith("/")
        assert entry["venv_python"].startswith("/")
    # Public helper surface exists.
    assert callable(module._build_pool)
    assert callable(module._deploy_to_remote)
    assert callable(module._smoke_test_remote)

    # Smoke test verifies the full stage-2 evaluator dep surface (not just torch).
    # This list must cover every top-level import the evaluator transitively triggers
    # — misprovisioned remotes must be caught pre-dispatch, not mid-training.
    required = set(module._REMOTE_REQUIRED_IMPORTS)
    required_mods = (
        "torch",
        "safetensors",
        "huggingface_hub",
        "datasets",
        "pandas",
        "numpy",
        "sklearn.model_selection",
        "scipy",
    )
    for mod in required_mods:
        assert mod in required, f"smoke test missing required module: {mod}"


def test_football2vec_stage2_alias_target_imports():
    """The alias target module exposes train_and_evaluate bound to stage-2 function."""
    from evolve.targets.football2vec import VALIDATION_PROFILE as PARENT_PROFILE
    from evolve.targets.football2vec.evaluator import train_and_evaluate_stage2
    from evolve.targets.football2vec_stage2 import VALIDATION_PROFILE
    from evolve.targets.football2vec_stage2.evaluator import (
        _apply_program_adversary,
        train_and_evaluate,
    )

    # train_and_evaluate IS train_and_evaluate_stage2 (direct alias, not a wrapper).
    assert train_and_evaluate is train_and_evaluate_stage2
    # Validation profile is shared with the parent football2vec target.
    assert VALIDATION_PROFILE is PARENT_PROFILE
    # _apply_program_adversary re-exported so the evaluator path works identically.
    assert callable(_apply_program_adversary)


def test_card_wf_evolve_football2vec_l2_stage2_yaml_parses():
    """New workflow card parses via WorkflowCard.from_yaml_file and carries expected fields."""
    from pathlib import Path

    from workflows.card import WorkflowCard

    card_path = Path("workflow-cards/wf-evolve-football2vec-l2-stage2.yaml")
    assert card_path.exists()
    card = WorkflowCard.from_yaml_file(card_path)
    assert card.id == "wf-evolve-football2vec-l2-stage2"
    assert card.type == "training"
    # Execution phase parity enforced by a separate repo test (test_card_cost_phase_parity);
    # here just spot-check that the training phase is populated.
    assert card.execution is not None
    assert card.execution.training is not None
    assert card.cost is not None
    assert card.cost.training is not None
    # Source links point at the new EV2 modules.
    source_links = card.links.source_code if card.links else []
    flat = " ".join(str(x) for x in source_links)
    assert "seed_programs_stage2" in flat
    assert "football2vec_adversary" in flat
    """Seed with wrong custom_embed parameter list is rejected."""
    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE

    source = """
def custom_layers(hidden_dim, num_competitions):
    return {"grl": GradientReversal(lambda_=1.0),
            "classifier": torch.nn.Linear(hidden_dim, num_competitions)}

def custom_embed(self, encoder_output):
    return self.classifier(self.grl(encoder_output[:, 0]))
"""
    ok, _reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert not ok

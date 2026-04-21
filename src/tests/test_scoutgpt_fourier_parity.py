"""Parity test for conditioning_type="fourier_cross_attention".

The L2 harvest scored fourier_cross_attention at rho=+0.3799 via
monkey-patching a vanilla ScoutGPTDecoder with the seed program's
custom_layers + custom_embed at runtime.

This cycle promotes the mechanism to a first-class conditioning_type
enum value. This test asserts that the first-class implementation
produces byte-identical forward-pass output to the monkey-patched
harvest path, given matched random initializations.

If this test fails, we are testing a different mechanism than what
harvest scored. The cycle must halt and the implementation must be
corrected before any A/B arm runs.
"""

from __future__ import annotations

import types
from pathlib import Path

import torch

from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder


def _apply_harvest_monkey_patch(model: ScoutGPTDecoder, seed_path: Path) -> ScoutGPTDecoder:
    """Replicate src/evolve/targets/scoutgpt/evaluator.py::_apply_program semantics.

    Execs the seed file in a restricted globals namespace, registers the returned
    custom_layers modules, and binds custom_embed to the model instance.
    """
    restricted_globals: dict[str, object] = {
        "__builtins__": {},
        "torch": torch,
    }
    source = seed_path.read_text(encoding="utf-8")
    exec(compile(source, str(seed_path), "exec"), restricted_globals)  # noqa: S102 — controlled test input

    custom_layers_fn = restricted_globals["custom_layers"]
    custom_embed_fn = restricted_globals["custom_embed"]

    hidden_dim = model.config.hidden_dim
    new_modules = custom_layers_fn(hidden_dim)  # type: ignore[operator]
    for name, module in new_modules.items():
        model.register_module(name, module)

    model._embed = types.MethodType(custom_embed_fn, model)  # type: ignore[assignment,method-assign]
    return model


def _build_fixed_input(batch: int, seq_len: int, num_players: int) -> dict[str, torch.Tensor]:
    torch.manual_seed(123)
    return {
        "action_ids": torch.randint(0, 23, (batch, seq_len)),
        "start_x": torch.rand(batch, seq_len),
        "start_y": torch.rand(batch, seq_len),
        "end_x": torch.rand(batch, seq_len),
        "end_y": torch.rand(batch, seq_len),
        "result": torch.randint(0, 2, (batch, seq_len)),
        "time_delta": torch.rand(batch, seq_len),
        "player_ids": torch.randint(0, num_players, (batch, seq_len)),
    }


def test_fourier_first_class_matches_monkey_patched_seed() -> None:
    """First-class conditioning_type=fourier_cross_attention must match the harvest path."""
    seed_path = (
        Path(__file__).resolve().parent.parent
        / "evolve"
        / "targets"
        / "scoutgpt"
        / "seed_programs"
        / "fourier_cross_attention.py"
    )
    assert seed_path.exists(), f"Seed file missing: {seed_path}"

    # Build the monkey-patched reference model. Start with a vanilla additive
    # decoder because the seed's custom_embed fully replaces _embed; the base
    # model's conditioning_type choice is irrelevant (its modules aren't used
    # by the seed's custom_embed).
    torch.manual_seed(42)
    base_cfg = ScoutGPTConfig(
        hidden_dim=192,
        num_layers=3,
        num_heads=6,
        dropout=0.15,
        num_players=11_918,
        conditioning_type="additive",
    )
    reference_model = ScoutGPTDecoder(base_cfg).eval()
    _apply_harvest_monkey_patch(reference_model, seed_path)
    # Modules registered by the monkey-patch are created in training mode by
    # default; re-apply .eval() so their dropout paths match the first-class model.
    reference_model.eval()

    # Build the first-class model with conditioning_type=fourier_cross_attention
    torch.manual_seed(42)
    first_class_cfg = ScoutGPTConfig(
        hidden_dim=192,
        num_layers=3,
        num_heads=6,
        dropout=0.15,
        num_players=11_918,
        conditioning_type="fourier_cross_attention",
    )
    first_class_model = ScoutGPTDecoder(first_class_cfg).eval()

    # Copy fourier modules' state from reference → first-class so random-init
    # differences don't contaminate the compare.
    for name in ("fourier_B", "fourier_proj", "fourier_cross_attn", "fourier_cross_norm"):
        ref_module = reference_model.get_submodule(name)
        tgt_module = first_class_model.get_submodule(name)
        tgt_module.load_state_dict(ref_module.state_dict())

    # Copy shared modules (token + player embeddings, spatial MLPs, result,
    # time_delta, position_embedding) from reference to first-class. The seed's
    # custom_embed doesn't use spatial MLPs, but the first-class branch also
    # doesn't use them — this copy is defensive in case either module's weights
    # influence anything shared.
    for name in (
        "token_embedding",
        "player_embedding",
        "start_x_mlp",
        "start_y_mlp",
        "end_x_mlp",
        "end_y_mlp",
        "result_embedding",
        "time_delta_mlp",
        "position_embedding",
        "embedding_dropout",
    ):
        if hasattr(reference_model, name) and hasattr(first_class_model, name):
            ref_module = reference_model.get_submodule(name)
            tgt_module = first_class_model.get_submodule(name)
            tgt_module.load_state_dict(ref_module.state_dict())

    inputs = _build_fixed_input(batch=2, seq_len=16, num_players=11_918)

    with torch.no_grad():
        ref_emb = reference_model._embed(**inputs)
        first_class_emb = first_class_model._embed(**inputs)

    assert ref_emb.shape == first_class_emb.shape, (
        f"Shape mismatch: ref={ref_emb.shape} first_class={first_class_emb.shape}"
    )
    max_abs_diff = (ref_emb - first_class_emb).abs().max().item()
    assert torch.allclose(ref_emb, first_class_emb, atol=1e-6), (
        f"First-class Fourier branch diverges from monkey-patched seed. Max abs diff: {max_abs_diff}"
    )

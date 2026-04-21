"""Parity test for conditioning_type="swiglu".

Analogue of test_scoutgpt_fourier_parity.py for the second promoted
mechanism. See that file for the full rationale.
"""

from __future__ import annotations

import types
from pathlib import Path

import torch

from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder


def _apply_harvest_monkey_patch(model: ScoutGPTDecoder, seed_path: Path) -> ScoutGPTDecoder:
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


def test_swiglu_first_class_matches_monkey_patched_seed() -> None:
    seed_path = (
        Path(__file__).resolve().parent.parent
        / "evolve"
        / "targets"
        / "scoutgpt"
        / "seed_programs"
        / "swiglu_conditioning.py"
    )
    assert seed_path.exists(), f"Seed file missing: {seed_path}"

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

    torch.manual_seed(42)
    first_class_cfg = ScoutGPTConfig(
        hidden_dim=192,
        num_layers=3,
        num_heads=6,
        dropout=0.15,
        num_players=11_918,
        conditioning_type="swiglu",
    )
    first_class_model = ScoutGPTDecoder(first_class_cfg).eval()

    # Copy swiglu modules' state from reference → first-class so random-init
    # differences don't contaminate the compare.
    for name in ("swiglu_w1", "swiglu_w2", "swiglu_proj", "swiglu_norm"):
        ref_module = reference_model.get_submodule(name)
        tgt_module = first_class_model.get_submodule(name)
        tgt_module.load_state_dict(ref_module.state_dict())

    # Copy shared modules (the swiglu seed uses the existing spatial MLPs,
    # token + player + result + time_delta embeddings — all must match).
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
        f"First-class Swiglu branch diverges from monkey-patched seed. Max abs diff: {max_abs_diff}"
    )

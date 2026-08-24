"""ScoutGPT ``num_players`` must be data-driven (``len(player_id_map)``), never the hardcoded
ScoutGPTConfig default (11_918) that silently drifted behind the data and overflowed the player
embedding at 12_054 players after the sk-4.90.1 rebuild (CUDA gather index-out-of-bounds).
"""

from __future__ import annotations

import argparse

import pytest


def _args(variant: str = "learnable") -> argparse.Namespace:
    """Minimal Namespace with the attrs _build_scoutgpt_config reads."""
    return argparse.Namespace(
        variant=variant,
        conditioning_type=None,
        hidden_dim=None,
        num_layers=None,
        num_heads=None,
    )


def test_build_config_uses_caller_num_players_not_hardcoded_default() -> None:
    import scripts.train_scoutgpt_hf as trainer
    from analytics.scoutgpt_decoder import ScoutGPTConfig

    # The dataclass default is the value that drifts behind the data — documented here so a
    # future change to it is a conscious one, and to prove the derived value differs from it.
    assert ScoutGPTConfig().num_players == 11_918

    # The sk-4.90.1 reality (more players than the default) must flow through unchanged.
    assert trainer._build_scoutgpt_config(_args(), num_players=12_054).num_players == 12_054
    # An arbitrary value too — proving it is the CALLER's count, not any constant.
    assert trainer._build_scoutgpt_config(_args(), num_players=7).num_players == 7


def test_player_index_guard_passes_when_indices_fit() -> None:
    import scripts.train_scoutgpt_hf as trainer

    # max index 11 < num_players 12 (== len(map)) -> no raise.
    trainer._assert_player_indices_in_range([[0, 5, 11], [3, 7]], num_players=12, n_map=12)


def test_player_index_guard_raises_on_overflow() -> None:
    import scripts.train_scoutgpt_hf as trainer

    # This is exactly the 12_054-vs-11_918 shape: an index at num_players overflows the embedding.
    with pytest.raises(RuntimeError, match=r"num_players 11918 .* embedding would overflow"):
        trainer._assert_player_indices_in_range([[0, 11_917], [11_918]], num_players=11_918, n_map=12_054)


def test_evolve_evaluator_forces_num_players_from_data() -> None:
    """The evolve research evaluator (sibling of the trainer) must ALSO derive num_players from
    the data, ignoring any candidate value or the ScoutGPTConfig default — same 12_054-vs-11_918
    player-embedding overflow class. num_players is data-determined, never a search dimension.
    """
    from evolve.targets.scoutgpt.evaluator import _build_config_from_candidate

    # The candidate carries the stale/default 11_918; the data has 12_054 → the data must win.
    cfg = _build_config_from_candidate({"num_players": 11_918, "hidden_dim": 128}, num_players=12_054)
    assert cfg.num_players == 12_054
    assert cfg.hidden_dim == 128  # other architecture keys still pass through

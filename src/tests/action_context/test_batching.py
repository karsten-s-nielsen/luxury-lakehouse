"""Unit tests for analytics.action_context.batching (ADR-047 amendment 2).

The frame batch size is part of the domain contract (window-dependent features
+ M13 action ownership), so its resolution — per-provider defaults, env hook,
run override — must be deterministic and fail loud on nonsense input.
"""

from __future__ import annotations

import pytest

from analytics.action_context import batching
from analytics.action_context.batching import (
    DEFAULT_FRAME_BATCH_SIZE,
    FRAME_BATCH_SIZE_BY_PROVIDER,
    resolve_frame_batch_size,
)


class TestProviderDefaults:
    def test_every_tracking_provider_gets_the_prod_proven_floor(self) -> None:
        # The 2026-06-11 OOM census (run 883267532931612): 13/16 tracking units
        # OOMed the 1 GB serverless UDF cap at 2500 — gradientsports 4/4,
        # idsse 4/4, metrica 3/4, skillcorner 2/4. Until a provider re-earns a
        # larger size with a passing scoped prod run on the current column set,
        # everyone resolves the universally prod-proven 250.
        for provider in ("idsse", "gradientsports", "metrica", "skillcorner"):
            assert resolve_frame_batch_size(provider) == 250, provider

    def test_unknown_provider_falls_back_to_default(self) -> None:
        assert resolve_frame_batch_size("some-future-provider") == DEFAULT_FRAME_BATCH_SIZE

    def test_default_is_the_conservative_floor(self) -> None:
        # Every per-provider entry must be a deliberate, documented INCREASE over
        # the floor — a map entry BELOW the default would mean the default itself
        # is unproven for that provider, which contradicts its definition.
        assert DEFAULT_FRAME_BATCH_SIZE == 250
        for provider, size in FRAME_BATCH_SIZE_BY_PROVIDER.items():
            assert size >= DEFAULT_FRAME_BATCH_SIZE, (provider, size)


class TestOverridePrecedence:
    def test_explicit_override_beats_everything(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(batching.ENV_VAR, "777")
        assert resolve_frame_batch_size("idsse", override=1000) == 1000

    def test_env_var_beats_provider_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(batching.ENV_VAR, "1250")
        assert resolve_frame_batch_size("idsse") == 1250
        assert resolve_frame_batch_size("skillcorner") == 1250

    def test_blank_env_var_is_ignored(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(batching.ENV_VAR, "   ")
        assert resolve_frame_batch_size("idsse") == 250


class TestValidation:
    @pytest.mark.parametrize("bad", [0, -1, -2500])
    def test_nonpositive_override_raises(self, bad: int) -> None:
        with pytest.raises(ValueError, match="must be > 0"):
            resolve_frame_batch_size("idsse", override=bad)

    def test_non_integer_env_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(batching.ENV_VAR, "lots")
        with pytest.raises(ValueError, match="positive integer"):
            resolve_frame_batch_size("idsse")

    def test_nonpositive_env_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(batching.ENV_VAR, "0")
        with pytest.raises(ValueError, match="must be > 0"):
            resolve_frame_batch_size("idsse")

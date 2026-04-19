"""Tests for src/analytics/rope.py — rotary position embedding primitive (Su et al. 2021).

Covers:
    - RotaryEmbedding precomputed cos/sin tables.
    - rotate_half helper (LLaMA-style first-half / second-half split).
    - apply_rotary_pos_emb rotation applied to (q, k) tensors.
    - Defining property: dot product <q_m, k_n> depends only on (m - n).
"""

from __future__ import annotations

import pytest

pytest.importorskip("torch")

import torch

from analytics.rope import RotaryEmbedding, apply_rotary_pos_emb, rotate_half


class TestRotaryEmbedding:
    """Precomputed RoPE cos/sin tables."""

    def test_forward_returns_cos_sin_with_expected_shape(self) -> None:
        """forward(seq_len) → (cos, sin) each shaped (seq_len, head_dim)."""
        rope = RotaryEmbedding(head_dim=32, max_seq_len=128)
        cos, sin = rope(seq_len=64)
        assert cos.shape == (64, 32)
        assert sin.shape == (64, 32)

    def test_rejects_odd_head_dim(self) -> None:
        """head_dim must be even — pairs (d_i, d_{i + d/2}) rotate together."""
        with pytest.raises(ValueError, match="even"):
            RotaryEmbedding(head_dim=31, max_seq_len=128)

    def test_rejects_seq_len_exceeding_max(self) -> None:
        """forward(seq_len > max_seq_len) raises rather than silently indexing out of bounds."""
        rope = RotaryEmbedding(head_dim=32, max_seq_len=128)
        with pytest.raises(ValueError, match="max_seq_len"):
            rope(seq_len=129)


class TestRotateHalf:
    """LLaMA-style half-rotation helper."""

    def test_hand_constructed_tensor(self) -> None:
        """rotate_half(x) = concat(-second_half, first_half) along last dim."""
        x = torch.tensor([[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]])
        expected = torch.tensor([[-4.0, -5.0, -6.0, 1.0, 2.0, 3.0]])
        assert torch.equal(rotate_half(x), expected)


class TestApplyRotaryPosEmb:
    """RoPE rotation applied to Q/K tensors."""

    def test_identity_at_position_zero(self) -> None:
        """At position 0, cos=1 and sin=0, so output == input (no rotation)."""
        torch.manual_seed(0)
        head_dim = 8
        rope = RotaryEmbedding(head_dim=head_dim, max_seq_len=4)
        cos, sin = rope(seq_len=4)
        q = torch.randn(2, 3, 4, head_dim)  # (batch, num_heads, seq_len, head_dim)
        k = torch.randn(2, 3, 4, head_dim)
        q_rot, k_rot = apply_rotary_pos_emb(q, k, cos, sin)
        # Position 0 slice is at index 2 (seq dim): q_rot[..., 0, :] should match q[..., 0, :]
        assert torch.allclose(q_rot[..., 0, :], q[..., 0, :], atol=1e-6)
        assert torch.allclose(k_rot[..., 0, :], k[..., 0, :], atol=1e-6)

    def test_preserves_per_pair_norm(self) -> None:
        """Rotation preserves ||(x_i, x_{i + d/2})||² for every pair at every position.

        Given pair ``(a, b)`` rotated to ``(a cos θ - b sin θ, a sin θ + b cos θ)``:
            (a cos - b sin)² + (a sin + b cos)² = a² + b²
        """
        torch.manual_seed(1)
        head_dim = 16
        seq_len = 32
        rope = RotaryEmbedding(head_dim=head_dim, max_seq_len=seq_len)
        cos, sin = rope(seq_len=seq_len)
        q = torch.randn(2, 4, seq_len, head_dim)
        k = torch.randn(2, 4, seq_len, head_dim)
        q_rot, k_rot = apply_rotary_pos_emb(q, k, cos, sin)
        # Split each tensor into (first_half, second_half) then check per-pair norms match.
        for original, rotated in [(q, q_rot), (k, k_rot)]:
            a, b = original.chunk(2, dim=-1)
            a_rot, b_rot = rotated.chunk(2, dim=-1)
            original_sq = a**2 + b**2
            rotated_sq = a_rot**2 + b_rot**2
            assert torch.allclose(original_sq, rotated_sq, atol=1e-5)

    def test_reference_parity_per_pair_rotation(self) -> None:
        """Matches an explicit per-pair rotation matrix applied over Python loops.

        For each pair ``(x[i], x[i + d/2])`` at position ``m`` with frequency
        ``θ_i = base^(-2i/d)`` we expect:
            x_new[i]       = x[i] * cos(m θ_i) - x[i + d/2] * sin(m θ_i)
            x_new[i + d/2] = x[i] * sin(m θ_i) + x[i + d/2] * cos(m θ_i)
        """
        torch.manual_seed(2)
        head_dim = 8
        seq_len = 5
        base = 10000.0
        q = torch.randn(1, 2, seq_len, head_dim)
        k = torch.randn(1, 2, seq_len, head_dim)

        # Build reference output via explicit loop over positions and pairs.
        half = head_dim // 2
        inv_freq = 1.0 / (base ** (torch.arange(0, head_dim, 2, dtype=torch.float32) / head_dim))
        q_ref = q.clone()
        k_ref = k.clone()
        for m in range(seq_len):
            for i in range(half):
                theta = m * inv_freq[i]
                c, s = torch.cos(theta), torch.sin(theta)
                q_i = q[..., m, i]
                q_ih = q[..., m, i + half]
                q_ref[..., m, i] = q_i * c - q_ih * s
                q_ref[..., m, i + half] = q_i * s + q_ih * c
                k_i = k[..., m, i]
                k_ih = k[..., m, i + half]
                k_ref[..., m, i] = k_i * c - k_ih * s
                k_ref[..., m, i + half] = k_i * s + k_ih * c

        rope = RotaryEmbedding(head_dim=head_dim, max_seq_len=seq_len, base=base)
        cos, sin = rope(seq_len=seq_len)
        q_rot, k_rot = apply_rotary_pos_emb(q, k, cos, sin)
        assert torch.allclose(q_rot, q_ref, atol=1e-6)
        assert torch.allclose(k_rot, k_ref, atol=1e-6)

    def test_relative_position_dot_product(self) -> None:
        """Defining RoPE property: ``<rotate(q, m), rotate(k, n)>`` depends only on ``m - n``.

        Broadcasts the same q, k vectors across all sequence positions then
        verifies dot products at position pairs with the same relative
        offset are equal regardless of absolute positions.
        """
        torch.manual_seed(3)
        head_dim = 16
        seq_len = 32
        rope = RotaryEmbedding(head_dim=head_dim, max_seq_len=seq_len)
        cos, sin = rope(seq_len=seq_len)
        q_vec = torch.randn(head_dim)
        k_vec = torch.randn(head_dim)
        # Broadcast the same q, k vector across every sequence position.
        q = q_vec.view(1, 1, 1, head_dim).expand(1, 1, seq_len, head_dim).contiguous()
        k = k_vec.view(1, 1, 1, head_dim).expand(1, 1, seq_len, head_dim).contiguous()
        q_rot, k_rot = apply_rotary_pos_emb(q, k, cos, sin)
        # dot_table[m, n] = <q_rot[..., m, :], k_rot[..., n, :]>
        dot_table = torch.einsum("bhmd,bhnd->bhmn", q_rot, k_rot)[0, 0]
        # For every fixed relative offset delta = m - n, dot values should be constant.
        for delta in [-5, -1, 0, 1, 7]:
            values = [dot_table[m, m - delta].item() for m in range(seq_len) if 0 <= m - delta < seq_len]
            assert len(values) >= 2
            assert max(values) - min(values) < 1e-4, (
                f"dot products depend on absolute position for delta={delta}: "
                f"min={min(values):.6f} max={max(values):.6f}"
            )

    def test_autograd_passes_through(self) -> None:
        """Gradients flow through both q and k with no NaN / inf and correct shapes."""
        torch.manual_seed(4)
        head_dim = 8
        seq_len = 6
        rope = RotaryEmbedding(head_dim=head_dim, max_seq_len=seq_len)
        cos, sin = rope(seq_len=seq_len)
        q = torch.randn(2, 2, seq_len, head_dim, requires_grad=True)
        k = torch.randn(2, 2, seq_len, head_dim, requires_grad=True)
        q_rot, k_rot = apply_rotary_pos_emb(q, k, cos, sin)
        (q_rot.sum() + k_rot.sum()).backward()
        assert q.grad is not None and k.grad is not None
        assert q.grad.shape == q.shape
        assert k.grad.shape == k.shape
        assert torch.isfinite(q.grad).all()
        assert torch.isfinite(k.grad).all()

    def test_byte_equivalent_to_hf_llama_reference(self) -> None:
        """Cross-check: our RoPE produces bit-identical output to HF LLaMA's reference.

        Regression guard: if someone "simplifies" our implementation (e.g., swaps
        the pairing convention, changes the frequency formula, reorders sin/cos),
        this test catches divergence from the published reference.

        Reference transcribed verbatim from HuggingFace Transformers
        ``models/llama/modeling_llama.py`` (v4.45.x).
        """
        head_dim, num_heads, seq_len, batch = 32, 6, 20, 2

        # HF LLaMA-style frequency + table construction.
        inv_freq = 1.0 / (10000.0 ** (torch.arange(0, head_dim, 2, dtype=torch.float32) / head_dim))
        t = torch.arange(seq_len, dtype=torch.float32)
        freqs = torch.outer(t, inv_freq)
        emb = torch.cat((freqs, freqs), dim=-1)
        hf_cos = emb.cos()
        hf_sin = emb.sin()

        # HF LLaMA-style rotation helpers.
        def hf_rotate_half(x: torch.Tensor) -> torch.Tensor:
            x1 = x[..., : x.shape[-1] // 2]
            x2 = x[..., x.shape[-1] // 2 :]
            return torch.cat((-x2, x1), dim=-1)

        def hf_apply(
            q: torch.Tensor, k: torch.Tensor, cos: torch.Tensor, sin: torch.Tensor
        ) -> tuple[torch.Tensor, torch.Tensor]:
            cos_b = cos.unsqueeze(0).unsqueeze(0)
            sin_b = sin.unsqueeze(0).unsqueeze(0)
            return (q * cos_b) + (hf_rotate_half(q) * sin_b), (k * cos_b) + (hf_rotate_half(k) * sin_b)

        # Compare tables.
        rope = RotaryEmbedding(head_dim=head_dim, max_seq_len=seq_len)
        cos, sin = rope(seq_len=seq_len)
        assert torch.equal(cos, hf_cos), "cos table diverges from HF LLaMA reference"
        assert torch.equal(sin, hf_sin), "sin table diverges from HF LLaMA reference"

        # Compare rotations on the same random input.
        torch.manual_seed(0)
        q = torch.randn(batch, num_heads, seq_len, head_dim)
        k = torch.randn(batch, num_heads, seq_len, head_dim)
        mine_q, mine_k = apply_rotary_pos_emb(q, k, cos, sin)
        hf_q, hf_k = hf_apply(q, k, hf_cos, hf_sin)
        assert torch.equal(mine_q, hf_q), "Q rotation diverges from HF LLaMA reference"
        assert torch.equal(mine_k, hf_k), "K rotation diverges from HF LLaMA reference"

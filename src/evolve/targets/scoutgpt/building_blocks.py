"""Pre-validated building blocks for Level 2 code evolution.

These nn.Module classes encapsulate complex architectural patterns that
would be error-prone for an LLM to write from scratch (fiddly indexing,
parameter budgeting, numerical stability). They are exposed in the
restricted globals of the exec() environment so the LLM can instantiate
them in custom_layers() and call them in custom_embed().

Design principle: each block is a self-contained LEGO brick — the LLM
decides which bricks to use and how to compose them, but the internal
mechanics are pre-validated and crash-proof.
"""

from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F  # noqa: N812


class MoERouter(nn.Module):
    """Lightweight Mixture-of-Experts with top-k routing.

    A small router network selects the top-k experts based on a
    conditioning signal (e.g., player embedding). Each expert is an
    independent MLP. Outputs are weighted by routing probabilities.

    Usage in custom_layers::

        def custom_layers(hidden_dim):
            return {"moe": MoERouter(hidden_dim, n_experts=4, top_k=2)}

    Usage in custom_embed::

        routed = self.moe(action_emb, player_emb)  # (B, S, hd)

    Args:
        hidden_dim: Input and output dimension.
        n_experts: Number of expert MLPs. Default 4.
        top_k: Number of experts activated per token. Default 2.
        temperature: Softmax temperature for routing. Default 1.0.
    """

    def __init__(
        self,
        hidden_dim: int,
        n_experts: int = 4,
        top_k: int = 2,
        temperature: float = 1.0,
    ) -> None:
        super().__init__()
        self.n_experts = n_experts
        self.top_k = min(top_k, n_experts)
        self.temperature = temperature

        # Router: conditioning input -> expert logits
        self.router = nn.Linear(hidden_dim, n_experts)

        # Experts: independent 2-layer MLPs
        self.experts = nn.ModuleList([
            nn.Sequential(
                nn.Linear(hidden_dim, hidden_dim),
                nn.GELU(),
                nn.Linear(hidden_dim, hidden_dim),
            )
            for _ in range(n_experts)
        ])

    def forward(self, x: torch.Tensor, conditioning: torch.Tensor) -> torch.Tensor:
        """Route x through top-k experts based on conditioning signal.

        Args:
            x: Input tensor (B, S, hidden_dim) — e.g., action embedding.
            conditioning: Routing signal (B, S, hidden_dim) — e.g., player embedding.

        Returns:
            Weighted expert output (B, S, hidden_dim).
        """
        # Compute routing weights
        logits = self.router(conditioning) / self.temperature  # (B, S, n_experts)
        top_k_logits, top_k_indices = torch.topk(logits, self.top_k, dim=-1)  # (B, S, k)
        top_k_weights = F.softmax(top_k_logits, dim=-1)  # (B, S, k)

        # Evaluate all experts (simple loop — n_experts is small)
        expert_outputs = torch.stack([expert(x) for expert in self.experts], dim=-2)  # (B, S, n_experts, hd)

        # Gather top-k expert outputs
        k_indices = top_k_indices.unsqueeze(-1).expand(-1, -1, -1, x.size(-1))  # (B, S, k, hd)
        selected = torch.gather(expert_outputs, dim=-2, index=k_indices)  # (B, S, k, hd)

        # Weighted combination
        return (top_k_weights.unsqueeze(-1) * selected).sum(dim=-2)  # (B, S, hd)


class HyperLinear(nn.Module):
    """Hypernetwork that generates a linear transformation conditioned on input.

    A small MLP takes a conditioning signal (e.g., player embedding) and
    generates weights for a linear transformation applied to the input.
    Uses low-rank factorization to keep parameters manageable.

    Usage in custom_layers::

        def custom_layers(hidden_dim):
            return {"hyper": HyperLinear(hidden_dim, rank=16)}

    Usage in custom_embed::

        transformed = self.hyper(action_emb, player_emb)  # (B, S, hd)

    Args:
        hidden_dim: Input and output dimension.
        rank: Rank of the factorized weight generation. Default 16.
            Generated weight is W = A @ B where A is (hd, rank) and B is (rank, hd).
            Total generated params per token: 2 * hd * rank + hd (bias).
    """

    def __init__(self, hidden_dim: int, rank: int = 16) -> None:
        super().__init__()
        self.hidden_dim = hidden_dim
        self.rank = rank

        # Hypernetwork: conditioning -> factorized weight components + bias
        # Output size: rank * hd (A) + rank * hd (B) + hd (bias)
        output_size = 2 * rank * hidden_dim + hidden_dim
        self.hyper_net = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.GELU(),
            nn.Linear(hidden_dim // 2, output_size),
        )

    def forward(self, x: torch.Tensor, conditioning: torch.Tensor) -> torch.Tensor:
        """Apply a dynamically generated linear transformation to x.

        Args:
            x: Input tensor (B, S, hidden_dim) — e.g., action embedding.
            conditioning: Weight generator input (B, S, hidden_dim) — e.g., player embedding.

        Returns:
            Transformed tensor (B, S, hidden_dim).
        """
        hd, r = self.hidden_dim, self.rank
        params = self.hyper_net(conditioning)  # (B, S, output_size)

        # Split into factorized components
        a_flat = params[..., :r * hd]                          # (B, S, r*hd)
        b_flat = params[..., r * hd:2 * r * hd]               # (B, S, r*hd)
        bias = params[..., 2 * r * hd:]                        # (B, S, hd)

        # Reshape for batched matmul: x @ A @ B + bias
        a_mat = a_flat.reshape(*a_flat.shape[:-1], hd, r)      # (B, S, hd, r)
        b_mat = b_flat.reshape(*b_flat.shape[:-1], r, hd)      # (B, S, r, hd)

        # x: (B, S, hd) -> (B, S, 1, hd) @ (B, S, hd, r) -> (B, S, 1, r)
        mid = torch.matmul(x.unsqueeze(-2), a_mat).squeeze(-2)  # (B, S, r)
        # (B, S, 1, r) @ (B, S, r, hd) -> (B, S, 1, hd)
        out = torch.matmul(mid.unsqueeze(-2), b_mat).squeeze(-2)  # (B, S, hd)

        return out + bias


class KANLayer(nn.Module):
    """Kolmogorov-Arnold Network layer with learnable RBF activations.

    Instead of fixed activations at nodes (like ReLU in standard MLPs),
    KAN places learnable Gaussian Radial Basis Function activations on
    the edges. Each edge has a set of RBF centers and a learned width,
    enabling highly expressive function approximation with fewer parameters.

    Usage in custom_layers::

        def custom_layers(hidden_dim):
            return {"kan": KANLayer(hidden_dim, hidden_dim, n_basis=8)}

    Usage in custom_embed::

        out = self.kan(spatial_features)  # (B, S, hd)

    Args:
        in_dim: Input dimension.
        out_dim: Output dimension.
        n_basis: Number of RBF basis functions per edge. Default 8.
    """

    def __init__(self, in_dim: int, out_dim: int, n_basis: int = 8) -> None:
        super().__init__()
        self.in_dim = in_dim
        self.out_dim = out_dim
        self.n_basis = n_basis

        # RBF centers: spread uniformly in [-1, 1], one set shared across edges
        centers = torch.linspace(-1.0, 1.0, n_basis)
        self.register_buffer("centers", centers)  # (n_basis,)

        # Learnable width (inverse bandwidth) for the Gaussians
        self.log_sigma = nn.Parameter(torch.zeros(1))

        # Learnable weights: for each (in, out) edge, n_basis coefficients
        # Factored as two matrices to keep params reasonable
        self.edge_weights = nn.Parameter(torch.randn(in_dim, out_dim, n_basis) * 0.01)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Apply KAN transformation with learnable RBF activations.

        Args:
            x: Input tensor (..., in_dim). Works with any leading dimensions.

        Returns:
            Output tensor (..., out_dim).
        """
        sigma = self.log_sigma.exp().clamp(min=0.1)

        # Compute RBF activations: phi(x_i) for each input dimension
        # x: (..., in_dim) -> (..., in_dim, 1)
        # centers: (n_basis,) broadcast
        x_expanded = x.unsqueeze(-1)  # (..., in_dim, 1)
        rbf = torch.exp(-((x_expanded - self.centers) ** 2) / (2 * sigma ** 2))  # (..., in_dim, n_basis)

        # Weighted sum over basis functions, then sum over input dimensions
        # edge_weights: (in_dim, out_dim, n_basis)
        # rbf: (..., in_dim, n_basis) -> (..., in_dim, 1, n_basis)
        rbf_expanded = rbf.unsqueeze(-2)  # (..., in_dim, 1, n_basis)
        weighted = (rbf_expanded * self.edge_weights).sum(dim=-1)  # (..., in_dim, out_dim)
        return weighted.sum(dim=-2)  # (..., out_dim)

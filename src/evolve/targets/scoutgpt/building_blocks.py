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
        self.experts = nn.ModuleList(
            [
                nn.Sequential(
                    nn.Linear(hidden_dim, hidden_dim),
                    nn.GELU(),
                    nn.Linear(hidden_dim, hidden_dim),
                )
                for _ in range(n_experts)
            ]
        )

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
        a_flat = params[..., : r * hd]  # (B, S, r*hd)
        b_flat = params[..., r * hd : 2 * r * hd]  # (B, S, r*hd)
        bias = params[..., 2 * r * hd :]  # (B, S, hd)

        # Reshape for batched matmul: x @ A @ B + bias
        a_mat = a_flat.reshape(*a_flat.shape[:-1], hd, r)  # (B, S, hd, r)
        b_mat = b_flat.reshape(*b_flat.shape[:-1], r, hd)  # (B, S, r, hd)

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
        centers: torch.Tensor = self.centers  # type: ignore[assignment]  # registered buffer
        rbf = torch.exp(-((x_expanded - centers) ** 2) / (2 * sigma**2))  # (..., in_dim, n_basis)

        # Weighted sum over basis functions, then sum over input dimensions
        # edge_weights: (in_dim, out_dim, n_basis)
        # rbf: (..., in_dim, n_basis) -> (..., in_dim, 1, n_basis)
        rbf_expanded = rbf.unsqueeze(-2)  # (..., in_dim, 1, n_basis)
        weighted = (rbf_expanded * self.edge_weights).sum(dim=-1)  # (..., in_dim, out_dim)
        return weighted.sum(dim=-2)  # (..., out_dim)


class AdaLNZero(nn.Module):
    """Adaptive Layer Normalization with zero-initialized residual gates (DiT).

    The conditioning signal (e.g., player embedding) generates per-block
    scale, shift, AND residual gate parameters for a LayerNorm + residual
    connection. Superset of FiLM: modulates both the normalization AND
    the residual contribution.

    Zero-initialization: the gate projection starts at zero, so the block
    begins as a pure residual and conditioning is learned incrementally.

    Usage in custom_layers::

        def custom_layers(hidden_dim):
            return {"adaln": AdaLNZero(hidden_dim)}

    Usage in custom_embed::

        # x = pre-norm input, conditioning = player embedding
        normed, gate = self.adaln(x, player_emb)
        # Use normed as input to attention/MLP, gate to scale the residual:
        # x = x + gate * attention(normed)

    Reference: Peebles & Xie (2023), "Scalable Diffusion Models with
    Transformers" (DiT). Widely adopted in robotics (pi0, ACT2) 2024.

    Args:
        hidden_dim: Feature dimension.
        conditioning_dim: Conditioning input dimension. Default: same as hidden_dim.
    """

    def __init__(self, hidden_dim: int, conditioning_dim: int | None = None) -> None:
        super().__init__()
        c_dim = conditioning_dim or hidden_dim
        self.norm = nn.LayerNorm(hidden_dim)
        # Projects conditioning to (scale, shift, gate) — 3 * hidden_dim
        self.proj = nn.Linear(c_dim, 3 * hidden_dim)
        # Zero-init the gate portion so block starts as pure residual
        nn.init.zeros_(self.proj.weight[2 * hidden_dim :])
        nn.init.zeros_(self.proj.bias[2 * hidden_dim :])

    def forward(self, x: torch.Tensor, conditioning: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        """Apply adaptive layer norm and return (normed_output, residual_gate).

        Args:
            x: Input tensor (B, S, hidden_dim).
            conditioning: Conditioning signal (B, S, hidden_dim) or (B, hidden_dim).

        Returns:
            Tuple of (normed_modulated, gate) both (B, S, hidden_dim).
            Use as: x = x + gate * sublayer(normed_modulated)
        """
        params = self.proj(conditioning)  # (B, [S,] 3*hd)
        if params.dim() == 2:
            params = params.unsqueeze(1)  # (B, 1, 3*hd) — broadcast over seq
        scale, shift, gate = params.chunk(3, dim=-1)
        normed = self.norm(x) * (1 + scale) + shift
        return normed, gate


class CrossLayer(nn.Module):
    """DCN-V2 Cross Layer for explicit polynomial feature interactions.

    Computes bounded-degree feature crossing between a base embedding
    (e.g., player) and the current hidden state (e.g., action), producing
    higher-order interactions that FiLM (first-order only) cannot express.

    x_{l+1} = x_0 ⊙ (W · x_l + b) + x_l

    Stacking N cross layers produces N-th order feature interactions.

    Usage in custom_layers::

        def custom_layers(hidden_dim):
            return {"cross": CrossLayer(hidden_dim)}

    Usage in custom_embed::

        # x_0 = player_emb (base), x = action_emb (evolving state)
        x = self.cross(x, player_emb)  # one layer of polynomial crossing

    Reference: Wang et al. (2021), "DCN V2: Improved Deep & Cross Network."

    Args:
        hidden_dim: Feature dimension.
    """

    def __init__(self, hidden_dim: int) -> None:
        super().__init__()
        self.weight = nn.Linear(hidden_dim, hidden_dim, bias=True)

    def forward(self, x: torch.Tensor, x_base: torch.Tensor) -> torch.Tensor:
        """Apply one cross layer.

        Args:
            x: Current hidden state (B, S, hidden_dim).
            x_base: Base embedding for crossing (B, S, hidden_dim).

        Returns:
            Crossed output (B, S, hidden_dim).
        """
        return x_base * self.weight(x) + x


class CompetitiveGate(nn.Module):
    """Competitive advantage gate inspired by pitch control physics.

    Instead of gating by absolute feature values, this gate computes a
    *relative advantage* between two competing signals — e.g., "how much
    more relevant is this player than the average alternative?" The gate
    value depends on the difference between the primary and competitor
    scores, passed through a sigmoid with learnable sharpness.

    gate = sigmoid(k * (primary_score - competitor_score))

    This pattern appears in Spearman's pitch control (TTI advantage over
    nearest opponent) and is a domain-validated way to model competitive
    spatial interactions in football.

    Usage in custom_layers::

        def custom_layers(hidden_dim):
            return {"comp_gate": CompetitiveGate(hidden_dim)}

    Usage in custom_embed::

        # primary = player_emb, competitor = mean of all player embs or action_emb
        gated = self.comp_gate(action_emb, player_emb, action_emb)

    Args:
        hidden_dim: Feature dimension.
    """

    def __init__(self, hidden_dim: int) -> None:
        super().__init__()
        self.score_fn = nn.Linear(hidden_dim, hidden_dim)
        # Learnable sharpness — initialized to 1.0, can grow or shrink
        self.log_k = nn.Parameter(torch.zeros(1))

    def forward(
        self,
        x: torch.Tensor,
        primary: torch.Tensor,
        competitor: torch.Tensor,
    ) -> torch.Tensor:
        """Apply competitive gate to x.

        Args:
            x: Input to be gated (B, S, hidden_dim).
            primary: Signal favoring passage (B, S, hidden_dim) — e.g., player.
            competitor: Signal opposing passage (B, S, hidden_dim) — e.g., action mean.

        Returns:
            Gated output (B, S, hidden_dim).
        """
        k = self.log_k.exp()
        primary_score = self.score_fn(primary)
        competitor_score = self.score_fn(competitor)
        gate = torch.sigmoid(k * (primary_score - competitor_score))
        return gate * x


class GradientReversal(nn.Module):
    """Gradient Reversal Layer (GRL) for adversarial debiasing.

    Identity in the forward pass, negated gradient in the backward pass.
    Used to suppress a conditioning variable from leaking into the
    representation — "negative conditioning." For example, preventing
    team identity from being encoded in player embeddings.

    Usage in custom_layers::

        def custom_layers(hidden_dim):
            return {
                "grl": GradientReversal(lambda_=0.1),
                "debias_head": torch.nn.Linear(hidden_dim, num_teams),
            }

    Usage in custom_embed::

        # After computing emb, add adversarial branch:
        reversed_emb = self.grl(emb)
        team_pred = self.debias_head(reversed_emb)
        # team_pred is NOT used in the output — the gradient from a
        # team classification loss flows backward through the GRL,
        # pushing the embedding AWAY from encoding team identity.

    Reference: Ganin et al. (2016), "Domain-Adversarial Training of
    Neural Networks." Used in Football2Vec v2 for identity debiasing.

    Args:
        lambda_: Gradient scaling factor. Higher = stronger reversal.
    """

    def __init__(self, lambda_: float = 1.0) -> None:
        super().__init__()
        self.lambda_ = lambda_

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Identity forward, reversed gradient backward."""
        return _GradientReversalFn.apply(x, self.lambda_)


class _GradientReversalFn(torch.autograd.Function):
    """Autograd function for gradient reversal."""

    @staticmethod
    def forward(ctx: torch.autograd.function.FunctionCtx, x: torch.Tensor, lambda_: float) -> torch.Tensor:
        ctx.save_for_backward(torch.tensor(lambda_))
        return x.clone()

    @staticmethod
    def backward(ctx: torch.autograd.function.FunctionCtx, grad_output: torch.Tensor) -> tuple[torch.Tensor, None]:
        (lambda_,) = ctx.saved_tensors  # type: ignore[attr-defined]  # autograd Function
        return -lambda_ * grad_output, None


class AdaptiveBandwidth(nn.Module):
    """State-dependent attention bandwidth inspired by Bekkers' vision model.

    A scalar "intensity" signal (e.g., time_delta, speed, action magnitude)
    controls the sharpness of a Gaussian attention kernel applied to the
    input. Higher intensity = narrower, more focused conditioning; lower
    intensity = broader, more diffuse conditioning.

    Implements: output = x * softmax(scores / temperature(intensity))
    where temperature = base_temp / (1 + k * intensity).

    Usage in custom_layers::

        def custom_layers(hidden_dim):
            return {"adaptive_bw": AdaptiveBandwidth(hidden_dim)}

    Usage in custom_embed::

        # time_delta controls how focused the player conditioning is
        focused = self.adaptive_bw(action_emb, player_emb, time_delta)

    Reference: Bekkers (2026), "Wide Open Gazes" — speed-dependent
    vision narrowing: c_a = min(0.3·v + 0.2, 0.5).

    Args:
        hidden_dim: Feature dimension.
    """

    def __init__(self, hidden_dim: int) -> None:
        super().__init__()
        self.score_proj = nn.Linear(hidden_dim, hidden_dim)
        # Learnable base temperature and intensity scaling
        self.base_temp = nn.Parameter(torch.ones(1))
        self.intensity_scale = nn.Parameter(torch.tensor(0.3))

    def forward(
        self,
        x: torch.Tensor,
        conditioning: torch.Tensor,
        intensity: torch.Tensor,
    ) -> torch.Tensor:
        """Apply bandwidth-adaptive gating.

        Args:
            x: Input tensor (B, S, hidden_dim).
            conditioning: Conditioning signal (B, S, hidden_dim).
            intensity: Scalar signal controlling bandwidth (B, S) — e.g., time_delta.

        Returns:
            Bandwidth-modulated output (B, S, hidden_dim).
        """
        # Compute attention scores from conditioning
        scores = self.score_proj(conditioning)  # (B, S, hd)

        # State-dependent temperature: higher intensity -> lower temp -> sharper
        temp = self.base_temp / (1 + self.intensity_scale.abs() * intensity.unsqueeze(-1))
        temp = temp.clamp(min=0.1)  # numerical stability

        # Apply temperature-scaled softmax gating
        weights = F.softmax(scores / temp, dim=-1)  # (B, S, hd)
        return x * weights


class RatioGate(nn.Module):
    """Reference-normalized ratio gate inspired by PAUSA and Bekkers.

    Gates by the ratio of a signal to its reference maximum:
    gate = clamp(f(x) / max(f(x_ref), eps), 0, 1).

    This answers "what fraction of the possible signal does this input
    capture?" — a fundamentally different question than sigmoid gating
    (absolute threshold) or competitive gating (relative advantage).

    Two modes:
    - spatial: ratio against the max over the sequence dimension
    - channel: ratio against the max over the feature dimension

    Usage in custom_layers::

        def custom_layers(hidden_dim):
            return {"ratio_gate": RatioGate(hidden_dim)}

    Usage in custom_embed::

        gated = self.ratio_gate(action_emb, player_emb)  # (B, S, hd)

    Reference: Lee et al. (2026), "Valuing La Pausa" — temporal/spatial
    ratio gates. Bekkers (2026), "Wide Open Gazes" — coverage ratio.

    Args:
        hidden_dim: Feature dimension.
        mode: "spatial" (max over seq dim) or "channel" (max over feature dim).
    """

    def __init__(self, hidden_dim: int, mode: str = "spatial") -> None:
        super().__init__()
        if mode not in ("spatial", "channel"):
            msg = f"mode must be 'spatial' or 'channel', got {mode!r}"
            raise ValueError(msg)
        self.mode = mode
        self.score_fn = nn.Linear(hidden_dim, hidden_dim)

    def forward(self, x: torch.Tensor, conditioning: torch.Tensor) -> torch.Tensor:
        """Apply ratio-normalized gate.

        Args:
            x: Input to be gated (B, S, hidden_dim).
            conditioning: Reference signal (B, S, hidden_dim).

        Returns:
            Ratio-gated output (B, S, hidden_dim).
        """
        scores = self.score_fn(conditioning)  # (B, S, hd)

        if self.mode == "spatial":
            ref_max = scores.max(dim=1, keepdim=True).values  # (B, 1, hd)
        else:
            ref_max = scores.max(dim=-1, keepdim=True).values  # (B, S, 1)

        ratio = (scores / ref_max.clamp(min=1e-8)).clamp(0.0, 1.0)
        return x * ratio

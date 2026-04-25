"""Seed 6 — dual_head_ensemble.

Hypothesis: two parallel adversaries with different capacities both act on the
same GRL output; their logits are averaged before the cross-entropy loss.
Different-capacity discriminators pick up on different competition signatures,
and averaging pushes the encoder to defeat both simultaneously — analogous to
multi-scale GAN discriminators.

Architecture: CLS pool -> GRL -> {linear_head, mlp_2layer_head} -> mean-average
              of their logits -> (B, num_comp).
"""


def custom_layers(hidden_dim, num_competitions):
    return {
        "grl": GradientReversal(lambda_=1.0),
        # Head A: single linear (baseline capacity).
        "head_linear": torch.nn.Linear(hidden_dim, num_competitions),
        # Head B: 2-layer MLP (higher capacity).
        "head_mlp": torch.nn.Sequential(
            torch.nn.Linear(hidden_dim, hidden_dim),
            torch.nn.GELU(),
            torch.nn.LayerNorm(hidden_dim),
            torch.nn.Linear(hidden_dim, num_competitions),
        ),
    }


def custom_embed(self, encoder_output, attention_mask):
    cls = encoder_output[:, 0]
    reversed_cls = self.grl(cls)
    a = self.head_linear(reversed_cls)
    b = self.head_mlp(reversed_cls)
    return 0.5 * (a + b)

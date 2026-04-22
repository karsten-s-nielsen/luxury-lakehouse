"""Seed 1 — deep_mlp_2layer.

Hypothesis: a stronger (deeper) adversary recovers more competition signal from the
encoder's CLS embedding. Under gradient reversal, this forces the encoder to suppress
more of that signal — producing a more cleanly debiased embedding.

Architecture: CLS pool -> GRL -> Linear(hd, hd) -> GELU -> LayerNorm -> Linear(hd, num_comp).
"""


def custom_layers(hidden_dim, num_competitions):
    return {
        "grl": GradientReversal(lambda_=1.0),
        "mlp": torch.nn.Sequential(
            torch.nn.Linear(hidden_dim, hidden_dim),
            torch.nn.GELU(),
            torch.nn.LayerNorm(hidden_dim),
            torch.nn.Linear(hidden_dim, num_competitions),
        ),
    }


def custom_embed(self, encoder_output, attention_mask):
    cls = encoder_output[:, 0]
    return self.mlp(self.grl(cls))

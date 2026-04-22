"""Seed 2 — deep_mlp_3layer.

Hypothesis: even higher-capacity adversary than deep_mlp_2layer. Tests whether
continuing to scale adversary capacity yields increasing or diminishing returns on
debias pressure.

Architecture: CLS pool -> GRL -> Linear(hd, 2*hd) -> GELU -> LayerNorm ->
              Linear(2*hd, hd) -> GELU -> LayerNorm -> Linear(hd, num_comp).
"""


def custom_layers(hidden_dim, num_competitions):
    wide = hidden_dim * 2
    return {
        "grl": GradientReversal(lambda_=1.0),
        "mlp": torch.nn.Sequential(
            torch.nn.Linear(hidden_dim, wide),
            torch.nn.GELU(),
            torch.nn.LayerNorm(wide),
            torch.nn.Linear(wide, hidden_dim),
            torch.nn.GELU(),
            torch.nn.LayerNorm(hidden_dim),
            torch.nn.Linear(hidden_dim, num_competitions),
        ),
    }


def custom_embed(self, encoder_output, attention_mask):
    cls = encoder_output[:, 0]
    return self.mlp(self.grl(cls))

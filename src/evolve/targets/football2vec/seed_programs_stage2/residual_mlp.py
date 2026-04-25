"""Seed 5 — residual_mlp.

Hypothesis: same capacity as deep_mlp_2layer but with a residual connection through
the MLP block. Residuals are known to improve gradient flow in deep networks; for
an adversary, better gradient flow means the adversary can more effectively propagate
the debias signal back to the encoder.

Architecture: CLS pool -> GRL -> (Linear(hd,hd) -> GELU -> Linear(hd,hd)) + residual
              -> LayerNorm -> Linear(hd, num_comp).

`torch.nn.functional.gelu` is used inline rather than `torch.nn.GELU` in the custom_embed
body since the whole pre-norm residual block is expressed functionally.
"""


def custom_layers(hidden_dim, num_competitions):
    return {
        "grl": GradientReversal(lambda_=1.0),
        "fc1": torch.nn.Linear(hidden_dim, hidden_dim),
        "fc2": torch.nn.Linear(hidden_dim, hidden_dim),
        "ln": torch.nn.LayerNorm(hidden_dim),
        "classifier": torch.nn.Linear(hidden_dim, num_competitions),
    }


def custom_embed(self, encoder_output, attention_mask):
    cls = encoder_output[:, 0]
    h = self.grl(cls)
    mid = self.fc2(torch.nn.functional.gelu(self.fc1(h)))
    h = self.ln(h + mid)
    return self.classifier(h)

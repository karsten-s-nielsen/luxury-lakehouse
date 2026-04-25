"""Seed 3 — cross_attention_adversary.

Hypothesis: each competition class has its own learnable query that cross-attends
over the reversed-gradient per-token encoder output to aggregate evidence FOR that
class directly. Structurally distinct from attention_pool_head (which pools first
then classifies). Mechanistic match with ScoutGPT's cross-attention Fourier finding
(PR #163 -> #166 -> #176): cross-attention produced ScoutGPT's largest rho wins.

Architecture: GRL(per-token) -> 22 learnable competition queries cross-attend
              over reversed per-token output -> Linear(hd, 1) -> squeeze -> (B, num_comp).

References:
    Carion et al. (2020). "DETR" — learnable object queries.
    Lee et al. (2019). "Set Transformer" — per-class query attention.

The learnable class-queries parameter is held inside a `torch.nn.Embedding`
submodule (of size num_competitions x hidden_dim) — this sidesteps the
need to expose `torch.nn.Parameter` at module-register time, since the
validator's custom_layers return-dict registers nn.Module values only.
"""


def custom_layers(hidden_dim, num_competitions):
    return {
        "grl": GradientReversal(lambda_=1.0),
        # One embedding row per competition serves as the learnable class query.
        "class_queries_emb": torch.nn.Embedding(num_competitions, hidden_dim),
        # Fixed 4-head attention; hidden_dim must be divisible by 4 (192 is).
        "cross_attn": torch.nn.MultiheadAttention(hidden_dim, 4, batch_first=True),
        "out_proj": torch.nn.Linear(hidden_dim, 1),
    }


def custom_embed(self, encoder_output, attention_mask):
    reversed_tokens = self.grl(encoder_output)
    batch_size = reversed_tokens.size(0)
    num_comp = self.class_queries_emb.num_embeddings
    indices = torch.arange(num_comp, device=reversed_tokens.device)
    queries = self.class_queries_emb(indices).unsqueeze(0).expand(batch_size, -1, -1)
    # key_padding_mask semantics: True = position ignored
    key_padding_mask = ~attention_mask
    attended, _ = self.cross_attn(queries, reversed_tokens, reversed_tokens, key_padding_mask=key_padding_mask)
    return self.out_proj(attended).squeeze(-1)

"""Seed 4 — attention_pool_head.

Hypothesis: if competition signal is localized at specific token positions
(kickoff locations, unique end-of-half patterns, etc.), an attention-pool
adversary can focus on them. A single learnable query attends over the reversed
per-token output; the softmax-weighted pool is then classified by a Linear head.

Architecture: GRL(per-token) -> single learnable query -> MultiheadAttention pool
              over sequence (masked) -> Linear(hd, num_comp).

Distinct from cross_attention_adversary (seed 3) which has num_competitions learnable
queries that cross-attend then project to per-class scores directly.

The single-query vector is held inside a ``torch.nn.Embedding(1, hidden_dim)``
submodule (index 0 extracted at forward time) so it can be registered as a child
module — the validator only registers nn.Module values from custom_layers.
"""


def custom_layers(hidden_dim, num_competitions):
    return {
        "grl": GradientReversal(lambda_=1.0),
        "query_emb": torch.nn.Embedding(1, hidden_dim),
        # 4-head attention (hidden_dim=192 is divisible by 4).
        "attn": torch.nn.MultiheadAttention(hidden_dim, 4, batch_first=True),
        "classifier": torch.nn.Linear(hidden_dim, num_competitions),
    }


def custom_embed(self, encoder_output, attention_mask):
    reversed_tokens = self.grl(encoder_output)
    batch_size = reversed_tokens.size(0)
    # Single learnable query, tiled for batch: (1, hidden_dim) -> (B, 1, hidden_dim)
    query = self.query_emb(torch.zeros(1, dtype=torch.long, device=reversed_tokens.device))
    query_batched = query.unsqueeze(0).expand(batch_size, -1, -1)
    # MultiheadAttention handles masking internally via key_padding_mask (True = ignore).
    key_padding_mask = ~attention_mask
    attended, _ = self.attn(query_batched, reversed_tokens, reversed_tokens, key_padding_mask=key_padding_mask)
    pooled = attended.squeeze(1)  # (B, hidden_dim)
    return self.classifier(pooled)

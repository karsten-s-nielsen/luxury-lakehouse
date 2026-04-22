"""EV2 Phase 1 seed programs for Football2Vec v2 stage-2 adversary architecture search.

Each .py file in this directory defines TWO functions:

- ``custom_layers(hidden_dim, num_competitions)`` returns a ``dict[str, nn.Module]``
  of adversary submodules (must include a ``"grl"`` key — the training loop patches
  ``adversary.grl.lambda_val`` per epoch).

- ``custom_embed(self, encoder_output, attention_mask)`` returns adversary logits of
  shape ``(B, num_competitions)``. The function name is inherited from the existing
  ``code_validator.py`` (which hardcodes ``custom_embed`` + ``custom_layers``), but
  semantically this is the **adversary forward** — not a spatial embedding function.
  The evaluator's ``_apply_program_adversary`` wraps layers + forward into a dynamic
  ``nn.Module`` that delegates its forward to this function.

Seeds are validated against ``FOOTBALL2VEC_ADVERSARY_PROFILE`` before exec under
restricted globals (``__builtins__: {}`` + whitelisted building-block classes from
``src/evolve/targets/scoutgpt/building_blocks.py``). See ADR-001.

See docs/superpowers/specs/2026-04-23-ev2-football2vec-l2-adversarial-design.md.
"""

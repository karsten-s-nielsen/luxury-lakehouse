"""Football2Vec evolve target — Level 1 stage-1 search + Level 2 stage-2 adversary search.

EV1 (PR #158): L1 search over stage-1 hyperparameters + architectural enums.
EV2 (this cycle): L2 search over stage-2 adversary architecture + L1 search
over lambda schedule shape / max / warmup.
"""

from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE

# Generic name used by runner.py to look up the profile for any target.
VALIDATION_PROFILE = FOOTBALL2VEC_ADVERSARY_PROFILE

__all__ = ["FOOTBALL2VEC_ADVERSARY_PROFILE", "VALIDATION_PROFILE"]

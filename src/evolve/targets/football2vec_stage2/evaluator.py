"""Football2Vec stage-2 evaluator — alias so BackendPool dispatches to stage-2 entry point.

``BackendPool.train(..., target="football2vec_stage2", ...)`` loads
``evolve.targets.football2vec_stage2.evaluator`` and calls ``train_and_evaluate``.
We bind that name to the stage-2 function from the parent ``football2vec`` evaluator.
"""

from evolve.targets.football2vec.evaluator import (
    _apply_program_adversary,
)
from evolve.targets.football2vec.evaluator import (
    train_and_evaluate_stage2 as train_and_evaluate,
)

__all__ = ["_apply_program_adversary", "train_and_evaluate"]

"""Football2Vec stage-2 evolve target — thin alias over football2vec's stage-2 evaluator.

Exists so ``BackendPool`` (which dispatches through ``evolve.targets.<target>.evaluator``)
can route EV2 stage-2 candidates to ``train_and_evaluate_stage2`` without modifying the
generic backend code. The alias target's ``evaluator.train_and_evaluate`` is bound to
``football2vec.evaluator.train_and_evaluate_stage2``; the validation profile is shared
with the parent ``football2vec`` target.

Pattern matches the existing ScoutGPT / Football2Vec L1 target layout — a routine alias
module is the standard way to wire a new stage into the existing dispatch abstraction.
"""

from evolve.targets.football2vec import VALIDATION_PROFILE

__all__ = ["VALIDATION_PROFILE"]

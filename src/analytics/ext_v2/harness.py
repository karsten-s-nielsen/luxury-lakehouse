"""Phase 0 Optuna harness for the ExT v2 reproduction.

Per design spec §6 + locked decision A (single-source ``fct_action_values``,
NLL on ``action_type='pass'`` subset, hash-based 15% match holdout):

1. Split input ``actions`` into train/holdout via deterministic hash on
   ``(competition_id, match_key)``.
2. Filter holdout to passes only (``action_type='pass'``).
3. Run Optuna study with ``direction='minimize'`` and ``n_trials=1`` —
   Phase 0 has no axes active (the objective makes zero
   ``trial.suggest_*`` calls). Phases 1-4 add axes incrementally without
   restructuring the harness.
4. Re-fit ``SinghProducer`` on train and return ``Phase0Result`` exposing
   the best XTGrid, best NLL, study, and dataset metadata.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, cast

import optuna
import pandas as pd

from analytics.expected_threat import XTGrid
from analytics.ext_v2.fitness import compute_holdout_nll
from analytics.ext_v2.holdout import DEFAULT_HOLDOUT_FRACTION, holdout_split
from analytics.ext_v2.producer import SinghProducer
from analytics.ext_v2.transition import GridSpec

if TYPE_CHECKING:
    # Import KDESmoothedProducer only for type checking — avoids circular import
    # at runtime (kde.py and producer.py both reachable via the producer chain).
    from analytics.ext_v2.kde import KdeKernel
    from analytics.ext_v2.producer import KDESmoothedProducer

_HARNESS_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "competition_id",
    "match_key",
    "type_name",
    "result_name",
    "action_type",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
)


@dataclass(frozen=True)
class Phase0Result:
    """End-to-end Phase 0 harness output.

    Attributes:
        best_trial: The Optuna trial with the lowest NLL (Phase 0 has only
            one trial, so this is trivially trial 0).
        best_xt_grid: ``XTGrid`` produced by re-fitting ``SinghProducer``
            on the train fold with the best trial's hyperparameters.
        best_nll: Held-out NLL on the pass-only subset of the holdout fold.
        study: The Optuna study object — exposed for downstream analysis
            (per-trial logs, study.trials_dataframe(), etc.).
        n_train_actions: Row count of the train fold.
        n_holdout_passes: Row count of the pass-only holdout subset.
        producer: The fitted ``SinghProducer`` (exposes
            ``.transition_matrix`` for downstream NLL re-evaluation).
    """

    best_trial: optuna.trial.FrozenTrial
    best_xt_grid: XTGrid
    best_nll: float
    study: optuna.Study
    n_train_actions: int
    n_holdout_passes: int
    producer: SinghProducer


@dataclass(frozen=True)
class Phase1Result:
    """End-to-end Phase 1 harness output.

    Attributes:
        best_trial: The Optuna trial with the lowest ``nll_primary``.
        best_xt_grid: ``XTGrid`` produced by re-fitting ``KDESmoothedProducer``
            on the train fold with the best trial's hyperparameters.
        best_nll: ``nll_primary`` of the best trial -- the stop-condition metric.
        best_nll_floorless: ``nll_floorless`` user_attr of the best trial --
            the eps-free diagnostic per spec section 10.3 Q4.
        study: The Optuna study object -- exposed for downstream analysis
            (per-trial logs, study.trials_dataframe(), plateau check).
        n_train_actions: Row count of the train fold.
        n_holdout_passes: Row count of the pass-only holdout subset.
        producer: The fitted ``KDESmoothedProducer`` of the best config.
    """

    best_trial: optuna.trial.FrozenTrial
    best_xt_grid: XTGrid
    best_nll: float
    best_nll_floorless: float
    study: optuna.Study
    n_train_actions: int
    n_holdout_passes: int
    producer: KDESmoothedProducer


def objective(
    trial: optuna.trial.Trial,
    train_actions: pd.DataFrame,
    holdout_passes: pd.DataFrame,
    *,
    grid: GridSpec,
) -> float:
    """Phase 0 Optuna objective — fits ``SinghProducer``, returns held-out NLL.

    No ``trial.suggest_*`` calls in Phase 0 (no axes active). Phases 1-4
    activate axes via this same function: each phase adds suggests at the
    top, then constructs the appropriate ``Producer`` subclass.
    """
    del trial  # Phase 0: no axes — trial is unused but kept for ABI parity
    producer = SinghProducer(grid=grid).fit(train_actions)
    return compute_holdout_nll(producer, holdout_passes, grid=grid)


def objective_phase1(
    trial: optuna.trial.Trial,
    train_actions: pd.DataFrame,
    holdout_passes: pd.DataFrame,
    *,
    grid: GridSpec,
) -> float:
    """Phase 1 Optuna objective — KDE-smoothed Singh, three-axis search.

    Activates the three KDE axes per spec section 10.3 paragraph 4 and logs the
    eps-free NLL diagnostic per Q4.

    Args:
        trial: Optuna trial.
        train_actions: Train fold of fct_action_values rows.
        holdout_passes: Holdout fold filtered to ``action_type='pass'``.
        grid: Pitch-grid binning spec.

    Returns:
        ``nll_primary`` -- held-out NLL with eps=1e-10 floor (Phase 1 stop-
        condition metric, comparable to Phase 0).
    """
    from analytics.ext_v2.producer import KDESmoothedProducer

    # Optuna's suggest_categorical returns the bare value type (str/bool); cast
    # narrows to the KdeKernel Literal that KDESmoothedProducer expects. The
    # categorical-axis values list IS the runtime guarantee that the cast holds.
    kernel = cast("KdeKernel", trial.suggest_categorical("kde_kernel", ["gaussian", "epanechnikov", "tophat"]))
    bandwidth = trial.suggest_float("kde_bandwidth", 0.01, 2.0, log=True)
    adaptive = trial.suggest_categorical("kde_adaptive", [True, False])

    producer = KDESmoothedProducer(
        grid=grid,
        kernel=kernel,
        bandwidth=bandwidth,
        adaptive=adaptive,
    ).fit(train_actions)

    nll_primary = compute_holdout_nll(producer, holdout_passes, grid=grid, eps=1e-10)
    nll_floorless = compute_holdout_nll(producer, holdout_passes, grid=grid, eps=1e-300)
    trial.set_user_attr("nll_floorless", nll_floorless)
    return nll_primary


def run_phase0_harness(
    actions: pd.DataFrame,
    *,
    grid: GridSpec | None = None,
    holdout_fraction: float = DEFAULT_HOLDOUT_FRACTION,
    n_trials: int = 1,
    study_name: str | None = None,
) -> Phase0Result:
    """Run the Phase 0 ExT v2 harness end-to-end.

    Args:
        actions: SPADL action rows from ``fct_action_values``. Must contain
            ``competition_id, match_key, type_name, result_name, action_type,
            start_{x,y}, end_{x,y}``.
        grid: Pitch-grid binning. Defaults to ``GridSpec()`` (12x8 SPADL).
        holdout_fraction: Fraction of matches to send to holdout via hash
            (default 0.15 per design spec §5.3).
        n_trials: Optuna trials. Phase 0: 1 (no axes). Kept as a parameter
            for forward compatibility with Phases 1-4.
        study_name: Optuna study name. Default: an autogenerated UUID.

    Returns:
        ``Phase0Result`` — see dataclass docstring.

    Raises:
        ValueError: if required columns are missing or ``actions`` is empty.
    """
    missing = [col for col in _HARNESS_REQUIRED_COLUMNS if col not in actions.columns]
    if missing:
        msg = f"actions missing required columns: {missing}"
        raise ValueError(msg)
    if actions.empty:
        msg = "actions is empty — nothing to fit"
        raise ValueError(msg)

    grid = grid if grid is not None else GridSpec()

    train_actions, holdout_actions = holdout_split(actions, holdout_fraction=holdout_fraction)
    holdout_passes = holdout_actions[holdout_actions["action_type"] == "pass"].copy()

    study = optuna.create_study(direction="minimize", study_name=study_name)
    study.optimize(
        lambda trial: objective(trial, train_actions, holdout_passes, grid=grid),
        n_trials=n_trials,
    )

    # Re-fit on train with the best trial's params (Phase 0: no params, so
    # any of the n_trials trials is equivalent — first trial's grid suffices).
    producer = SinghProducer(grid=grid).fit(train_actions)

    return Phase0Result(
        best_trial=study.best_trial,
        best_xt_grid=producer.xt_grid,
        best_nll=study.best_value,
        study=study,
        n_train_actions=len(train_actions),
        n_holdout_passes=len(holdout_passes),
        producer=producer,
    )


def run_phase1_harness(
    actions: pd.DataFrame,
    *,
    grid: GridSpec | None = None,
    holdout_fraction: float = DEFAULT_HOLDOUT_FRACTION,
    n_trials: int = 500,
    study_name: str | None = None,
    study_storage: str | None = None,
    callbacks: Sequence[object] | None = None,
) -> Phase1Result:
    """Run the Phase 1 ExT v2 KDE-smoothed harness end-to-end.

    Args:
        actions: SPADL action rows from ``fct_action_values``. Same column
            requirements as Phase 0.
        grid: Pitch-grid binning. Defaults to ``GridSpec()``.
        holdout_fraction: Match-stratified holdout fraction (default 0.15
            per spec section 10.3 Q6 -- unchanged from Phase 0).
        n_trials: Optuna trials. Default 500 per spec section 10.3 Q5.
        study_name: Optuna study name. Default: an autogenerated UUID.
        study_storage: Optuna storage URL (e.g. ``"sqlite:///path/to/db"``).
            None -> in-memory (not resumable).
        callbacks: Optional Optuna callbacks (e.g. ``MLflowCallback``). The
            run script wires these; the library function stays MLflow-dep-free.

    Returns:
        ``Phase1Result`` -- see dataclass docstring.

    Raises:
        ValueError: if required columns are missing or ``actions`` is empty.
    """
    from analytics.ext_v2.producer import KDESmoothedProducer

    missing = [col for col in _HARNESS_REQUIRED_COLUMNS if col not in actions.columns]
    if missing:
        msg = f"actions missing required columns: {missing}"
        raise ValueError(msg)
    if actions.empty:
        msg = "actions is empty -- nothing to fit"
        raise ValueError(msg)

    grid = grid if grid is not None else GridSpec()

    train_actions, holdout_actions = holdout_split(actions, holdout_fraction=holdout_fraction)
    holdout_passes = holdout_actions[holdout_actions["action_type"] == "pass"].copy()

    study = optuna.create_study(
        direction="minimize",
        study_name=study_name,
        storage=study_storage,
        load_if_exists=study_storage is not None,
    )
    study.optimize(
        lambda trial: objective_phase1(trial, train_actions, holdout_passes, grid=grid),
        n_trials=n_trials,
        callbacks=list(callbacks) if callbacks else None,  # type: ignore[arg-type]
    )

    best = study.best_trial
    # Re-fit KDESmoothedProducer with best params for the returned XTGrid + producer.
    # Optuna stores params as Any in trial.params; cast the kernel back to its
    # Literal type — the suggest_categorical call site is the runtime guarantee.
    producer = KDESmoothedProducer(
        grid=grid,
        kernel=cast("KdeKernel", best.params["kde_kernel"]),
        bandwidth=best.params["kde_bandwidth"],
        adaptive=best.params["kde_adaptive"],
    ).fit(train_actions)

    return Phase1Result(
        best_trial=best,
        best_xt_grid=producer.xt_grid,
        best_nll=study.best_value,
        best_nll_floorless=float(best.user_attrs.get("nll_floorless", float("nan"))),
        study=study,
        n_train_actions=len(train_actions),
        n_holdout_passes=len(holdout_passes),
        producer=producer,
    )

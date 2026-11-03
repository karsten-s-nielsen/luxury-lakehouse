"""The Ops page actions, each routed through the guarded dispatch (spec §6.2, §8; plan Task 8 + the live-web addendum).

Every `on_action` a page button carries is registered here as a named action on an `ActionRegistry`; the live Taipy
socket dispatches by that name with the caller's `Principal`. A write attributes the change to the authenticated
operator (`principal.user`), never a client-sent name, and the page holds no config logic (§8). The preview is a
`VIEW_PRIVATE` read; the socket carries no CSRF token (its defence is the origin lock + operator-only connect).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final

from lakehouse.config.model import Clause, ModuleId, ScheduleConfig
from lakehouse.services.authz import ActionRegistry, Permission, Principal
from lakehouse.services.filters import FilterDiff
from lakehouse.ui.pages.filters import diff_text

if TYPE_CHECKING:
    from lakehouse.ui.shell import Services

__all__ = [
    "ACTION_NAMES",
    "ACTION_OUTCOMES",
    "READ_ONLY_ACTIONS",
    "ActionOutcome",
    "build_action_registry",
]

# Every action name a page button references with `on_action=`. The no-bypass test checks each page's `on_action`
# tokens against this set, and `build_action_registry` registers exactly these (spec §6.2, OBS-B21).
ACTION_NAMES: frozenset[str] = frozenset(
    {
        "on_run_now",
        "on_module_active",
        "on_save_filter",
        "on_preview_filter",
        "on_save_schedule",
        "on_pause_schedule",
        "on_resume_schedule",
        "on_accept_recommended",
        "on_undo_batch",
        "on_decide",
    }
)


# An action's outcome for the operator: `(state, result) -> message`. The dispatcher shows "Done: <message>." in the
# page notice, so the operator reads what happened in plain words, never the raw action name ("Done: on_save_filter."
# was the former notice). An outcome may also place its result on the page (the Preview diff, beside its button).
ActionOutcome = Callable[[Any, Any], str]


def _fixed(message: str) -> ActionOutcome:
    def outcome(state: Any, result: Any) -> str:
        return message

    return outcome


def _preview_outcome(state: Any, result: Any) -> str:
    """Show what saving the draft would change beside the Preview button, and in the notice (spec §19.5)."""
    text = diff_text(result) if isinstance(result, FilterDiff) else "no preview available"
    state.filter_diff = f"**If you save this filter:** {text}"
    return f"preview ready ({text})"


ACTION_OUTCOMES: Final[Mapping[str, ActionOutcome]] = {
    "on_run_now": _fixed("a run started; follow it on the Runs page"),
    "on_module_active": _fixed("the source's Active setting is saved"),
    "on_save_filter": _fixed("the filter is saved; the next run downloads the new selection"),
    "on_preview_filter": _preview_outcome,
    "on_save_schedule": _fixed("the schedule is saved"),
    "on_pause_schedule": _fixed("the schedule is paused"),
    "on_resume_schedule": _fixed("the schedule is resumed"),
    "on_accept_recommended": _fixed("the recommended items are accepted"),
    "on_undo_batch": _fixed("the batch is undone"),
    "on_decide": _fixed("the decision is saved"),
}

# Reads: the dispatcher does not re-run the page refresh after them. The refresh re-seeds the filter draft from the
# saved filter, so refreshing after a Preview would discard the unsaved selection the operator just previewed.
READ_ONLY_ACTIONS: Final = frozenset({"on_preview_filter"})


def _operator(principal: Principal) -> str:
    return str(principal.user)


def build_action_registry(services: Services) -> ActionRegistry:
    """The registry of every Ops action, bound to `services`. Writes require `WRITE`; Preview requires
    `VIEW_PRIVATE` (§6.2, §8)."""
    registry = ActionRegistry()

    def on_run_now(principal: Principal) -> Any:
        return services.runs.trigger()

    def on_module_active(principal: Principal, *, module_id: ModuleId, active: bool) -> int:
        return services.settings.set_module_active(module_id, active=active, operator=_operator(principal))

    def on_save_filter(
        principal: Principal,
        *,
        module_id: ModuleId,
        clauses: Sequence[Clause],
        statsbomb_360_only: bool | None = None,
    ) -> int:
        return services.settings.set_module_filter(
            module_id, clauses=clauses, statsbomb_360_only=statsbomb_360_only, operator=_operator(principal)
        )

    def on_preview_filter(
        principal: Principal,
        *,
        module_id: ModuleId,
        clauses: Sequence[Clause],
        statsbomb_360_only: bool | None = None,
    ) -> FilterDiff:
        # a read: what saving the draft would add or stop (§19.5); it lists the source and never ingests (§8)
        return services.filters.diff(module_id, list(clauses), statsbomb_360_only=statsbomb_360_only)

    def on_save_schedule(principal: Principal, *, cron: str | None, enabled: bool, expected_version: int) -> Any:
        cfg = ScheduleConfig(enabled=enabled, cron=cron)
        return services.schedule.set(cfg, expected_version=expected_version, operator=_operator(principal))

    def on_pause_schedule(principal: Principal, *, cron: str | None, expected_version: int) -> Any:
        cfg = ScheduleConfig(enabled=False, cron=cron)
        return services.schedule.set(cfg, expected_version=expected_version, operator=_operator(principal))

    def on_resume_schedule(principal: Principal, *, cron: str | None, expected_version: int) -> Any:
        cfg = ScheduleConfig(enabled=True, cron=cron)
        return services.schedule.set(cfg, expected_version=expected_version, operator=_operator(principal))

    def on_accept_recommended(principal: Principal, *, entity: Any = None, batch_filter: str | None = None) -> str:
        # a blank entity / filter from the page means "all recommended" (page-binding PB7)
        return services.review.accept_recommended(entity or None, batch_filter or None, operator=_operator(principal))

    def on_undo_batch(principal: Principal, *, batch_id: str) -> str:
        return services.review.undo_batch(batch_id, operator=_operator(principal))

    def on_decide(principal: Principal, *, item_id: str, action: Any, reason: str) -> None:
        return services.review.decide(item_id, action, operator=_operator(principal), reason=reason)

    registry.register("on_run_now", on_run_now, permission=Permission.WRITE)
    registry.register("on_module_active", on_module_active, permission=Permission.WRITE)
    registry.register("on_save_filter", on_save_filter, permission=Permission.WRITE)
    registry.register("on_preview_filter", on_preview_filter, permission=Permission.VIEW_PRIVATE)  # operator read
    registry.register("on_save_schedule", on_save_schedule, permission=Permission.WRITE)
    registry.register("on_pause_schedule", on_pause_schedule, permission=Permission.WRITE)
    registry.register("on_resume_schedule", on_resume_schedule, permission=Permission.WRITE)
    registry.register("on_accept_recommended", on_accept_recommended, permission=Permission.WRITE)
    registry.register("on_undo_batch", on_undo_batch, permission=Permission.WRITE)
    registry.register("on_decide", on_decide, permission=Permission.WRITE)
    return registry

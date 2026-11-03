"""The live Taipy action dispatcher (live-web addendum §2, §3; plan L5).

A Taipy page button `on_action=<name>` sends an ACTION message. Taipy finds no bound function of that name, so it
calls the Gui-level `on_action` handler installed here (`gui.py` `__on_action`). The handler reads the caller's
connect-time `Principal` by the socket id (a socket message carries no reliable session, so the principal captured at
connect in the `ConnectionRegistry` is the authority — L4/L5), collects the action's inputs from the page state, and
routes through `authz.dispatch`. Dispatch gates on the action's permission and attributes the write to the operator;
a refusal or a service error sets a visible notice and changes nothing (§8; no silent swallow, ADR-002).

The input contract — the state variables each action reads — is named in `_collect`. Binding those variables into the
live Gui is the separate presentation concern the shell owns; this module is the authorization + dispatch seam only.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, cast

from lakehouse.services.authz import ActionRegistry, Unauthorized
from lakehouse.web.sessions import ConnectionRegistry

__all__ = ["ACTION_NOTICE_VAR", "install_live_actions"]

# The bound state variable the shell renders to show the outcome of the last action (done / refused / error).
ACTION_NOTICE_VAR = "ll_action_notice"


def _notify(state: Any, message: str) -> None:
    """Show the outcome to the operator. Never raises, so a display gap cannot crash the socket handler."""
    try:
        setattr(state, ACTION_NOTICE_VAR, message)
    except Exception:  # noqa: BLE001, S110 - a notice is best-effort; the action result already stands
        pass


def _collect(name: str, state: Any) -> dict[str, Any]:
    """The inputs each action reads from the page state. This is the state-variable contract the live pages bind to;
    an unbound variable reads as its default and the service layer validates it (never a silent wrong write)."""

    def g(var: str, default: Any = None) -> Any:
        return getattr(state, var, default)

    if name == "on_run_now":
        return {}
    # audit F7: the Data sources page uses the one shared Source selector (`filter_module`) for both the active toggle
    # and the what-to-download editor, so these actions read the module id from there. An unpicked source reads as ""
    # and the service rejects it with a visible notice — never a substituted module (the former "statsbomb_public"
    # default would have written to a source the operator never picked).
    if name == "on_module_active":
        return {"module_id": g("filter_module", ""), "active": bool(g("draft_active", False))}
    if name in ("on_preview_filter", "on_save_filter"):
        return {
            "module_id": g("filter_module", ""),
            "clauses": g("draft_clauses", []),
            "statsbomb_360_only": g("draft_360"),
        }
    if name == "on_save_schedule":
        return {
            "cron": g("draft_cron"),
            "enabled": bool(g("draft_enabled", False)),
            "expected_version": int(g("schedule_version", 0)),
        }
    if name in ("on_pause_schedule", "on_resume_schedule"):
        return {"cron": g("draft_cron"), "expected_version": int(g("schedule_version", 0))}
    if name == "on_accept_recommended":
        return {"entity": g("decide_entity"), "batch_filter": g("accept_batch_filter")}
    if name == "on_undo_batch":
        return {"batch_id": str(g("undo_batch_id", ""))}
    if name == "on_decide":
        return {
            "item_id": str(g("decide_item", "")),
            "action": g("decide_action"),
            "reason": str(g("decide_reason", "")),
        }
    return {}


def install_live_actions(
    gui: Any,
    *,
    registry: ActionRegistry,
    connections: ConnectionRegistry,
    refreshers: Mapping[str, Callable[[Any], None]] | None = None,
    outcomes: Mapping[str, Callable[[Any, Any], str]] | None = None,
    read_only: frozenset[str] = frozenset(),
) -> None:
    """Install the Gui-level action dispatcher. Every page button routes here, resolves its principal and dispatches.
    `refreshers` maps a ROUTE to its page's `refresh(state)`; a successful write re-runs the ACTIVE page's refresh
    (`state.current_page`) so the page shows the new state (page-binding addendum §4). Keying by route, not by action,
    because an action like `on_run_now` is declared on several pages — an action-keyed map would always refresh the
    wrong page. A `read_only` action (a Preview) skips that refresh, because the refresh re-seeds the editor draft the
    read just described. `outcomes` maps an action to `(state, result) -> message`: the plain-words outcome shown as
    "Done: <message>." (it may also place the result on the page); an action without one shows a generic message,
    never its raw name."""
    from flask import request  # Flask is a web-layer dependency (import-linter)

    page_refreshers = refreshers or {}
    action_outcomes = outcomes or {}

    def _sid() -> str:
        return cast("str", request.sid)  # type: ignore[attr-defined]

    def _dispatch(state: Any, element_id: str, payload: Any) -> None:
        data = cast("Mapping[str, Any]", payload)  # Taipy sends the action payload as a dict
        raw = data.get("action", "")
        name = raw if isinstance(raw, str) else ""
        principal = connections.principal_of(_sid())
        if principal is None:
            _notify(state, "Your session is no longer connected. Sign in again.")
            return
        # S13 Save guard: a failed Source reload left the filter draft stale (the on_change dispatcher set
        # `draft_stale`), so saving now would write the old module's clauses into the new module (V2UI-04 on the error
        # path). Refuse the save until a reload succeeds and clears the flag.
        if name == "on_save_filter" and bool(getattr(state, "draft_stale", False)):
            _notify(
                state, "The source did not reload, so the filter draft may be stale. Reselect the source, then save."
            )
            return
        kwargs = _collect(name, state)
        try:
            result = registry.dispatch(name, principal, **kwargs)
        except Unauthorized:
            _notify(state, "You do not have permission for that action.")
            return
        except Exception as exc:  # noqa: BLE001 - surface every service/validation error to the operator (ADR-002), never swallow
            _notify(state, f"That action did not apply: {exc}")
            return
        if name not in read_only:
            refresh = page_refreshers.get(str(getattr(state, "current_page", "")))
            if refresh is not None:
                refresh(state)  # re-fill the ACTIVE page's display from the services (§4), keyed by route not action
        outcome = action_outcomes.get(name)
        # after the refresh, so an outcome that places its result on the page is not cleared by the refresh
        _notify(state, f"Done: {outcome(state, result) if outcome is not None else 'the action is complete'}.")

    gui.on_action = _dispatch

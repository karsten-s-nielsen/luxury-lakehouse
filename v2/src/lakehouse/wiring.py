"""The composition root (plan Task 26): the real channel adapter of each module and the five provider parsers.

Adapters and parsers are independent packages (spec §6, an import-linter contract); this module is the one place that
knows both, and the CLI and the services' callers build the run from it. Building an adapter calls no network. The
pining token comes from the environment (owner, else list-only, else the documented public token); a GitHub token,
when set, only raises GitHub's rate limit.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

from lakehouse.config.model import ModuleConfig, Settings
from lakehouse.ids.native import Provider
from lakehouse.net.http import HttpClient
from lakehouse.parsers.base import ProviderParser
from lakehouse.parsers.gradientsports import GradientSportsParser
from lakehouse.parsers.idsse import IdsseParser
from lakehouse.parsers.skillcorner import SkillCornerParser
from lakehouse.parsers.statsbomb import StatsBombParser
from lakehouse.parsers.wyscout import WyscoutParser
from lakehouse.services.account import AccountService
from lakehouse.sources.base import ChannelAdapter
from lakehouse.sources.cache import DiskCache
from lakehouse.sources.github import GitHubClient
from lakehouse.sources.gradientsports_public import GradientSportsPublicAdapter
from lakehouse.sources.idsse_public import IdssePublicAdapter
from lakehouse.sources.pining import PINING_PROVIDERS, PiningAdapter, token_from_env
from lakehouse.sources.skillcorner_public import SkillCornerPublicAdapter
from lakehouse.sources.statsbomb_public import StatsBombPublicAdapter
from lakehouse.sources.wyscout_public import WyscoutPublicAdapter
from lakehouse.storage.base import Storage
from lakehouse.storage.roots import ResolvedRoots, default_bootstrap_file, open_storage, resolve_roots

if TYPE_CHECKING:
    from lakehouse.ui.shell import Services

__all__ = [
    "build_account_service",
    "build_adapter",
    "build_adapters",
    "build_services",
    "cache_dir",
    "default_parsers",
    "open_lake",
]

# Derived from the single pining registry (sources.pining.PINING_PROVIDERS) so a new pining provider is declared in
# exactly one place; build_adapter maps `<provider>_pining` -> the shared PiningAdapter.
_PINING_PROVIDERS: dict[str, Provider] = {entry.module_id: entry.provider for entry in PINING_PROVIDERS}


def cache_dir(env: Mapping[str, str]) -> Path:
    """The listing cache: `LAKEHOUSE_CACHE_DIR`, else the user's cache folder (never the lake)."""
    chosen = env.get("LAKEHOUSE_CACHE_DIR")
    if chosen:
        return Path(chosen)
    if sys.platform == "win32":
        base = Path(env.get("LOCALAPPDATA") or Path.home() / "AppData" / "Local")
    elif sys.platform == "darwin":
        base = Path.home() / "Library" / "Caches"
    else:
        base = Path(env.get("XDG_CACHE_HOME") or Path.home() / ".cache")
    return base / "lakehouse" / "cache"


def build_adapter(
    module: ModuleConfig,
    *,
    settings: Settings,
    env: Mapping[str, str],
    cache: Path,
    http: HttpClient | None = None,
) -> ChannelAdapter:
    """The adapter of one module, with its listing cache under `cache/<module id>`."""
    client = http if http is not None else HttpClient()
    listing_cache = DiskCache(cache / module.module_id)
    module_id = module.module_id
    if module_id in _PINING_PROVIDERS:
        return PiningAdapter(
            _PINING_PROVIDERS[module_id],
            http=client,
            base_url=settings.pining_base_url,
            token=token_from_env(env),
            module=module,
        )
    github = GitHubClient(client, env.get("GITHUB_TOKEN") or None)
    if module_id == "statsbomb_public":
        return StatsBombPublicAdapter(github, module, listing_cache)
    if module_id == "skillcorner_public":
        return SkillCornerPublicAdapter(github, module, listing_cache)
    if module_id == "idsse_public":
        return IdssePublicAdapter(client, module, listing_cache)
    if module_id == "wyscout_public":
        return WyscoutPublicAdapter(client, module, listing_cache)
    if module_id == "gradientsports_public":
        return GradientSportsPublicAdapter(client, module, listing_cache)
    raise ValueError(f"no adapter for the module {module_id!r}")


def build_adapters(settings: Settings, *, env: Mapping[str, str] | None = None) -> dict[str, ChannelAdapter]:
    """The adapter of every module of the saved config (active or not: a pining module is listed for visibility)."""
    chosen = os.environ if env is None else env
    folder = cache_dir(chosen)
    client = HttpClient()
    return {
        module.module_id: build_adapter(module, settings=settings, env=chosen, cache=folder, http=client)
        for module in settings.modules
    }


def build_account_service(env: Mapping[str, str]) -> AccountService:
    """The local operator account of the lake that `env` resolves to (the web app's composition root for local mode)."""
    resolved = resolve_roots(cli_lake=None, cli_restricted=None, env=env, bootstrap_file=default_bootstrap_file(env))
    return AccountService(open_storage(resolved, env))


def default_parsers() -> dict[Provider, ProviderParser]:
    return {
        "statsbomb": StatsBombParser(),
        "wyscout": WyscoutParser(),
        "idsse": IdsseParser(),
        "skillcorner": SkillCornerParser(),
        "gradientsports": GradientSportsParser(),
    }


def open_lake(env: Mapping[str, str]) -> tuple[Storage, ResolvedRoots]:
    """Resolve the lake roots from the environment and open the storage (the web entrypoint's one lake-access seam)."""
    resolved = resolve_roots(cli_lake=None, cli_restricted=None, env=env, bootstrap_file=default_bootstrap_file(env))
    return open_storage(resolved, env), resolved


def build_services(storage: Storage, resolved: ResolvedRoots, *, env: Mapping[str, str]) -> Services:
    """The Ops services, one of each (plan Task 28/29). The composition root the web entrypoint uses: it reaches the
    lake packages so `lakehouse.web` need not. On an HF lake the run trigger and the schedule drive the owner-created
    HF job; on a local lake they run a detached process and the native OS task."""
    from lakehouse.services.filters import FilterService
    from lakehouse.services.modules import ModuleService
    from lakehouse.services.review import ReviewService
    from lakehouse.services.runs import HfRunTrigger, LocalRunTrigger, RunService
    from lakehouse.services.schedule import ScheduleService
    from lakehouse.services.settings import SettingsService
    from lakehouse.ui.shell import Services

    on_hf = resolved.roots.lake.hf_bucket is not None
    settings = SettingsService(storage, resolved)
    adapters = build_adapters(settings.load()[0], env=env)

    def adapter_for(module: ModuleConfig) -> ChannelAdapter:
        return adapters[module.module_id]

    if on_hf:
        from lakehouse.schedule.hf_jobs import HfJobsSchedule

        hf_schedule = HfJobsSchedule.from_env(env)
        runs = RunService(storage, trigger=HfRunTrigger(hf_schedule))
        schedule = ScheduleService(settings, hf_schedule, env=env)
    else:
        from lakehouse.schedule.base import backend_for_host

        runs = RunService(storage, trigger=LocalRunTrigger())
        schedule = ScheduleService(settings, backend_for_host(), env=env)
    return Services(
        settings=settings,
        modules=ModuleService(storage, env=env),
        filters=FilterService(storage, adapter_for),
        runs=runs,
        review=ReviewService(storage),
        schedule=schedule,
    )

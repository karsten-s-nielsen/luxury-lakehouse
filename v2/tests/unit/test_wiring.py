"""The composition root (plan Task 26): the real adapter of every module and the five parsers, built without any
network call."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from lakehouse.config.model import load_defaults
from lakehouse.wiring import build_adapter, cache_dir, default_parsers


class NoNetwork:
    def get(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("an adapter called the network while it was built")

    def post_json(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("an adapter called the network while it was built")


def test_every_module_has_an_adapter(tmp_path: Path) -> None:
    settings = load_defaults("laptop")
    for module in settings.modules:
        adapter = build_adapter(module, settings=settings, env={}, cache=tmp_path, http=NoNetwork())  # type: ignore[arg-type]
        assert adapter.module_id == module.module_id
        assert adapter.channel == ("pining" if module.module_id.endswith("_pining") else "public")
        assert module.module_id.startswith(adapter.provider)


def test_default_parsers() -> None:
    parsers = default_parsers()
    assert set(parsers) == {"statsbomb", "wyscout", "idsse", "skillcorner", "gradientsports"}
    assert all(parser.provider == provider and parser.version for provider, parser in parsers.items())


def test_cache_dir(tmp_path: Path) -> None:
    assert cache_dir({"LAKEHOUSE_CACHE_DIR": str(tmp_path / "c")}) == tmp_path / "c"
    assert cache_dir({}).name == "cache" and "lakehouse" in str(cache_dir({}))


def test_unknown_module_refused(tmp_path: Path) -> None:
    settings = load_defaults("laptop")
    odd = settings.modules[0].model_copy(update={"module_id": "nobody_public"})
    with pytest.raises(ValueError, match="no adapter"):
        build_adapter(odd, settings=settings, env={}, cache=tmp_path, http=NoNetwork())  # type: ignore[arg-type]

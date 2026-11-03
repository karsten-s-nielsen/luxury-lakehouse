"""S1 Task 2: `init` warns loudly and creates unencrypted on a keyless local root (owner decision, plan T2).

The warning is gated on `resolved.roots.lake.local_path is not None and key is None` (`cli._init`): an `hf://`
root has `local_path is None`, so `init` never warns there, and `resolve_encryption_key` returns `None` for an
`hf://` lake regardless of the var (pinned in `test_encryption_key.py`). A full `init` on an `hf://` root needs the
HF backend, so it is not a unit test (it would be an `hf`-marked live test); the no-warning guarantee is the
`local_path` condition here plus that resolver pin (audit S1-IMPL-13, T2).
"""

from __future__ import annotations

from pathlib import Path

from clikit import Cli

from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV


def _env(tmp_path: Path) -> dict[str, str]:
    return {
        "LAKEHOUSE_CONFIG_DIR": str(tmp_path / "config"),
        "LAKEHOUSE_CACHE_DIR": str(tmp_path / "cache"),
        "USERNAME": "ops",
    }


def test_keyless_local_init_warns(tmp_path: Path) -> None:
    code, [line] = Cli(_env(tmp_path))("init", "--lake", str(tmp_path / "lake"))
    assert code == 0
    assert line["warning"]["kind"] == "local_lake_unencrypted"
    assert ENCRYPTION_KEY_ENV in line["warning"]["message"]
    assert line["encrypted"] is False


def test_local_init_with_key_does_not_warn(tmp_path: Path) -> None:
    env = {**_env(tmp_path), ENCRYPTION_KEY_ENV: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
    code, [line] = Cli(env)("init", "--lake", str(tmp_path / "lake"))
    assert code == 0
    assert "warning" not in line
    assert line["encrypted"] is True

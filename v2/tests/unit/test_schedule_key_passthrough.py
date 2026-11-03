"""S1 Task 5: the encryption key never reaches a scheduled task definition (plan T5, B-17/B-20).

The app does not populate the scheduled process's environment (launchd does not inherit a shell env); the operator
does that through platform ops-config. The app's job is to keep the key out of the written task file.

The guard this test bites is `cli._task_env`'s `key != ENCRYPTION_KEY_ENV` exclusion (S1-IMPL-09): remove it and the
key lands in the task env, so this test goes RED. `schedule/base.check_env` is only a defence-in-depth secondary —
it never sees the key because `_task_env` filters it first, so the plan's named `check_env` mutant does not bite;
`_task_env` is the primary guard, and the mutant above is its biting test.
"""

from __future__ import annotations

from pathlib import Path

from clikit import Cli, FakeBackend

from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV

_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"


def _env(tmp_path: Path) -> dict[str, str]:
    return {
        "LAKEHOUSE_CONFIG_DIR": str(tmp_path / "config"),
        "LAKEHOUSE_CACHE_DIR": str(tmp_path / "cache"),
        "USERNAME": "ops",
    }


def test_encryption_key_never_reaches_the_task_file(tmp_path: Path) -> None:
    env = {**_env(tmp_path), ENCRYPTION_KEY_ENV: _KEY, "PATH": "/bin"}
    Cli(env)("init", "--lake", str(tmp_path / "lake"))
    backend = FakeBackend()
    c = Cli(env, schedule_backend=lambda: backend)
    assert c("config", "set", "schedule.cron", "0 6 * * *")[0] == 0
    assert c("config", "set", "schedule.enabled", "true")[0] == 0
    code, [line] = c("schedule", "install", "--system", "--run-as", "ops")
    assert code == 0 and line["installed"] is True
    [(kind, (_cron, _system, task_env, _run_as))] = backend.calls
    assert kind == "install"
    assert ENCRYPTION_KEY_ENV not in task_env, "the encryption key must never be written to a task definition"

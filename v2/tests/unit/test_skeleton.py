"""Skeleton tests: package metadata, errors, JSON logging, secret masking and telemetry names."""

from __future__ import annotations

import ast
import io
import json
import logging
import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

import lakehouse
from lakehouse import errors, telemetry
from lakehouse import logging as lh_logging
from lakehouse.mask import Masker

SECRET = "hf_abc123secretvalue"  # noqa: S105 - a fake test value, not a credential
SRC_ROOT = Path(lakehouse.__file__).resolve().parent


def test_version_is_string() -> None:
    assert isinstance(lakehouse.__version__, str)
    assert lakehouse.__version__


def test_errors_subclass_base() -> None:
    assert errors.__all__
    for name in errors.__all__:
        cls = getattr(errors, name)
        assert isinstance(cls, type)
        assert issubclass(cls, errors.LakehouseError)


def test_get_logger_writes_one_json_line(capsys: pytest.CaptureFixture[str]) -> None:
    lh_logging.get_logger("x").info("m", extra={"rows": 3})
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["msg"] == "m"
    assert record["rows"] == 3
    assert record["level"] == "INFO"
    assert record["logger"].endswith("x")
    assert "ts" in record


def test_secret_values_masked(capsys: pytest.CaptureFixture[str]) -> None:
    lh_logging.install_secret_masking({"HF_TOKEN": SECRET})
    try:
        lh_logging.get_logger("masking").info("token is %s", SECRET)
        print(f"printed {SECRET}")
        sys.stdout.flush()
        out = capsys.readouterr().out
    finally:
        lh_logging.remove_secret_masking()
    assert SECRET not in out
    assert out.count("***") >= 2


def test_secret_in_uncaught_exception_masked(tmp_path: Path) -> None:
    script = tmp_path / "boom.py"
    script.write_text(
        textwrap.dedent(
            """
            import os
            from lakehouse.logging import install_secret_masking
            install_secret_masking(os.environ)
            raise RuntimeError("leaked " + os.environ["HF_TOKEN"])
            """
        ),
        encoding="utf-8",
    )
    env = {**os.environ, "HF_TOKEN": SECRET}
    result = subprocess.run(  # noqa: S603
        [sys.executable, str(script)], capture_output=True, text=True, env=env, check=False, timeout=60
    )
    assert result.returncode != 0
    assert "RuntimeError" in result.stderr
    assert SECRET not in result.stderr
    assert "***" in result.stderr


def test_masker_masks_secret_split_across_chunks() -> None:
    masker = Masker([SECRET])
    out = io.BytesIO()
    stream = masker.stream(out)
    half = len(SECRET) // 2
    stream.feed(b"before " + SECRET[:half].encode())
    stream.feed(SECRET[half:].encode() + b" after\n")
    stream.close()
    text = out.getvalue().decode()
    assert SECRET not in text
    assert text == "before *** after\n"


def test_masker_ignores_short_values() -> None:
    masker = Masker(["abc", ""])
    assert masker.mask_bytes(b"abc def") == b"abc def"


def test_mask_wrapper_covers_failing_pytest(tmp_path: Path) -> None:
    test_file = tmp_path / "test_leak.py"
    test_file.write_text(
        textwrap.dedent(
            """
            import logging
            import os

            def test_leak():
                secret = os.environ["HF_TOKEN"]
                print("captured " + secret)
                logging.getLogger("child.logger").warning("logged %s", secret)
                assert secret == "something else"
            """
        ),
        encoding="utf-8",
    )
    env = {**os.environ, "HF_TOKEN": SECRET}
    cmd = [
        sys.executable,
        "-m",
        "lakehouse.mask",
        "--",
        sys.executable,
        "-m",
        "pytest",
        str(test_file),
        "-p",
        "no:cacheprovider",
        "-o",
        "addopts=",
        "--rootdir",
        str(tmp_path),
    ]
    result = subprocess.run(  # noqa: S603
        cmd, capture_output=True, text=True, env=env, cwd=tmp_path, check=False, timeout=120
    )
    combined = result.stdout + result.stderr
    assert "1 failed" in combined
    assert SECRET not in combined
    assert "***" in combined
    assert result.returncode == 1


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signal forwarding; runs on macOS and Linux")
def test_mask_wrapper_forwards_sigterm(tmp_path: Path) -> None:
    child = tmp_path / "child.py"
    child.write_text(
        textwrap.dedent(
            """
            import signal
            import sys
            import time

            def handler(signum, frame):
                print("got-term", flush=True)
                sys.exit(7)

            signal.signal(signal.SIGTERM, handler)
            print("ready", flush=True)
            time.sleep(60)
            """
        ),
        encoding="utf-8",
    )
    proc = subprocess.Popen(  # noqa: S603
        [sys.executable, "-m", "lakehouse.mask", "--", sys.executable, str(child)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    deadline = time.monotonic() + 30
    line = ""
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if "ready" in line:
            break
    assert "ready" in line
    proc.send_signal(signal.SIGTERM)
    rest, _ = proc.communicate(timeout=30)
    assert "got-term" in rest
    assert proc.returncode == 7


TELEMETRY = SRC_ROOT / "telemetry.py"


def _calls(root: Path) -> list[tuple[Path, str, object]]:
    """Every call of a span function under `root`: (file, function name, first argument)."""
    found: list[tuple[Path, str, object]] = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else func.id if isinstance(func, ast.Name) else ""
            if name in {"start_as_current_span", "start_span", "step_span"}:
                first = node.args[0] if node.args else None
                found.append((path, name, first.value if isinstance(first, ast.Constant) else first))
    return found


def _string_constants(root: Path) -> set[str]:
    found: set[str] = set()
    for path in root.rglob("*.py"):
        if path == TELEMETRY:
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), filename=str(path))):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                found.add(node.value)
    return found


def test_span_names_registered() -> None:
    for path, function, name in _calls(SRC_ROOT):
        if function != "step_span":
            assert path == TELEMETRY, f"{path}: open spans with telemetry.step_span, not {function}"
            continue
        assert isinstance(name, str), f"{path}: span name must be a string literal"
        assert name in telemetry.SPAN_NAMES, f"{path}: span {name!r} is not in SPAN_NAMES"


def test_every_registered_name_is_used() -> None:
    """The name lists hold no dead names: each span name opens a step and each attribute name is set somewhere."""
    used = {name for _, function, name in _calls(SRC_ROOT) if function == "step_span"}
    assert used == telemetry.SPAN_NAMES
    assert telemetry.ATTRIBUTE_NAMES <= _string_constants(SRC_ROOT)


def test_step_span_logs_source_rows_and_timing(capsys: pytest.CaptureFixture[str]) -> None:
    log = lh_logging.get_logger("step-test")
    with telemetry.step_span("lakehouse.run.write", {"lakehouse.module_id": "idsse_public"}, log=log) as step:
        step.set("lakehouse.rows", 3)
    with (
        pytest.raises(RuntimeError, match="boom"),
        telemetry.step_span("lakehouse.run.parse", {"lakehouse.provider": "idsse"}, log=log),
    ):
        raise RuntimeError("boom")
    ok, failed = (json.loads(line) for line in capsys.readouterr().out.splitlines()[-2:])
    assert (ok["msg"], ok["source"], ok["rows"], ok["status"]) == ("lakehouse.run.write", "idsse_public", 3, "ok")
    assert ok["module_id"] == "idsse_public"
    assert isinstance(ok["elapsed_ms"], int)
    assert (failed["msg"], failed["source"], failed["status"]) == ("lakehouse.run.parse", "idsse", "error")
    assert "rows" not in failed


def test_step_span_refuses_unregistered_names() -> None:
    log = lh_logging.get_logger("step-test")
    with pytest.raises(ValueError, match="SPAN_NAMES"), telemetry.step_span("lakehouse.nope", log=log):  # type: ignore[arg-type]
        pass
    with (
        pytest.raises(ValueError, match="ATTRIBUTE_NAMES"),
        telemetry.step_span(
            "lakehouse.run",
            {"lakehouse.nope": 1},  # type: ignore[dict-item]
            log=log,
        ),
    ):
        pass
    with telemetry.step_span("lakehouse.run", log=log) as step, pytest.raises(ValueError, match="ATTRIBUTE_NAMES"):
        step.set("lakehouse.nope", 1)  # type: ignore[arg-type]


def test_telemetry_names_are_namespaced() -> None:
    assert telemetry.SPAN_NAMES
    assert telemetry.ATTRIBUTE_NAMES
    for name in telemetry.SPAN_NAMES | telemetry.ATTRIBUTE_NAMES:
        assert name.startswith("lakehouse.")


def test_logging_uses_no_root_handlers() -> None:
    lh_logging.get_logger("y")
    assert not any(isinstance(h, lh_logging.JsonLineHandler) for h in logging.getLogger().handlers)

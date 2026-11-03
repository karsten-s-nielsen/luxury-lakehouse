"""Secret masking for process output (spec §20).

`Masker` replaces every known secret value with `***`. `python -m lakehouse.mask -- <command> [args...]` runs a command
as a child process and masks everything it writes to stdout and stderr. That covers output that no in-process filter
reaches: pytest's own report, captured output, child processes and C-level prints. Every HF Job command runs under it.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
from collections.abc import Iterable, Mapping, Sequence
from types import FrameType
from typing import IO, Final, Protocol

from lakehouse.limits import raise_open_file_limit

SECRET_ENV_VARS: Final[tuple[str, ...]] = (
    "HF_TOKEN",
    "HF_S3_ACCESS_KEY_ID",
    "HF_S3_SECRET_ACCESS_KEY",
    "PINING_FOR_THE_DATA_TOKEN",
    "PINING_LIST_TOKEN",
    "GITHUB_TOKEN",
    "OAUTH_CLIENT_SECRET",
    "LAKEHOUSE_ENCRYPTION_KEY",
)
MASK: Final = "***"
# A value shorter than this is not treated as a secret: it would mask unrelated text everywhere.
MIN_SECRET_LENGTH: Final = 8
# A partial line longer than this is emitted (masked) without waiting for its newline.
MAX_PENDING_BYTES: Final = 64 * 1024
_USAGE: Final = "usage: python -m lakehouse.mask -- <command> [args...]\n"


class _ByteSink(Protocol):
    def write(self, data: bytes, /) -> object: ...

    def flush(self) -> object: ...


def secret_values(env: Mapping[str, str]) -> list[str]:
    """The values of the known secret variables that `env` sets."""
    return [env[name] for name in SECRET_ENV_VARS if env.get(name)]


class Masker:
    """Replaces every secret value with `***`, in text or in bytes."""

    def __init__(self, secrets: Iterable[str]) -> None:
        values = sorted({value for value in secrets if len(value) >= MIN_SECRET_LENGTH}, key=len, reverse=True)
        self._text: tuple[str, ...] = tuple(values)
        self._bytes: tuple[bytes, ...] = tuple(value.encode("utf-8") for value in values)
        longest = max((len(value) for value in self._bytes), default=0)
        self.overlap: int = max(longest - 1, 0)

    def mask_text(self, text: str) -> str:
        for value in self._text:
            text = text.replace(value, MASK)
        return text

    def mask_bytes(self, data: bytes) -> bytes:
        mask = MASK.encode("ascii")
        for value in self._bytes:
            data = data.replace(value, mask)
        return data

    def stream(self, sink: _ByteSink) -> MaskingStream:
        return MaskingStream(self, sink)


class MaskingStream:
    """A byte stream that masks complete lines, and long partial lines with an overlap kept back.

    A secret never holds a newline, so it can only be split across two writes inside one line. A pending partial line
    longer than MAX_PENDING_BYTES is masked and emitted, except its last `overlap` bytes, which may start a secret that
    the next write completes.
    """

    def __init__(self, masker: Masker, sink: _ByteSink) -> None:
        self._masker = masker
        self._sink = sink
        self._pending = b""

    def feed(self, data: bytes) -> None:
        self._pending += data
        emitted = False
        while (newline := self._pending.find(b"\n")) >= 0:
            line, self._pending = self._pending[: newline + 1], self._pending[newline + 1 :]
            self._sink.write(self._masker.mask_bytes(line))
            emitted = True
        if len(self._pending) > MAX_PENDING_BYTES:
            masked = self._masker.mask_bytes(self._pending)
            keep = self._masker.overlap
            head, self._pending = (masked[:-keep], masked[-keep:]) if keep else (masked, b"")
            self._sink.write(head)
            emitted = True
        if emitted:
            self._sink.flush()

    def close(self) -> None:
        if self._pending:
            self._sink.write(self._masker.mask_bytes(self._pending))
            self._pending = b""
        self._sink.flush()


def _pump(source: IO[bytes], stream: MaskingStream) -> None:
    fd = source.fileno()
    while chunk := os.read(fd, 65536):
        stream.feed(chunk)
    stream.close()


def _exit_code(returncode: int) -> int:
    # On POSIX a child killed by a signal has a negative return code; report it the shell way.
    return 128 - returncode if returncode < 0 else returncode


def main(argv: Sequence[str] | None = None) -> int:
    raise_open_file_limit()  # the wrapped command inherits it (T12 spec D1)
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) < 2 or args[0] != "--":
        sys.stderr.write(_USAGE)
        return 2
    masker = Masker(secret_values(os.environ))
    proc = subprocess.Popen(args[1:], stdout=subprocess.PIPE, stderr=subprocess.PIPE)  # noqa: S603 - the operator's own command

    def forward(signum: int, _frame: FrameType | None) -> None:
        if proc.poll() is not None:
            return
        if sys.platform == "win32":
            proc.terminate()
        else:
            proc.send_signal(signum)

    signal.signal(signal.SIGTERM, forward)
    if sys.platform == "win32":
        # The console already delivers Ctrl+C to the child; the wrapper only waits for it.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    else:
        signal.signal(signal.SIGINT, forward)

    if proc.stdout is None or proc.stderr is None:
        raise RuntimeError("the child process has no stdout or stderr pipe")
    pumps = [
        threading.Thread(target=_pump, args=(proc.stdout, masker.stream(sys.stdout.buffer)), daemon=True),
        threading.Thread(target=_pump, args=(proc.stderr, masker.stream(sys.stderr.buffer)), daemon=True),
    ]
    for pump in pumps:
        pump.start()
    returncode = proc.wait()
    for pump in pumps:
        pump.join()
    return _exit_code(returncode)


if __name__ == "__main__":
    sys.exit(main())

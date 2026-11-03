"""JSON-line logging on stdout (spec §21) and in-process secret masking (spec §20).

`get_logger` returns a logger under the `lakehouse` namespace that writes one JSON object per line to stdout.
`install_secret_masking` masks secret values in every log record and in everything written to `sys.stdout` and
`sys.stderr`. Output that no in-process filter reaches is covered by the `python -m lakehouse.mask` wrapper.
"""

from __future__ import annotations

import json
import logging
import sys
import threading
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, Final, TextIO

from lakehouse.mask import MAX_PENDING_BYTES, Masker, secret_values
from lakehouse.telemetry import current_trace_ids

ROOT_LOGGER: Final = "lakehouse"
_STANDARD_RECORD_ATTRS: Final[frozenset[str]] = frozenset(
    vars(logging.LogRecord("x", logging.INFO, "x", 0, "x", None, None))
) | {"message", "asctime"}

__all__ = [
    "JsonLineHandler",
    "active_masker",
    "get_logger",
    "install_secret_masking",
    "record_to_payload",
    "remove_secret_masking",
]


def record_to_payload(record: logging.LogRecord) -> dict[str, Any]:
    """The JSON object for a log record: the base fields, every non-standard extra, the exception text, and — when an
    SDK span is current — the trace correlation ids (spec §7.1, §21). The collected log copy (observability.py) reuses
    this so stdout and the collected copy carry the same fields."""
    payload: dict[str, Any] = {
        "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
        "level": record.levelname,
        "logger": record.name,
        "msg": record.getMessage(),
    }
    for key, value in vars(record).items():
        if key not in _STANDARD_RECORD_ATTRS and not key.startswith("_"):
            payload[key] = value
    if record.exc_info:
        payload["exc"] = logging.Formatter().formatException(record.exc_info)
    trace_id, span_id = current_trace_ids()
    if trace_id is not None:
        payload["trace_id"] = trace_id
        payload["span_id"] = span_id
    return payload


class JsonLineHandler(logging.Handler):
    """Writes each record as one JSON line to the current `sys.stdout` (resolved at emit time)."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            payload = record_to_payload(record)
            stream = sys.stdout
            stream.write(json.dumps(payload, default=str, ensure_ascii=False) + "\n")
            stream.flush()
        except Exception:  # noqa: BLE001 - logging.Handler contract: never raise from emit; handleError reports it
            self.handleError(record)


_config_lock = threading.Lock()


def _configured_root() -> logging.Logger:
    root = logging.getLogger(ROOT_LOGGER)
    with _config_lock:
        if not any(isinstance(handler, JsonLineHandler) for handler in root.handlers):
            root.addHandler(JsonLineHandler())
            root.setLevel(logging.INFO)
            root.propagate = False
    return root


def get_logger(name: str) -> logging.Logger:
    """A logger under the `lakehouse` namespace that writes JSON lines to stdout."""
    _configured_root()
    if name == ROOT_LOGGER or name.startswith(ROOT_LOGGER + "."):
        return logging.getLogger(name)
    return logging.getLogger(f"{ROOT_LOGGER}.{name}")


class _MaskingTextStream:
    """A text stream wrapper that masks secrets in whole lines before they reach the wrapped stream."""

    def __init__(self, wrapped: TextIO, masker: Masker) -> None:
        self.wrapped = wrapped
        self._masker = masker
        self._pending = ""
        self._lock = threading.Lock()

    @property
    def encoding(self) -> str:
        return self.wrapped.encoding

    def write(self, text: str) -> int:
        with self._lock:
            self._pending += text
            while (newline := self._pending.find("\n")) >= 0:
                line, self._pending = self._pending[: newline + 1], self._pending[newline + 1 :]
                self.wrapped.write(self._masker.mask_text(line))
            if len(self._pending) > MAX_PENDING_BYTES:
                masked = self._masker.mask_text(self._pending)
                keep = self._masker.overlap
                head, self._pending = (masked[:-keep], masked[-keep:]) if keep else (masked, "")
                self.wrapped.write(head)
        return len(text)

    def flush(self) -> None:
        with self._lock:
            if self._pending:
                self.wrapped.write(self._masker.mask_text(self._pending))
                self._pending = ""
        self.wrapped.flush()

    def isatty(self) -> bool:
        return self.wrapped.isatty()

    def fileno(self) -> int:
        return self.wrapped.fileno()

    def writable(self) -> bool:
        return True


_mask_lock = threading.Lock()
_original_factory: Callable[..., logging.LogRecord] | None = None
_active_masker: Masker | None = None


def active_masker() -> Masker | None:
    """The masker installed by `install_secret_masking`, or `None` when masking is off. The log collector
    (observability.py) uses it to mask every field of the collected copy, not only the message (spec §9)."""
    return _active_masker


def install_secret_masking(env: Mapping[str, str]) -> None:
    """Mask the secret values that `env` sets in every log record and in all stdout and stderr output.

    Calling it again replaces the earlier masking. `remove_secret_masking` undoes it.
    """
    global _original_factory, _active_masker
    masker = Masker(secret_values(env))
    with _mask_lock:
        _remove_locked()
        _active_masker = masker
        base_factory = logging.getLogRecordFactory()

        def masked_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
            record = base_factory(*args, **kwargs)
            try:
                message = record.getMessage()
            except (TypeError, ValueError):  # a malformed format string: leave the record as it is
                return record
            record.msg = masker.mask_text(message)
            record.args = ()
            return record

        _original_factory = base_factory
        logging.setLogRecordFactory(masked_factory)
        sys.stdout = _MaskingTextStream(sys.stdout, masker)  # type: ignore[assignment]
        sys.stderr = _MaskingTextStream(sys.stderr, masker)  # type: ignore[assignment]


def _remove_locked() -> None:
    global _original_factory, _active_masker
    _active_masker = None
    if _original_factory is not None:
        logging.setLogRecordFactory(_original_factory)
        _original_factory = None
    for name in ("stdout", "stderr"):
        stream: object = getattr(sys, name)
        if isinstance(stream, _MaskingTextStream):
            stream.flush()
            setattr(sys, name, stream.wrapped)


def remove_secret_masking() -> None:
    """Undo `install_secret_masking`: restore the record factory and the original streams."""
    with _mask_lock:
        _remove_locked()

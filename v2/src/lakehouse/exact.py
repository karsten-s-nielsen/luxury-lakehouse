"""Exactness to integer millimetres (spec §10.2; the owner moved the unit from centimetres on 2026-10-27).

Coordinates are stored as exact `int32` millimetres. A source value with more than 3 decimals cannot be stored exactly,
so it fails loudly. The check runs on the source representation:

- text sources (JSON, JSONL, XML, CSV): the decimal text itself, before any float conversion;
- binary floats: the shortest text that round-trips in the value's own dtype (float32 or float64), then the text rule;
- IDSSE XML: the `X` and `Y` attribute text, in one streaming pass (silly-kicks rounds its output to 4 decimals, so a
  check on its output would miss a source value such as `12.340049`).

A float NaN is read as a missing value (null), never as a number; an infinity fails.
"""

from __future__ import annotations

import json
import math
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final, Literal, cast
from xml.sax.handler import ContentHandler
from xml.sax.xmlreader import AttributesImpl, Locator

import defusedxml.sax
import numpy as np
import pyarrow as pa

from lakehouse.errors import InexactValueError

_INT32_MIN: Final = -(2**31)
_INT32_MAX: Final = 2**31 - 1
_THOUSAND: Final = Decimal(1000)

FloatDtype = Literal["float32", "float64"]
Source = Literal["text", "float32", "float64"]


def text_to_mm(text: str) -> int:
    """Exact millimetres from decimal text; raises InexactValueError when the text has more than 3 decimals."""
    try:
        value = Decimal(text.strip())
    except InvalidOperation:
        raise InexactValueError(f"{text!r} is not a decimal number") from None
    if not value.is_finite():
        raise InexactValueError(f"{text!r} is not a finite number")
    mm = value * _THOUSAND
    if mm != mm.to_integral_value():
        raise InexactValueError(f"{text!r} has more than 3 decimals, so it is not exact in millimetres")
    result = int(mm)
    if not _INT32_MIN <= result <= _INT32_MAX:
        raise InexactValueError(f"{text!r} is out of the int32 millimetre range")
    return result


def _shortest_text(value: float, dtype: FloatDtype) -> str:
    typed = np.float32(value) if dtype == "float32" else np.float64(value)
    return np.format_float_positional(typed, unique=True, trim="-")


def float_to_mm(value: float, dtype: FloatDtype) -> int:
    """Exact millimetres from a binary float, judged by its shortest round-trip text in its own dtype."""
    if math.isnan(value) or math.isinf(value):
        raise InexactValueError(f"{value!r} is not a finite number")
    try:
        return text_to_mm(_shortest_text(value, dtype))
    except InexactValueError as exc:
        raise InexactValueError(f"{value!r} ({dtype}): {exc}") from None


def column_to_mm(values: pa.Array[Any], *, source: Source) -> pa.Int32Array:
    """Exact millimetres for a whole column; nulls (and float NaN) stay null.

    Each distinct value is checked once, so a tracking column of millions of rows costs one check per distinct
    coordinate.
    """
    if source == "text":
        texts = cast(list[str | None], values.to_pylist())
        cache: dict[str, int] = {}
        out: list[int | None] = []
        for text in texts:
            if text is None:
                out.append(None)
                continue
            if text not in cache:
                cache[text] = text_to_mm(text)
            out.append(cache[text])
        return pa.array(out, type=pa.int32())
    floats = values.to_numpy(zero_copy_only=False).astype(np.float64 if source == "float64" else np.float32)
    missing = np.isnan(floats)
    present = floats[~missing]
    uniques, inverse = np.unique(present, return_inverse=True)
    mms = np.array([float_to_mm(float(u), source) for u in uniques], dtype=np.int32)
    result = np.zeros(len(floats), dtype=np.int32)
    result[~missing] = mms[inverse]
    return pa.array(result, type=pa.int32(), mask=missing)


class _AttributeDecimalsHandler(ContentHandler):
    def __init__(self, path: Path, element: str, attributes: tuple[str, ...]) -> None:
        super().__init__()
        self._path = path
        self._element = element
        self._attributes = attributes
        self._locator: Locator | None = None

    def setDocumentLocator(self, locator: Locator) -> None:  # noqa: N802 - SAX API name
        self._locator = locator

    def startElement(self, name: str, attrs: AttributesImpl) -> None:  # noqa: N802 - SAX API name
        if name != self._element:
            return
        for attribute in self._attributes:
            text = attrs.get(attribute)
            if text is None:
                continue
            try:
                text_to_mm(text)
            except InexactValueError as exc:
                line = self._locator.getLineNumber() if self._locator is not None else -1
                raise InexactValueError(f"{self._path.name}: line {line}: {attribute}={text!r}: {exc}") from None


def check_xml_attribute_decimals(path: Path, *, element: str, attributes: tuple[str, ...]) -> None:
    """Stream an XML file and fail on the first `element` whose `attributes` text has more than 3 decimals.

    Uses defusedxml's SAX parser: no tree is built, and entity and DTD tricks are refused.
    """
    parser = defusedxml.sax.make_parser()
    parser.setContentHandler(_AttributeDecimalsHandler(path, element, attributes))
    parser.parse(str(path))


def _exact_float(text: str) -> float:
    value = float(text)
    if Decimal(repr(value)) != Decimal(text):
        raise InexactValueError(f"the JSON number {text} has more digits than a float64 holds")
    return value


def loads_exact(data: bytes | str) -> Any:
    """`json.loads` that refuses any number whose text a float64 cannot reproduce exactly."""
    return json.loads(data, parse_float=_exact_float)

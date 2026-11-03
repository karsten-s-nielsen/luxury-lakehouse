"""Exactness to integer millimetres (spec §10.2 as amended by the owner on 2026-10-27; §23.6 "Exactness" and "IDSSE
exactness")."""

from __future__ import annotations

from pathlib import Path

import defusedxml
import numpy as np
import pyarrow as pa
import pytest

from lakehouse.errors import InexactValueError
from lakehouse.exact import check_xml_attribute_decimals, column_to_mm, float_to_mm, text_to_mm


def test_text_three_decimals_ok() -> None:
    assert text_to_mm("12.345") == 12345
    assert text_to_mm("12.34") == 12340
    assert text_to_mm("12.3") == 12300
    assert text_to_mm("12") == 12000


def test_text_four_decimals_fails() -> None:
    with pytest.raises(InexactValueError, match=r"12\.3456"):
        text_to_mm("12.3456")


def test_text_exponent_and_trailing_zero_forms() -> None:
    assert text_to_mm("1e-03") == 1
    assert text_to_mm("12.3450") == 12345
    with pytest.raises(InexactValueError):
        text_to_mm("1e-05")


def test_text_rejects_non_numbers() -> None:
    for bad in ("NaN", "inf", "", "12,34", "abc"):
        with pytest.raises(InexactValueError):
            text_to_mm(bad)


def test_out_of_int32_range_fails() -> None:
    with pytest.raises(InexactValueError, match="int32"):
        text_to_mm("2147483.648")


def test_float32_shortest_text_ok() -> None:
    assert float_to_mm(float(np.float32(12.345)), "float32") == 12345


def test_float64_four_decimals_fails() -> None:
    with pytest.raises(InexactValueError, match=r"12\.3456"):
        float_to_mm(12.3456, "float64")


def test_float64_three_decimals_ok() -> None:
    assert float_to_mm(52.5, "float64") == 52500
    assert float_to_mm(-34.012, "float64") == -34012


def test_float32_value_read_as_float64_is_inexact() -> None:
    # np.float32(12.34) widened to float64 is 12.34000015258789...: only its own dtype gives the short text.
    with pytest.raises(InexactValueError):
        float_to_mm(float(np.float32(12.34)), "float64")


def test_negative_and_zero() -> None:
    assert text_to_mm("-0.001") == -1
    assert text_to_mm("0") == 0
    assert text_to_mm("-0") == 0
    assert float_to_mm(-0.0, "float64") == 0


def test_column_to_mm_float32_with_nulls_and_nan() -> None:
    values = [*pa.array(np.array([12.34, -0.5, 12.34, np.nan], dtype=np.float32)).to_pylist(), None]
    arr = pa.array(values, type=pa.float32())
    out = column_to_mm(arr, source="float32")
    assert out.type == pa.int32()
    assert out.to_pylist() == [12340, -500, 12340, None, None]


def test_column_to_mm_text() -> None:
    out = column_to_mm(pa.array(["1.5", "-2.255", None]), source="text")
    assert out.to_pylist() == [1500, -2255, None]


def test_column_to_mm_names_the_inexact_value() -> None:
    with pytest.raises(InexactValueError, match=r"1\.2345"):
        column_to_mm(pa.array([1.5, 1.2345], type=pa.float64()), source="float64")


def test_xml_attribute_rounding_trap(tmp_path: Path) -> None:
    # silly-kicks rounds parsed values to 4 decimals, so a check on its output would pass 12.340049 as 12.34.
    xml = tmp_path / "positions.xml"
    xml.write_text(
        '<?xml version="1.0" encoding="utf-8"?>\n'
        '<Positions><FrameSet><Frame N="1" X="12.34" Y="-3.50"/>\n'
        '<Frame N="2" X="12.340049" Y="1.00"/></FrameSet></Positions>\n',
        encoding="utf-8",
    )
    assert round(12.340049, 4) == 12.34
    with pytest.raises(InexactValueError, match=r"line 3.*12\.340049"):
        check_xml_attribute_decimals(xml, element="Frame", attributes=("X", "Y"))


def test_xml_attribute_check_passes_exact_file(tmp_path: Path) -> None:
    xml = tmp_path / "positions.xml"
    xml.write_text('<Positions><Frame N="1" X="12.345" Y="-3.5" Z="0.1234"/></Positions>', encoding="utf-8")
    check_xml_attribute_decimals(xml, element="Frame", attributes=("X", "Y"))


def test_xml_check_refuses_entity_expansion(tmp_path: Path) -> None:
    xml = tmp_path / "bomb.xml"
    xml.write_text(
        '<?xml version="1.0"?><!DOCTYPE r [<!ENTITY a "x">]><Positions><Frame X="&a;"/></Positions>',
        encoding="utf-8",
    )
    with pytest.raises(defusedxml.EntitiesForbidden):
        check_xml_attribute_decimals(xml, element="Frame", attributes=("X",))

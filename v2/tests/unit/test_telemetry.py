"""The OpenTelemetry SDK provider factory and the cycle-2 span attributes (spec §7.1; plan Task 1)."""

from __future__ import annotations

import logging

from lakehouse.telemetry import ATTRIBUTE_NAMES, install_providers, record_skipped, step_span

_LOG = logging.getLogger("lakehouse.test")


def test_new_attribute_names_registered() -> None:
    assert {
        "lakehouse.layer",
        "lakehouse.state",
        "lakehouse.execution_context",
        "lakehouse.flavour",
    } <= ATTRIBUTE_NAMES


def test_providers_off_is_noop() -> None:
    providers = install_providers("off")
    try:
        assert providers.span_exporter is None
        with step_span("lakehouse.run.write", {"lakehouse.layer": "bronze"}, log=_LOG):
            pass  # no SDK: records nothing, does not raise
    finally:
        install_providers("off")


def test_providers_memory_records_span_with_attributes() -> None:
    providers = install_providers("memory")
    try:
        with step_span("lakehouse.run.write", {"lakehouse.layer": "bronze"}, log=_LOG):
            pass
        assert providers.span_exporter is not None
        write = next(s for s in providers.span_exporter.get_finished_spans() if s.name == "lakehouse.run.write")
        assert write.attributes is not None
        assert write.attributes["lakehouse.layer"] == "bronze"
        assert write.attributes["lakehouse.state"] == "ok"
    finally:
        install_providers("off")


def test_skipped_step_emits_span() -> None:
    providers = install_providers("memory")
    try:
        record_skipped("lakehouse.run.dbt", {"lakehouse.layer": "dbt"}, log=_LOG)
        assert providers.span_exporter is not None
        dbt = next(s for s in providers.span_exporter.get_finished_spans() if s.name == "lakehouse.run.dbt")
        assert dbt.attributes is not None
        assert dbt.attributes["lakehouse.state"] == "skipped"
        assert dbt.end_time is not None and dbt.start_time is not None and dbt.end_time >= dbt.start_time
    finally:
        install_providers("off")

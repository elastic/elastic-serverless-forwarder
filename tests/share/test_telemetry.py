# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import pytest

from share.telemetry import is_telemetry_enabled


@pytest.mark.unit
def test_is_telemetry_enabled_with_true_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TELEMETRY_DEPLOYMENT_ID", "123")

    monkeypatch.setenv("TELEMETRY_ENABLED", "true")
    assert is_telemetry_enabled() is True

    monkeypatch.setenv("TELEMETRY_ENABLED", "True")
    assert is_telemetry_enabled() is True

    monkeypatch.setenv("TELEMETRY_ENABLED", "1")
    assert is_telemetry_enabled() is True

    monkeypatch.setenv("TELEMETRY_ENABLED", "yes")
    assert is_telemetry_enabled() is True

    monkeypatch.setenv("TELEMETRY_ENABLED", "on")
    assert is_telemetry_enabled() is True


@pytest.mark.unit
def test_is_telemetry_enabled_with_false_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TELEMETRY_DEPLOYMENT_ID", "123")

    monkeypatch.setenv("TELEMETRY_ENABLED", "false")
    assert is_telemetry_enabled() is False

    monkeypatch.setenv("TELEMETRY_ENABLED", "False")
    assert is_telemetry_enabled() is False

    monkeypatch.setenv("TELEMETRY_ENABLED", "0")
    assert is_telemetry_enabled() is False

    monkeypatch.setenv("TELEMETRY_ENABLED", "no")
    assert is_telemetry_enabled() is False

    monkeypatch.setenv("TELEMETRY_ENABLED", "off")
    assert is_telemetry_enabled() is False


@pytest.mark.unit
def test_is_telemetry_enabled_with_no_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TELEMETRY_DEPLOYMENT_ID", "123")

    # if there is no `TELEMETRY_ENABLED` env variable,
    # the telemetry should be disabled
    monkeypatch.delenv("TELEMETRY_ENABLED", raising=False)
    assert is_telemetry_enabled() is False


@pytest.mark.unit
def test_is_telemetry_enabled_with_invalid_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TELEMETRY_DEPLOYMENT_ID", "123")

    # if the `TELEMETRY_ENABLED` env variable contains,
    # an invalid value, the telemetry should be disabled
    monkeypatch.setenv("TELEMETRY_ENABLED", "hey")
    assert is_telemetry_enabled() is False


def test_is_telemetry_enabled_with_no_deployment_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TELEMETRY_DEPLOYMENT_ID", raising=False)

    # if there is no `TELEMETRY_DEPLOYMENT_ID` env variable,
    # the telemetry should be disabled regardless of the
    # value of `TELEMETRY_ENABLED`
    monkeypatch.setenv("TELEMETRY_ENABLED", "true")
    assert is_telemetry_enabled() is False

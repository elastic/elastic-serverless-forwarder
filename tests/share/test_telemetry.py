import pytest

from share.telemetry import is_telemetry_enabled


@pytest.mark.unit
def test_is_telemetry_enabled_with_true_values(monkeypatch: pytest.MonkeyPatch) -> None:
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
    # in there is not `TELEMETRY_ENABLED` env variable,
    # the telemetry should be disabled
    monkeypatch.delenv("TELEMETRY_ENABLED")
    assert is_telemetry_enabled() is False


@pytest.mark.unit
def test_is_telemetry_enabled_with_invalid_value(monkeypatch: pytest.MonkeyPatch) -> None:
    # in there is not `TELEMETRY_ENABLED` env variable,
    # the telemetry should be disabled
    monkeypatch.setenv("TELEMETRY_ENABLED", "hey")
    assert is_telemetry_enabled() is False

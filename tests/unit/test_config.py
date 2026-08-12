from __future__ import annotations

import json
from pathlib import Path

import pytest

from broka.config import BrokerConfig, ConfigLoader, FrozenMapping, SecretValue, environment_config
from broka.exceptions import ConfigurationError


def test_configuration_precedence_is_deterministic(tmp_path: Path) -> None:
    path = tmp_path / "pyev.json"
    path.write_text(
        json.dumps(
            {
                "engine": "file",
                "engines": {"redis": {"url": "redis://file", "db": 1}},
            }
        ),
        encoding="utf-8",
    )
    config = ConfigLoader().load(
        defaults={"engine": "default", "engines": {"redis": {"db": 0}}},
        files=path,
        framework={"engine": "framework", "engines": {"redis": {"db": 2}}},
        environ={"PYEV_ENGINE": "env", "PYEV_ENGINES__REDIS__DB": "3"},
        overrides={"engine": "construction"},
    )

    assert config.engine == "construction"
    assert config.engine_settings("redis")["url"] == "redis://file"
    assert config.engine_settings("redis")["db"] == 3


def test_environment_provider_uses_pyev_prefix_and_json_values() -> None:
    values = environment_config(
        {
            "OTHER_ENGINE": "ignored",
            "PYEV_ALLOW_MEMORY_FALLBACK": "true",
            "PYEV_ENGINES__MEMORY__CAPACITY": "128",
        }
    )

    assert values == {
        "allow_memory_fallback": True,
        "engines": {"memory": {"capacity": 128}},
    }


def test_config_is_immutable_and_redacts_secrets_and_urls() -> None:
    config = BrokerConfig.from_mapping(
        {
            "engine": "redis",
            "engines": {
                "redis": {
                    "url": "redis://alice:password@example.test:6379/0",
                    "password": "swordfish",
                }
            },
        }
    )
    settings = config.engine_settings()

    assert isinstance(settings["password"], SecretValue)
    assert "swordfish" not in repr(config)
    assert "password@example" not in repr(config)
    assert settings.as_dict(reveal_secrets=True)["password"] == "swordfish"
    with pytest.raises(TypeError):
        settings["password"] = "changed"  # type: ignore[index]


def test_malformed_credential_url_is_still_redacted() -> None:
    config = BrokerConfig.from_mapping(
        {"engines": {"redis": {"url": "redis://alice:password@example.test:bad/0"}}}
    )

    assert "password" not in repr(config)


def test_from_mapping_does_not_read_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYEV_ENGINE", "redis")

    assert BrokerConfig.from_mapping().engine == "memory"


def test_validation_reports_path_and_type_without_value() -> None:
    with pytest.raises(ConfigurationError) as caught:
        BrokerConfig.from_mapping({"engines": {"redis": "secret-connection-string"}})

    message = str(caught.value)
    assert "engines.redis" in message
    assert "str" in message
    assert "secret-connection-string" not in message


def test_nested_mapping_is_frozen() -> None:
    value = FrozenMapping({"outer": {"inner": [1, 2]}})

    assert isinstance(value["outer"], FrozenMapping)
    assert value.get_path("outer.inner") == (1, 2)

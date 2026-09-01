from pathlib import Path

import pytest

from telegram_mcp.config import Settings
from telegram_mcp.errors import ConfigurationError


def base_env() -> dict[str, str]:
    return {
        "TELEGRAM_BOT_TOKEN": "123456:test-token",
        "TELEGRAM_ALLOWED_USER_IDS": "42",
    }


def test_local_defaults_are_private_and_polling() -> None:
    settings = Settings.from_env(base_env())
    assert settings.transport == "stdio"
    assert settings.ingress_mode == "polling"
    assert settings.host == "127.0.0.1"
    assert settings.allowed_user_ids == frozenset({42})
    assert settings.sqlite_synchronous == "FULL"
    assert len(settings.bot_key) == 24
    assert "test-token" not in settings.bot_key


def test_deny_by_default_requires_an_allowlist() -> None:
    with pytest.raises(ConfigurationError, match="Deny-by-default"):
        Settings.from_env({"TELEGRAM_BOT_TOKEN": "1:x"})


def test_allow_all_must_be_explicit() -> None:
    settings = Settings.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "TELEGRAM_ALLOW_ALL": "true"})
    assert settings.allow_all


def test_http_requires_independent_long_token_and_webhook_secret() -> None:
    env = base_env() | {
        "BRIDGE_TRANSPORT": "http",
        "MCP_AUTH_TOKEN": "m" * 32,
        "TELEGRAM_WEBHOOK_SECRET": "telegram_secret-1" + "x" * 20,
    }
    settings = Settings.from_env(env)
    assert settings.ingress_mode == "webhook"

    with pytest.raises(ConfigurationError, match="MCP_AUTH_TOKEN"):
        Settings.from_env(env | {"MCP_AUTH_TOKEN": "short"})
    with pytest.raises(ConfigurationError, match="TELEGRAM_WEBHOOK_SECRET"):
        Settings.from_env(env | {"TELEGRAM_WEBHOOK_SECRET": "not allowed!"})
    with pytest.raises(ConfigurationError, match="visible ASCII"):
        Settings.from_env(env | {"MCP_AUTH_TOKEN": "m" * 31 + "\n"})


def test_supplied_webhook_secret_is_strong_even_when_ingress_is_disabled() -> None:
    with pytest.raises(ConfigurationError, match="TELEGRAM_WEBHOOK_SECRET"):
        Settings.from_env(
            base_env()
            | {
                "TELEGRAM_INGRESS_MODE": "disabled",
                "TELEGRAM_WEBHOOK_SECRET": "guessable",
            }
        )


def test_stdio_rejects_webhook_mode() -> None:
    with pytest.raises(ConfigurationError, match="requires BRIDGE_TRANSPORT=http"):
        Settings.from_env(
            base_env()
            | {
                "TELEGRAM_INGRESS_MODE": "webhook",
                "TELEGRAM_WEBHOOK_SECRET": "valid_secret" + "x" * 24,
            }
        )


def test_parses_negative_chat_ids_without_float_conversion() -> None:
    settings = Settings.from_env(
        {
            "TELEGRAM_BOT_TOKEN": "1:x",
            "TELEGRAM_ALLOWED_CHAT_IDS": "-1001234567890, 99",
            "BRIDGE_DATABASE_PATH": "custom/queue.sqlite3",
        }
    )
    assert -1001234567890 in settings.allowed_chat_ids
    assert settings.database_path == Path("custom/queue.sqlite3")


def test_public_url_must_be_https() -> None:
    with pytest.raises(ConfigurationError, match="absolute HTTPS"):
        Settings.from_env(base_env() | {"PUBLIC_BASE_URL": "http://example.test"})


@pytest.mark.parametrize(
    "name,value",
    [
        ("TELEGRAM_CONNECT_TIMEOUT", "nan"),
        ("TELEGRAM_CONNECT_TIMEOUT", "inf"),
        ("TELEGRAM_CONNECT_TIMEOUT", "-inf"),
        ("TELEGRAM_CONNECT_TIMEOUT", "0"),
        ("TELEGRAM_CONNECT_TIMEOUT", "-1"),
        ("TELEGRAM_REQUEST_TIMEOUT", "not-a-number"),
    ],
)
def test_telegram_timeouts_must_be_finite_positive_numbers(name: str, value: str) -> None:
    with pytest.raises(ConfigurationError, match=name):
        Settings.from_env(base_env() | {name: value})


@pytest.mark.parametrize(
    "url",
    [
        "https://",
        "https://user:password@api.telegram.org",
        "https://api.telegram.org?debug=true",
        "https://api.telegram.org#fragment",
        "http://api.telegram.org",
        "javascript://api.telegram.org",
    ],
)
def test_telegram_api_base_rejects_ambiguous_or_unsafe_urls(url: str) -> None:
    with pytest.raises(ConfigurationError, match="TELEGRAM_API_BASE"):
        Settings.from_env(base_env() | {"TELEGRAM_API_BASE": url})


def test_loopback_http_telegram_api_base_is_allowed_for_local_test_servers() -> None:
    settings = Settings.from_env(base_env() | {"TELEGRAM_API_BASE": "http://127.0.0.1:8081"})
    assert settings.telegram_api_base == "http://127.0.0.1:8081"


@pytest.mark.parametrize(
    "url",
    [
        "https://user:password@bridge.example",
        "https://bridge.example?secret=value",
        "https://bridge.example#fragment",
        "//bridge.example",
    ],
)
def test_public_base_url_rejects_credentials_query_fragment_and_relative_urls(url: str) -> None:
    with pytest.raises(ConfigurationError, match="PUBLIC_BASE_URL"):
        Settings.from_env(base_env() | {"PUBLIC_BASE_URL": url})


@pytest.mark.parametrize(
    "path",
    [
        "telegram/webhook",
        "//telegram/webhook",
        "/telegram/../webhook",
        "/telegram/webhook?debug=1",
        "/telegram/webhook#fragment",
        "/telegram/%2e%2e/webhook",
        "/telegram\\webhook",
        "/telegram webook",
    ],
)
def test_webhook_path_is_a_simple_absolute_route(path: str) -> None:
    with pytest.raises(ConfigurationError, match="TELEGRAM_WEBHOOK_PATH"):
        Settings.from_env(base_env() | {"TELEGRAM_WEBHOOK_PATH": path})


@pytest.mark.parametrize("path", ["/mcp", "/healthz", "/readyz", "/.well-known/auth"])
def test_webhook_path_cannot_shadow_bridge_routes(path: str) -> None:
    with pytest.raises(ConfigurationError, match="TELEGRAM_WEBHOOK_PATH"):
        Settings.from_env(base_env() | {"TELEGRAM_WEBHOOK_PATH": path})


@pytest.mark.parametrize("host", ["", "bad host", "host/path", "host\nname"])
def test_mcp_bind_host_has_a_safe_shape(host: str) -> None:
    with pytest.raises(ConfigurationError, match="MCP_HOST"):
        Settings.from_env(base_env() | {"MCP_HOST": host})


def test_telegram_ids_outside_documented_52_bit_range_are_rejected() -> None:
    too_large = str(1 << 52)
    with pytest.raises(ConfigurationError, match="52-bit"):
        Settings.from_env(base_env() | {"TELEGRAM_ALLOWED_CHAT_IDS": too_large})


def test_ingress_user_allowlist_does_not_become_an_outbound_chat_allowlist() -> None:
    settings = Settings.from_env(base_env())
    assert not settings.outbound_target_is_explicitly_allowed(42)

    settings = Settings.from_env(base_env() | {"TELEGRAM_ALLOWED_CHAT_IDS": "42"})
    assert settings.outbound_target_is_explicitly_allowed(42)

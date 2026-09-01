"""Configuration parsing with secure, explicit defaults."""

from __future__ import annotations

import hashlib
import math
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from telegram_mcp.errors import ConfigurationError

_SECRET_RE = re.compile(r"^[A-Za-z0-9_-]{1,256}$")
_BOT_TOKEN_RE = re.compile(r"^[0-9]{1,20}:[A-Za-z0-9_-]+$")
_WEBHOOK_PATH_RE = re.compile(r"^/(?!/)[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*$")
_HOST_RE = re.compile(r"^[A-Za-z0-9.:-]{1,253}$")
_LOOPBACK = {"127.0.0.1", "localhost", "::1"}
_RESERVED_ROUTES = {"/healthz", "/readyz", "/mcp"}
_MAX_TELEGRAM_ID = (1 << 52) - 1
DEFAULT_ALLOWED_UPDATES = (
    "message",
    "edited_message",
    "channel_post",
    "edited_channel_post",
    "callback_query",
)


def _csv_ints(raw: str | None, name: str) -> frozenset[int]:
    if not raw or not raw.strip():
        return frozenset()
    values: set[int] = set()
    for item in raw.split(","):
        try:
            value = int(item.strip())
        except ValueError as exc:
            raise ConfigurationError(f"{name} must be a comma-separated list of integer IDs") from exc
        if not -_MAX_TELEGRAM_ID <= value <= _MAX_TELEGRAM_ID:
            raise ConfigurationError(f"{name} contains an ID outside Telegram's 52-bit range")
        values.add(value)
    return frozenset(values)


def _csv_strings(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def _boolean(raw: str | None, default: bool = False) -> bool:
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"Expected a boolean value, got {raw!r}")


def _integer(raw: str | None, default: int, name: str, minimum: int, maximum: int) -> int:
    try:
        value = default if raw is None else int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
    return value


def _positive_float(
    raw: str | None,
    default: float,
    name: str,
    *,
    minimum: float = 0.05,
    maximum: float = 300.0,
) -> float:
    try:
        value = default if raw is None else float(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a number") from exc
    if not math.isfinite(value) or not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} must be finite and between {minimum} and {maximum}")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    telegram_bot_token: str
    database_path: Path = Path("data/bridge.sqlite3")
    transport: str = "stdio"
    ingress_mode: str = "polling"
    host: str = "127.0.0.1"
    port: int = 8000
    mcp_auth_token: str | None = None
    mcp_allowed_hosts: tuple[str, ...] = ("127.0.0.1:*", "localhost:*", "[::1]:*")
    mcp_allowed_origins: tuple[str, ...] = ()
    webhook_secret: str | None = None
    webhook_path: str = "/telegram/webhook"
    public_base_url: str | None = None
    allowed_chat_ids: frozenset[int] = field(default_factory=frozenset)
    allowed_user_ids: frozenset[int] = field(default_factory=frozenset)
    allow_all: bool = False
    allowed_updates: tuple[str, ...] = DEFAULT_ALLOWED_UPDATES
    max_update_bytes: int = 1_048_576
    max_queue_events: int = 100_000
    max_claim_events: int = 50
    max_wait_seconds: int = 30
    sqlite_synchronous: str = "FULL"
    retention_days: int = 30
    telegram_api_base: str = "https://api.telegram.org"
    telegram_connect_timeout: float = 5.0
    telegram_request_timeout: float = 15.0
    telegram_poll_timeout: int = 50
    log_level: str = "INFO"

    @property
    def bot_key(self) -> str:
        """Stable, non-secret per-token namespace for durable deduplication."""
        return hashlib.sha256(self.telegram_bot_token.encode("utf-8")).hexdigest()[:24]

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if environ is None else environ
        token = env.get("TELEGRAM_BOT_TOKEN", "").strip()
        if not token:
            raise ConfigurationError("TELEGRAM_BOT_TOKEN is required")
        if not _BOT_TOKEN_RE.fullmatch(token):
            raise ConfigurationError("TELEGRAM_BOT_TOKEN has an invalid or unsafe shape")

        transport = env.get("BRIDGE_TRANSPORT", "stdio").strip().lower()
        if transport not in {"stdio", "http"}:
            raise ConfigurationError("BRIDGE_TRANSPORT must be 'stdio' or 'http'")
        ingress_default = "polling" if transport == "stdio" else "webhook"
        ingress_mode = env.get("TELEGRAM_INGRESS_MODE", ingress_default).strip().lower()
        if ingress_mode not in {"polling", "webhook", "disabled"}:
            raise ConfigurationError("TELEGRAM_INGRESS_MODE must be polling, webhook, or disabled")

        settings = cls(
            telegram_bot_token=token,
            database_path=Path(env.get("BRIDGE_DATABASE_PATH", "data/bridge.sqlite3")),
            transport=transport,
            ingress_mode=ingress_mode,
            host=env.get("MCP_HOST", "127.0.0.1").strip(),
            port=_integer(env.get("MCP_PORT"), 8000, "MCP_PORT", 1, 65535),
            mcp_auth_token=env.get("MCP_AUTH_TOKEN") or None,
            mcp_allowed_hosts=_csv_strings(env.get("MCP_ALLOWED_HOSTS"))
            or ("127.0.0.1:*", "localhost:*", "[::1]:*"),
            mcp_allowed_origins=_csv_strings(env.get("MCP_ALLOWED_ORIGINS")),
            webhook_secret=env.get("TELEGRAM_WEBHOOK_SECRET") or None,
            webhook_path=env.get("TELEGRAM_WEBHOOK_PATH", "/telegram/webhook").strip(),
            public_base_url=env.get("PUBLIC_BASE_URL") or None,
            allowed_chat_ids=_csv_ints(env.get("TELEGRAM_ALLOWED_CHAT_IDS"), "TELEGRAM_ALLOWED_CHAT_IDS"),
            allowed_user_ids=_csv_ints(env.get("TELEGRAM_ALLOWED_USER_IDS"), "TELEGRAM_ALLOWED_USER_IDS"),
            allow_all=_boolean(env.get("TELEGRAM_ALLOW_ALL")),
            allowed_updates=_csv_strings(env.get("TELEGRAM_ALLOWED_UPDATES")) or DEFAULT_ALLOWED_UPDATES,
            max_update_bytes=_integer(
                env.get("BRIDGE_MAX_UPDATE_BYTES"), 1_048_576, "BRIDGE_MAX_UPDATE_BYTES", 1024, 10_000_000
            ),
            max_queue_events=_integer(
                env.get("BRIDGE_MAX_QUEUE_EVENTS"), 100_000, "BRIDGE_MAX_QUEUE_EVENTS", 100, 10_000_000
            ),
            max_claim_events=_integer(
                env.get("BRIDGE_MAX_CLAIM_EVENTS"), 50, "BRIDGE_MAX_CLAIM_EVENTS", 1, 500
            ),
            max_wait_seconds=_integer(
                env.get("BRIDGE_MAX_WAIT_SECONDS"), 30, "BRIDGE_MAX_WAIT_SECONDS", 1, 55
            ),
            sqlite_synchronous=env.get("SQLITE_SYNCHRONOUS", "FULL").strip().upper(),
            retention_days=_integer(env.get("BRIDGE_RETENTION_DAYS"), 30, "BRIDGE_RETENTION_DAYS", 1, 3650),
            telegram_api_base=env.get("TELEGRAM_API_BASE", "https://api.telegram.org").rstrip("/"),
            telegram_connect_timeout=_positive_float(
                env.get("TELEGRAM_CONNECT_TIMEOUT"),
                5.0,
                "TELEGRAM_CONNECT_TIMEOUT",
            ),
            telegram_request_timeout=_positive_float(
                env.get("TELEGRAM_REQUEST_TIMEOUT"),
                15.0,
                "TELEGRAM_REQUEST_TIMEOUT",
            ),
            telegram_poll_timeout=_integer(
                env.get("TELEGRAM_POLL_TIMEOUT"), 50, "TELEGRAM_POLL_TIMEOUT", 1, 50
            ),
            log_level=env.get("LOG_LEVEL", "INFO").upper(),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if self.sqlite_synchronous not in {"FULL", "EXTRA", "NORMAL"}:
            raise ConfigurationError("SQLITE_SYNCHRONOUS must be FULL, EXTRA, or NORMAL")
        if self.log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ConfigurationError("LOG_LEVEL is invalid")
        if not self.allow_all and not (self.allowed_chat_ids or self.allowed_user_ids):
            raise ConfigurationError(
                "Deny-by-default is active: set TELEGRAM_ALLOWED_CHAT_IDS/TELEGRAM_ALLOWED_USER_IDS, "
                "or explicitly set TELEGRAM_ALLOW_ALL=true"
            )
        if not _WEBHOOK_PATH_RE.fullmatch(self.webhook_path):
            raise ConfigurationError("TELEGRAM_WEBHOOK_PATH must be a simple absolute route path")
        if self.webhook_path in _RESERVED_ROUTES or self.webhook_path.startswith("/.well-known/"):
            raise ConfigurationError("TELEGRAM_WEBHOOK_PATH conflicts with a reserved bridge route")
        if not _HOST_RE.fullmatch(self.host):
            raise ConfigurationError("MCP_HOST is invalid")
        if self.webhook_secret is not None and (
            not _SECRET_RE.fullmatch(self.webhook_secret) or len(self.webhook_secret) < 32
        ):
            raise ConfigurationError(
                "TELEGRAM_WEBHOOK_SECRET must contain at least 32 safe random characters"
            )
        if self.ingress_mode == "webhook":
            if not self.webhook_secret:
                raise ConfigurationError("TELEGRAM_WEBHOOK_SECRET is required in webhook mode")
        if self.transport == "stdio" and self.ingress_mode == "webhook":
            raise ConfigurationError("Webhook ingress requires BRIDGE_TRANSPORT=http")
        if self.transport == "http":
            if not self.mcp_auth_token or len(self.mcp_auth_token) < 32:
                raise ConfigurationError("MCP_AUTH_TOKEN of at least 32 characters is required for HTTP")
            if len(self.mcp_auth_token) > 512 or not all(
                0x21 <= ord(character) <= 0x7E for character in self.mcp_auth_token
            ):
                raise ConfigurationError("MCP_AUTH_TOKEN must contain 32-512 visible ASCII characters")
            loopback_only = all(
                allowed.startswith(("127.0.0.1", "localhost", "[::1]")) for allowed in self.mcp_allowed_hosts
            )
            if self.host not in _LOOPBACK and loopback_only:
                raise ConfigurationError("Set MCP_ALLOWED_HOSTS explicitly when binding beyond loopback")
        api_url = urlsplit(self.telegram_api_base)
        invalid_api_url = (
            not api_url.hostname
            or not api_url.netloc
            or api_url.username is not None
            or api_url.password is not None
            or bool(api_url.query or api_url.fragment)
        )
        if invalid_api_url:
            raise ConfigurationError(
                "TELEGRAM_API_BASE must be an absolute URL without credentials/query/fragment"
            )
        if api_url.scheme != "https" and not (api_url.scheme == "http" and api_url.hostname in _LOOPBACK):
            raise ConfigurationError("TELEGRAM_API_BASE must use HTTPS (except loopback test servers)")
        if self.public_base_url:
            public_url = urlsplit(self.public_base_url)
            if (
                public_url.scheme != "https"
                or not public_url.hostname
                or public_url.username is not None
                or public_url.password is not None
                or bool(public_url.query or public_url.fragment)
            ):
                raise ConfigurationError("PUBLIC_BASE_URL must be an absolute HTTPS URL")

    def outbound_target_is_explicitly_allowed(self, chat_id: int) -> bool:
        """Return whether a chat is an explicit outbound target.

        User IDs are intentionally not treated as chat IDs. Telegram private-chat IDs often
        happen to match user IDs, but relying on that coincidence would turn an ingress actor
        allowlist into an outbound destination allowlist.
        """
        return chat_id in self.allowed_chat_ids

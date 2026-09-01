"""Logging setup that never emits configured credentials."""

from __future__ import annotations

import logging
import re
import sys
from collections.abc import Iterable

_BOT_URL_RE = re.compile(r"(?i)(/bot)[0-9]{1,20}:[A-Za-z0-9_-]+(?=/)")
_RAW_BOT_TOKEN_RE = re.compile(r"(?<![A-Za-z0-9_-])[0-9]{1,20}:[A-Za-z0-9_-]{8,}(?![A-Za-z0-9_-])")


def redact_text(value: object, secrets: Iterable[str] = ()) -> str:
    """Return a printable value with Bot API URL tokens and known secrets removed."""

    redacted = _BOT_URL_RE.sub(r"\1<redacted>", str(value))
    redacted = _RAW_BOT_TOKEN_RE.sub("<redacted>", redacted)
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "<redacted>")
    return redacted


class SecretRedactingFormatter(logging.Formatter):
    """Redact after formatting so message arguments and tracebacks are both covered."""

    def __init__(self, *, secrets: Iterable[str]) -> None:
        super().__init__("%(asctime)s %(levelname)s %(name)s: %(message)s")
        self._secrets = tuple(secret for secret in secrets if secret)

    def format(self, record: logging.LogRecord) -> str:
        return redact_text(super().format(record), self._secrets)


def configure_logging(level: str, *, secrets: Iterable[str] = ()) -> None:
    """Install one stderr handler; stdout stays reserved for MCP stdio frames."""

    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(SecretRedactingFormatter(secrets=secrets))
    root.addHandler(handler)
    root.setLevel(level)
    # HTTPX normally logs the full request URL, which contains the Telegram token.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

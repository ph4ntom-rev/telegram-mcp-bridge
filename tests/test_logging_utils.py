from __future__ import annotations

import logging

import pytest

from telegram_mcp.logging_utils import SecretRedactingFormatter, configure_logging, redact_text


def test_redacts_bot_urls_and_all_configured_secrets() -> None:
    token = "123:TOP_SECRET"
    assert redact_text(f"https://api.telegram.org/bot{token}/getMe") == (
        "https://api.telegram.org/bot<redacted>/getMe"
    )
    assert redact_text(f"raw={token}", [token]) == "raw=<redacted>"
    assert redact_text(f"raw={token}") == "raw=<redacted>"


def test_formatter_redacts_message_arguments_and_traceback() -> None:
    token = "123:TOP_SECRET"
    formatter = SecretRedactingFormatter(secrets=[token])
    try:
        raise RuntimeError(f"https://api.telegram.org/bot{token}/sendMessage")
    except RuntimeError:
        record = logging.LogRecord(
            "test",
            logging.ERROR,
            __file__,
            1,
            "failed with %s",
            (token,),
            __import__("sys").exc_info(),
        )
    rendered = formatter.format(record)
    assert token not in rendered
    assert "<redacted>" in rendered


def test_configure_logging_reserves_stdout_and_redacts_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    token = "123:TOP_SECRET"
    try:
        configure_logging("INFO", secrets=[token])
        logging.getLogger("bridge-test").info("token=%s", token)
        captured = capsys.readouterr()
        assert captured.out == ""
        assert token not in captured.err
        assert "token=<redacted>" in captured.err
        assert logging.getLogger("httpx").level == logging.WARNING
        assert logging.getLogger("httpcore").level == logging.WARNING
    finally:
        configure_logging("WARNING")

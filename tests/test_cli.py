from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from telegram_mcp import cli
from telegram_mcp.config import Settings
from telegram_mcp.errors import ConfigurationError


def settings(path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "telegram_bot_token": "123:secret",
        "database_path": path,
        "allowed_chat_ids": frozenset({42}),
        "allowed_user_ids": frozenset({42}),
    }
    values.update(overrides)
    return Settings(**values)


def test_parser_supports_all_operator_commands() -> None:
    parser = cli._parser()
    assert parser.parse_args(["serve", "--transport", "stdio"]).command == "serve"
    assert parser.parse_args(["doctor"]).command == "doctor"
    parsed = parser.parse_args(["webhook", "set", "--drop-pending"])
    assert parsed.webhook_command == "set"
    assert parsed.drop_pending is True


def test_load_settings_from_env_file_and_transport_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "TELEGRAM_BOT_TOKEN=123:secret\nTELEGRAM_ALLOWED_CHAT_IDS=42\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_ALLOWED_CHAT_IDS", raising=False)
    monkeypatch.delenv("BRIDGE_TRANSPORT", raising=False)
    parsed = argparse.Namespace(env_file=env_file, transport="stdio")
    loaded = cli._load_settings(parsed)
    assert loaded.transport == "stdio"
    assert loaded.allowed_chat_ids == frozenset({42})
    assert loaded.database_path == (tmp_path / "data" / "bridge.sqlite3").resolve()


@pytest.mark.asyncio
async def test_run_dispatches_serve_doctor_and_webhook(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    stdio = AsyncMock()
    http = AsyncMock()
    doctor = AsyncMock(return_value={"ok": True})
    webhook = AsyncMock(return_value={"ok": True, "action": "set"})
    monkeypatch.setattr(cli, "_serve_stdio", stdio)
    monkeypatch.setattr(cli, "_serve_http", http)
    monkeypatch.setattr(cli, "_doctor", doctor)
    monkeypatch.setattr(cli, "_webhook", webhook)

    local = settings(tmp_path / "one.sqlite3")
    await cli._run(argparse.Namespace(command="serve"), local)
    stdio.assert_awaited_once_with(local)

    remote = settings(tmp_path / "two.sqlite3", transport="http")
    await cli._run(argparse.Namespace(command="serve"), remote)
    http.assert_awaited_once_with(remote)

    await cli._run(argparse.Namespace(command="doctor"), local)
    assert json.loads(capsys.readouterr().out) == {"ok": True}

    await cli._run(
        argparse.Namespace(command="webhook", webhook_command="set", drop_pending=True),
        local,
    )
    webhook.assert_awaited_once_with(local, action="set", drop_pending=True)
    assert json.loads(capsys.readouterr().out)["action"] == "set"

    with pytest.raises(RuntimeError):
        await cli._run(argparse.Namespace(command="unknown"), local)


@pytest.mark.asyncio
async def test_doctor_redacts_to_public_bot_and_bridge_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class FakeTelegram:
        started = False

        async def start(self) -> None:
            self.started = True
            events.append("telegram-start")

        async def close(self) -> None:
            self.started = False
            events.append("telegram-close")

        async def get_me(self) -> dict[str, Any]:
            return {"id": 123, "username": "bridge_bot"}

        async def get_webhook_info(self) -> dict[str, Any]:
            return {"url": "https://example.test/hook"}

    class FakeStorage:
        async def open(self) -> None:
            events.append("storage-open")

        async def close(self) -> None:
            events.append("storage-close")

    class FakeService:
        async def status(self) -> dict[str, Any]:
            return {"ready": True}

    class FakeRuntime:
        telegram = FakeTelegram()
        storage = FakeStorage()
        service = FakeService()

        def __init__(self, _: Settings) -> None:
            pass

    monkeypatch.setattr(cli, "BridgeRuntime", FakeRuntime)
    result = await cli._doctor(settings(tmp_path / "bridge.sqlite3"))
    assert result == {
        "ok": True,
        "bot": {"id": "123", "username": "bridge_bot"},
        "telegram_webhook_configured": True,
        "bridge": {"ready": True},
    }
    assert events == ["storage-open", "telegram-start", "telegram-close", "storage-close"]


@pytest.mark.asyncio
async def test_webhook_commands_and_validation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    instances: list[Any] = []

    class FakeClient:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.set_webhook = AsyncMock(return_value=True)
            self.delete_webhook = AsyncMock(return_value=True)
            self.started = False
            instances.append(self)

        async def start(self) -> None:
            self.started = True

        async def close(self) -> None:
            self.started = False

    monkeypatch.setattr(cli, "TelegramClient", FakeClient)
    configured = settings(
        tmp_path / "bridge.sqlite3",
        public_base_url="https://bridge.example",
        webhook_secret="w" * 32,
    )
    assert await cli._webhook(configured, action="set", drop_pending=False) == {
        "ok": True,
        "action": "set",
        "drop_pending": False,
    }
    instances[-1].set_webhook.assert_awaited_once()
    assert await cli._webhook(configured, action="delete", drop_pending=True) == {
        "ok": True,
        "action": "delete",
        "drop_pending": True,
    }
    instances[-1].delete_webhook.assert_awaited_once_with(drop_pending_updates=True)

    with pytest.raises(ConfigurationError, match="PUBLIC_BASE_URL"):
        await cli._webhook(settings(tmp_path / "missing.sqlite3"), action="set", drop_pending=False)
    with pytest.raises(ConfigurationError, match="WEBHOOK_SECRET"):
        await cli._webhook(
            settings(tmp_path / "short.sqlite3", public_base_url="https://bridge.example"),
            action="set",
            drop_pending=False,
        )
    assert all(instance.started is False for instance in instances)


@pytest.mark.asyncio
async def test_serve_stdio_owns_runtime_and_facade(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []

    class FakeRuntime:
        service = SimpleNamespace()

        def __init__(self, _: Settings) -> None:
            pass

        async def start(self, *, start_ingress: bool) -> None:
            assert start_ingress
            events.append("start")

        async def stop(self) -> None:
            events.append("stop")

    class FakeServer:
        async def run_stdio_async(self) -> None:
            events.append("serve")

    monkeypatch.setattr(cli, "BridgeRuntime", FakeRuntime)
    monkeypatch.setattr(cli, "create_mcp_server", lambda *args, **kwargs: FakeServer())
    await cli._serve_stdio(settings(tmp_path / "bridge.sqlite3"))
    assert events == ["start", "serve", "stop"]


@pytest.mark.asyncio
async def test_serve_http_builds_one_worker_without_access_logs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeConfig:
        def __init__(self, app: object, **kwargs: Any) -> None:
            captured.update({"app": app, **kwargs})

    class FakeServer:
        def __init__(self, config: object) -> None:
            captured["config"] = config

        async def serve(self) -> None:
            captured["served"] = True

    monkeypatch.setattr(cli, "create_application", lambda _: SimpleNamespace(app="app"))
    monkeypatch.setattr(cli.uvicorn, "Config", FakeConfig)
    monkeypatch.setattr(cli.uvicorn, "Server", FakeServer)
    configured = settings(tmp_path / "bridge.sqlite3", transport="http")
    await cli._serve_http(configured)
    assert captured["workers"] == 1
    assert captured["access_log"] is False
    assert captured["served"] is True


def test_main_maps_configuration_error_and_interrupt_to_exit_codes(monkeypatch: pytest.MonkeyPatch) -> None:
    parsed = argparse.Namespace(command="doctor", env_file=Path(".env"))

    class FakeParser:
        def parse_args(self) -> argparse.Namespace:
            return parsed

    monkeypatch.setattr(cli, "_parser", lambda: FakeParser())
    monkeypatch.setattr(cli, "_load_settings", lambda _: (_ for _ in ()).throw(ConfigurationError("bad")))
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2

    monkeypatch.setattr(cli, "_load_settings", lambda _: settings(Path("bridge.sqlite3")))
    monkeypatch.setattr(cli, "_configure", lambda _: None)

    def interrupt(coroutine: Any) -> None:
        coroutine.close()
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.asyncio, "run", interrupt)
    with pytest.raises(SystemExit) as interrupted:
        cli.main()
    assert interrupted.value.code == 130

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from telegram_mcp.app import create_application
from telegram_mcp.config import Settings
from telegram_mcp.runtime import BridgeRuntime


def http_settings(path: Path) -> Settings:
    return Settings(
        telegram_bot_token="123:secret",
        database_path=path,
        transport="http",
        ingress_mode="webhook",
        host="127.0.0.1",
        port=8000,
        mcp_auth_token="m" * 32,
        webhook_secret="w" * 32,
        allowed_chat_ids=frozenset({42}),
        allowed_user_ids=frozenset({42}),
        max_update_bytes=1024,
    )


def telegram_update(update_id: int = 1) -> dict[str, object]:
    return {
        "update_id": update_id,
        "message": {
            "message_id": 9,
            "date": 1_700_000_000,
            "chat": {"id": 42, "type": "private"},
            "from": {"id": 42, "is_bot": False},
            "text": "hello",
        },
    }


@pytest.mark.asyncio
async def test_webhook_auth_validation_dedup_and_durable_commit(tmp_path: Path) -> None:
    settings = http_settings(tmp_path / "bridge.sqlite3")
    runtime = BridgeRuntime(settings, telegram_transport=httpx.MockTransport(lambda _: httpx.Response(500)))
    bundle = create_application(settings, runtime=runtime)
    transport = httpx.ASGITransport(app=bundle.app)
    async with bundle.app.router.lifespan_context(bundle.app):
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost:8000") as client:
            assert (await client.get("/healthz")).status_code == 200
            ready = await client.get("/readyz")
            assert ready.status_code == 200
            assert ready.json() == {"status": "ready"}
            assert (await client.post(settings.webhook_path, json=telegram_update())).status_code == 403
            assert (
                await client.post(
                    settings.webhook_path,
                    content=b"{}",
                    headers={
                        "X-Telegram-Bot-Api-Secret-Token": settings.webhook_secret or "",
                        "Content-Type": "text/plain",
                    },
                )
            ).status_code == 415

            headers = {"X-Telegram-Bot-Api-Secret-Token": settings.webhook_secret or ""}
            inserted = await client.post(settings.webhook_path, json=telegram_update(), headers=headers)
            duplicate = await client.post(settings.webhook_path, json=telegram_update(), headers=headers)
            assert inserted.json() == {"ok": True, "disposition": "inserted"}
            assert duplicate.json() == {"ok": True, "disposition": "duplicate"}

            # A 2xx means the row can already be read from the durable database.
            event = await runtime.storage.get_event(settings.bot_key, 1)
            assert event is not None
            assert event.payload["message"]["text"] == "hello"  # type: ignore[index]


@pytest.mark.asyncio
async def test_webhook_rejects_duplicate_keys_bad_json_and_oversize(tmp_path: Path) -> None:
    settings = http_settings(tmp_path / "bridge.sqlite3")
    runtime = BridgeRuntime(settings, telegram_transport=httpx.MockTransport(lambda _: httpx.Response(500)))
    bundle = create_application(settings, runtime=runtime)
    transport = httpx.ASGITransport(app=bundle.app)
    headers = {
        "X-Telegram-Bot-Api-Secret-Token": settings.webhook_secret or "",
        "Content-Type": "application/json",
    }
    async with bundle.app.router.lifespan_context(bundle.app):
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost:8000") as client:
            duplicate = b'{"update_id":1,"update_id":2}'
            duplicate_response = await client.post(
                settings.webhook_path,
                content=duplicate,
                headers=headers,
            )
            bad_json_response = await client.post(
                settings.webhook_path,
                content=b"{",
                headers=headers,
            )
            assert duplicate_response.status_code == 400
            assert bad_json_response.status_code == 400
            nonstandard_number = await client.post(
                settings.webhook_path,
                content=b'{"update_id":NaN}',
                headers=headers,
            )
            assert nonstandard_number.status_code == 400
            assert (
                await client.post(settings.webhook_path, content=b" " * 1025, headers=headers)
            ).status_code == 413


@pytest.mark.asyncio
async def test_mcp_http_enforces_bearer_host_and_origin(tmp_path: Path) -> None:
    settings = http_settings(tmp_path / "bridge.sqlite3")
    runtime = BridgeRuntime(settings, telegram_transport=httpx.MockTransport(lambda _: httpx.Response(500)))
    bundle = create_application(settings, runtime=runtime)
    transport = httpx.ASGITransport(app=bundle.app)
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {"name": "pytest", "version": "1"},
        },
    }
    accept = "application/json, text/event-stream"
    async with bundle.app.router.lifespan_context(bundle.app):
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost:8000") as client:
            missing = await client.post("/mcp", json=body, headers={"Accept": accept})
            assert missing.status_code == 401
            authorized = await client.post(
                "/mcp",
                json=body,
                headers={"Accept": accept, "Authorization": f"Bearer {settings.mcp_auth_token}"},
            )
            assert authorized.status_code == 200
            assert authorized.json()["result"]["serverInfo"]["name"] == "telegram-mcp-bridge"

            hostile_host = await client.post(
                "/mcp",
                json=body,
                headers={
                    "Accept": accept,
                    "Authorization": f"Bearer {settings.mcp_auth_token}",
                    "Host": "evil.example",
                },
            )
            assert hostile_host.status_code in {400, 403, 421}
            hostile_origin = await client.post(
                "/mcp",
                json=body,
                headers={
                    "Accept": accept,
                    "Authorization": f"Bearer {settings.mcp_auth_token}",
                    "Origin": "https://evil.example",
                },
            )
            assert hostile_origin.status_code in {400, 403}


def test_application_never_embeds_secrets_in_routes_or_repr(tmp_path: Path) -> None:
    settings = http_settings(tmp_path / "bridge.sqlite3")
    bundle = create_application(settings)
    rendered = json.dumps([getattr(route, "path", "") for route in bundle.app.routes]) + repr(bundle.app)
    assert settings.telegram_bot_token not in rendered
    assert settings.mcp_auth_token not in rendered
    assert settings.webhook_secret not in rendered


@pytest.mark.asyncio
async def test_webhook_route_is_absent_when_ingress_is_not_webhook(tmp_path: Path) -> None:
    settings = Settings(
        telegram_bot_token="123:secret",
        database_path=tmp_path / "bridge.sqlite3",
        transport="http",
        ingress_mode="disabled",
        mcp_auth_token="m" * 32,
        webhook_secret="w" * 32,
        allowed_chat_ids=frozenset({42}),
    )
    runtime = BridgeRuntime(
        settings,
        telegram_transport=httpx.MockTransport(lambda _: httpx.Response(500)),
    )
    bundle = create_application(settings, runtime=runtime)
    transport = httpx.ASGITransport(app=bundle.app)
    async with bundle.app.router.lifespan_context(bundle.app):
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost:8000") as client:
            response = await client.post(
                settings.webhook_path,
                json=telegram_update(),
                headers={"X-Telegram-Bot-Api-Secret-Token": settings.webhook_secret or ""},
            )
            assert response.status_code == 404
            assert await runtime.storage.get_event(settings.bot_key, 1) is None

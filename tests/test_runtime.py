from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest

from telegram_mcp.config import Settings
from telegram_mcp.instance_lock import InstanceAlreadyRunningError
from telegram_mcp.runtime import BridgeRuntime


def polling_settings(path: Path, *, ingress_mode: str = "polling") -> Settings:
    return Settings(
        telegram_bot_token="123:secret",
        database_path=path,
        ingress_mode=ingress_mode,
        allowed_chat_ids=frozenset({42}),
        allowed_user_ids=frozenset({42}),
        telegram_poll_timeout=1,
    )


@pytest.mark.asyncio
async def test_runtime_poller_commits_before_advancing_offset_and_stops_cleanly(tmp_path: Path) -> None:
    calls = 0
    later = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        assert request.url.path.endswith("/getUpdates")
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "result": [
                        {
                            "update_id": 7,
                            "message": {
                                "message_id": 3,
                                "chat": {"id": 42, "type": "private"},
                                "from": {"id": 42, "is_bot": False},
                                "text": "from polling",
                            },
                        }
                    ],
                },
            )
        await later.wait()
        return httpx.Response(200, json={"ok": True, "result": []})

    settings = polling_settings(tmp_path / "bridge.sqlite3")
    runtime = BridgeRuntime(settings, telegram_transport=httpx.MockTransport(handler))
    await runtime.start()
    try:
        for _ in range(200):
            if await runtime.storage.get_event(settings.bot_key, 7) is not None:
                break
            await asyncio.sleep(0.005)
        event = await runtime.storage.get_event(settings.bot_key, 7)
        assert event is not None
        assert event.payload["message"]["text"] == "from polling"  # type: ignore[index]
        assert await runtime.storage.get_poll_offset(settings.bot_key) == 8
        status = await runtime.public_status()
        assert status["ready"] is True
        assert status["last_poll_success_at"] is not None
    finally:
        await runtime.stop()
    assert runtime.ready is False
    await runtime.stop()  # idempotent shutdown


@pytest.mark.asyncio
async def test_runtime_rejects_malformed_batch_without_offset_then_recovers(tmp_path: Path) -> None:
    calls = 0
    later = asyncio.Event()

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(200, json={"ok": True, "result": [{"not_update_id": 1}]})
        if calls == 2:
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "result": [
                        {
                            "update_id": 9,
                            "message": {
                                "message_id": 1,
                                "chat": {"id": 42, "type": "private"},
                                "from": {"id": 42},
                                "text": "recovered",
                            },
                        }
                    ],
                },
            )
        await later.wait()
        return httpx.Response(200, json={"ok": True, "result": []})

    settings = polling_settings(tmp_path / "bridge.sqlite3")
    runtime = BridgeRuntime(settings, telegram_transport=httpx.MockTransport(handler))
    await runtime.start()
    try:
        # The first failure sleeps for 250 ms, then the identical offset is retried.
        for _ in range(200):
            if await runtime.storage.get_event(settings.bot_key, 9) is not None:
                break
            await asyncio.sleep(0.01)
        assert calls >= 2
        assert await runtime.storage.get_poll_offset(settings.bot_key) == 10
        assert (await runtime.public_status())["poll_failures"] == 0
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_runtime_can_start_without_ingress_and_context_manager_closes(tmp_path: Path) -> None:
    settings = polling_settings(tmp_path / "bridge.sqlite3", ingress_mode="disabled")
    runtime = BridgeRuntime(
        settings,
        telegram_transport=httpx.MockTransport(lambda _: httpx.Response(500)),
    )
    async with runtime:
        assert runtime.ready
        await runtime.start()  # idempotent start
        assert runtime._poll_task is None
    assert not runtime.storage.is_open
    assert not runtime.telegram.started


@pytest.mark.asyncio
async def test_runtime_rejects_second_server_for_same_database(tmp_path: Path) -> None:
    configured = polling_settings(tmp_path / "bridge.sqlite3", ingress_mode="disabled")
    transport = httpx.MockTransport(lambda _: httpx.Response(500))
    first = BridgeRuntime(configured, telegram_transport=transport)
    second = BridgeRuntime(configured, telegram_transport=transport)
    await first.start()
    try:
        with pytest.raises(InstanceAlreadyRunningError):
            await second.start()
        assert first.ready
    finally:
        await first.stop()

    await second.start()
    assert second.ready
    await second.stop()


@pytest.mark.asyncio
async def test_cancelled_shutdown_finishes_before_releasing_instance_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured = polling_settings(tmp_path / "bridge.sqlite3", ingress_mode="disabled")
    transport = httpx.MockTransport(lambda _: httpx.Response(500))
    runtime = BridgeRuntime(configured, telegram_transport=transport)
    await runtime.start()
    close_started = asyncio.Event()
    allow_close = asyncio.Event()
    original_close = runtime.telegram.close

    async def slow_close() -> None:
        close_started.set()
        await allow_close.wait()
        await original_close()

    monkeypatch.setattr(runtime.telegram, "close", slow_close)
    shutdown = asyncio.create_task(runtime.stop())
    await close_started.wait()
    shutdown.cancel()
    await asyncio.sleep(0)
    shutdown.cancel()
    await asyncio.sleep(0)
    assert not shutdown.done()
    assert runtime._instance_lock.acquired
    allow_close.set()
    with pytest.raises(asyncio.CancelledError):
        await shutdown
    assert not runtime.storage.is_open
    assert not runtime.telegram.started
    assert not runtime._instance_lock.acquired


@pytest.mark.asyncio
async def test_repeated_cancel_during_failed_start_closes_before_unlock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured = polling_settings(tmp_path / "bridge.sqlite3", ingress_mode="disabled")
    runtime = BridgeRuntime(
        configured,
        telegram_transport=httpx.MockTransport(lambda _: httpx.Response(500)),
    )
    recovery_started = asyncio.Event()
    close_started = asyncio.Event()
    allow_close = asyncio.Event()
    original_close = runtime.telegram.close

    async def blocked_recovery(*, retry_expired_outbox: bool) -> None:
        assert retry_expired_outbox is False
        recovery_started.set()
        await asyncio.Event().wait()

    async def slow_close() -> None:
        close_started.set()
        await allow_close.wait()
        await original_close()

    monkeypatch.setattr(runtime.storage, "recover", blocked_recovery)
    monkeypatch.setattr(runtime.telegram, "close", slow_close)
    startup = asyncio.create_task(runtime.start())
    await recovery_started.wait()
    startup.cancel()
    await close_started.wait()
    startup.cancel()
    await asyncio.sleep(0)
    assert not startup.done()
    assert runtime._instance_lock.acquired
    allow_close.set()
    with pytest.raises(asyncio.CancelledError):
        await startup
    assert not runtime.storage.is_open
    assert not runtime.telegram.started
    assert not runtime._instance_lock.acquired

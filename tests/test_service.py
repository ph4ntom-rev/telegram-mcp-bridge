from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from telegram_mcp.config import Settings
from telegram_mcp.errors import AuthorizationError, IdempotencyConflictError
from telegram_mcp.service import BridgeService
from telegram_mcp.storage import MAX_LEASE_SECONDS, LeaseConflictError, SQLiteStorage
from telegram_mcp.telegram import TelegramAPIError


def update(update_id: int, *, chat_id: int = 42, user_id: int = 42, text: str = "hello") -> dict[str, Any]:
    return {
        "update_id": update_id,
        "message": {
            "message_id": update_id + 100,
            "date": 1_700_000_000,
            "chat": {"id": chat_id, "type": "private"},
            "from": {"id": user_id, "is_bot": False},
            "text": text,
        },
    }


class FakeTelegram:
    started = True

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.release_send: asyncio.Event | None = None
        self.failure: TelegramAPIError | None = None

    async def send_message(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self.release_send is not None:
            await self.release_send.wait()
        if self.failure is not None:
            raise self.failure
        return {"message_id": 900 + len(self.calls), "chat": {"id": kwargs["chat_id"]}}

    async def edit_message(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {"message_id": kwargs["message_id"]}

    async def delete_message(self, **kwargs: Any) -> bool:
        self.calls.append(kwargs)
        return True

    async def send_typing(self, **kwargs: Any) -> bool:
        self.calls.append(kwargs)
        return True

    async def answer_callback(self, **kwargs: Any) -> bool:
        self.calls.append(kwargs)
        return True


def settings(path: Path, *, chats: frozenset[int] = frozenset({42})) -> Settings:
    return Settings(
        telegram_bot_token="123:secret",
        database_path=path,
        allowed_chat_ids=chats,
        allowed_user_ids=frozenset({42}),
        max_wait_seconds=2,
        telegram_request_timeout=0.2,
    )


async def make_service(
    path: Path,
    *,
    chats: frozenset[int] = frozenset({42}),
) -> tuple[BridgeService, SQLiteStorage, FakeTelegram]:
    configured = settings(path, chats=chats)
    storage = SQLiteStorage(path, max_claim_events=20)
    await storage.open()
    telegram = FakeTelegram()
    service = BridgeService(
        settings=configured,
        storage=storage,
        telegram=telegram,  # type: ignore[arg-type]
    )
    await service.bootstrap_authorizations()
    return service, storage, telegram


@pytest.mark.asyncio
async def test_bootstrap_replaces_acl_and_ingress_cannot_expand_it(tmp_path: Path) -> None:
    database = tmp_path / "bridge.sqlite3"
    storage = SQLiteStorage(database)
    await storage.open()
    try:
        await storage.set_chat_authorization(settings(database).bot_key, 99)
        telegram = FakeTelegram()
        service = BridgeService(
            settings=settings(database),
            storage=storage,
            telegram=telegram,  # type: ignore[arg-type]
        )
        await service.bootstrap_authorizations()
        assert not await storage.is_chat_authorized(settings(database).bot_key, 99, permission="write")
        assert await storage.is_chat_authorized(settings(database).bot_key, 42, permission="write")

        # The user is allowed but the chat is not: AND policy rejects it and no durable ACL is created.
        result = await service.ingest_update(update(1, chat_id=99, user_id=42))
        assert result["disposition"] == "ignored"
        assert not await storage.is_chat_authorized(settings(database).bot_key, 99, permission="write")
    finally:
        await storage.close()


@pytest.mark.asyncio
async def test_wait_claim_partial_ack_release_and_redelivery(tmp_path: Path) -> None:
    service, storage, _ = await make_service(tmp_path / "bridge.sqlite3")
    try:
        await service.ingest_update(update(10))
        await service.ingest_update(update(11))
        delivery = await service.wait_updates(
            consumer_id="codex",
            limit=2,
            wait_seconds=0,
            lease_seconds=30,
            include_raw=False,
        )
        assert [event["update_id"] for event in delivery["events"]] == [10, 11]
        token = delivery["lease_token"]
        event_ids = [event["event_id"] for event in delivery["events"]]
        assert isinstance(token, str)

        first_ack = await service.ack_updates(lease_token=token, event_ids=[event_ids[0]])
        assert first_ack == {"acknowledged": 1, "remaining": 1, "duplicate": False}
        released = await service.release_updates(lease_token=token)
        assert released == {"released": 1}

        again = await service.wait_updates(
            consumer_id="codex",
            limit=2,
            wait_seconds=0,
            lease_seconds=30,
            include_raw=True,
        )
        assert [event["update_id"] for event in again["events"]] == [11]
        assert again["events"][0]["raw"]["message"]["text"] == "hello"
    finally:
        await storage.close()


@pytest.mark.asyncio
async def test_ack_is_replay_safe_after_complete_lease(tmp_path: Path) -> None:
    service, storage, _ = await make_service(tmp_path / "bridge.sqlite3")
    try:
        await service.ingest_update(update(20))
        delivery = await service.wait_updates(
            consumer_id="codex",
            limit=1,
            wait_seconds=0,
            lease_seconds=30,
            include_raw=False,
        )
        token = delivery["lease_token"]
        assert isinstance(token, str)
        assert (await service.ack_updates(lease_token=token, event_ids=None))["duplicate"] is False
        duplicate = await service.ack_updates(lease_token=token, event_ids=None)
        assert duplicate == {"acknowledged": 1, "duplicate": True}
        with pytest.raises(LeaseConflictError):
            await service.release_updates(lease_token=token)
    finally:
        await storage.close()


@pytest.mark.asyncio
async def test_concurrent_same_send_hits_telegram_once(tmp_path: Path) -> None:
    service, storage, telegram = await make_service(tmp_path / "bridge.sqlite3")
    telegram.release_send = asyncio.Event()
    kwargs = {
        "chat_id": "42",
        "text": "one logical message",
        "idempotency_key": "reply:10:answer",
        "format": "plain",
        "message_thread_id": None,
        "reply_to_message_id": None,
        "disable_notification": False,
        "protect_content": False,
        "link_preview": False,
    }
    try:
        first = asyncio.create_task(service.send_text(**kwargs))
        for _ in range(100):
            if telegram.calls:
                break
            await asyncio.sleep(0)
        second = asyncio.create_task(service.send_text(**kwargs))
        await asyncio.sleep(0)
        telegram.release_send.set()
        left, right = await asyncio.gather(first, second)
        assert len(telegram.calls) == 1
        assert left["delivery_state"] == right["delivery_state"] == "sent"
        assert {left["replayed"], right["replayed"]} == {False, True}
        assert left["messages"][0]["message_id"] == right["messages"][0]["message_id"]
    finally:
        await storage.close()


@pytest.mark.asyncio
async def test_ambiguous_send_is_durable_and_never_auto_retried(tmp_path: Path) -> None:
    service, storage, telegram = await make_service(tmp_path / "bridge.sqlite3")
    telegram.failure = TelegramAPIError("connection ended", delivery_state="uncertain")
    kwargs = {
        "chat_id": "42",
        "text": "maybe delivered",
        "idempotency_key": "reply:21:ambiguous",
        "format": "plain",
        "message_thread_id": None,
        "reply_to_message_id": None,
        "disable_notification": False,
        "protect_content": False,
        "link_preview": False,
    }
    try:
        first = await service.send_text(**kwargs)
        replay = await service.send_text(**kwargs)
        assert first["delivery_state"] == replay["delivery_state"] == "uncertain"
        assert replay["replayed"] is True
        assert len(telegram.calls) == 1
    finally:
        await storage.close()


@pytest.mark.asyncio
async def test_cancelled_send_is_marked_uncertain(tmp_path: Path) -> None:
    service, storage, telegram = await make_service(tmp_path / "bridge.sqlite3")
    telegram.release_send = asyncio.Event()
    try:
        task = asyncio.create_task(
            service.send_text(
                chat_id="42",
                text="cancel me",
                idempotency_key="reply:cancel:0001",
                format="plain",
                message_thread_id=None,
                reply_to_message_id=None,
                disable_notification=False,
                protect_content=False,
                link_preview=False,
            )
        )
        for _ in range(100):
            if telegram.calls:
                break
            await asyncio.sleep(0.005)
        assert telegram.calls
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        record = await storage.get_outbox(settings(tmp_path / "bridge.sqlite3").bot_key, outbox_id=1)
        assert record is not None
        assert record.status == "uncertain"
    finally:
        await storage.close()


@pytest.mark.asyncio
async def test_send_rejects_acl_conflict_and_long_formatted_text_before_http(tmp_path: Path) -> None:
    service, storage, telegram = await make_service(tmp_path / "bridge.sqlite3")
    base = {
        "format": "plain",
        "message_thread_id": None,
        "reply_to_message_id": None,
        "disable_notification": False,
        "protect_content": False,
        "link_preview": False,
    }
    try:
        with pytest.raises(AuthorizationError):
            await service.send_text(
                chat_id="99",
                text="no",
                idempotency_key="reply:99:denied",
                **base,
            )
        await service.send_text(
            chat_id="42",
            text="original",
            idempotency_key="reply:22:conflict",
            **base,
        )
        with pytest.raises(IdempotencyConflictError):
            await service.send_text(
                chat_id="42",
                text="changed",
                idempotency_key="reply:22:conflict",
                **base,
            )
        with pytest.raises(ValueError, match="Formatted"):
            await service.send_text(
                chat_id="42",
                text="x" * 4097,
                idempotency_key="reply:22:formatted",
                **{**base, "format": "html"},
            )
        assert len(telegram.calls) == 1
    finally:
        await storage.close()


@pytest.mark.asyncio
async def test_reply_uses_only_stored_authorized_event_routing(tmp_path: Path) -> None:
    service, storage, telegram = await make_service(tmp_path / "bridge.sqlite3")
    try:
        ingested = await service.ingest_update(update(30))
        result = await service.reply(
            event_id=ingested["event_id"],
            text="reply",
            idempotency_key="reply:30:result",
            format="plain",
            disable_notification=False,
            protect_content=True,
            link_preview=False,
        )
        assert result["delivery_state"] == "sent"
        assert telegram.calls[0]["chat_id"] == 42
        assert telegram.calls[0]["reply_to_message_id"] == 130
    finally:
        await storage.close()


@pytest.mark.asyncio
async def test_plain_text_is_chunked_and_replayed_per_part(tmp_path: Path) -> None:
    service, storage, telegram = await make_service(tmp_path / "bridge.sqlite3")
    try:
        result = await service.send_text(
            chat_id="42",
            text="a" * 5000,
            idempotency_key="send:long:0001",
            format="plain",
            message_thread_id=None,
            reply_to_message_id=None,
            disable_notification=False,
            protect_content=False,
            link_preview=False,
        )
        assert result["delivery_state"] == "sent"
        assert len(result["messages"]) == len(telegram.calls) == 2
        assert "".join(call["text"] for call in telegram.calls) == "a" * 5000
        assert all(len(call["text"]) <= 4096 for call in telegram.calls)
    finally:
        await storage.close()


@pytest.mark.asyncio
async def test_send_lease_covers_rate_limit_and_network_waits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, storage, _telegram = await make_service(tmp_path / "bridge.sqlite3")
    original_claim = storage.claim_outbox_id
    observed: list[float] = []

    async def capture_claim(
        outbox_id: int,
        *,
        lease_seconds: float = 30,
        now: float | None = None,
    ) -> Any:
        observed.append(lease_seconds)
        return await original_claim(outbox_id, lease_seconds=lease_seconds, now=now)

    monkeypatch.setattr(storage, "claim_outbox_id", capture_claim)
    try:
        result = await service.send_text(
            chat_id="42",
            text="lease",
            idempotency_key="send:lease:0001",
            format="plain",
            message_thread_id=None,
            reply_to_message_id=None,
            disable_notification=False,
            protect_content=False,
            link_preview=False,
        )
        assert result["delivery_state"] == "sent"
        assert observed == [MAX_LEASE_SECONDS]
    finally:
        await storage.close()

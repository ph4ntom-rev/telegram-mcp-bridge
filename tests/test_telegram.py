import asyncio
import json
import logging
import traceback
from typing import Any

import httpx
import pytest

from telegram_mcp.telegram import TelegramAPIError, TelegramClient, TelegramRateLimiter


class NoopLimiter(TelegramRateLimiter):
    async def acquire(self, chat_id: int) -> None:
        del chat_id


class RecordingLimiter(NoopLimiter):
    def __init__(self) -> None:
        super().__init__()
        self.penalties: list[float] = []

    async def penalize(self, seconds: float) -> None:
        self.penalties.append(seconds)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay


def ok(result: object) -> httpx.Response:
    return httpx.Response(200, json={"ok": True, "result": result})


@pytest.mark.asyncio
async def test_send_message_uses_safe_defaults_and_reply_parameters() -> None:
    captured: list[dict[str, Any]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.append(json.loads(request.content))
        return ok({"message_id": 10, "chat": {"id": 42}})

    client = TelegramClient(
        token="123:secret",
        transport=httpx.MockTransport(handler),
        rate_limiter=NoopLimiter(),
    )
    async with client:
        result = await client.send_message(chat_id=42, text="hello", reply_to_message_id=9)
    assert result["message_id"] == 10
    assert captured[0]["link_preview_options"] == {"is_disabled": True}
    assert captured[0]["reply_parameters"]["message_id"] == 9
    assert "parse_mode" not in captured[0]


@pytest.mark.asyncio
async def test_429_honors_retry_after_then_succeeds() -> None:
    calls = 0
    sleeps: list[float] = []
    limiter = RecordingLimiter()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                429,
                json={
                    "ok": False,
                    "error_code": 429,
                    "description": "Too Many Requests",
                    "parameters": {"retry_after": 2},
                },
            )
        return ok({"message_id": 1})

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    client = TelegramClient(
        token="123:secret",
        transport=httpx.MockTransport(handler),
        sleep=sleep,
        random_uniform=lambda _a, _b: 0.1,
        rate_limiter=limiter,
    )
    async with client:
        await client.send_message(chat_id=1, text="x")
    assert calls == 2
    assert sleeps == [2.1]
    assert limiter.penalties == [2.0]


@pytest.mark.asyncio
async def test_read_timeout_on_send_is_uncertain_and_never_retried() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("secret URL must not escape", request=request)

    client = TelegramClient(
        token="123:top-secret",
        transport=httpx.MockTransport(handler),
        rate_limiter=NoopLimiter(),
    )
    async with client:
        with pytest.raises(TelegramAPIError) as exc_info:
            await client.send_message(chat_id=1, text="x")
    assert calls == 1
    assert exc_info.value.delivery_state == "uncertain"
    assert "top-secret" not in str(exc_info.value)


@pytest.mark.asyncio
async def test_connect_failure_is_retried_for_send() -> None:
    calls = 0
    sleeps: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise httpx.ConnectError("offline", request=request)
        return ok({"message_id": 3})

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    client = TelegramClient(
        token="123:secret",
        transport=httpx.MockTransport(handler),
        sleep=sleep,
        random_uniform=lambda _a, _b: 0,
        rate_limiter=NoopLimiter(),
    )
    async with client:
        result = await client.send_message(chat_id=1, text="x")
    assert result["message_id"] == 3
    assert calls == 3
    assert len(sleeps) == 2


@pytest.mark.asyncio
async def test_permanent_400_is_not_retried() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(400, json={"ok": False, "error_code": 400, "description": "Bad Request"})

    client = TelegramClient(
        token="123:secret",
        transport=httpx.MockTransport(handler),
        rate_limiter=NoopLimiter(),
    )
    async with client:
        with pytest.raises(TelegramAPIError) as exc_info:
            await client.send_message(chat_id=1, text="x")
    assert calls == 1
    assert exc_info.value.delivery_state == "not_sent"
    assert not exc_info.value.retriable


@pytest.mark.asyncio
async def test_bot_token_is_absent_from_httpx_info_logs(caplog: pytest.LogCaptureFixture) -> None:
    token = "123:caplog-secret"

    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return ok({"id": 123, "is_bot": True, "first_name": "Bridge"})

    caplog.set_level(logging.INFO)
    client = TelegramClient(token=token, transport=httpx.MockTransport(handler))
    async with client:
        await client.get_me()

    assert logging.getLogger("httpx").getEffectiveLevel() >= logging.WARNING
    assert logging.getLogger("httpcore").getEffectiveLevel() >= logging.WARNING
    assert token not in caplog.text
    assert "caplog-secret" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [httpx.WriteTimeout, httpx.ProxyError])
async def test_transport_errors_are_sanitized_without_cause_or_token_in_traceback(
    error_type: type[httpx.TransportError],
) -> None:
    token = "123:trace-secret"
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise error_type(f"failed request to {request.url}", request=request)

    client = TelegramClient(
        token=token,
        transport=httpx.MockTransport(handler),
        rate_limiter=NoopLimiter(),
    )
    async with client:
        with pytest.raises(TelegramAPIError) as exc_info:
            await client.send_message(chat_id=1, text="x")

    error = exc_info.value
    rendered = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    assert calls == 1
    assert error.delivery_state == "uncertain"
    assert error.__cause__ is None
    assert error.__suppress_context__
    assert token not in str(error)
    assert token not in repr(error)
    assert token not in rendered


@pytest.mark.asyncio
async def test_bot_token_is_redacted_if_an_upstream_error_description_echoes_it() -> None:
    token = "123:description-secret"

    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            400,
            json={
                "ok": False,
                "error_code": 400,
                "description": f"bad request for bot {token}",
            },
        )

    client = TelegramClient(
        token=token,
        transport=httpx.MockTransport(handler),
        rate_limiter=NoopLimiter(),
    )
    async with client:
        with pytest.raises(TelegramAPIError) as exc_info:
            await client.send_message(chat_id=1, text="x")

    assert token not in str(exc_info.value)
    assert "description-secret" not in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation",
    ["set_webhook_drop", "delete_webhook_drop", "edit", "delete", "answer_callback"],
)
async def test_non_replayable_operations_are_never_retried_after_ambiguous_transport_failure(
    operation: str,
) -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("response lost", request=request)

    client = TelegramClient(
        token="123:secret",
        transport=httpx.MockTransport(handler),
        rate_limiter=NoopLimiter(),
    )
    async with client:
        with pytest.raises(TelegramAPIError) as exc_info:
            if operation == "set_webhook_drop":
                await client.set_webhook(
                    url="https://bridge.example/telegram/webhook",
                    secret_token="s" * 32,
                    allowed_updates=["message"],
                    drop_pending_updates=True,
                )
            elif operation == "delete_webhook_drop":
                await client.delete_webhook(drop_pending_updates=True)
            elif operation == "edit":
                await client.edit_message(chat_id=1, message_id=2, text="edited")
            elif operation == "delete":
                await client.delete_message(chat_id=1, message_id=2)
            else:
                await client.answer_callback(callback_query_id="callback")

    assert calls == 1
    assert exc_info.value.delivery_state == "uncertain"
    assert exc_info.value.__cause__ is None


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["set_webhook", "delete_webhook"])
async def test_webhook_configuration_without_drop_can_be_retried_safely(operation: str) -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ReadTimeout("response lost", request=request)
        return ok(True)

    client = TelegramClient(token="123:secret", transport=httpx.MockTransport(handler))
    async with client:
        if operation == "set_webhook":
            result = await client.set_webhook(
                url="https://bridge.example/telegram/webhook",
                secret_token="s" * 32,
                allowed_updates=["message"],
                drop_pending_updates=False,
            )
        else:
            result = await client.delete_webhook(drop_pending_updates=False)

    assert result is True
    assert calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_item",
    [None, "not-an-update", {}, {"update_id": True}, {"update_id": -1}],
)
async def test_get_updates_rejects_entire_malformed_batch_without_filtering(bad_item: object) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return ok([{"update_id": 10, "message": {}}, bad_item, {"update_id": 12, "message": {}}])

    client = TelegramClient(token="123:secret", transport=httpx.MockTransport(handler))
    async with client:
        with pytest.raises(TelegramAPIError, match="malformed update batch"):
            await client.get_updates(offset=None, allowed_updates=["message"])


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_result", [{}, {"id": True}, None, True])
async def test_get_me_rejects_malformed_success_schema(bad_result: object) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return ok(bad_result)

    client = TelegramClient(token="123:secret", transport=httpx.MockTransport(handler))
    async with client:
        with pytest.raises(TelegramAPIError, match="invalid getMe result"):
            await client.get_me()


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_result", [{}, {"message_id": True}, None, False])
async def test_send_message_rejects_malformed_success_schema_as_uncertain(bad_result: object) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return ok(bad_result)

    client = TelegramClient(
        token="123:secret",
        transport=httpx.MockTransport(handler),
        rate_limiter=NoopLimiter(),
    )
    async with client:
        with pytest.raises(TelegramAPIError) as exc_info:
            await client.send_message(chat_id=1, text="x")

    assert exc_info.value.delivery_state == "uncertain"


@pytest.mark.asyncio
async def test_malformed_2xx_response_to_send_is_delivery_uncertain() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(200, content=b"not-json")

    client = TelegramClient(
        token="123:secret",
        transport=httpx.MockTransport(handler),
        rate_limiter=NoopLimiter(),
    )
    async with client:
        with pytest.raises(TelegramAPIError) as exc_info:
            await client.send_message(chat_id=1, text="x")

    assert exc_info.value.delivery_state == "uncertain"


@pytest.mark.asyncio
async def test_long_retry_after_sets_shared_cooldown_even_when_not_slept_inline() -> None:
    limiter = RecordingLimiter()
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            429,
            json={
                "ok": False,
                "error_code": 429,
                "description": "Too Many Requests",
                "parameters": {"retry_after": 21},
            },
        )

    client = TelegramClient(
        token="123:secret",
        transport=httpx.MockTransport(handler),
        rate_limiter=limiter,
    )
    async with client:
        with pytest.raises(TelegramAPIError) as exc_info:
            await client.send_message(chat_id=1, text="x")

    assert calls == 1
    assert exc_info.value.retry_after == 21
    assert limiter.penalties == [21.0]


@pytest.mark.asyncio
async def test_rate_limiter_enforces_group_twenty_per_minute_guardrail() -> None:
    clock = FakeClock()
    limiter = TelegramRateLimiter(
        global_per_second=100,
        per_chat_interval=1.02,
        group_interval=3.05,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )
    await limiter.acquire(-1001234567890)
    await limiter.acquire(-1001234567890)
    assert clock.sleeps == pytest.approx([3.05])


@pytest.mark.asyncio
async def test_rate_limiter_enforces_global_window() -> None:
    clock = FakeClock()
    limiter = TelegramRateLimiter(
        global_per_second=2,
        per_chat_interval=0,
        group_interval=0,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )
    await limiter.acquire(1)
    await limiter.acquire(2)
    await limiter.acquire(3)
    assert clock.sleeps == pytest.approx([1.0])


@pytest.mark.asyncio
async def test_rate_limiter_applies_shared_cooldown_to_other_chats() -> None:
    clock = FakeClock()
    limiter = TelegramRateLimiter(
        global_per_second=100,
        per_chat_interval=0,
        group_interval=0,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )
    await limiter.penalize(5.0)
    await limiter.acquire(999)
    assert clock.sleeps == pytest.approx([5.0])


@pytest.mark.asyncio
async def test_concurrent_sends_to_same_chat_are_serialized_in_call_order() -> None:
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    observed: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        text = str(payload["text"])
        observed.append(text)
        if text == "first":
            first_entered.set()
            await release_first.wait()
        return ok({"message_id": len(observed), "chat": {"id": 42}})

    client = TelegramClient(
        token="123:secret",
        transport=httpx.MockTransport(handler),
        rate_limiter=NoopLimiter(),
    )
    async with client:
        first = asyncio.create_task(client.send_message(chat_id=42, text="first"))
        await first_entered.wait()
        second = asyncio.create_task(client.send_message(chat_id=42, text="second"))
        await asyncio.sleep(0)
        assert observed == ["first"]
        release_first.set()
        await asyncio.gather(first, second)

    assert observed == ["first", "second"]

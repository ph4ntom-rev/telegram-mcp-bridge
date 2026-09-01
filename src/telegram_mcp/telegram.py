"""A pooled Telegram Bot API client with conservative retry semantics."""

from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from collections import OrderedDict, deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

import httpx

from telegram_mcp.errors import BridgeError

logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
_METHOD_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]{0,63}$")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]+")


class TelegramAPIError(BridgeError):
    """A sanitized Telegram failure with explicit delivery semantics."""

    def __init__(
        self,
        message: str,
        *,
        error_code: int | None = None,
        retry_after: int | None = None,
        migrate_to_chat_id: int | None = None,
        retriable: bool = False,
        delivery_state: str = "not_sent",
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.retry_after = retry_after
        self.migrate_to_chat_id = migrate_to_chat_id
        self.retriable = retriable
        self.delivery_state = delivery_state

    def as_dict(self) -> dict[str, Any]:
        return {
            "message": str(self),
            "error_code": self.error_code,
            "retry_after": self.retry_after,
            "migrate_to_chat_id": self.migrate_to_chat_id,
            "retriable": self.retriable,
            "delivery_state": self.delivery_state,
        }


class TelegramRateLimiter:
    """Small in-process guardrail; Telegram's 429 remains authoritative."""

    def __init__(
        self,
        *,
        global_per_second: int = 28,
        per_chat_interval: float = 1.02,
        group_interval: float = 3.05,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._global_per_second = global_per_second
        self._per_chat_interval = per_chat_interval
        self._group_interval = group_interval
        self._sleep = sleep
        self._monotonic = monotonic
        self._lock = asyncio.Lock()
        self._global: deque[float] = deque()
        self._chat_last: OrderedDict[int, float] = OrderedDict()
        self._blocked_until = 0.0

    async def acquire(self, chat_id: int) -> None:
        while True:
            async with self._lock:
                now = self._monotonic()
                while self._global and now - self._global[0] >= 1.0:
                    self._global.popleft()
                wait_cooldown = max(0.0, self._blocked_until - now)
                wait_global = 0.0
                if len(self._global) >= self._global_per_second:
                    wait_global = max(0.0, 1.0 - (now - self._global[0]))
                last = self._chat_last.get(chat_id)
                interval = self._group_interval if chat_id < 0 else self._per_chat_interval
                wait_chat = 0.0 if last is None else max(0.0, interval - (now - last))
                delay = max(wait_cooldown, wait_global, wait_chat)
                if delay <= 0:
                    stamped = self._monotonic()
                    self._global.append(stamped)
                    self._chat_last[chat_id] = stamped
                    self._chat_last.move_to_end(chat_id)
                    while len(self._chat_last) > 4_096:
                        self._chat_last.popitem(last=False)
                    return
            await self._sleep(delay)

    async def penalize(self, seconds: float) -> None:
        if seconds <= 0:
            return
        async with self._lock:
            self._blocked_until = max(self._blocked_until, self._monotonic() + seconds)


class TelegramClient:
    """Only exposes a deliberately small, typed Bot API surface."""

    def __init__(
        self,
        *,
        token: str,
        api_base: str = "https://api.telegram.org",
        connect_timeout: float = 5.0,
        request_timeout: float = 15.0,
        poll_timeout: int = 50,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_uniform: Callable[[float, float], float] = random.uniform,
        rate_limiter: TelegramRateLimiter | None = None,
    ) -> None:
        self._token = token
        self._api_base = api_base.rstrip("/")
        self._connect_timeout = connect_timeout
        self._request_timeout = request_timeout
        self._poll_timeout = poll_timeout
        self._transport = transport
        self._sleep = sleep
        self._random_uniform = random_uniform
        self._client: httpx.AsyncClient | None = None
        self._rate_limiter = rate_limiter or TelegramRateLimiter(sleep=sleep)
        self._chat_locks: OrderedDict[int, asyncio.Lock] = OrderedDict()
        self._chat_locks_guard = asyncio.Lock()

    @property
    def started(self) -> bool:
        return self._client is not None

    async def start(self) -> None:
        if self._client is not None:
            return
        timeout = httpx.Timeout(
            connect=self._connect_timeout,
            read=self._request_timeout,
            write=self._request_timeout,
            pool=self._connect_timeout,
        )
        self._client = httpx.AsyncClient(
            timeout=timeout,
            limits=httpx.Limits(max_connections=32, max_keepalive_connections=16, keepalive_expiry=30.0),
            transport=self._transport,
            http2=self._transport is None,
            follow_redirects=False,
            trust_env=False,
            headers={"User-Agent": "telegram-mcp-bridge/1.0"},
        )

    async def close(self) -> None:
        if self._client is None:
            return
        client, self._client = self._client, None
        await client.aclose()

    async def __aenter__(self) -> TelegramClient:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    def _url(self, method: str) -> str:
        if not _METHOD_RE.fullmatch(method):
            raise ValueError("Invalid internal Telegram method name")
        return f"{self._api_base}/bot{self._token}/{method}"

    async def _chat_lock(self, chat_id: int) -> asyncio.Lock:
        async with self._chat_locks_guard:
            lock = self._chat_locks.get(chat_id)
            if lock is None:
                lock = asyncio.Lock()
                self._chat_locks[chat_id] = lock
            self._chat_locks.move_to_end(chat_id)
            if len(self._chat_locks) > 4_096:
                for old_chat, old_lock in tuple(self._chat_locks.items()):
                    if old_chat != chat_id and not old_lock.locked():
                        del self._chat_locks[old_chat]
                        break
            return lock

    def _safe_description(self, value: object) -> str:
        if not isinstance(value, str):
            return "Telegram API request failed"
        sanitized = value.replace(self._token, "<redacted>")
        sanitized = _CONTROL_RE.sub(" ", sanitized).strip()
        return sanitized[:512] or "Telegram API request failed"

    def _error_from_response(
        self,
        data: object,
        status_code: int,
        *,
        idempotent: bool,
    ) -> TelegramAPIError:
        body = data if isinstance(data, Mapping) else {}
        description = body.get("description")
        safe_description = self._safe_description(description)
        raw_parameters = body.get("parameters")
        parameters: Mapping[str, Any] = raw_parameters if isinstance(raw_parameters, Mapping) else {}
        retry_after_value = parameters.get("retry_after")
        migrate = parameters.get("migrate_to_chat_id")
        code = body.get("error_code")
        error_code = code if isinstance(code, int) else status_code
        is_5xx = status_code >= 500 or (isinstance(error_code, int) and error_code >= 500)
        return TelegramAPIError(
            safe_description,
            error_code=error_code,
            retry_after=(
                retry_after_value
                if isinstance(retry_after_value, int)
                and not isinstance(retry_after_value, bool)
                and retry_after_value >= 0
                else None
            ),
            migrate_to_chat_id=migrate if isinstance(migrate, int) else None,
            retriable=status_code == 429 or is_5xx,
            delivery_state="not_sent" if idempotent or not is_5xx else "uncertain",
        )

    async def _request_json(
        self,
        method: str,
        payload: Mapping[str, Any] | None = None,
        *,
        idempotent: bool,
        timeout: httpx.Timeout | None = None,
        attempts: int = 3,
    ) -> Any:
        client = self._client
        if client is None:
            raise RuntimeError("Telegram client is not started")
        last_error: TelegramAPIError | None = None
        for attempt in range(attempts):
            try:
                response = await client.post(self._url(method), json=dict(payload or {}), timeout=timeout)
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
                last_error = TelegramAPIError(
                    "Could not connect to Telegram",
                    retriable=True,
                    delivery_state="not_sent",
                )
                if attempt + 1 < attempts:
                    await self._sleep(min(2.0, 0.2 * (2**attempt)) + self._random_uniform(0.0, 0.1))
                    continue
                raise last_error from None
            except httpx.TransportError:
                if not idempotent:
                    raise TelegramAPIError(
                        "Telegram connection ended before delivery could be confirmed",
                        retriable=False,
                        delivery_state="uncertain",
                    ) from None
                last_error = TelegramAPIError(
                    "Telegram transport failed",
                    retriable=True,
                    delivery_state="not_sent",
                )
                if attempt + 1 < attempts:
                    await self._sleep(min(2.0, 0.2 * (2**attempt)) + self._random_uniform(0.0, 0.1))
                    continue
                raise last_error from None

            try:
                data: object = response.json()
            except ValueError:
                data = {}
            ok = isinstance(data, Mapping) and data.get("ok") is True
            if response.is_success and ok:
                return data.get("result") if isinstance(data, Mapping) else None
            if response.is_success:
                raise TelegramAPIError(
                    "Telegram returned a malformed success response",
                    retriable=False,
                    delivery_state="not_sent" if idempotent else "uncertain",
                )

            error = self._error_from_response(data, response.status_code, idempotent=idempotent)
            last_error = error
            if error.retry_after is not None:
                await self._rate_limiter.penalize(float(error.retry_after))
            if error.retry_after is not None and error.retry_after <= 20 and attempt + 1 < attempts:
                await self._sleep(error.retry_after + self._random_uniform(0.05, 0.25))
                continue
            if idempotent and error.retriable and error.error_code != 429 and attempt + 1 < attempts:
                await self._sleep(min(3.0, 0.25 * (2**attempt)) + self._random_uniform(0.0, 0.2))
                continue
            raise error
        raise last_error or TelegramAPIError("Telegram API request failed")

    async def get_me(self) -> dict[str, Any]:
        result = await self._request_json("getMe", idempotent=True)
        if (
            not isinstance(result, Mapping)
            or not isinstance(result.get("id"), int)
            or isinstance(result.get("id"), bool)
        ):
            raise TelegramAPIError("Telegram returned an invalid getMe result")
        return dict(result)

    async def get_webhook_info(self) -> dict[str, Any]:
        result = await self._request_json("getWebhookInfo", idempotent=True)
        if not isinstance(result, Mapping) or not isinstance(result.get("url"), str):
            raise TelegramAPIError("Telegram returned an invalid getWebhookInfo result")
        return dict(result)

    async def get_updates(
        self,
        *,
        offset: int | None,
        allowed_updates: Sequence[str],
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "timeout": self._poll_timeout,
            "limit": min(100, max(1, limit)),
            "allowed_updates": list(allowed_updates),
        }
        if offset is not None:
            payload["offset"] = offset
        timeout = httpx.Timeout(
            connect=self._connect_timeout,
            read=self._poll_timeout + self._connect_timeout + 5.0,
            write=self._request_timeout,
            pool=self._connect_timeout,
        )
        result = await self._request_json("getUpdates", payload, idempotent=True, timeout=timeout, attempts=2)
        if not isinstance(result, list):
            raise TelegramAPIError("Telegram returned an invalid getUpdates result")
        validated: list[dict[str, Any]] = []
        for item in result:
            if (
                not isinstance(item, Mapping)
                or not isinstance(item.get("update_id"), int)
                or isinstance(item.get("update_id"), bool)
                or int(item["update_id"]) < 0
            ):
                raise TelegramAPIError("Telegram returned a malformed update batch")
            validated.append(dict(item))
        return validated

    async def set_webhook(
        self,
        *,
        url: str,
        secret_token: str,
        allowed_updates: Sequence[str],
        drop_pending_updates: bool = False,
        max_connections: int = 8,
    ) -> bool:
        result = await self._request_json(
            "setWebhook",
            {
                "url": url,
                "secret_token": secret_token,
                "allowed_updates": list(allowed_updates),
                "drop_pending_updates": drop_pending_updates,
                "max_connections": min(100, max(1, max_connections)),
            },
            idempotent=not drop_pending_updates,
        )
        if result is not True:
            raise TelegramAPIError("Telegram returned an invalid setWebhook result")
        return True

    async def delete_webhook(self, *, drop_pending_updates: bool = False) -> bool:
        result = await self._request_json(
            "deleteWebhook",
            {"drop_pending_updates": drop_pending_updates},
            idempotent=not drop_pending_updates,
        )
        if result is not True:
            raise TelegramAPIError("Telegram returned an invalid deleteWebhook result")
        return True

    async def send_message(
        self,
        *,
        chat_id: int,
        text: str,
        parse_mode: str | None = None,
        message_thread_id: int | None = None,
        reply_to_message_id: int | None = None,
        disable_notification: bool = False,
        protect_content: bool = False,
        link_preview: bool = False,
    ) -> dict[str, Any]:
        lock = await self._chat_lock(chat_id)
        async with lock:
            await self._rate_limiter.acquire(chat_id)
            payload: dict[str, Any] = {
                "chat_id": chat_id,
                "text": text,
                "disable_notification": disable_notification,
                "protect_content": protect_content,
                "link_preview_options": {"is_disabled": not link_preview},
            }
            if parse_mode:
                payload["parse_mode"] = parse_mode
            if message_thread_id is not None:
                payload["message_thread_id"] = message_thread_id
            if reply_to_message_id is not None:
                payload["reply_parameters"] = {
                    "message_id": reply_to_message_id,
                    "allow_sending_without_reply": True,
                }
            result = await self._request_json("sendMessage", payload, idempotent=False)
        if (
            not isinstance(result, Mapping)
            or not isinstance(result.get("message_id"), int)
            or isinstance(result.get("message_id"), bool)
        ):
            raise TelegramAPIError(
                "Telegram returned an invalid sendMessage result",
                delivery_state="uncertain",
            )
        return dict(result)

    async def edit_message(
        self,
        *,
        chat_id: int,
        message_id: int,
        text: str,
        parse_mode: str | None = None,
        link_preview: bool = False,
    ) -> dict[str, Any] | bool:
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "link_preview_options": {"is_disabled": not link_preview},
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        result = await self._request_json("editMessageText", payload, idempotent=False)
        if result is True:
            return True
        if (
            not isinstance(result, Mapping)
            or not isinstance(result.get("message_id"), int)
            or isinstance(result.get("message_id"), bool)
        ):
            raise TelegramAPIError(
                "Telegram returned an invalid editMessageText result",
                delivery_state="uncertain",
            )
        return dict(result)

    async def delete_message(self, *, chat_id: int, message_id: int) -> bool:
        result = await self._request_json(
            "deleteMessage",
            {"chat_id": chat_id, "message_id": message_id},
            idempotent=False,
        )
        if result is not True:
            raise TelegramAPIError(
                "Telegram returned an invalid deleteMessage result",
                delivery_state="uncertain",
            )
        return True

    async def send_typing(self, *, chat_id: int, message_thread_id: int | None = None) -> bool:
        payload: dict[str, Any] = {"chat_id": chat_id, "action": "typing"}
        if message_thread_id is not None:
            payload["message_thread_id"] = message_thread_id
        result = await self._request_json("sendChatAction", payload, idempotent=True)
        if result is not True:
            raise TelegramAPIError("Telegram returned an invalid sendChatAction result")
        return True

    async def answer_callback(
        self,
        *,
        callback_query_id: str,
        text: str | None = None,
        show_alert: bool = False,
    ) -> bool:
        payload: dict[str, Any] = {"callback_query_id": callback_query_id, "show_alert": show_alert}
        if text:
            payload["text"] = text
        result = await self._request_json("answerCallbackQuery", payload, idempotent=False)
        if result is not True:
            raise TelegramAPIError(
                "Telegram returned an invalid answerCallbackQuery result",
                delivery_state="uncertain",
            )
        return True

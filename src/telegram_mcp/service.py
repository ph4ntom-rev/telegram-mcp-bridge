"""Bridge policy, inbox lease workflow and durable outbound idempotency."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Any

from telegram_mcp.config import Settings
from telegram_mcp.errors import AuthorizationError, BridgeError
from telegram_mcp.storage import MAX_LEASE_SECONDS, LeaseConflictError, OutboxRecord, SQLiteStorage
from telegram_mcp.telegram import TelegramAPIError, TelegramClient
from telegram_mcp.text import split_text
from telegram_mcp.updates import actor_is_allowed, event_summary, parse_update

logger = logging.getLogger(__name__)
_CONSUMER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
_IDEMPOTENCY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")
_DECIMAL_ID_RE = re.compile(r"^-?[0-9]{1,20}$")
_PARSE_MODES = {"plain": None, "html": "HTML", "markdown_v2": "MarkdownV2"}


class ChangeSignal:
    """Monotonic condition variable without missed wakeups."""

    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._version = 0

    @property
    def version(self) -> int:
        return self._version

    async def notify(self) -> None:
        async with self._condition:
            self._version += 1
            self._condition.notify_all()

    async def wait_for_change(self, known_version: int, timeout: float) -> bool:
        async with self._condition:
            if self._version != known_version:
                return True
            try:
                await asyncio.wait_for(
                    self._condition.wait_for(lambda: self._version != known_version),
                    timeout=timeout,
                )
            except TimeoutError:
                return False
            return True


class BridgeService:
    def __init__(
        self,
        *,
        settings: Settings,
        storage: SQLiteStorage,
        telegram: TelegramClient,
        update_signal: ChangeSignal | None = None,
        outbox_signal: ChangeSignal | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.settings = settings
        self.storage = storage
        self.telegram = telegram
        self._updates = update_signal or ChangeSignal()
        self._outbox = outbox_signal or ChangeSignal()
        self._clock = clock
        self._active_leases: dict[str, tuple[int, ...]] = {}
        self._completed_leases: OrderedDict[str, int] = OrderedDict()
        self._lease_lock = asyncio.Lock()

    async def bootstrap_authorizations(self) -> None:
        # Configuration is authoritative. Replacement makes revocation effective on restart;
        # an inbound update must never expand the durable outbound ACL.
        await self.storage.replace_chat_authorizations(
            self.settings.bot_key,
            self.settings.allowed_chat_ids,
            can_read=True,
            can_write=True,
        )

    async def ingest_update(self, update: Mapping[str, Any]) -> dict[str, Any]:
        envelope = parse_update(update)
        if not actor_is_allowed(
            envelope,
            allow_all=self.settings.allow_all,
            allowed_chat_ids=self.settings.allowed_chat_ids,
            allowed_user_ids=self.settings.allowed_user_ids,
        ):
            return {"disposition": "ignored", "update_id": envelope.update_id}
        outcome = await self.storage.enqueue_update(
            self.settings.bot_key,
            update,
            update_id=envelope.update_id,
            chat_id=envelope.chat_id,
        )
        if outcome.disposition == "inserted":
            await self._updates.notify()
        elif outcome.disposition == "conflict":
            logger.critical(
                "Telegram update fingerprint conflict for bot namespace and update_id=%s",
                envelope.update_id,
            )
        return {
            "disposition": outcome.disposition,
            "event_id": outcome.event_id,
            "update_id": outcome.update_id,
        }

    async def ingest_poll_batch(self, updates: list[dict[str, Any]]) -> dict[str, int]:
        accepted: list[dict[str, Any]] = []
        chat_ids: list[int | None] = []
        highest: int | None = None
        ignored = 0
        for update in updates:
            envelope = parse_update(update)
            highest = envelope.update_id if highest is None else max(highest, envelope.update_id)
            if not actor_is_allowed(
                envelope,
                allow_all=self.settings.allow_all,
                allowed_chat_ids=self.settings.allowed_chat_ids,
                allowed_user_ids=self.settings.allowed_user_ids,
            ):
                ignored += 1
                continue
            accepted.append(update)
            chat_ids.append(envelope.chat_id)
        next_offset = highest + 1 if highest is not None else None
        result = await self.storage.ingest_poll_batch(
            self.settings.bot_key,
            accepted,
            next_offset=next_offset,
            chat_ids=chat_ids,
            advance_offset=next_offset is not None,
        )
        if result.inserted:
            await self._updates.notify()
        if result.conflicts:
            logger.critical("Telegram polling batch contained %s fingerprint conflicts", result.conflicts)
        return {
            "inserted": result.inserted,
            "duplicates": result.duplicates,
            "conflicts": result.conflicts,
            "ignored": ignored,
        }

    @staticmethod
    def _validate_consumer(consumer_id: str) -> str:
        if not _CONSUMER_RE.fullmatch(consumer_id):
            raise ValueError("consumer_id must match ^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
        return consumer_id

    async def wait_updates(
        self,
        *,
        consumer_id: str,
        limit: int,
        wait_seconds: int,
        lease_seconds: int,
        include_raw: bool,
    ) -> dict[str, Any]:
        self._validate_consumer(consumer_id)
        if not 1 <= limit <= self.settings.max_claim_events:
            raise ValueError(f"limit must be between 1 and {self.settings.max_claim_events}")
        if not 0 <= wait_seconds <= self.settings.max_wait_seconds:
            raise ValueError(f"wait_seconds must be between 0 and {self.settings.max_wait_seconds}")
        if not 10 <= lease_seconds <= 3_600:
            raise ValueError("lease_seconds must be between 10 and 3600")
        deadline = asyncio.get_running_loop().time() + wait_seconds
        while True:
            version = self._updates.version
            claim = await self.storage.claim_events(
                self.settings.bot_key,
                limit=limit,
                lease_seconds=lease_seconds,
            )
            if claim.events:
                lease_token = claim.lease_token
                if lease_token is None:
                    raise BridgeError("Durable inbox returned an invalid lease")
                event_ids = tuple(event.event_id for event in claim.events)
                async with self._lease_lock:
                    self._active_leases[lease_token] = event_ids
                return {
                    "consumer_id": consumer_id,
                    "lease_token": lease_token,
                    "lease_expires_at": self._iso(claim.lease_until),
                    "events": [event_summary(event.as_row(), include_raw) for event in claim.events],
                    "has_more": len(claim.events) == limit,
                    "delivery_guarantee": "at_least_once_until_ack",
                }
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                empty_lease = dict.fromkeys(("lease_token", "lease_expires_at"))
                return {
                    "consumer_id": consumer_id,
                    **empty_lease,
                    "events": [],
                    "has_more": False,
                    "delivery_guarantee": "at_least_once_until_ack",
                }
            await self._updates.wait_for_change(version, remaining)

    async def ack_updates(self, *, lease_token: str, event_ids: list[int] | None) -> dict[str, Any]:
        async with self._lease_lock:
            if lease_token in self._completed_leases:
                return {
                    "acknowledged": self._completed_leases[lease_token],
                    "duplicate": True,
                }
            leased = self._active_leases.get(lease_token)
        if leased is None:
            raise LeaseConflictError("Unknown or expired inbox lease token")
        selected = leased if event_ids is None else tuple(dict.fromkeys(event_ids))
        if not selected or not set(selected).issubset(leased):
            raise LeaseConflictError("event_ids must be a non-empty subset of the active lease")
        acknowledged = await self.storage.ack_events(self.settings.bot_key, selected, lease_token)
        remaining = tuple(event_id for event_id in leased if event_id not in set(selected))
        async with self._lease_lock:
            if remaining:
                self._active_leases[lease_token] = remaining
            else:
                self._active_leases.pop(lease_token, None)
                self._completed_leases[lease_token] = acknowledged
                while len(self._completed_leases) > 1_000:
                    self._completed_leases.popitem(last=False)
        return {"acknowledged": acknowledged, "remaining": len(remaining), "duplicate": False}

    async def release_updates(self, *, lease_token: str) -> dict[str, Any]:
        async with self._lease_lock:
            leased = self._active_leases.get(lease_token)
        if leased is None:
            raise LeaseConflictError("Unknown or expired inbox lease token")
        released = await self.storage.release_events(self.settings.bot_key, leased, lease_token)
        async with self._lease_lock:
            self._active_leases.pop(lease_token, None)
        await self._updates.notify()
        return {"released": released}

    async def peek_updates(
        self,
        *,
        after_event_id: int,
        limit: int,
        include_raw: bool,
    ) -> dict[str, Any]:
        if after_event_id < 0:
            raise ValueError("after_event_id must be non-negative")
        if not 1 <= limit <= self.settings.max_claim_events:
            raise ValueError(f"limit must be between 1 and {self.settings.max_claim_events}")
        events = await self.storage.peek_events(
            self.settings.bot_key,
            after_event_id=after_event_id,
            limit=limit,
        )
        return {
            "events": [event_summary(event.as_row(), include_raw) for event in events],
            "next_event_id": events[-1].event_id if events else after_event_id,
        }

    async def list_chats(self, *, limit: int) -> dict[str, Any]:
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        chats = await self.storage.list_authorized_chats(self.settings.bot_key, limit=limit)
        return {
            "chats": [
                {
                    "chat_id": str(chat.chat_id),
                    "alias": chat.alias,
                    "can_read": chat.can_read,
                    "can_write": chat.can_write,
                }
                for chat in chats
            ]
        }

    async def status(self) -> dict[str, Any]:
        metrics = await self.storage.metrics(bot_key=self.settings.bot_key)
        return {
            "ready": self.storage.is_open and self.telegram.started,
            "transport": self.settings.transport,
            "ingress_mode": self.settings.ingress_mode,
            "inbox": metrics.inbox_states,
            "outbox": metrics.outbox_states,
            "inbox_pending": metrics.inbox_pending,
            "outbox_pending": metrics.outbox_pending,
            "attention_required": {
                "quarantined_inbox": metrics.inbox_states.get("quarantined", 0),
                "uncertain_outbox": metrics.outbox_states.get("uncertain", 0),
            },
            "oldest_inbox_age_seconds": metrics.oldest_inbox_age_seconds,
            "oldest_outbox_age_seconds": metrics.oldest_outbox_age_seconds,
        }

    @staticmethod
    def _parse_chat_id(chat_id: str) -> int:
        if not isinstance(chat_id, str) or not _DECIMAL_ID_RE.fullmatch(chat_id):
            raise ValueError("chat_id must be a decimal integer encoded as a string")
        value = int(chat_id)
        if not -((1 << 52) - 1) <= value <= (1 << 52) - 1:
            raise ValueError("chat_id is outside Telegram's 52-bit range")
        return value

    @staticmethod
    def _validate_idempotency_key(key: str) -> str:
        if not _IDEMPOTENCY_RE.fullmatch(key):
            raise ValueError("idempotency_key must match ^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")
        return key

    @staticmethod
    def _validate_text(text: str) -> None:
        if not text:
            raise ValueError("text must not be empty")
        if len(text) > 131_072:
            raise ValueError("text may not exceed 131072 characters")
        try:
            text.encode("utf-8", errors="strict")
        except UnicodeEncodeError as exc:
            raise ValueError("text contains an unpaired Unicode surrogate") from exc

    async def _ensure_outbound_chat(self, chat_id: int) -> None:
        if not await self.storage.is_chat_authorized(
            self.settings.bot_key,
            chat_id,
            permission="write",
        ):
            raise AuthorizationError("Outbound Telegram chat is not authorized")

    async def send_text(
        self,
        *,
        chat_id: str,
        text: str,
        idempotency_key: str,
        format: str,
        message_thread_id: int | None,
        reply_to_message_id: int | None,
        disable_notification: bool,
        protect_content: bool,
        link_preview: bool,
    ) -> dict[str, Any]:
        target = self._parse_chat_id(chat_id)
        self._validate_text(text)
        self._validate_idempotency_key(idempotency_key)
        if format not in _PARSE_MODES:
            raise ValueError("format must be plain, html, or markdown_v2")
        if message_thread_id is not None and message_thread_id <= 0:
            raise ValueError("message_thread_id must be positive")
        if reply_to_message_id is not None and reply_to_message_id <= 0:
            raise ValueError("reply_to_message_id must be positive")
        await self._ensure_outbound_chat(target)
        if format != "plain" and len(text) > 4096:
            raise ValueError("Formatted messages longer than 4096 characters require entity-aware splitting")
        chunks = [text] if format != "plain" else split_text(text)
        plan_input = json.dumps(
            {
                "chat_id": target,
                "text": text,
                "format": format,
                "message_thread_id": message_thread_id,
                "reply_to_message_id": reply_to_message_id,
                "disable_notification": disable_notification,
                "protect_content": protect_content,
                "link_preview": link_preview,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        plan_hash = hashlib.sha256(plan_input).hexdigest()
        messages: list[dict[str, Any]] = []
        replayed = True
        for index, chunk in enumerate(chunks):
            part_key = idempotency_key if index == 0 else f"{idempotency_key}:p{index + 1:04d}"
            payload = {
                "chat_id": target,
                "telegram": {
                    "text": chunk,
                    "parse_mode": _PARSE_MODES[format],
                    "message_thread_id": message_thread_id,
                    "reply_to_message_id": reply_to_message_id if index == 0 else None,
                    "disable_notification": disable_notification,
                    "protect_content": protect_content,
                    "link_preview": link_preview,
                },
                "bridge_plan_hash": plan_hash,
                "part": index + 1,
                "parts": len(chunks),
            }
            part = await self._deliver_outbox_part(
                chat_id=target,
                idempotency_key=part_key,
                payload=payload,
            )
            messages.append(part)
            replayed = replayed and bool(part.get("replayed"))
            if part["delivery_state"] != "sent":
                return {
                    "delivery_state": part["delivery_state"],
                    "idempotency_key": idempotency_key,
                    "chat_id": str(target),
                    "replayed": replayed,
                    "messages": messages,
                    "next_part": index + 1,
                }
        return {
            "delivery_state": "sent",
            "idempotency_key": idempotency_key,
            "chat_id": str(target),
            "replayed": replayed,
            "messages": messages,
        }

    async def _deliver_outbox_part(
        self,
        *,
        chat_id: int,
        idempotency_key: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        reservation = await self.storage.reserve_outbox(
            self.settings.bot_key,
            idempotency_key,
            chat_id=chat_id,
            method="sendMessage",
            payload=payload,
        )
        record = reservation.record
        deadline = asyncio.get_running_loop().time() + self.settings.telegram_request_timeout + 5.0
        while True:
            if record.status == "sent":
                return self._outbox_result(record, replayed=not reservation.created)
            if record.status in {"uncertain", "dead"}:
                return self._outbox_result(record, replayed=not reservation.created)
            if record.status in {"pending", "retry"}:
                claim = await self.storage.claim_outbox_id(
                    record.outbox_id,
                    lease_seconds=MAX_LEASE_SECONDS,
                )
                if claim.records:
                    lease_token = claim.lease_token
                    if lease_token is None:
                        raise BridgeError("Durable outbox returned an invalid lease")
                    return await self._send_claimed_part(claim.records[0], lease_token)
            version = self._outbox.version
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return {
                    "outbox_id": record.outbox_id,
                    "delivery_state": "pending",
                    "replayed": not reservation.created,
                }
            await self._outbox.wait_for_change(version, min(remaining, 2.0))
            refreshed = await self.storage.get_outbox(
                self.settings.bot_key,
                outbox_id=record.outbox_id,
            )
            if refreshed is None:
                raise BridgeError("Durable outbox record disappeared")
            record = refreshed

    async def _send_claimed_part(self, record: OutboxRecord, lease_token: str) -> dict[str, Any]:
        telegram_payload = record.payload.get("telegram")
        if not isinstance(telegram_payload, Mapping):
            await self.storage.mark_outbox_dead(
                record.outbox_id,
                lease_token,
                error="Invalid durable Telegram payload",
            )
            await self._outbox.notify()
            return {"outbox_id": record.outbox_id, "delivery_state": "failed", "replayed": False}
        request_started = False
        try:
            raw_text = telegram_payload.get("text")
            if not isinstance(raw_text, str):
                raise ValueError("text is not a string")
            parse_mode_value = telegram_payload.get("parse_mode")
            if parse_mode_value is not None and not isinstance(parse_mode_value, str):
                raise ValueError("parse_mode is not a string")
            request_started = True
            response = await self.telegram.send_message(
                chat_id=record.chat_id,
                text=raw_text,
                parse_mode=parse_mode_value,
                message_thread_id=self._optional_positive_int(
                    telegram_payload.get("message_thread_id"),
                    "message_thread_id",
                ),
                reply_to_message_id=self._optional_positive_int(
                    telegram_payload.get("reply_to_message_id"),
                    "reply_to_message_id",
                ),
                disable_notification=bool(telegram_payload.get("disable_notification", False)),
                protect_content=bool(telegram_payload.get("protect_content", False)),
                link_preview=bool(telegram_payload.get("link_preview", False)),
            )
            message_id = response.get("message_id")
            if not isinstance(message_id, int) or isinstance(message_id, bool):
                raise TelegramAPIError(
                    "Telegram returned a message without an integer message_id",
                    delivery_state="uncertain",
                )
            sent = await self.storage.mark_outbox_sent(record.outbox_id, lease_token, message_id)
            return self._outbox_result(sent, replayed=False)
        except TelegramAPIError as exc:
            if exc.delivery_state == "uncertain":
                failed = await self.storage.mark_outbox_uncertain(
                    record.outbox_id,
                    lease_token,
                    error=str(exc),
                )
            elif exc.retriable:
                delay = float(exc.retry_after or min(30, 2 ** min(record.attempts, 5)))
                failed = await self.storage.mark_outbox_retry(
                    record.outbox_id,
                    lease_token,
                    retry_at=self._clock() + delay,
                    error=str(exc),
                )
            else:
                failed = await self.storage.mark_outbox_dead(
                    record.outbox_id,
                    lease_token,
                    error=str(exc),
                )
            return self._outbox_result(failed, replayed=False)
        except asyncio.CancelledError:
            # Once the HTTP call may have started, replay would risk a duplicate.  Persist the
            # conservative state even when the MCP caller disconnects mid-tool.
            marker = self.storage.mark_outbox_uncertain(
                record.outbox_id,
                lease_token,
                error="MCP request was cancelled while Telegram delivery was in progress",
            )
            try:
                await asyncio.shield(marker)
            except Exception as exc:
                logger.error("Could not persist cancelled outbox state (%s)", type(exc).__name__)
            raise
        except (KeyError, TypeError, ValueError) as exc:
            failed = await self.storage.mark_outbox_dead(
                record.outbox_id,
                lease_token,
                error=f"Invalid durable outbound payload ({type(exc).__name__})",
            )
            return self._outbox_result(failed, replayed=False)
        except Exception as exc:
            # Unexpected failures after entering TelegramClient are ambiguous.  Before that point
            # they indicate a corrupt local payload and are safe to mark dead.
            if request_started:
                failed = await self.storage.mark_outbox_uncertain(
                    record.outbox_id,
                    lease_token,
                    error=f"Unexpected delivery failure ({type(exc).__name__})",
                )
            else:
                failed = await self.storage.mark_outbox_dead(
                    record.outbox_id,
                    lease_token,
                    error=f"Invalid durable outbound payload ({type(exc).__name__})",
                )
            return self._outbox_result(failed, replayed=False)
        finally:
            await self._outbox.notify()

    @staticmethod
    def _outbox_result(record: OutboxRecord, *, replayed: bool) -> dict[str, Any]:
        state = {
            "sent": "sent",
            "uncertain": "uncertain",
            "dead": "failed",
            "retry": "pending",
            "pending": "pending",
            "sending": "pending",
        }[record.status]
        part = record.payload.get("part")
        telegram_payload = record.payload.get("telegram")
        text = telegram_payload.get("text") if isinstance(telegram_payload, dict) else None
        return {
            "outbox_id": record.outbox_id,
            "delivery_state": state,
            "message_id": record.telegram_message_id,
            "part": part,
            "characters": len(text) if isinstance(text, str) else None,
            "text_sha256": (
                hashlib.sha256(text.encode("utf-8")).hexdigest() if isinstance(text, str) else None
            ),
            "replayed": replayed,
            "error": record.last_error if state != "sent" else None,
            "next_action": (
                "Inspect Telegram delivery, then use queue resolve-outbox to confirm or discard; "
                "do not resend blindly"
                if state == "uncertain"
                else None
            ),
        }

    async def reply(
        self,
        *,
        event_id: int,
        text: str,
        idempotency_key: str,
        format: str,
        disable_notification: bool,
        protect_content: bool,
        link_preview: bool,
    ) -> dict[str, Any]:
        event = await self.storage.get_event_by_event_id(self.settings.bot_key, event_id)
        if event is None:
            raise ValueError("Authorized Telegram event was not found")
        envelope = parse_update(event.payload)
        if envelope.chat_id is None or envelope.message_id is None:
            raise ValueError("Event has no replyable Telegram message")
        return await self.send_text(
            chat_id=str(envelope.chat_id),
            text=text,
            idempotency_key=idempotency_key,
            format=format,
            message_thread_id=envelope.thread_id,
            reply_to_message_id=envelope.message_id,
            disable_notification=disable_notification,
            protect_content=protect_content,
            link_preview=link_preview,
        )

    async def edit_text(
        self,
        *,
        chat_id: str,
        message_id: int,
        text: str,
        format: str,
        link_preview: bool,
    ) -> dict[str, Any]:
        target = self._parse_chat_id(chat_id)
        self._validate_text(text)
        if len(text) > 4096:
            raise ValueError("Edited message text may not exceed 4096 characters")
        if format not in _PARSE_MODES:
            raise ValueError("format must be plain, html, or markdown_v2")
        if message_id <= 0:
            raise ValueError("message_id must be positive")
        await self._ensure_outbound_chat(target)
        result = await self.telegram.edit_message(
            chat_id=target,
            message_id=message_id,
            text=text,
            parse_mode=_PARSE_MODES[format],
            link_preview=link_preview,
        )
        return {"delivery_state": "confirmed", "result": result}

    async def delete_message(self, *, chat_id: str, message_id: int) -> dict[str, Any]:
        target = self._parse_chat_id(chat_id)
        if message_id <= 0:
            raise ValueError("message_id must be positive")
        await self._ensure_outbound_chat(target)
        deleted = await self.telegram.delete_message(chat_id=target, message_id=message_id)
        return {"delivery_state": "confirmed", "deleted": deleted}

    async def typing(self, *, chat_id: str, message_thread_id: int | None) -> dict[str, Any]:
        target = self._parse_chat_id(chat_id)
        await self._ensure_outbound_chat(target)
        sent = await self.telegram.send_typing(chat_id=target, message_thread_id=message_thread_id)
        return {"sent": sent}

    async def answer_callback(
        self,
        *,
        event_id: int,
        text: str | None,
        show_alert: bool,
    ) -> dict[str, Any]:
        event = await self.storage.get_event_by_event_id(self.settings.bot_key, event_id)
        if event is None:
            raise ValueError("Authorized callback event was not found")
        callback = event.payload.get("callback_query")
        callback_query_id = callback.get("id") if isinstance(callback, Mapping) else None
        if not isinstance(callback_query_id, str):
            raise ValueError("Event is not a Telegram callback query")
        if not 1 <= len(callback_query_id) <= 256:
            raise ValueError("callback_query_id length is invalid")
        if text is not None and len(text) > 200:
            raise ValueError("callback answer text may not exceed 200 characters")
        answered = await self.telegram.answer_callback(
            callback_query_id=callback_query_id,
            text=text,
            show_alert=show_alert,
        )
        return {"answered": answered}

    @staticmethod
    def _optional_positive_int(value: object, name: str) -> int | None:
        if value is None:
            return None
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
        return value

    @staticmethod
    def _iso(value: float | None) -> str | None:
        if value is None:
            return None
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()

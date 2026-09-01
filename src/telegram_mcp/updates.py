"""Forward-compatible Telegram Update parsing and safe summaries."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from telegram_mcp.errors import BridgeError

_MESSAGE_KEYS = (
    "message",
    "edited_message",
    "channel_post",
    "edited_channel_post",
    "business_message",
    "edited_business_message",
    "guest_message",
)
_KNOWN_UPDATE_KEYS = (
    *_MESSAGE_KEYS,
    "business_connection",
    "deleted_business_messages",
    "message_reaction",
    "message_reaction_count",
    "inline_query",
    "chosen_inline_result",
    "callback_query",
    "shipping_query",
    "pre_checkout_query",
    "purchased_paid_media",
    "poll",
    "poll_answer",
    "my_chat_member",
    "chat_member",
    "chat_join_request",
    "chat_boost",
    "removed_chat_boost",
    "managed_bot",
    "subscription",
)


@dataclass(frozen=True, slots=True)
class UpdateEnvelope:
    update_id: int
    update_type: str
    chat_id: int | None
    message_id: int | None
    user_id: int | None
    thread_id: int | None
    telegram_date: int | None
    text: str | None
    chat: dict[str, Any] | None
    sender: dict[str, Any] | None
    attachments: tuple[dict[str, Any], ...]


def _dict(value: object) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, Mapping) else None


def _int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _message_for(update: Mapping[str, Any]) -> dict[str, Any] | None:
    for key in _MESSAGE_KEYS:
        message = _dict(update.get(key))
        if message is not None:
            return message
    callback = _dict(update.get("callback_query"))
    if callback:
        return _dict(callback.get("message"))
    chat_event_keys = (
        "my_chat_member",
        "chat_member",
        "chat_join_request",
        "message_reaction",
        "message_reaction_count",
        "deleted_business_messages",
        "chat_boost",
        "removed_chat_boost",
    )
    for key in chat_event_keys:
        member_update = _dict(update.get(key))
        if member_update:
            return member_update
    return None


def _sender_for(update: Mapping[str, Any], message: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if message:
        sender = _dict(message.get("from"))
        if sender:
            return sender
    actor_update_keys = (
        "callback_query",
        "inline_query",
        "chosen_inline_result",
        "shipping_query",
        "pre_checkout_query",
    )
    for key in actor_update_keys:
        value = _dict(update.get(key))
        if value:
            sender = _dict(value.get("from"))
            if sender:
                return sender
    for key in ("my_chat_member", "chat_member", "chat_join_request", "message_reaction"):
        value = _dict(update.get(key))
        if value:
            sender = _dict(value.get("from") or value.get("user"))
            if sender:
                return sender
    poll_answer = _dict(update.get("poll_answer"))
    if poll_answer:
        sender = _dict(poll_answer.get("user") or poll_answer.get("voter_chat"))
        if sender:
            return sender
    paid = _dict(update.get("purchased_paid_media"))
    if paid:
        sender = _dict(paid.get("from"))
        if sender:
            return sender
    business = _dict(update.get("business_connection"))
    if business:
        sender = _dict(business.get("user"))
        if sender:
            return sender
    for key in ("managed_bot", "subscription"):
        value = _dict(update.get(key))
        if value:
            sender = _dict(value.get("user"))
            if sender:
                return sender
    return None


def _attachments(message: Mapping[str, Any] | None) -> tuple[dict[str, Any], ...]:
    if not message:
        return ()
    found: list[dict[str, Any]] = []
    for kind in ("audio", "document", "animation", "video", "video_note", "voice", "sticker"):
        item = _dict(message.get(kind))
        if item and isinstance(item.get("file_id"), str):
            found.append(
                {
                    "kind": kind,
                    "file_id": item["file_id"],
                    "file_unique_id": item.get("file_unique_id"),
                    "file_name": item.get("file_name"),
                    "mime_type": item.get("mime_type"),
                    "file_size": item.get("file_size"),
                }
            )
    photos = message.get("photo")
    if isinstance(photos, list):
        valid = [
            item for item in photos if isinstance(item, Mapping) and isinstance(item.get("file_id"), str)
        ]
        if valid:
            largest = valid[-1]
            found.append(
                {
                    "kind": "photo",
                    "file_id": largest["file_id"],
                    "file_unique_id": largest.get("file_unique_id"),
                    "file_size": largest.get("file_size"),
                    "width": largest.get("width"),
                    "height": largest.get("height"),
                }
            )
    return tuple(found)


def parse_update(update: Mapping[str, Any]) -> UpdateEnvelope:
    update_id = _int(update.get("update_id"))
    if update_id is None or update_id < 0:
        raise BridgeError("Telegram update_id must be a non-negative integer")
    update_type = next((key for key in _KNOWN_UPDATE_KEYS if key in update), "unknown")
    message = _message_for(update)
    chat = _dict(message.get("chat")) if message else None
    sender = _sender_for(update, message)
    business_connection = _dict(update.get("business_connection"))
    business_chat_id = _int(business_connection.get("user_chat_id")) if business_connection else None
    if chat is None and business_chat_id is not None:
        chat = {"id": business_chat_id, "type": "private"}
    text: str | None = None
    if message:
        candidate = message.get("text") if isinstance(message.get("text"), str) else message.get("caption")
        text = candidate if isinstance(candidate, str) else None
    callback = _dict(update.get("callback_query"))
    if callback and isinstance(callback.get("data"), str):
        text = callback["data"]
    return UpdateEnvelope(
        update_id=update_id,
        update_type=update_type,
        chat_id=_int(chat.get("id")) if chat else None,
        message_id=_int(message.get("message_id")) if message else None,
        user_id=_int(sender.get("id")) if sender else None,
        thread_id=_int(message.get("message_thread_id")) if message else None,
        telegram_date=(
            _int(message.get("date"))
            if message
            else _int(business_connection.get("date"))
            if business_connection
            else None
        ),
        text=text,
        chat=chat,
        sender=sender,
        attachments=_attachments(message),
    )


def actor_is_allowed(
    envelope: UpdateEnvelope,
    *,
    allow_all: bool,
    allowed_chat_ids: frozenset[int],
    allowed_user_ids: frozenset[int],
) -> bool:
    if allow_all:
        return True
    if not allowed_chat_ids and not allowed_user_ids:
        return False
    chat_ok = envelope.chat_id is not None and envelope.chat_id in allowed_chat_ids
    user_ok = envelope.user_id is not None and envelope.user_id in allowed_user_ids
    chat_dimension = not allowed_chat_ids or chat_ok
    user_dimension = not allowed_user_ids or user_ok
    return chat_dimension and user_dimension


def event_summary(row: Mapping[str, Any], include_raw: bool = False) -> dict[str, Any]:
    import json

    payload = json.loads(str(row["payload_json"]))
    envelope = parse_update(payload)
    result: dict[str, Any] = {
        "event_id": row["event_id"],
        "update_id": envelope.update_id,
        "type": envelope.update_type,
        "chat_id": str(envelope.chat_id) if envelope.chat_id is not None else None,
        "message_id": envelope.message_id,
        "thread_id": envelope.thread_id,
        "user_id": str(envelope.user_id) if envelope.user_id is not None else None,
        "text": envelope.text,
        "attachments": list(envelope.attachments),
        "received_at": datetime.fromtimestamp(float(row["received_at"]), tz=timezone.utc).isoformat(),
        "telegram_date": envelope.telegram_date,
        "untrusted_content": True,
    }
    if envelope.chat:
        result["chat"] = {
            key: envelope.chat.get(key)
            for key in ("id", "type", "title", "username", "first_name", "last_name")
            if key in envelope.chat
        }
    if envelope.sender:
        result["sender"] = {
            key: envelope.sender.get(key)
            for key in ("id", "is_bot", "username", "first_name", "last_name", "language_code")
            if key in envelope.sender
        }
    if include_raw:
        result["raw"] = payload
    return result

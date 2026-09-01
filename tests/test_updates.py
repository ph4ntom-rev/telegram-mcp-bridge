import pytest

from telegram_mcp.errors import BridgeError
from telegram_mcp.updates import actor_is_allowed, parse_update


def message_update(update_id: int = 7) -> dict[str, object]:
    return {
        "update_id": update_id,
        "message": {
            "message_id": 11,
            "message_thread_id": 3,
            "date": 1_700_000_000,
            "chat": {"id": -1001234567890, "type": "supergroup", "title": "Test"},
            "from": {"id": 42, "first_name": "Owner", "username": "mutable"},
            "caption": "hello",
            "document": {
                "file_id": "opaque-file-id",
                "file_unique_id": "stable-id",
                "file_name": "../../hostile.txt",
                "file_size": 12,
            },
        },
    }


def test_parses_message_and_attachment_as_opaque_metadata() -> None:
    parsed = parse_update(message_update())
    assert parsed.update_type == "message"
    assert parsed.chat_id == -1001234567890
    assert parsed.user_id == 42
    assert parsed.message_id == 11
    assert parsed.thread_id == 3
    assert parsed.text == "hello"
    assert parsed.attachments[0]["file_id"] == "opaque-file-id"


def test_callback_uses_callback_sender_and_message_chat() -> None:
    parsed = parse_update(
        {
            "update_id": 9,
            "callback_query": {
                "id": "cb",
                "from": {"id": 5},
                "data": "approve",
                "message": {"message_id": 4, "chat": {"id": 8}, "date": 10},
            },
        }
    )
    assert parsed.update_type == "callback_query"
    assert parsed.user_id == 5
    assert parsed.chat_id == 8
    assert parsed.text == "approve"


def test_unknown_update_is_forward_compatible() -> None:
    parsed = parse_update({"update_id": 999, "future_update": {"anything": True}})
    assert parsed.update_type == "unknown"
    assert parsed.chat_id is None


@pytest.mark.parametrize("bad", [{}, {"update_id": True}, {"update_id": -1}, {"update_id": "1"}])
def test_rejects_invalid_update_id(bad: dict[str, object]) -> None:
    with pytest.raises(BridgeError):
        parse_update(bad)


def test_allowlist_uses_numeric_ids_not_username() -> None:
    parsed = parse_update(message_update())
    assert actor_is_allowed(
        parsed,
        allow_all=False,
        allowed_chat_ids=frozenset(),
        allowed_user_ids=frozenset({42}),
    )
    assert not actor_is_allowed(
        parsed,
        allow_all=False,
        allowed_chat_ids=frozenset(),
        allowed_user_ids=frozenset({7}),
    )


@pytest.mark.parametrize(
    "update_type",
    [
        "message",
        "edited_message",
        "channel_post",
        "edited_channel_post",
        "business_connection",
        "business_message",
        "edited_business_message",
        "deleted_business_messages",
        "guest_message",
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
    ],
)
def test_recognizes_every_bot_api_10_2_update_variant(update_type: str) -> None:
    parsed = parse_update({"update_id": 100, update_type: {}})
    assert parsed.update_type == update_type


@pytest.mark.parametrize("update_type", ["business_message", "edited_business_message", "guest_message"])
def test_current_message_variants_extract_common_message_fields(update_type: str) -> None:
    parsed = parse_update(
        {
            "update_id": 101,
            update_type: {
                "message_id": 12,
                "message_thread_id": 4,
                "date": 1_800_000_000,
                "chat": {"id": -1001234567890, "type": "supergroup"},
                "from": {"id": 55, "first_name": "Actor"},
                "text": "payload",
            },
        }
    )
    assert parsed.update_type == update_type
    assert parsed.chat_id == -1001234567890
    assert parsed.user_id == 55
    assert parsed.message_id == 12
    assert parsed.thread_id == 4
    assert parsed.telegram_date == 1_800_000_000
    assert parsed.text == "payload"


@pytest.mark.parametrize(
    "update_type,payload,expected_chat_id,expected_message_id",
    [
        (
            "deleted_business_messages",
            {"business_connection_id": "bc", "chat": {"id": 700}, "message_ids": [3, 4]},
            700,
            None,
        ),
        (
            "message_reaction_count",
            {"chat": {"id": -800}, "message_id": 9, "date": 1_800_000_001, "reactions": []},
            -800,
            9,
        ),
        (
            "chat_boost",
            {"chat": {"id": -900}, "boost": {"boost_id": "boost"}},
            -900,
            None,
        ),
        (
            "removed_chat_boost",
            {"chat": {"id": -901}, "boost_id": "boost", "remove_date": 1_800_000_002},
            -901,
            None,
        ),
    ],
)
def test_chat_bearing_non_message_updates_extract_chat_and_message_ids(
    update_type: str,
    payload: dict[str, object],
    expected_chat_id: int,
    expected_message_id: int | None,
) -> None:
    parsed = parse_update({"update_id": 102, update_type: payload})
    assert parsed.chat_id == expected_chat_id
    assert parsed.message_id == expected_message_id


def test_business_connection_extracts_user_private_chat_and_date() -> None:
    parsed = parse_update(
        {
            "update_id": 103,
            "business_connection": {
                "id": "bc",
                "user": {"id": 77, "first_name": "Business owner"},
                "user_chat_id": 7700,
                "date": 1_800_000_003,
                "is_enabled": True,
            },
        }
    )
    assert parsed.user_id == 77
    assert parsed.chat_id == 7700
    assert parsed.telegram_date == 1_800_000_003


@pytest.mark.parametrize(
    "update_type,payload,user_id",
    [
        ("purchased_paid_media", {"from": {"id": 81}, "paid_media_payload": "p"}, 81),
        ("poll_answer", {"poll_id": "poll", "user": {"id": 82}, "option_ids": [0]}, 82),
        ("managed_bot", {"user": {"id": 83}, "bot": {"id": 9001, "is_bot": True}}, 83),
        (
            "subscription",
            {"user": {"id": 84}, "invoice_payload": "invoice", "state": "active"},
            84,
        ),
    ],
)
def test_actor_bearing_non_message_updates_extract_user(
    update_type: str,
    payload: dict[str, object],
    user_id: int,
) -> None:
    parsed = parse_update({"update_id": 104, update_type: payload})
    assert parsed.user_id == user_id


def test_allowlists_use_and_semantics_when_both_dimensions_are_configured() -> None:
    parsed = parse_update(message_update())
    assert actor_is_allowed(
        parsed,
        allow_all=False,
        allowed_chat_ids=frozenset({-1001234567890}),
        allowed_user_ids=frozenset({42}),
    )
    assert not actor_is_allowed(
        parsed,
        allow_all=False,
        allowed_chat_ids=frozenset({-1001234567890}),
        allowed_user_ids=frozenset({999}),
    )
    assert not actor_is_allowed(
        parsed,
        allow_all=False,
        allowed_chat_ids=frozenset({999}),
        allowed_user_ids=frozenset({42}),
    )


def test_empty_allowlists_fail_closed_unless_allow_all_is_explicit() -> None:
    parsed = parse_update(message_update())
    assert not actor_is_allowed(
        parsed,
        allow_all=False,
        allowed_chat_ids=frozenset(),
        allowed_user_ids=frozenset(),
    )
    assert actor_is_allowed(
        parsed,
        allow_all=True,
        allowed_chat_ids=frozenset(),
        allowed_user_ids=frozenset(),
    )

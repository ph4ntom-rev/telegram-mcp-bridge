from __future__ import annotations

from typing import Any

import pytest

from telegram_mcp.mcp_server import INSTRUCTIONS, RuntimeFacade, StaticTokenVerifier, create_mcp_server


@pytest.mark.asyncio
async def test_tool_surface_and_annotations_are_intentional() -> None:
    server = create_mcp_server(RuntimeFacade(), auth_token=None)
    tools = {tool.name: tool for tool in await server.list_tools()}
    assert {
        "telegram_wait_updates",
        "telegram_ack_updates",
        "telegram_release_updates",
        "telegram_peek_updates",
        "telegram_list_chats",
        "telegram_bridge_status",
        "telegram_send_text",
        "telegram_reply",
        "telegram_edit_text",
        "telegram_delete_message",
        "telegram_typing",
        "telegram_answer_callback",
    } == set(tools)
    assert tools["telegram_send_text"].annotations is not None
    assert tools["telegram_send_text"].annotations.idempotent_hint is True
    assert tools["telegram_delete_message"].annotations is not None
    assert tools["telegram_delete_message"].annotations.idempotent_hint is False
    assert tools["telegram_peek_updates"].annotations is not None
    assert tools["telegram_peek_updates"].annotations.read_only_hint is True


@pytest.mark.asyncio
async def test_static_token_verifier_returns_no_secret() -> None:
    verifier = StaticTokenVerifier("top-secret-token")
    assert await verifier.verify_token("wrong") is None
    access = await verifier.verify_token("top-secret-token")
    assert access is not None
    assert access.token != "top-secret-token"
    assert len(access.token) == 64
    assert "top-secret-token" not in repr(access)


def test_codex_visible_instruction_prefix_is_complete() -> None:
    assert len(INSTRUCTIONS) <= 512
    assert "untrusted" in INSTRUCTIONS
    assert "idempotency_key" in INSTRUCTIONS
    assert "never ack first" in INSTRUCTIONS
    assert "never retried" in INSTRUCTIONS


@pytest.mark.asyncio
async def test_every_tool_delegates_through_runtime_facade() -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    class Operations:
        def __getattr__(self, name: str) -> Any:
            async def invoke(**kwargs: Any) -> dict[str, Any]:
                calls.append((name, kwargs))
                return {"operation": name}

            return invoke

    facade = RuntimeFacade()
    with pytest.raises(RuntimeError, match="not ready"):
        facade.require()
    facade.operations = Operations()  # type: ignore[assignment]
    server = create_mcp_server(facade, auth_token=None)
    invocations = {
        "telegram_wait_updates": {},
        "telegram_ack_updates": {"lease_token": "lease"},
        "telegram_release_updates": {"lease_token": "lease"},
        "telegram_peek_updates": {},
        "telegram_list_chats": {},
        "telegram_bridge_status": {},
        "telegram_send_text": {
            "chat_id": "42",
            "text": "hello",
            "idempotency_key": "reply:1:answer",
        },
        "telegram_reply": {
            "event_id": 1,
            "text": "hello",
            "idempotency_key": "reply:1:reply",
        },
        "telegram_edit_text": {"chat_id": "42", "message_id": 1, "text": "edited"},
        "telegram_delete_message": {"chat_id": "42", "message_id": 1},
        "telegram_typing": {"chat_id": "42"},
        "telegram_answer_callback": {"event_id": 1},
    }
    for name, arguments in invocations.items():
        result = await server.call_tool(name, arguments)
        assert result is not None
    assert [name for name, _ in calls] == [
        "wait_updates",
        "ack_updates",
        "release_updates",
        "peek_updates",
        "list_chats",
        "status",
        "send_text",
        "reply",
        "edit_text",
        "delete_message",
        "typing",
        "answer_callback",
    ]

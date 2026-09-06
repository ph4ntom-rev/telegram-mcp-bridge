"""Production-image smoke check using installed dependencies, no external network."""

from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path

import httpx

from telegram_mcp.app import create_application
from telegram_mcp.config import Settings
from telegram_mcp.storage import SQLiteStorage


async def main() -> None:
    if hasattr(os, "geteuid"):
        assert os.geteuid() != 0, "The production image must run without root"
    with tempfile.TemporaryDirectory() as directory:
        path, snapshot = Path(directory) / "bridge.sqlite3", Path(directory) / "snapshot.sqlite3"
        settings = Settings(
            telegram_bot_token="123:fixture",
            database_path=path,
            transport="http",
            ingress_mode="disabled",
            mcp_auth_token="x" * 32,
            allowed_chat_ids=frozenset({42}),
            max_delivery_attempts=1,
        )
        bundle = create_application(settings)
        async with bundle.app.router.lifespan_context(bundle.app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=bundle.app), base_url="http://127.0.0.1:8000"
            ) as client:
                assert (await client.get("/readyz")).status_code == 200
                assert (await client.post("/mcp")).status_code == 401
                response = await client.post(
                    "/mcp",
                    headers={
                        "Authorization": "Bearer " + settings.mcp_auth_token,
                        "Accept": "application/json, text/event-stream",
                    },
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2025-03-26",
                            "capabilities": {},
                            "clientInfo": {"name": "container-test", "version": "1"},
                        },
                    },
                )
                assert response.status_code == 200 and "serverInfo" in response.text
            storage = bundle.runtime.storage
            row = await storage.enqueue_update(settings.bot_key, {"update_id": 1})
            claim = await storage.claim_events(settings.bot_key)
            assert claim.lease_token
            await storage.release_events(settings.bot_key, [row.event_id], claim.lease_token)
            assert len((await storage.review_queue(settings.bot_key))["quarantined_inbox"]) == 1
            assert (await storage.backup(snapshot))["ok"]
        async with SQLiteStorage(snapshot) as restored:
            assert len((await restored.review_queue(settings.bot_key))["quarantined_inbox"]) == 1
    print("Docker runtime, authentication, quarantine and snapshot checks passed")


if __name__ == "__main__":
    asyncio.run(main())

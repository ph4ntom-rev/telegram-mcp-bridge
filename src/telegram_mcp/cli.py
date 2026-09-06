"""Operator CLI and MCP entry point."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sqlite3
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import uvicorn
from dotenv import load_dotenv

from telegram_mcp.app import create_application
from telegram_mcp.config import Settings
from telegram_mcp.errors import BridgeError, ConfigurationError
from telegram_mcp.instance_lock import InstanceLock
from telegram_mcp.logging_utils import configure_logging, redact_text
from telegram_mcp.mcp_server import RuntimeFacade, create_mcp_server
from telegram_mcp.runtime import BridgeRuntime
from telegram_mcp.storage import SQLiteStorage
from telegram_mcp.telegram import TelegramClient

_WEBHOOK_SECRET_RE = re.compile(r"^[A-Za-z0-9_-]{32,256}$")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="telegram-mcp",
        description="Durable low-latency MCP bridge for Telegram",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    serve = commands.add_parser("serve", help="run the bridge")
    serve.add_argument("--env-file", type=Path, default=Path(".env"))
    serve.add_argument("--transport", choices=("stdio", "http"))

    doctor = commands.add_parser("doctor", help="validate configuration and connectivity")
    doctor.add_argument("--env-file", type=Path, default=Path(".env"))

    queue = commands.add_parser("queue", help="offline queue review, resolution and complete backup")
    queue_commands = queue.add_subparsers(dest="queue_command", required=True)
    for name in ("inspect", "requeue", "discard", "resolve-outbox", "backup"):
        operation = queue_commands.add_parser(name)
        operation.add_argument("--env-file", type=Path, default=Path(".env"))
        if name == "inspect":
            operation.add_argument("--limit", type=int, default=50)
        elif name == "backup":
            operation.add_argument("destination", type=Path)
        else:
            operation.add_argument("id", type=int)
            if name == "resolve-outbox":
                choice = operation.add_mutually_exclusive_group(required=True)
                choice.add_argument(
                    "--message-id", type=int, help="message ID independently verified in Telegram"
                )
                choice.add_argument(
                    "--discard", action="store_true", help="retain a terminal record without retry"
                )

    webhook = commands.add_parser("webhook", help="manage Telegram webhook registration")
    webhook_commands = webhook.add_subparsers(dest="webhook_command", required=True)
    for name in ("set", "delete"):
        child = webhook_commands.add_parser(name)
        child.add_argument("--env-file", type=Path, default=Path(".env"))
        child.add_argument(
            "--drop-pending",
            action="store_true",
            help="irreversibly discard pending Telegram updates",
        )
    return parser


def _load_settings(args: argparse.Namespace) -> Settings:
    env_file: Path = args.env_file
    load_dotenv(env_file, override=False)
    transport = getattr(args, "transport", None)
    if transport:
        os.environ["BRIDGE_TRANSPORT"] = transport
    settings = Settings.from_env()
    if not settings.database_path.is_absolute():
        settings = replace(
            settings,
            database_path=(env_file.expanduser().resolve().parent / settings.database_path).resolve(),
        )
    return settings


def _configure(settings: Settings) -> None:
    configure_logging(
        settings.log_level,
        secrets=(
            settings.telegram_bot_token,
            settings.mcp_auth_token or "",
            settings.webhook_secret or "",
        ),
    )


async def _serve_stdio(settings: Settings) -> None:
    runtime = BridgeRuntime(settings)
    facade = RuntimeFacade()
    server = create_mcp_server(facade, auth_token=None, log_level=settings.log_level)
    await runtime.start(start_ingress=True)
    facade.operations = runtime.service
    try:
        await server.run_stdio_async()
    finally:
        facade.operations = None
        await runtime.stop()


async def _serve_http(settings: Settings) -> None:
    bundle = create_application(settings)
    config = uvicorn.Config(
        bundle.app,
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
        access_log=False,
        server_header=False,
        date_header=False,
        workers=1,
    )
    await uvicorn.Server(config).serve()


async def _doctor(settings: Settings) -> dict[str, Any]:
    """Check Telegram and storage without running mutable startup recovery.

    Operators commonly run ``doctor`` next to a live bridge.  Calling
    ``BridgeRuntime.start`` here would treat that process's valid leases as
    crash leftovers, so diagnostics opens only the two resources it reads.
    """

    runtime = BridgeRuntime(settings)
    await runtime.storage.open()
    try:
        await runtime.telegram.start()
        try:
            me = await runtime.telegram.get_me()
            webhook = await runtime.telegram.get_webhook_info()
            status = await runtime.service.status()
            return {
                "ok": True,
                "bot": {
                    "id": str(me["id"]),
                    "username": me.get("username"),
                },
                "telegram_webhook_configured": bool(webhook.get("url")),
                "bridge": status,
            }
        finally:
            await runtime.telegram.close()
    finally:
        await runtime.storage.close()


async def _webhook(settings: Settings, *, action: str, drop_pending: bool) -> dict[str, Any]:
    client = TelegramClient(
        token=settings.telegram_bot_token,
        api_base=settings.telegram_api_base,
        connect_timeout=settings.telegram_connect_timeout,
        request_timeout=settings.telegram_request_timeout,
        poll_timeout=settings.telegram_poll_timeout,
    )
    await client.start()
    try:
        if action == "set":
            if not settings.public_base_url:
                raise ConfigurationError("PUBLIC_BASE_URL is required to register a webhook")
            if not settings.webhook_secret or not _WEBHOOK_SECRET_RE.fullmatch(settings.webhook_secret):
                raise ConfigurationError("TELEGRAM_WEBHOOK_SECRET must contain 32-256 safe random characters")
            await client.set_webhook(
                url=f"{settings.public_base_url.rstrip('/')}{settings.webhook_path}",
                secret_token=settings.webhook_secret,
                allowed_updates=settings.allowed_updates,
                drop_pending_updates=drop_pending,
            )
        else:
            await client.delete_webhook(drop_pending_updates=drop_pending)
        return {"ok": True, "action": action, "drop_pending": drop_pending}
    finally:
        await client.close()


async def _queue(args: argparse.Namespace, settings: Settings) -> dict[str, Any]:
    if not settings.database_path.is_file():
        raise BridgeError("Database does not exist; refusing to create an empty operator database")
    # No startup recovery, authorization bootstrap, network client or send worker.
    # Requiring a stopped service also makes administrative resolutions unambiguous.
    with InstanceLock(settings.database_path):
        async with SQLiteStorage(settings.database_path) as storage:
            action = args.queue_command
            if action == "inspect":
                return await storage.review_queue(settings.bot_key, limit=args.limit)
            if action == "backup":
                return await storage.backup(args.destination)
            if action in ("requeue", "discard"):
                await storage.resolve_quarantine(settings.bot_key, args.id, action=action)
            elif action == "resolve-outbox":
                await storage.resolve_uncertain(
                    settings.bot_key,
                    args.id,
                    telegram_message_id=args.message_id,
                    discard=args.discard,
                )
            else:
                raise ValueError("Unknown queue operation")
            return {"ok": True, "action": action, "id": args.id, "telegram_requests": 0}


async def _run(args: argparse.Namespace, settings: Settings) -> None:
    if args.command == "serve":
        if settings.transport == "stdio":
            await _serve_stdio(settings)
        else:
            await _serve_http(settings)
        return
    if args.command == "doctor":
        print(json.dumps(await _doctor(settings), ensure_ascii=False, indent=2))
        return
    if args.command == "queue":
        print(json.dumps(await _queue(args, settings), ensure_ascii=False, indent=2))
        return
    if args.command == "webhook":
        result = await _webhook(
            settings,
            action=args.webhook_command,
            drop_pending=bool(args.drop_pending),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    raise RuntimeError("Unknown command")


def main() -> None:
    args = _parser().parse_args()
    try:
        settings = _load_settings(args)
        _configure(settings)
        asyncio.run(_run(args, settings))
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except (BridgeError, ConfigurationError, ValueError, OSError, sqlite3.Error) as exc:
        print(f"telegram-mcp: {redact_text(exc)}", file=sys.stderr)
        raise SystemExit(2) from None

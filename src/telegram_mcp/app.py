"""Hardened HTTP composition: webhook, health checks and stateless MCP."""

from __future__ import annotations

import json
import logging
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address
from typing import Any

from mcp.server import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import BaseRoute, Mount, Route

from telegram_mcp.config import Settings
from telegram_mcp.errors import QueueFullError
from telegram_mcp.mcp_server import RuntimeFacade, create_mcp_server
from telegram_mcp.runtime import BridgeRuntime

logger = logging.getLogger(__name__)
_WILDCARD_BIND_HOSTS = frozenset((str(IPv4Address(0)), str(IPv6Address(0))))


class DuplicateJSONKeyError(ValueError):
    """A webhook body contains an ambiguous object with duplicate names."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateJSONKeyError("Webhook JSON contains a duplicate object key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"Non-standard JSON constant is not allowed: {value}")


@dataclass(frozen=True, slots=True)
class ApplicationBundle:
    app: Starlette
    runtime: BridgeRuntime
    mcp_server: MCPServer[dict[str, Any]]
    facade: RuntimeFacade


def _auth_base_url(settings: Settings) -> str:
    if settings.public_base_url:
        return settings.public_base_url.rstrip("/")
    host = "localhost" if settings.host in _WILDCARD_BIND_HOSTS else settings.host
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{settings.port}"


def create_application(
    settings: Settings,
    *,
    runtime: BridgeRuntime | None = None,
) -> ApplicationBundle:
    """Build an ASGI app without starting network or database resources."""

    bridge_runtime = runtime or BridgeRuntime(settings)
    facade = RuntimeFacade()
    mcp_server = create_mcp_server(
        facade,
        auth_token=settings.mcp_auth_token,
        auth_base_url=_auth_base_url(settings),
        log_level=settings.log_level,
    )
    transport_security = TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=list(settings.mcp_allowed_hosts),
        allowed_origins=list(settings.mcp_allowed_origins),
    )
    mcp_app = mcp_server.streamable_http_app(
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=True,
        max_request_body_size=settings.max_update_bytes,
        transport_security=transport_security,
        host=settings.host,
    )

    async def health(_: Request) -> Response:
        return JSONResponse({"status": "ok"})

    async def ready(_: Request) -> Response:
        is_ready = bridge_runtime.ready
        return JSONResponse(
            {"status": "ready" if is_ready else "not_ready"},
            status_code=200 if is_ready else 503,
        )

    async def webhook(request: Request) -> Response:
        expected = settings.webhook_secret
        presented = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if expected is None or not secrets.compare_digest(presented, expected):
            return JSONResponse({"error": "forbidden"}, status_code=403)
        content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            return JSONResponse({"error": "application/json required"}, status_code=415)
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > settings.max_update_bytes:
                return JSONResponse({"error": "request too large"}, status_code=413)
        try:
            update = json.loads(
                bytes(body),
                object_pairs_hook=_unique_object,
                parse_constant=_reject_json_constant,
            )
            if not isinstance(update, dict):
                raise ValueError("Webhook JSON root must be an object")
            result = await bridge_runtime.service.ingest_update(update)
        except (UnicodeDecodeError, json.JSONDecodeError, DuplicateJSONKeyError, ValueError):
            return JSONResponse({"error": "invalid Telegram update"}, status_code=400)
        except QueueFullError:
            # Non-2xx asks Telegram to retry.  Nothing has been acknowledged yet.
            return JSONResponse({"error": "queue full"}, status_code=503)
        except Exception as exc:
            logger.error("Webhook was not committed (%s)", type(exc).__name__)
            return JSONResponse({"error": "temporarily unavailable"}, status_code=503)
        return JSONResponse(
            {
                "ok": True,
                "disposition": result["disposition"],
            }
        )

    @asynccontextmanager
    async def lifespan(_: Starlette) -> AsyncIterator[None]:
        await bridge_runtime.start(start_ingress=True)
        facade.operations = bridge_runtime.service
        try:
            async with mcp_server.session_manager.run():
                yield
        finally:
            facade.operations = None
            await bridge_runtime.stop()

    routes: list[BaseRoute] = [
        Route("/healthz", health, methods=["GET"]),
        Route("/readyz", ready, methods=["GET"]),
    ]
    if settings.ingress_mode == "webhook":
        routes.append(Route(settings.webhook_path, webhook, methods=["POST"]))
    routes.append(Mount("/", app=mcp_app))
    app = Starlette(
        debug=False,
        routes=routes,
        lifespan=lifespan,
    )
    return ApplicationBundle(app=app, runtime=bridge_runtime, mcp_server=mcp_server, facade=facade)

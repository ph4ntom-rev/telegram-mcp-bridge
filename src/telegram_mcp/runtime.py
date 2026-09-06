"""Lifecycle and resilient Telegram polling for the bridge."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Coroutine
from typing import Any

import httpx

from telegram_mcp.config import Settings
from telegram_mcp.instance_lock import InstanceLock
from telegram_mcp.service import BridgeService
from telegram_mcp.storage import SQLiteStorage
from telegram_mcp.telegram import TelegramAPIError, TelegramClient

logger = logging.getLogger(__name__)


async def _finish_cleanup(operation: Coroutine[Any, Any, None], *, name: str) -> None:
    """Finish resource cleanup before propagating even repeated cancellation."""

    task = asyncio.create_task(operation, name=name)
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError as cancellation:
        try:
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
            task.result()
        finally:
            raise cancellation


class BridgeRuntime:
    """Own all resources and keep ingress failures isolated from the MCP process."""

    def __init__(
        self,
        settings: Settings,
        *,
        telegram_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.settings = settings
        self.storage = SQLiteStorage(
            settings.database_path,
            max_queue_events=settings.max_queue_events,
            max_claim_events=settings.max_claim_events,
            max_delivery_attempts=settings.max_delivery_attempts,
            max_payload_bytes=settings.max_update_bytes,
            synchronous=settings.sqlite_synchronous,  # type: ignore[arg-type]
        )
        self.telegram = TelegramClient(
            token=settings.telegram_bot_token,
            api_base=settings.telegram_api_base,
            connect_timeout=settings.telegram_connect_timeout,
            request_timeout=settings.telegram_request_timeout,
            poll_timeout=settings.telegram_poll_timeout,
            transport=telegram_transport,
        )
        self.service = BridgeService(settings=settings, storage=self.storage, telegram=self.telegram)
        self._instance_lock = InstanceLock(settings.database_path)
        self._poll_task: asyncio.Task[None] | None = None
        self._started = False
        self._stopping = False
        self._poll_failures = 0
        self._last_poll_success_at: float | None = None
        self._last_poll_error_type: str | None = None

    @property
    def ready(self) -> bool:
        return self._started and not self._stopping and self.storage.is_open and self.telegram.started

    async def start(self, *, start_ingress: bool = True) -> None:
        if self._started:
            return
        self._stopping = False
        self._instance_lock.acquire()
        try:
            await self.storage.open()
            recovery = await self.storage.recover(retry_expired_outbox=False)
            await self.service.bootstrap_authorizations()
            cutoff = time.time() - (self.settings.retention_days * 86_400)
            await self.storage.purge_acked(
                acked_before=cutoff,
                bot_key=self.settings.bot_key,
                limit=10_000,
            )
            await self.telegram.start()
        except BaseException:

            async def cleanup_failed_start() -> None:
                try:
                    await self.telegram.close()
                finally:
                    try:
                        await self.storage.close()
                    finally:
                        self._instance_lock.release()

            await _finish_cleanup(cleanup_failed_start(), name="telegram-bridge-startup-cleanup")
            raise
        self._started = True
        if recovery.inbox_requeued or recovery.outbox_recovered:
            logger.warning(
                "Recovered inbox=%s and ambiguous outbox=%s records",
                recovery.inbox_requeued,
                recovery.outbox_recovered,
            )
        if start_ingress and self.settings.ingress_mode == "polling":
            self._poll_task = asyncio.create_task(self._poll_loop(), name="telegram-long-poll")

    async def stop(self) -> None:
        async def operation() -> None:
            if not self._started and not self.storage.is_open and not self.telegram.started:
                return
            self._stopping = True
            task, self._poll_task = self._poll_task, None
            try:
                if task is not None:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
            finally:
                try:
                    await self.telegram.close()
                finally:
                    try:
                        await self.storage.close()
                    finally:
                        self._started = False
                        self._instance_lock.release()

        await _finish_cleanup(operation(), name="telegram-bridge-shutdown")

    async def _poll_loop(self) -> None:
        backoff = 0.25
        while True:
            try:
                offset = await self.storage.get_poll_offset(self.settings.bot_key)
                updates = await self.telegram.get_updates(
                    offset=offset,
                    allowed_updates=self.settings.allowed_updates,
                )
                await self.service.ingest_poll_batch(updates)
                self._poll_failures = 0
                self._last_poll_error_type = None
                self._last_poll_success_at = time.time()
                backoff = 0.25
            except asyncio.CancelledError:
                raise
            except TelegramAPIError as exc:
                self._poll_failures += 1
                self._last_poll_error_type = type(exc).__name__
                delay = float(exc.retry_after) if exc.retry_after else backoff
                logger.warning(
                    "Telegram polling failed (%s); retrying in %.2fs",
                    type(exc).__name__,
                    min(delay, 30.0),
                )
                await asyncio.sleep(min(delay, 30.0))
                backoff = min(30.0, backoff * 2.0)
            except Exception as exc:  # keep the daemon alive; details stay local and credential-free
                self._poll_failures += 1
                self._last_poll_error_type = type(exc).__name__
                logger.error(
                    "Polling batch was not committed (%s); offset is unchanged",
                    type(exc).__name__,
                )
                await asyncio.sleep(backoff)
                backoff = min(30.0, backoff * 2.0)

    async def public_status(self) -> dict[str, Any]:
        status = await self.service.status()
        status.update(
            {
                "ready": self.ready,
                "poll_failures": self._poll_failures,
                "last_poll_error_type": self._last_poll_error_type,
                "last_poll_success_at": self._last_poll_success_at,
            }
        )
        return status

    async def __aenter__(self) -> BridgeRuntime:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

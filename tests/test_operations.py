from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pytest

from telegram_mcp.cli import _parser, _queue, _run
from telegram_mcp.config import Settings
from telegram_mcp.errors import BridgeError
from telegram_mcp.instance_lock import InstanceAlreadyRunningError, InstanceLock
from telegram_mcp.storage import LeaseConflictError, OutboxStateError, SQLiteStorage, StorageError

BOT = "operator-test"


async def uncertain(storage: SQLiteStorage, bot: str = BOT) -> int:
    await storage.set_chat_authorization(bot, 42, can_write=True)
    reserved = await storage.reserve_outbox(
        bot,
        "operator:delivery",
        chat_id=42,
        method="sendMessage",
        payload={"chat_id": 42, "text": "private body"},
        now=1,
    )
    claim = await storage.claim_outbox_id(reserved.record.outbox_id, now=1)
    assert claim.lease_token
    await storage.mark_outbox_uncertain(reserved.record.outbox_id, claim.lease_token, error="fixture", now=2)
    return reserved.record.outbox_id


async def poison(storage: SQLiteStorage, bot: str = BOT) -> int:
    event = await storage.enqueue_update(
        bot, {"update_id": 1, "message": {"text": "private body"}}, received_at=1
    )
    for now in range(2, 4):
        claim = await storage.claim_events(bot, limit=1, now=now)
        assert claim.lease_token
        await storage.release_events(bot, [event.event_id], claim.lease_token, now=now)
    return event.event_id


async def test_poison_does_not_starve_queue_and_survives_restart(tmp_path: Path) -> None:
    path = tmp_path / "queue.sqlite3"
    async with SQLiteStorage(path, max_delivery_attempts=2) as storage:
        event_id = await poison(storage)
        await storage.enqueue_update(BOT, {"update_id": 2}, received_at=4)
        claim = await storage.claim_events(BOT, limit=1, now=5)
        assert [e.update_id for e in claim.events] == [2]
        review = await storage.review_queue(BOT)
        assert review["quarantined_inbox"][0]["event_id"] == event_id
        assert "private body" not in json.dumps(review)
        metrics = await storage.metrics(bot_key=BOT, now=5)
        assert metrics.inbox_states == {"quarantined": 1, "leased": 1}
        assert metrics.inbox_pending == 2  # Quarantine still consumes bounded storage capacity.
    async with SQLiteStorage(path, max_delivery_attempts=2) as storage:
        assert len((await storage.review_queue(BOT))["quarantined_inbox"]) == 1
        with pytest.raises(StorageError):
            await storage.resolve_quarantine("other-bot", event_id, action="requeue")
        await storage.resolve_quarantine(BOT, event_id, action="requeue", now=6)
        claim = await storage.claim_events(BOT, limit=1, now=7)
        assert claim.events[0].event_id == event_id and claim.events[0].attempts == 1


async def test_live_lease_is_preserved_but_expired_exhausted_lease_is_quarantined(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "db", max_delivery_attempts=1) as storage:
        row = await storage.enqueue_update(BOT, {"update_id": 1}, received_at=1)
        claim = await storage.claim_events(BOT, lease_seconds=5, now=2)
        assert claim.lease_token
        assert not (await storage.claim_events(BOT, now=3)).events
        assert not (await storage.review_queue(BOT))["quarantined_inbox"]
        assert not (await storage.claim_events(BOT, now=8)).events
        assert len((await storage.review_queue(BOT))["quarantined_inbox"]) == 1
        with pytest.raises(LeaseConflictError):
            await storage.ack_events(BOT, [row.event_id], claim.lease_token, now=8)
        await storage.resolve_quarantine(BOT, row.event_id, action="discard", now=9)
        assert (await storage.metrics(now=10)).inbox_states == {"acked": 1}
        assert not (await storage.review_queue(BOT))["quarantined_inbox"]
        assert await storage.purge_acked(acked_before=11) == 1


@pytest.mark.parametrize("discard", [False, True])
async def test_uncertain_resolution_preserves_idempotency_without_send(tmp_path: Path, discard: bool) -> None:
    async with SQLiteStorage(tmp_path / "db") as storage:
        outbox_id = await uncertain(storage)
        review = await storage.review_queue(BOT)
        assert review["uncertain_outbox"][0]["outbox_id"] == outbox_id
        assert "private body" not in json.dumps(review)
        with pytest.raises(OutboxStateError):
            await storage.resolve_uncertain("other-bot", outbox_id, discard=True)
        await storage.resolve_uncertain(
            BOT, outbox_id, discard=discard, telegram_message_id=None if discard else 123, now=3
        )
        record = await storage.get_outbox(BOT, outbox_id=outbox_id)
        assert record and record.status == ("dead" if discard else "sent")
        assert record.telegram_message_id == (None if discard else 123)
        assert record.sent_at == (None if discard else 3)
        replay = await storage.reserve_outbox(
            BOT,
            "operator:delivery",
            chat_id=42,
            method="sendMessage",
            payload={"chat_id": 42, "text": "private body"},
            now=4,
        )
        assert replay.created is False
        assert not (await storage.claim_outbox_id(outbox_id, now=4)).records
        with pytest.raises(OutboxStateError):
            await storage.resolve_uncertain(BOT, outbox_id, discard=True)


async def test_full_backup_restores_quarantine_offsets_and_outbox_ledger(tmp_path: Path) -> None:
    path, backup = tmp_path / "original", tmp_path / "backup"
    async with SQLiteStorage(path, max_delivery_attempts=2) as storage:
        await poison(storage)
        outbox_id = await uncertain(storage)
        await storage.ingest_poll_batch(BOT, [{"update_id": 100}], received_at=4)
        await storage.enqueue_update("other-bot", {"update_id": 1}, received_at=5)
        report = await storage.backup(backup)
        assert report["ok"] and report["schema_version"] == 2
        with pytest.raises(FileExistsError):
            await storage.backup(backup)
        with pytest.raises(FileExistsError):
            await storage.backup(path)
    if os.name != "nt":
        assert backup.stat().st_mode & 0o777 == 0o600
    async with SQLiteStorage(backup) as restored:
        assert await restored.get_poll_offset(BOT) == 101
        assert len((await restored.review_queue(BOT))["quarantined_inbox"]) == 1
        assert (await restored.get_outbox(BOT, outbox_id=outbox_id)).status == "uncertain"
        assert await restored.get_event("other-bot", 1) is not None
        await restored.recover(now=200)
        assert not (await restored.claim_outbox_id(outbox_id, now=200)).records


async def test_disk_full_rolls_back_event_and_offset(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "limited") as storage:
        await storage.ingest_poll_batch(BOT, [{"update_id": 1}], received_at=1)
        page_count = (await storage._fetchone("PRAGMA page_count"))[0]
        await storage._conn().execute(f"PRAGMA max_page_count={int(page_count)}")
        with pytest.raises(sqlite3.OperationalError, match="full"):
            await storage.ingest_poll_batch(BOT, [{"update_id": 2, "data": "x" * 100_000}], received_at=2)
        assert await storage.get_poll_offset(BOT) == 2
        assert await storage.get_event(BOT, 2) is None
        assert (await storage.metrics(now=3)).inbox_pending == 1


async def test_failed_backup_removes_only_its_incomplete_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with SQLiteStorage(tmp_path / "source") as storage:
        await storage.enqueue_update(BOT, {"update_id": 1})

        async def fail(*args: object, **kwargs: object) -> None:
            raise sqlite3.OperationalError("database or disk is full")

        monkeypatch.setattr(storage._conn(), "backup", fail)
        destination = tmp_path / "partial"
        with pytest.raises(sqlite3.OperationalError, match="full"):
            await storage.backup(destination)
        assert not destination.exists()
        assert await storage.get_event(BOT, 1) is not None


async def test_cancelled_backup_finishes_before_releasing_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with SQLiteStorage(tmp_path / "source") as storage:
        await storage.enqueue_update(BOT, {"update_id": 1})
        original = storage._conn().backup
        entered, proceed = asyncio.Event(), asyncio.Event()

        async def delayed(*args: Any, **kwargs: Any) -> None:
            entered.set()
            await proceed.wait()
            await original(*args, **kwargs)

        monkeypatch.setattr(storage._conn(), "backup", delayed)
        destination = tmp_path / "complete"
        task = asyncio.create_task(storage.backup(destination))
        await asyncio.wait_for(entered.wait(), timeout=3)
        task.cancel()
        proceed.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with SQLiteStorage(destination) as restored:
            assert await restored.get_event(BOT, 1) is not None


async def test_two_connections_quarantine_once_without_double_claiming(tmp_path: Path) -> None:
    path = tmp_path / "shared"
    async with (
        SQLiteStorage(path, max_delivery_attempts=1) as first,
        SQLiteStorage(
            path,
            max_delivery_attempts=1,
        ) as second,
    ):
        await first.enqueue_update(BOT, {"update_id": 1}, received_at=1)
        await first.claim_events(BOT, lease_seconds=1, now=1)
        await first.enqueue_update(BOT, {"update_id": 2}, received_at=2)
        claims = await asyncio.gather(first.claim_events(BOT, now=3), second.claim_events(BOT, now=3))
        assert sum(len(claim.events) for claim in claims) == 1
        assert (await first.metrics(bot_key=BOT, now=3)).counters["inbox_quarantined"] == 1


async def test_schema_v1_migration_keeps_existing_records(tmp_path: Path) -> None:
    path = tmp_path / "old"
    # Frozen original schema: user_version=1, no quarantine overlay.
    with sqlite3.connect(path) as connection:
        connection.executescript((Path(__file__).parent / "fixtures/schema_v1.sql").read_text())
        connection.execute("INSERT INTO poll_offsets VALUES (?, ?, ?)", (BOT, 123, 1))
    async with SQLiteStorage(path) as storage:
        assert await storage.get_poll_offset(BOT) == 123
        assert (await storage._fetchone("PRAGMA user_version"))[0] == 2
        assert await storage.review_queue(BOT) == {
            "quarantined_inbox": [],
            "uncertain_outbox": [],
            "limit": 50,
        }
    async with SQLiteStorage(path) as storage:
        assert await storage.get_poll_offset(BOT) == 123


async def test_operator_cli_is_offline_scoped_and_rejects_active_worker(tmp_path: Path) -> None:
    path = tmp_path / "db"
    settings = Settings(
        telegram_bot_token="123:fixture", database_path=path, allowed_chat_ids=frozenset({42})
    )
    args = _parser().parse_args(["queue", "inspect"])
    with pytest.raises(BridgeError, match="does not exist"):
        await _queue(args, settings)
    async with SQLiteStorage(path, max_delivery_attempts=2) as storage:
        event_id = await poison(storage, settings.bot_key)
        outbox_id = await uncertain(storage, settings.bot_key)
    with InstanceLock(path), pytest.raises(InstanceAlreadyRunningError):
        await _queue(args, settings)
    assert len((await _queue(args, settings))["quarantined_inbox"]) == 1
    await _queue(_parser().parse_args(["queue", "requeue", str(event_id)]), settings)
    result = await _queue(
        _parser().parse_args(["queue", "resolve-outbox", str(outbox_id), "--discard"]), settings
    )
    assert result["telegram_requests"] == 0
    await _run(_parser().parse_args(["queue", "backup", str(tmp_path / "snapshot")]), settings)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"discard": False},
        {"discard": True, "telegram_message_id": 1},
        {"telegram_message_id": 0},
        {"telegram_message_id": True},
    ],
)
async def test_uncertain_resolution_rejects_ambiguous_inputs(tmp_path: Path, kwargs: dict[str, Any]) -> None:
    async with SQLiteStorage(tmp_path / "db") as storage:
        with pytest.raises(ValueError):
            await storage.resolve_uncertain(BOT, 1, **kwargs)


async def test_abrupt_process_exit_recovers_without_resending(tmp_path: Path) -> None:
    path = tmp_path / "crashed"
    code = """
import asyncio, os, sys
from telegram_mcp.storage import SQLiteStorage
async def run():
    s = SQLiteStorage(sys.argv[1])
    await s.open()
    await s.enqueue_update("crash-bot", {"update_id":1}, received_at=1)
    await s.claim_events("crash-bot", lease_seconds=100, now=2)
    await s.set_chat_authorization("crash-bot",42,can_write=True)
    r = await s.reserve_outbox("crash-bot","crash:key",chat_id=42,
        method="sendMessage",payload={"text":"fixture"},now=2)
    await s.claim_outbox_id(r.record.outbox_id, lease_seconds=100, now=2)
    os._exit(23)
asyncio.run(run())
"""
    process = await asyncio.create_subprocess_exec(sys.executable, "-c", code, str(path))
    try:
        assert await asyncio.wait_for(process.wait(), timeout=15) == 23
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    async with SQLiteStorage(path) as storage:
        recovery = await storage.recover(now=3)
        assert recovery.inbox_requeued == 1 and recovery.outbox_recovered == 1
        assert (await storage.claim_events("crash-bot", now=4)).events[0].update_id == 1
        assert not (await storage.claim_outbox(bot_key="crash-bot", now=4)).records
        assert len((await storage.review_queue("crash-bot"))["uncertain_outbox"]) == 1

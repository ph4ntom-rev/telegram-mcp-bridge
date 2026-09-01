from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from telegram_mcp.errors import (
    AuthorizationError,
    IdempotencyConflictError,
    QueueFullError,
    RuntimeNotReadyError,
)
from telegram_mcp.storage import (
    MAX_LEASE_SECONDS,
    LeaseConflictError,
    OutboxStateError,
    SQLiteStorage,
)

BOT = "bot-test"


def update(update_id: int, text: str | None = None, *, chat_id: int = 100) -> dict[str, object]:
    return {
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "chat": {"id": chat_id, "type": "private"},
            "from": {"id": chat_id, "is_bot": False},
            "text": text or f"message-{update_id}",
        },
    }


async def open_storage(path: Path, **kwargs: object) -> SQLiteStorage:
    storage = SQLiteStorage(path, **kwargs)  # type: ignore[arg-type]
    await storage.open()
    return storage


@pytest.mark.asyncio
async def test_concurrent_identical_updates_are_inserted_once(tmp_path: Path) -> None:
    storage = await open_storage(tmp_path / "bridge.sqlite3", max_claim_events=100)
    try:
        results = await asyncio.gather(*(storage.enqueue_update(BOT, update(7)) for _ in range(50)))

        assert sum(result.disposition == "inserted" for result in results) == 1
        assert sum(result.disposition == "duplicate" for result in results) == 49
        assert len({result.event_id for result in results}) == 1

        claim = await storage.claim_events(BOT, limit=100, now=100, lease_seconds=30)
        assert [event.update_id for event in claim.events] == [7]
        assert claim.lease_token is not None

        metrics = await storage.metrics(bot_key=BOT, now=100)
        assert metrics.inbox_pending == 1
        assert metrics.counters["inbox_inserted"] == 1
        assert metrics.counters["inbox_duplicate"] == 49
    finally:
        await storage.close()


@pytest.mark.asyncio
async def test_two_connections_share_sqlite_dedup_and_claim_locking(tmp_path: Path) -> None:
    database = tmp_path / "bridge.sqlite3"
    first = await open_storage(database, max_claim_events=10)
    second = await open_storage(database, max_claim_events=10)
    try:
        outcomes = await asyncio.gather(
            *(store.enqueue_update(BOT, update(77)) for store in (first, second) for _ in range(10))
        )
        assert sum(outcome.disposition == "inserted" for outcome in outcomes) == 1
        assert sum(outcome.disposition == "duplicate" for outcome in outcomes) == 19

        left, right = await asyncio.gather(
            first.claim_events(BOT, limit=10, now=100, lease_seconds=30),
            second.claim_events(BOT, limit=10, now=100, lease_seconds=30),
        )
        assert sum(len(claim.events) for claim in (left, right)) == 1
    finally:
        await first.close()
        await second.close()


@pytest.mark.asyncio
async def test_cancelled_begin_cannot_leave_an_orphan_write_transaction(tmp_path: Path) -> None:
    database = tmp_path / "bridge.sqlite3"
    blocker = await open_storage(database, busy_timeout_ms=2_000)
    victim = await open_storage(database, busy_timeout_ms=2_000)
    try:
        # Hold SQLite's single writer slot so the victim's BEGIN is queued in
        # aiosqlite's worker thread, then cancel precisely in that window.
        await blocker._begin()
        enqueue = asyncio.create_task(victim.enqueue_update(BOT, update(88)))
        await asyncio.sleep(0.05)
        enqueue.cancel()
        await asyncio.sleep(0.05)
        assert not enqueue.done(), "cancellation must await the SQLite transaction boundary"

        await blocker._rollback()
        with pytest.raises(asyncio.CancelledError):
            await enqueue
        assert not victim._conn().in_transaction

        # The same connection remains usable; before the fix this raised
        # "cannot start a transaction within a transaction".
        outcome = await victim.enqueue_update(BOT, update(89))
        assert outcome.disposition == "inserted"
    finally:
        if blocker._conn().in_transaction:
            await blocker._rollback()
        await blocker.close()
        await victim.close()


@pytest.mark.asyncio
async def test_cancelled_close_still_joins_the_sqlite_worker(tmp_path: Path) -> None:
    storage = await open_storage(tmp_path / "bridge.sqlite3")
    await storage._lock.acquire()
    close = asyncio.create_task(storage.close())
    try:
        await asyncio.sleep(0)
        close.cancel()
        await asyncio.sleep(0)
        assert not close.done(), "cancelled shutdown must continue waiting for the storage lock"
    finally:
        storage._lock.release()

    with pytest.raises(asyncio.CancelledError):
        await close
    assert not storage.is_open
    await storage.close()  # Repeated close remains harmless after cancellation.


@pytest.mark.asyncio
async def test_same_update_id_with_different_hash_is_detected_without_replacement(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3") as storage:
        first = await storage.enqueue_update(BOT, update(12, "original"), received_at=10)
        conflict = await storage.enqueue_update(BOT, update(12, "tampered"), received_at=11)
        duplicate_with_different_key_order = await storage.enqueue_update(
            BOT,
            {
                "message": {
                    "text": "original",
                    "from": {"is_bot": False, "id": 100},
                    "chat": {"type": "private", "id": 100},
                    "message_id": 12,
                },
                "update_id": 12,
            },
            received_at=12,
        )

        assert first.disposition == "inserted"
        assert conflict.disposition == "conflict"
        assert conflict.event_id == first.event_id
        assert conflict.incoming_fingerprint != conflict.stored_fingerprint
        assert duplicate_with_different_key_order.disposition == "duplicate"

        persisted = await storage.get_event(BOT, 12)
        assert persisted is not None
        assert persisted.payload["message"]["text"] == "original"  # type: ignore[index]
        metrics = await storage.metrics(bot_key=BOT, now=20)
        assert metrics.counters["inbox_conflict"] == 1


@pytest.mark.asyncio
async def test_poll_batch_offset_and_queue_capacity_are_one_transaction(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3", max_queue_events=2) as storage:
        with pytest.raises(QueueFullError):
            await storage.ingest_poll_batch(BOT, [update(3), update(1), update(2)], next_offset=4)

        assert await storage.get_poll_offset(BOT) is None
        assert (await storage.metrics(bot_key=BOT, now=10)).inbox_pending == 0

        result = await storage.ingest_poll_batch(BOT, [update(3), update(1)], next_offset=4)
        assert result.inserted == 2
        assert result.next_offset == 4
        assert await storage.get_poll_offset(BOT) == 4

        claim = await storage.claim_events(BOT, limit=2, now=10, lease_seconds=30)
        # Local event_id is the durable arrival cursor. Telegram update_id may
        # be randomized after an idle week and therefore is not a safe order.
        assert [event.update_id for event in claim.events] == [3, 1]
        assert [event.event_id for event in claim.events] == sorted(event.event_id for event in claim.events)


@pytest.mark.asyncio
async def test_poll_offset_can_move_after_telegram_randomizes_update_ids(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3") as storage:
        await storage.ingest_poll_batch(BOT, [update(1_000)], next_offset=1_001)
        assert await storage.get_poll_offset(BOT) == 1_001

        # Telegram documents that after a week of inactivity the next update
        # ID may be randomized, so storage must not force a monotonic maximum.
        await storage.ingest_poll_batch(BOT, [], next_offset=23)
        assert await storage.get_poll_offset(BOT) == 23


@pytest.mark.asyncio
async def test_claims_are_atomic_and_expired_leases_are_redelivered(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3", max_claim_events=3) as storage:
        for update_id in (3, 1, 2):
            await storage.enqueue_update(BOT, update(update_id), received_at=1)

        first, second = await asyncio.gather(
            storage.claim_events(BOT, limit=2, now=100, lease_seconds=10),
            storage.claim_events(BOT, limit=2, now=100, lease_seconds=10),
        )
        claimed_ids = {event.update_id for event in (*first.events, *second.events)}
        assert claimed_ids == {1, 2, 3}
        assert not (
            {event.update_id for event in first.events} & {event.update_id for event in second.events}
        )

        two_event_claim = first if len(first.events) == 2 else second
        one_event_claim = second if len(first.events) == 2 else first
        assert two_event_claim.lease_token is not None
        assert one_event_claim.lease_token is not None

        with pytest.raises(LeaseConflictError):
            await storage.ack_events(
                BOT,
                [event.event_id for event in two_event_claim.events],
                "wrong-token-that-is-long-enough",
                now=101,
            )
        with pytest.raises(LeaseConflictError):
            await storage.ack_events(
                BOT,
                [event.event_id for event in two_event_claim.events],
                two_event_claim.lease_token,
                now=111,
            )

        redelivery = await storage.claim_events(BOT, limit=2, now=111, lease_seconds=10)
        assert {event.update_id for event in redelivery.events} == {
            event.update_id for event in two_event_claim.events
        }
        assert redelivery.lease_token not in {None, two_event_claim.lease_token}
        assert all(event.attempts == 2 for event in redelivery.events)

        assert redelivery.lease_token is not None
        released_id = redelivery.events[0].event_id
        acked_id = redelivery.events[1].event_id
        assert await storage.release_events(BOT, [released_id], redelivery.lease_token, now=112) == 1
        assert await storage.ack_events(BOT, [acked_id], redelivery.lease_token, now=112) == 1

        released_again = await storage.claim_events(BOT, limit=1, now=112, lease_seconds=10)
        assert [event.event_id for event in released_again.events] == [released_id]
        assert released_again.lease_token is not None
        await storage.ack_events(BOT, [released_id], released_again.lease_token, now=113)

        # Clean up the disjoint claim and prove all-or-nothing ACK semantics.
        with pytest.raises(LeaseConflictError):
            await storage.ack_events(
                BOT,
                [one_event_claim.events[0].event_id, 999_999],
                one_event_claim.lease_token,
                now=102,
            )
        await storage.ack_events(
            BOT,
            [one_event_claim.events[0].event_id],
            one_event_claim.lease_token,
            now=102,
        )


@pytest.mark.asyncio
async def test_purge_deletes_only_acked_rows(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3", max_claim_events=3) as storage:
        for update_id in (1, 2, 3):
            await storage.enqueue_update(BOT, update(update_id), received_at=1)
        claim = await storage.claim_events(BOT, limit=3, now=10, lease_seconds=100)
        assert claim.lease_token is not None

        by_update = {event.update_id: event for event in claim.events}
        await storage.ack_events(BOT, [by_update[1].event_id], claim.lease_token, now=11)
        await storage.release_events(BOT, [by_update[2].event_id], claim.lease_token, now=11)

        assert await storage.purge_acked(acked_before=12, bot_key=BOT) == 1
        assert await storage.get_event(BOT, 1) is None
        queued = await storage.get_event(BOT, 2)
        leased = await storage.get_event(BOT, 3)
        assert queued is not None and queued.state == "queued"
        assert leased is not None and leased.state == "leased"


@pytest.mark.asyncio
async def test_chat_authorization_is_deny_by_default_and_aliases_are_exact(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3") as storage:
        assert not await storage.is_chat_authorized(BOT, 100, permission="write")
        await storage.set_chat_authorization(BOT, 100, alias="owner", can_read=True, can_write=False)
        assert await storage.is_chat_authorized(BOT, "owner", permission="read")
        assert not await storage.is_chat_authorized(BOT, "owner", permission="write")

        with pytest.raises(AuthorizationError):
            await storage.reserve_outbox(
                BOT,
                "unauthorized",
                chat_id=100,
                method="sendMessage",
                payload={"chat_id": 100, "text": "no"},
            )

        await storage.set_chat_authorization(BOT, 100, alias="owner", can_read=True, can_write=True)
        assert await storage.is_chat_authorized(BOT, -100, permission="read") is False

        await storage.set_chat_authorization(BOT, -100, alias="group", can_read=True, can_write=True)
        listed = await storage.list_authorized_chats(BOT, limit=20)
        assert [(item.chat_id, item.alias) for item in listed] == [(-100, "group"), (100, "owner")]


@pytest.mark.asyncio
async def test_event_id_lookup_and_peek_are_non_mutating_local_cursor_reads(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3", max_claim_events=3) as storage:
        for update_id in (9_000, 4, 5):
            await storage.enqueue_update(BOT, update(update_id), received_at=1)

        page = await storage.peek_events(BOT, limit=2)
        assert [event.update_id for event in page] == [9_000, 4]
        assert all(event.state == "queued" for event in page)
        second_page = await storage.peek_events(BOT, after_event_id=page[-1].event_id, limit=20)
        assert [event.update_id for event in second_page] == [5]

        by_local_id = await storage.get_event_by_event_id(BOT, page[0].event_id)
        assert by_local_id is not None and by_local_id.update_id == 9_000
        assert await storage.get_event_by_event_id("another-bot", page[0].event_id) is None

        claim = await storage.claim_events(BOT, limit=1, now=10, lease_seconds=20)
        assert claim.lease_token is not None
        await storage.ack_events(BOT, [claim.events[0].event_id], claim.lease_token, now=11)
        assert [event.update_id for event in await storage.peek_events(BOT)] == [4, 5]
        assert [event.update_id for event in await storage.peek_events(BOT, include_acked=True)] == [
            9_000,
            4,
            5,
        ]


@pytest.mark.asyncio
async def test_current_chat_bearing_update_variants_are_indexed(tmp_path: Path) -> None:
    variants: list[tuple[str, dict[str, object], int]] = [
        ("edited_business_message", {"chat": {"id": -101}}, -101),
        ("guest_message", {"chat": {"id": -102}}, -102),
        ("deleted_business_messages", {"chat": {"id": -103}}, -103),
        ("message_reaction_count", {"chat": {"id": -104}}, -104),
        ("chat_boost", {"chat": {"id": -105}}, -105),
        ("removed_chat_boost", {"chat": {"id": -106}}, -106),
        ("business_connection", {"user_chat_id": 107}, 107),
        ("poll_answer", {"voter_chat": {"id": -108}}, -108),
    ]
    async with SQLiteStorage(tmp_path / "bridge.sqlite3") as storage:
        for update_id, (kind, body, expected_chat_id) in enumerate(variants, start=1):
            outcome = await storage.enqueue_update(BOT, {"update_id": update_id, kind: body})
            event = await storage.get_event_by_event_id(BOT, outcome.event_id)
            assert event is not None and event.chat_id == expected_chat_id


@pytest.mark.asyncio
async def test_outbox_reservation_is_concurrent_and_fingerprint_safe(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3") as storage:
        await storage.set_chat_authorization(BOT, 100, can_write=True)
        reservations = await asyncio.gather(
            *(
                storage.reserve_outbox(
                    BOT,
                    "reply:7:0",
                    chat_id=100,
                    method="sendMessage",
                    payload={"chat_id": 100, "text": "hello"},
                    now=10,
                )
                for _ in range(30)
            )
        )

        assert sum(reservation.created for reservation in reservations) == 1
        assert len({reservation.record.outbox_id for reservation in reservations}) == 1
        assert all(
            reservation.record.fingerprint == reservations[0].record.fingerprint
            for reservation in reservations
        )
        claims = await asyncio.gather(
            *(storage.claim_outbox_id(reservations[0].record.outbox_id, now=10) for _ in range(20))
        )
        assert sum(len(claim.records) for claim in claims) == 1

        with pytest.raises(IdempotencyConflictError):
            await storage.reserve_outbox(
                BOT,
                "reply:7:0",
                chat_id=100,
                method="sendMessage",
                payload={"chat_id": 100, "text": "different"},
                now=11,
            )

        persisted = await storage.get_outbox(BOT, idempotency_key="reply:7:0")
        assert persisted is not None
        assert persisted.payload["text"] == "hello"
        metrics = await storage.metrics(bot_key=BOT, now=12)
        assert metrics.counters["outbox_reserved"] == 1
        assert metrics.counters["outbox_reused"] == 29
        assert metrics.counters["outbox_conflict"] == 1


@pytest.mark.asyncio
async def test_specific_outbox_claim_progress_retry_and_sent(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3") as storage:
        with pytest.raises(OutboxStateError):
            await storage.claim_outbox_id(999, now=100)
        await storage.set_chat_authorization(BOT, 100, can_write=True)
        reservation = await storage.reserve_outbox(
            BOT,
            "reply:1:0",
            chat_id=100,
            method="sendMessage",
            payload={"chat_id": 100, "text": "hello"},
            now=100,
        )
        claim = await storage.claim_outbox_id(
            reservation.record.outbox_id,
            now=100,
            lease_seconds=10,
        )
        assert claim.lease_token is not None
        assert len(claim.records) == 1
        assert claim.records[0].status == "sending"
        assert claim.records[0].attempts == 1

        retry = await storage.mark_outbox_retry(
            reservation.record.outbox_id,
            claim.lease_token,
            retry_at=120,
            error="HTTP 429",
            now=101,
        )
        assert retry.status == "retry"
        assert (await storage.claim_outbox(bot_key=BOT, now=119)).records == ()

        due = await storage.claim_outbox(bot_key=BOT, now=120, lease_seconds=10)
        assert due.lease_token is not None
        assert [record.outbox_id for record in due.records] == [reservation.record.outbox_id]
        sent = await storage.mark_outbox_sent(
            reservation.record.outbox_id,
            due.lease_token,
            telegram_message_id=555,
            now=121,
        )
        assert sent.status == "sent"
        assert sent.telegram_message_id == 555
        assert sent.sent_at == 121

        reused = await storage.reserve_outbox(
            BOT,
            "reply:1:0",
            chat_id=100,
            method="sendMessage",
            payload={"chat_id": 100, "text": "hello"},
            now=122,
        )
        assert not reused.created
        assert reused.record.status == "sent"
        assert reused.record.telegram_message_id == 555


@pytest.mark.asyncio
async def test_expired_outbox_token_wins_before_recovery_and_is_stale_after_reclaim(
    tmp_path: Path,
) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3") as storage:
        await storage.set_chat_authorization(BOT, 100, can_write=True)
        completed = await storage.reserve_outbox(
            BOT,
            "reply:2:0",
            chat_id=100,
            method="sendMessage",
            payload={"chat_id": 100, "text": "confirmed before recovery"},
            now=100,
        )
        completed_claim = await storage.claim_outbox_id(
            completed.record.outbox_id,
            now=100,
            lease_seconds=5,
        )
        assert completed_claim.lease_token is not None
        sent = await storage.mark_outbox_sent(
            completed.record.outbox_id,
            completed_claim.lease_token,
            telegram_message_id=1,
            now=106,
        )
        assert sent.status == "sent"
        assert await storage.recover_outbox(now=106) == 0

        ambiguous = await storage.reserve_outbox(
            BOT,
            "reply:2:1",
            chat_id=100,
            method="sendMessage",
            payload={"chat_id": 100, "text": "recovery wins"},
            now=100,
        )
        old_claim = await storage.claim_outbox_id(
            ambiguous.record.outbox_id,
            now=100,
            lease_seconds=5,
        )
        assert old_claim.lease_token is not None
        assert await storage.recover_outbox(now=106) == 1
        uncertain = await storage.get_outbox(BOT, outbox_id=ambiguous.record.outbox_id)
        assert uncertain is not None and uncertain.status == "uncertain"

        with pytest.raises(LeaseConflictError):
            await storage.mark_outbox_sent(
                ambiguous.record.outbox_id,
                old_claim.lease_token,
                telegram_message_id=2,
                now=106,
            )
        await storage.requeue_outbox(ambiguous.record.outbox_id, now=107)
        retry_claim = await storage.claim_outbox_id(ambiguous.record.outbox_id, now=107)
        assert retry_claim.records[0].attempts == 2
        assert retry_claim.lease_token is not None
        with pytest.raises(LeaseConflictError):
            await storage.mark_outbox_dead(
                ambiguous.record.outbox_id,
                old_claim.lease_token,
                error="stale token after reclaim",
                now=108,
            )
        dead = await storage.mark_outbox_dead(
            ambiguous.record.outbox_id,
            retry_claim.lease_token,
            error="current owner",
            now=108,
        )
        assert dead.status == "dead"


@pytest.mark.asyncio
async def test_outbox_queue_bound_does_not_block_idempotent_replay(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3", max_outbox_events=1) as storage:
        await storage.set_chat_authorization(BOT, 100, can_write=True)
        first = await storage.reserve_outbox(
            BOT,
            "one",
            chat_id=100,
            method="sendMessage",
            payload={"chat_id": 100, "text": "one"},
            now=10,
        )
        replay = await storage.reserve_outbox(
            BOT,
            "one",
            chat_id=100,
            method="sendMessage",
            payload={"chat_id": 100, "text": "one"},
            now=10,
        )
        assert first.created and not replay.created

        with pytest.raises(QueueFullError):
            await storage.reserve_outbox(
                BOT,
                "two",
                chat_id=100,
                method="sendMessage",
                payload={"chat_id": 100, "text": "two"},
                now=10,
            )

        claim = await storage.claim_outbox_id(first.record.outbox_id, now=10)
        assert claim.lease_token is not None
        await storage.mark_outbox_dead(first.record.outbox_id, claim.lease_token, error="permanent", now=11)
        second = await storage.reserve_outbox(
            BOT,
            "two",
            chat_id=100,
            method="sendMessage",
            payload={"chat_id": 100, "text": "two"},
            now=12,
        )
        assert second.created
        with pytest.raises(QueueFullError):
            await storage.requeue_outbox(first.record.outbox_id, now=13)


@pytest.mark.asyncio
async def test_lifecycle_configuration_and_payload_validation(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        SQLiteStorage(tmp_path / "bad.sqlite3", max_queue_events=0)
    with pytest.raises(ValueError):
        SQLiteStorage(tmp_path / "bad.sqlite3", max_claim_events=501)
    with pytest.raises(ValueError):
        SQLiteStorage(tmp_path / "bad.sqlite3", max_outbox_events=0)
    with pytest.raises(ValueError):
        SQLiteStorage(tmp_path / "bad.sqlite3", max_payload_bytes=0)
    with pytest.raises(ValueError):
        SQLiteStorage(tmp_path / "bad.sqlite3", synchronous="OFF")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        SQLiteStorage(tmp_path / "bad.sqlite3", busy_timeout_ms=120_001)

    storage = SQLiteStorage(tmp_path / "bridge.sqlite3", max_payload_bytes=500)
    assert not storage.is_open
    with pytest.raises(RuntimeNotReadyError):
        await storage.get_poll_offset(BOT)

    await storage.open()
    await storage.open()  # Idempotent startup is useful to layered runtimes.
    assert storage.is_open
    try:
        with pytest.raises(ValueError):
            await storage.enqueue_update("unsafe bot key", update(1))
        with pytest.raises(ValueError):
            await storage.enqueue_update(BOT, '{"update_id":1,"update_id":2}')
        with pytest.raises(ValueError):
            await storage.enqueue_update(BOT, '{"update_id":NaN}')
        with pytest.raises(ValueError):
            await storage.enqueue_update(BOT, b"\xff")
        with pytest.raises(ValueError):
            await storage.enqueue_update(BOT, "[]")
        with pytest.raises(TypeError):
            await storage.enqueue_update(BOT, object())  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            await storage.enqueue_update(BOT, {"update_id": 1, "unsupported": {1, 2}})
        with pytest.raises(ValueError):
            await storage.enqueue_update(BOT, {"update_id": True}, update_id=1)
        with pytest.raises(ValueError):
            await storage.enqueue_update(BOT, update(1), update_id=2)
        with pytest.raises(ValueError):
            await storage.enqueue_update(BOT, update(1), chat_id=999)
        with pytest.raises(ValueError):
            await storage.enqueue_update(BOT, update(1), received_at=float("inf"))
        with pytest.raises(ValueError):
            await storage.enqueue_update(BOT, update(1, "x" * 1_000))

        explicit = await storage.enqueue_update(
            BOT,
            {"message": {"text": "explicit"}},
            update_id=9,
            chat_id=-55,
            received_at=10,
        )
        persisted = await storage.get_event(BOT, explicit.update_id)
        assert persisted is not None and persisted.chat_id == -55
        assert persisted.as_row()["payload_hash"] == persisted.fingerprint
    finally:
        await storage.close()
        await storage.close()
    assert not storage.is_open


@pytest.mark.asyncio
async def test_batch_internal_dedup_alias_claims_and_input_bounds(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3", max_claim_events=5) as storage:
        result = await storage.ingest_poll_batch(
            BOT,
            [update(4, "same"), update(4, "same"), update(4, "different")],
            advance_offset=False,
        )
        assert result.inserted == 1
        assert result.duplicates == 1
        assert result.conflicts == 1
        assert result.next_offset is None

        with pytest.raises(ValueError):
            await storage.ingest_poll_batch(BOT, [update(5)], update_ids=())
        with pytest.raises(ValueError):
            await storage.ingest_poll_batch(BOT, [update(5)], chat_ids=())
        with pytest.raises(ValueError):
            await storage.ingest_poll_batch(BOT, [update(5)] * 501)
        with pytest.raises(ValueError):
            await storage.ingest_poll_batch(BOT, [{"update_id": (1 << 63) - 1}])

        claim = await storage.claim_updates(BOT, now=10, lease_seconds=10)
        assert claim.lease_token is not None
        assert await storage.release_updates(BOT, [4, 4], claim.lease_token, now=11) == 1
        second = await storage.claim_updates(BOT, now=11, lease_seconds=10)
        assert second.lease_token is not None
        assert await storage.ack_updates(BOT, [4], second.lease_token, now=12) == 1
        assert await storage.ack_events(BOT, [], "", now=12) == 0
        assert (await storage.claim_events(BOT, now=12)).events == ()


@pytest.mark.asyncio
async def test_inbox_recovery_global_metrics_and_global_purge(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3", max_claim_events=5) as storage:
        await storage.enqueue_update("bot-a", update(1), received_at=20)
        await storage.enqueue_update("bot-b", update(2), received_at=30)
        first = await storage.claim_events("bot-a", now=40, lease_seconds=5)
        second = await storage.claim_events("bot-b", now=40, lease_seconds=5)
        assert first.lease_token is not None and second.lease_token is not None

        assert await storage.recover_inbox_leases(now=44) == 0
        assert await storage.recover_inbox_leases(now=45) == 2
        recovered = await storage.recover(now=45)
        assert recovered.inbox_requeued == 0
        assert recovered.outbox_recovered == 0
        assert recovered.outbox_recovery_status == "uncertain"

        first_again = await storage.claim_events("bot-a", now=45, lease_seconds=5)
        second_again = await storage.claim_events("bot-b", now=45, lease_seconds=5)
        assert first_again.lease_token is not None and second_again.lease_token is not None
        await storage.ack_updates("bot-a", [1], first_again.lease_token, now=46)
        await storage.ack_updates("bot-b", [2], second_again.lease_token, now=46)

        metrics = await storage.metrics(now=50)
        assert metrics.inbox_states == {"acked": 2}
        assert metrics.inbox_pending == 0
        assert metrics.oldest_inbox_age_seconds is None
        assert metrics.counters["inbox_inserted"] == 2
        assert await storage.purge_acked(acked_before=47, limit=1) == 1
        assert await storage.purge_acked(acked_before=47) == 1
        assert await storage.purge_acked(acked_before=47) == 0


@pytest.mark.asyncio
async def test_allowlist_replace_remove_and_validation(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3") as storage:
        assert await storage.replace_chat_authorizations(BOT, [3, 1, 3], now=10) == 2
        assert [item.chat_id for item in await storage.list_authorized_chats(BOT, limit=1)] == [1]
        numeric = await storage.get_chat_authorization(BOT, 3)
        assert numeric is not None and numeric.can_read and numeric.can_write
        assert await storage.remove_chat_authorization(BOT, 3)
        assert not await storage.remove_chat_authorization(BOT, 3)
        assert await storage.replace_chat_authorizations(BOT, [], now=11) == 0
        assert await storage.list_authorized_chats(BOT) == ()

        with pytest.raises(ValueError):
            await storage.set_chat_authorization(BOT, 1, alias="not safe")
        with pytest.raises(ValueError):
            await storage.set_chat_authorization(BOT, 1, can_read=1)  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            await storage.replace_chat_authorizations(BOT, [1], can_write=1)  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            await storage.get_chat_authorization(BOT, "not safe")
        with pytest.raises(ValueError):
            await storage.is_chat_authorized(BOT, 1, permission="admin")  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            await storage.list_authorized_chats(BOT, limit=0)


@pytest.mark.asyncio
async def test_outbox_all_transitions_recovery_metrics_and_validation(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3", max_claim_events=5) as storage:
        with pytest.raises(ValueError):
            await storage.reserve_outbox(
                BOT, "", chat_id=100, method="sendMessage", payload={}, require_authorized=False
            )
        with pytest.raises(ValueError):
            await storage.reserve_outbox(
                BOT, "bad\nkey", chat_id=100, method="sendMessage", payload={}, require_authorized=False
            )
        with pytest.raises(ValueError):
            await storage.reserve_outbox(
                BOT, "key", chat_id=100, method="../send", payload={}, require_authorized=False
            )
        with pytest.raises(ValueError):
            await storage.reserve_outbox(
                BOT,
                "key",
                chat_id=100,
                method="sendMessage",
                payload={"chat_id": 101},
                require_authorized=False,
            )

        await storage.set_chat_authorization("bot-a", 100, can_write=True)
        await storage.set_chat_authorization("bot-b", 200, can_write=True)
        one = await storage.reserve_outbox(
            "bot-a",
            "one",
            chat_id=100,
            method="sendMessage",
            payload={"text": "one"},
            now=10,
        )
        two = await storage.reserve_outbox(
            "bot-b",
            "two",
            chat_id=200,
            method="sendMessage",
            payload={"chat_id": 200, "text": "two"},
            now=20,
        )
        with pytest.raises(ValueError):
            await storage.get_outbox("bot-a")
        with pytest.raises(ValueError):
            await storage.get_outbox("bot-a", outbox_id=one.record.outbox_id, idempotency_key="one")
        assert (await storage.get_outbox("bot-a", outbox_id=one.record.outbox_id)) is not None

        global_claim = await storage.claim_outbox(limit=2, now=20, lease_seconds=5)
        assert global_claim.lease_token is not None
        assert [record.outbox_id for record in global_claim.records] == [
            one.record.outbox_id,
            two.record.outbox_id,
        ]
        uncertain = await storage.mark_outbox_uncertain(
            one.record.outbox_id,
            global_claim.lease_token,
            error="unknown\x00delivery",
            now=21,
        )
        dead = await storage.mark_outbox_dead(
            two.record.outbox_id,
            global_claim.lease_token,
            error="permanent",
            now=21,
        )
        assert uncertain.status == "uncertain" and uncertain.last_error == "unknowndelivery"
        assert dead.status == "dead"
        assert (await storage.claim_outbox(now=21)).records == ()

        await storage.requeue_outbox(one.record.outbox_id, now=22)
        await storage.requeue_outbox(two.record.outbox_id, now=22)
        retry_claim = await storage.claim_outbox(limit=2, now=22, lease_seconds=2)
        assert len(retry_claim.records) == 2
        assert await storage.recover_outbox(now=24, retry_expired=True) == 2
        retried = await storage.claim_outbox(limit=2, now=24, lease_seconds=10)
        assert retried.lease_token is not None and len(retried.records) == 2
        await storage.mark_outbox_dead(
            retried.records[0].outbox_id,
            retried.lease_token,
            error="done",
            now=25,
        )
        await storage.mark_outbox_dead(
            retried.records[1].outbox_id,
            retried.lease_token,
            error="done",
            now=25,
        )

        metrics = await storage.metrics(now=30)
        assert metrics.outbox_states == {"dead": 2}
        assert metrics.outbox_pending == 0
        assert metrics.oldest_outbox_age_seconds is None
        assert metrics.counters["outbox_dead"] == 3
        with pytest.raises(OutboxStateError):
            await storage.requeue_outbox(999, now=30)


@pytest.mark.asyncio
async def test_startup_recovery_reclaims_all_inherited_leases_without_waiting_for_expiry(
    tmp_path: Path,
) -> None:
    database = tmp_path / "bridge.sqlite3"
    first_process = await open_storage(database)
    try:
        await first_process.set_chat_authorization(BOT, 100, can_write=True)
        await first_process.enqueue_update(BOT, update(500), received_at=10)
        inbox = await first_process.claim_events(
            BOT,
            now=10,
            lease_seconds=MAX_LEASE_SECONDS,
        )
        reservation = await first_process.reserve_outbox(
            BOT,
            "inherited",
            chat_id=100,
            method="sendMessage",
            payload={"chat_id": 100, "text": "ambiguous after crash"},
            now=10,
        )
        outbox = await first_process.claim_outbox_id(
            reservation.record.outbox_id,
            now=10,
            lease_seconds=MAX_LEASE_SECONDS,
        )
        assert inbox.lease_until == 10 + MAX_LEASE_SECONDS
        assert outbox.lease_until == 10 + MAX_LEASE_SECONDS
    finally:
        await first_process.close()

    # Simulate a new process long before either inherited lease expires.
    async with SQLiteStorage(database) as restarted:
        assert await restarted.recover_inbox_leases(now=11) == 0
        assert await restarted.recover_outbox(now=11) == 0
        with pytest.raises(ValueError):
            await restarted.claim_events(BOT, now=11, lease_seconds=MAX_LEASE_SECONDS + 1)
        with pytest.raises(ValueError):
            await restarted.claim_outbox(now=11, lease_seconds=MAX_LEASE_SECONDS + 1)

        recovery = await restarted.recover(now=11)
        assert recovery.inbox_requeued == 1
        assert recovery.outbox_recovered == 1
        assert recovery.outbox_recovery_status == "uncertain"
        with pytest.raises(ValueError):
            await restarted.recover(now=11, retry_expired_outbox=True)

        redelivered = await restarted.claim_events(BOT, now=11, lease_seconds=10)
        assert [event.update_id for event in redelivered.events] == [500]
        inherited = await restarted.get_outbox(BOT, outbox_id=reservation.record.outbox_id)
        assert inherited is not None
        assert inherited.status == "uncertain"
        assert inherited.lease_token is None and inherited.lease_until is None


@pytest.mark.asyncio
async def test_outbox_claim_rechecks_revoked_authorization(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3", max_outbox_events=1) as storage:
        await storage.set_chat_authorization(BOT, 100, can_write=True)
        await storage.set_chat_authorization(BOT, 200, can_write=True)
        with pytest.raises(AuthorizationError):
            await storage.reserve_outbox(
                BOT,
                "no-bypass",
                chat_id=100,
                method="sendMessage",
                payload={"chat_id": 100, "text": "no"},
                require_authorized=False,
                now=9,
            )
        reservation = await storage.reserve_outbox(
            BOT,
            "revocation",
            chat_id=100,
            method="sendMessage",
            payload={"chat_id": 100, "text": "must not escape after revocation"},
            now=10,
        )
        assert await storage.remove_chat_authorization(BOT, 100)
        assert (await storage.claim_outbox(bot_key=BOT, now=10)).records == ()
        assert (await storage.claim_outbox_id(reservation.record.outbox_id, now=10)).records == ()
        quarantined = await storage.get_outbox(BOT, outbox_id=reservation.record.outbox_id)
        assert quarantined is not None and quarantined.status == "dead"

        # A revoked row is terminal and cannot permanently consume the bound.
        replacement = await storage.reserve_outbox(
            BOT,
            "replacement",
            chat_id=200,
            method="sendMessage",
            payload={"chat_id": 200, "text": "allowed"},
            now=10,
        )
        assert replacement.created

        await storage.set_chat_authorization(BOT, 100, can_write=True)
        claim_replacement = await storage.claim_outbox_id(replacement.record.outbox_id, now=10)
        assert claim_replacement.lease_token is not None
        await storage.mark_outbox_dead(
            replacement.record.outbox_id,
            claim_replacement.lease_token,
            error="free capacity",
            now=10,
        )
        await storage.requeue_outbox(reservation.record.outbox_id, now=10)
        claim = await storage.claim_outbox_id(reservation.record.outbox_id, now=10)
        assert claim.lease_token is not None
        await storage.remove_chat_authorization(BOT, 100)
        uncertain = await storage.get_outbox(BOT, outbox_id=reservation.record.outbox_id)
        assert uncertain is not None and uncertain.status == "uncertain"
        with pytest.raises(LeaseConflictError):
            await storage.mark_outbox_dead(
                reservation.record.outbox_id,
                claim.lease_token,
                error="stale worker",
                now=11,
            )

        await storage.set_chat_authorization(BOT, 100, can_write=True)
        await storage.requeue_outbox(reservation.record.outbox_id, now=12)
        assert await storage.remove_chat_authorization(BOT, 100)
        final = await storage.get_outbox(BOT, outbox_id=reservation.record.outbox_id)
        assert final is not None and final.status == "dead"


@pytest.mark.asyncio
async def test_authoritative_allowlist_replacement_quarantines_removed_targets(tmp_path: Path) -> None:
    async with SQLiteStorage(tmp_path / "bridge.sqlite3") as storage:
        await storage.replace_chat_authorizations(BOT, [100, 200], now=10)
        removed_target = await storage.reserve_outbox(
            BOT,
            "removed-target",
            chat_id=100,
            method="sendMessage",
            payload={"chat_id": 100, "text": "stop"},
            now=10,
        )
        retained_target = await storage.reserve_outbox(
            BOT,
            "retained-target",
            chat_id=200,
            method="sendMessage",
            payload={"chat_id": 200, "text": "continue"},
            now=10,
        )

        await storage.replace_chat_authorizations(BOT, [200], now=11)
        removed = await storage.get_outbox(BOT, outbox_id=removed_target.record.outbox_id)
        retained = await storage.get_outbox(BOT, outbox_id=retained_target.record.outbox_id)
        assert removed is not None and removed.status == "dead"
        assert retained is not None and retained.status == "pending"
        claim = await storage.claim_outbox(bot_key=BOT, now=11)
        assert [record.outbox_id for record in claim.records] == [retained_target.record.outbox_id]

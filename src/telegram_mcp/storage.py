"""Durable SQLite inbox/outbox for the Telegram MCP bridge.

The database, rather than an in-memory queue, is the source of truth.  All
state-changing operations are serialized through one connection and use
short ``BEGIN IMMEDIATE`` transactions.  This deliberately matches SQLite's
single-writer model while WAL keeps reads cheap.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import secrets
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, TypeVar

import aiosqlite

from telegram_mcp.errors import (
    AuthorizationError,
    BridgeError,
    IdempotencyConflictError,
    QueueFullError,
    RuntimeNotReadyError,
)

InboxState = Literal["queued", "leased", "acked"]
UpdateDisposition = Literal["inserted", "duplicate", "conflict"]
OutboxStatus = Literal["pending", "sending", "retry", "uncertain", "sent", "dead"]

_BOT_KEY_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_ALIAS_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_METHOD_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
_MAX_SQLITE_INTEGER = (1 << 63) - 1
_SCHEMA_VERSION = 1
_TERMINAL_OUTBOX = frozenset({"sent", "dead"})
MAX_LEASE_SECONDS = 86_400.0


class StorageError(BridgeError):
    """Base class for storage invariants that callers may safely inspect."""


class UpdateFingerprintConflictError(StorageError):
    """A Telegram update ID was observed with different canonical content."""


class LeaseConflictError(StorageError):
    """A lease is missing, expired, or belongs to another consumer."""


class OutboxStateError(StorageError):
    """An outbox transition is not valid for the record's current state."""


@dataclass(frozen=True, slots=True)
class EnqueueOutcome:
    event_id: int
    update_id: int
    disposition: UpdateDisposition
    incoming_fingerprint: str
    stored_fingerprint: str


@dataclass(frozen=True, slots=True)
class BatchIngestResult:
    outcomes: tuple[EnqueueOutcome, ...]
    next_offset: int | None

    @property
    def inserted(self) -> int:
        return sum(item.disposition == "inserted" for item in self.outcomes)

    @property
    def duplicates(self) -> int:
        return sum(item.disposition == "duplicate" for item in self.outcomes)

    @property
    def conflicts(self) -> int:
        return sum(item.disposition == "conflict" for item in self.outcomes)


@dataclass(frozen=True, slots=True)
class InboxEvent:
    event_id: int
    bot_key: str
    update_id: int
    payload_json: str
    fingerprint: str
    chat_id: int | None
    state: InboxState
    received_at: float
    attempts: int
    lease_token: str | None
    lease_until: float | None
    acked_at: float | None

    @property
    def payload(self) -> dict[str, Any]:
        value = json.loads(self.payload_json)
        if not isinstance(value, dict):  # The write path enforces this invariant.
            raise StorageError("Stored inbox payload is not a JSON object")
        return value

    def as_row(self) -> dict[str, Any]:
        """Return the mapping shape consumed by :func:`event_summary`."""

        return {
            "event_id": self.event_id,
            "bot_key": self.bot_key,
            "update_id": self.update_id,
            "payload_json": self.payload_json,
            "payload_hash": self.fingerprint,
            "chat_id": self.chat_id,
            "state": self.state,
            "received_at": self.received_at,
            "attempts": self.attempts,
            "lease_token": self.lease_token,
            "lease_until": self.lease_until,
            "acked_at": self.acked_at,
        }


@dataclass(frozen=True, slots=True)
class InboxClaim:
    lease_token: str | None
    lease_until: float | None
    events: tuple[InboxEvent, ...]


@dataclass(frozen=True, slots=True)
class ChatAuthorization:
    bot_key: str
    chat_id: int
    alias: str | None
    can_read: bool
    can_write: bool
    updated_at: float


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    outbox_id: int
    bot_key: str
    idempotency_key: str
    fingerprint: str
    chat_id: int
    method: str
    payload_json: str
    status: OutboxStatus
    attempts: int
    next_attempt_at: float
    lease_token: str | None
    lease_until: float | None
    telegram_message_id: int | None
    last_error: str | None
    created_at: float
    updated_at: float
    sent_at: float | None

    @property
    def payload(self) -> dict[str, Any]:
        value = json.loads(self.payload_json)
        if not isinstance(value, dict):
            raise StorageError("Stored outbox payload is not a JSON object")
        return value


@dataclass(frozen=True, slots=True)
class OutboxReservation:
    created: bool
    record: OutboxRecord


@dataclass(frozen=True, slots=True)
class OutboxClaim:
    lease_token: str | None
    lease_until: float | None
    records: tuple[OutboxRecord, ...]


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    inbox_requeued: int
    outbox_recovered: int
    outbox_recovery_status: Literal["retry", "uncertain"]


@dataclass(frozen=True, slots=True)
class StorageMetrics:
    inbox_states: dict[str, int]
    outbox_states: dict[str, int]
    counters: dict[str, int]
    inbox_pending: int
    outbox_pending: int
    oldest_inbox_age_seconds: float | None
    oldest_outbox_age_seconds: float | None


@dataclass(frozen=True, slots=True)
class _PreparedUpdate:
    update_id: int
    payload_json: str
    fingerprint: str
    chat_id: int | None
    received_at: float


T = TypeVar("T")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON object key: {key!r}")
        result[key] = value
    return result


def _reject_non_finite(value: str) -> None:
    raise ValueError(f"Non-finite JSON number is not allowed: {value}")


def _json_object(value: Mapping[str, Any] | str | bytes) -> tuple[dict[str, Any], str, str]:
    """Validate and canonically encode a JSON object, returning its SHA-256."""

    if isinstance(value, bytes):
        try:
            decoded: object = json.loads(
                value.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_non_finite,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as exc:
            raise ValueError("Payload must be valid UTF-8 JSON without duplicate keys") from exc
    elif isinstance(value, str):
        try:
            decoded = json.loads(
                value,
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_non_finite,
            )
        except (json.JSONDecodeError, ValueError, RecursionError) as exc:
            raise ValueError("Payload must be valid JSON without duplicate keys") from exc
    elif isinstance(value, Mapping):
        decoded = dict(value)
    else:
        raise TypeError("Payload must be a JSON object, string, or UTF-8 bytes")
    if not isinstance(decoded, dict):
        raise ValueError("Payload must be a JSON object")
    try:
        canonical = json.dumps(
            decoded, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
        )
        encoded = canonical.encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError, RecursionError) as exc:
        raise ValueError("Payload contains a value that cannot be represented as canonical JSON") from exc
    return decoded, canonical, hashlib.sha256(encoded).hexdigest()


def _reject_oversized_raw_payload(value: object, limit: int, label: str) -> None:
    """Bound raw JSON work before parsing/canonical sorting when possible."""

    if isinstance(value, bytes):
        length = len(value)
    elif isinstance(value, str):
        try:
            length = len(value.encode("utf-8"))
        except UnicodeEncodeError as exc:
            raise ValueError(f"{label} must contain valid Unicode") from exc
    else:
        return
    if length > limit:
        raise ValueError(f"{label} exceeds the configured storage byte limit")


def _validate_bot_key(bot_key: str) -> str:
    if not isinstance(bot_key, str) or not _BOT_KEY_RE.fullmatch(bot_key):
        raise ValueError("bot_key must contain 1-128 safe identifier characters")
    return bot_key


def _validate_integer(value: object, name: str, *, non_negative: bool = False) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    minimum = 0 if non_negative else -_MAX_SQLITE_INTEGER - 1
    if not minimum <= value <= _MAX_SQLITE_INTEGER:
        raise ValueError(f"{name} is outside SQLite's signed 64-bit range")
    return value


def _validate_time(value: float | int, name: str) -> float:
    result = float(value)
    if result < 0 or not math.isfinite(result):
        raise ValueError(f"{name} must be a finite non-negative timestamp")
    return result


def _extract_chat_id(update: Mapping[str, Any]) -> int | None:
    message_keys = (
        "message",
        "edited_message",
        "channel_post",
        "edited_channel_post",
        "business_message",
        "edited_business_message",
        "guest_message",
    )
    message: Mapping[str, Any] | None = None
    for key in message_keys:
        candidate = update.get(key)
        if isinstance(candidate, Mapping):
            message = candidate
            break
    callback = update.get("callback_query")
    if message is None and isinstance(callback, Mapping) and isinstance(callback.get("message"), Mapping):
        message = callback["message"]
    if message is not None and isinstance(message.get("chat"), Mapping):
        chat_id = message["chat"].get("id")
        if isinstance(chat_id, int) and not isinstance(chat_id, bool):
            return _validate_integer(chat_id, "chat_id")
    for key in (
        "my_chat_member",
        "chat_member",
        "chat_join_request",
        "message_reaction",
        "message_reaction_count",
        "deleted_business_messages",
        "chat_boost",
        "removed_chat_boost",
    ):
        candidate = update.get(key)
        if isinstance(candidate, Mapping) and isinstance(candidate.get("chat"), Mapping):
            chat_id = candidate["chat"].get("id")
            if isinstance(chat_id, int) and not isinstance(chat_id, bool):
                return _validate_integer(chat_id, "chat_id")
    business_connection = update.get("business_connection")
    if isinstance(business_connection, Mapping):
        user_chat_id = business_connection.get("user_chat_id")
        if isinstance(user_chat_id, int) and not isinstance(user_chat_id, bool):
            return _validate_integer(user_chat_id, "chat_id")
    poll_answer = update.get("poll_answer")
    if isinstance(poll_answer, Mapping) and isinstance(poll_answer.get("voter_chat"), Mapping):
        voter_chat_id = poll_answer["voter_chat"].get("id")
        if isinstance(voter_chat_id, int) and not isinstance(voter_chat_id, bool):
            return _validate_integer(voter_chat_id, "chat_id")
    return None


def _placeholders(count: int) -> str:
    if count <= 0:
        raise ValueError("A SQL IN-list cannot be empty")
    return ",".join("?" for _ in range(count))


def _unique_ints(values: Iterable[int], name: str) -> tuple[int, ...]:
    result: list[int] = []
    seen: set[int] = set()
    for value in values:
        validated = _validate_integer(value, name, non_negative=True)
        if validated not in seen:
            seen.add(validated)
            result.append(validated)
    return tuple(result)


class SQLiteStorage:
    """Single-node, durable inbox/outbox backed by SQLite WAL.

    The object owns one aiosqlite connection.  Call :meth:`open` once, or use
    it as an async context manager.  Multiple coroutines may call it safely;
    a lock prevents transaction statements from interleaving on the shared
    connection.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        max_queue_events: int = 100_000,
        max_claim_events: int = 50,
        max_outbox_events: int | None = None,
        max_payload_bytes: int = 1_048_576,
        synchronous: Literal["FULL", "EXTRA", "NORMAL"] = "FULL",
        busy_timeout_ms: int = 5_000,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if max_queue_events <= 0:
            raise ValueError("max_queue_events must be positive")
        if max_claim_events <= 0 or max_claim_events > 500:
            raise ValueError("max_claim_events must be between 1 and 500")
        if max_outbox_events is not None and max_outbox_events <= 0:
            raise ValueError("max_outbox_events must be positive")
        if max_payload_bytes <= 0:
            raise ValueError("max_payload_bytes must be positive")
        if synchronous not in {"FULL", "EXTRA", "NORMAL"}:
            raise ValueError("synchronous must be FULL, EXTRA, or NORMAL")
        if busy_timeout_ms < 0 or busy_timeout_ms > 120_000:
            raise ValueError("busy_timeout_ms must be between 0 and 120000")
        self.path = Path(path)
        self.max_queue_events = max_queue_events
        self.max_claim_events = max_claim_events
        self.max_outbox_events = max_outbox_events or max_queue_events
        self.max_payload_bytes = max_payload_bytes
        self.synchronous = synchronous
        self.busy_timeout_ms = busy_timeout_ms
        self._clock = clock
        self._connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> SQLiteStorage:
        await self.open()
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        await self.close()

    @property
    def is_open(self) -> bool:
        return self._connection is not None

    def _conn(self) -> aiosqlite.Connection:
        if self._connection is None:
            raise RuntimeNotReadyError("SQLite storage is not open")
        return self._connection

    def _now(self, value: float | None) -> float:
        return _validate_time(self._clock() if value is None else value, "now")

    async def open(self) -> None:
        async with self._lock:
            if self._connection is not None:
                return
            self.path.parent.mkdir(parents=True, exist_ok=True)
            connection = await aiosqlite.connect(
                str(self.path),
                timeout=self.busy_timeout_ms / 1_000,
                isolation_level=None,
            )
            connection.row_factory = aiosqlite.Row
            try:
                await connection.execute("PRAGMA foreign_keys=ON")
                await connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
                cursor = await connection.execute("PRAGMA journal_mode=WAL")
                journal_row = await cursor.fetchone()
                await cursor.close()
                if journal_row is None or str(journal_row[0]).lower() != "wal":
                    raise StorageError("SQLite refused WAL journal mode; use a local writable filesystem")
                await connection.execute(f"PRAGMA synchronous={self.synchronous}")
                await connection.execute("PRAGMA wal_autocheckpoint=1000")
                cursor = await connection.execute("PRAGMA user_version")
                version_row = await cursor.fetchone()
                await cursor.close()
                version = int(version_row[0]) if version_row is not None else 0
                if version > _SCHEMA_VERSION:
                    raise StorageError("Database schema is newer than this bridge")
                if version == 0:
                    await connection.executescript(self._schema_sql())
                elif version != _SCHEMA_VERSION:
                    raise StorageError("Unsupported database schema version")
            except BaseException:
                await connection.close()
                raise
            self._connection = connection

    async def close(self) -> None:
        async def operation() -> None:
            async with self._lock:
                if self._connection is None:
                    return
                connection = self._connection
                self._connection = None
                try:
                    await connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
                finally:
                    await connection.close()

        # aiosqlite owns a worker thread.  Once shutdown starts, finish it even
        # if the outer runtime task is cancelled while waiting for our lock.
        await self._finish(operation)

    @staticmethod
    def _schema_sql() -> str:
        return """
        CREATE TABLE IF NOT EXISTS inbox_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_key TEXT NOT NULL,
            update_id INTEGER NOT NULL CHECK(update_id >= 0),
            payload_json TEXT NOT NULL,
            payload_hash TEXT NOT NULL CHECK(length(payload_hash) = 64),
            chat_id INTEGER,
            state TEXT NOT NULL DEFAULT 'queued'
                CHECK(state IN ('queued', 'leased', 'acked')),
            received_at REAL NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
            lease_token TEXT,
            lease_until REAL,
            acked_at REAL,
            UNIQUE(bot_key, update_id),
            CHECK(
                (state = 'leased' AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
                OR (state <> 'leased' AND lease_token IS NULL AND lease_until IS NULL)
            )
        );
        CREATE INDEX IF NOT EXISTS inbox_claim_idx
            ON inbox_events(bot_key, event_id)
            WHERE state IN ('queued', 'leased');
        CREATE INDEX IF NOT EXISTS inbox_purge_idx ON inbox_events(state, acked_at);

        CREATE TABLE IF NOT EXISTS poll_offsets (
            bot_key TEXT PRIMARY KEY,
            next_offset INTEGER NOT NULL CHECK(next_offset >= 0),
            updated_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chat_authorizations (
            bot_key TEXT NOT NULL,
            chat_id INTEGER NOT NULL,
            alias TEXT,
            can_read INTEGER NOT NULL CHECK(can_read IN (0, 1)),
            can_write INTEGER NOT NULL CHECK(can_write IN (0, 1)),
            updated_at REAL NOT NULL,
            PRIMARY KEY(bot_key, chat_id),
            UNIQUE(bot_key, alias)
        );

        CREATE TABLE IF NOT EXISTS outbox (
            outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_key TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            fingerprint TEXT NOT NULL CHECK(length(fingerprint) = 64),
            chat_id INTEGER NOT NULL,
            method TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL
                CHECK(status IN ('pending', 'sending', 'retry', 'uncertain', 'sent', 'dead')),
            attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
            next_attempt_at REAL NOT NULL,
            lease_token TEXT,
            lease_until REAL,
            telegram_message_id INTEGER,
            last_error TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            sent_at REAL,
            UNIQUE(bot_key, idempotency_key),
            CHECK(
                (status = 'sending' AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
                OR (status <> 'sending' AND lease_token IS NULL AND lease_until IS NULL)
            )
        );
        CREATE INDEX IF NOT EXISTS outbox_due_idx
            ON outbox(next_attempt_at, created_at, outbox_id)
            WHERE status IN ('pending', 'retry');
        CREATE INDEX IF NOT EXISTS outbox_bot_due_idx
            ON outbox(bot_key, next_attempt_at, created_at, outbox_id)
            WHERE status IN ('pending', 'retry');

        CREATE TABLE IF NOT EXISTS storage_counters (
            bot_key TEXT NOT NULL,
            name TEXT NOT NULL,
            value INTEGER NOT NULL DEFAULT 0 CHECK(value >= 0),
            PRIMARY KEY(bot_key, name)
        );

        PRAGMA user_version=1;
        """

    async def _finish(self, operation: Callable[[], Any]) -> Any:
        """Finish commit/rollback even when the caller is cancelled."""

        task = asyncio.create_task(operation())
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError as cancellation:
            try:
                # A task can be cancelled more than once during shutdown.  Do
                # not let a later cancellation propagate into the SQLite
                # worker future; wait until the transaction boundary has a
                # known outcome before returning cancellation to the caller.
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        continue
                task.result()
            finally:
                raise cancellation

    async def _begin(self) -> None:
        async def operation() -> None:
            cursor = await self._conn().execute("BEGIN IMMEDIATE")
            await cursor.close()

        await self._finish(operation)

    async def _commit(self) -> None:
        await self._finish(self._conn().commit)

    async def _rollback(self) -> None:
        await self._finish(self._conn().rollback)

    async def _fetchone(self, sql: str, parameters: Sequence[object] = ()) -> aiosqlite.Row | None:
        cursor = await self._conn().execute(sql, parameters)
        try:
            return await cursor.fetchone()
        finally:
            await cursor.close()

    async def _fetchall(self, sql: str, parameters: Sequence[object] = ()) -> list[aiosqlite.Row]:
        cursor = await self._conn().execute(sql, parameters)
        try:
            return list(await cursor.fetchall())
        finally:
            await cursor.close()

    async def _counter(self, bot_key: str, name: str, amount: int = 1) -> None:
        if amount <= 0:
            return
        await self._conn().execute(
            """
            INSERT INTO storage_counters(bot_key, name, value) VALUES (?, ?, ?)
            ON CONFLICT(bot_key, name) DO UPDATE SET value = value + excluded.value
            """,
            (bot_key, name, amount),
        )

    async def _quarantine_unauthorized_outbox(
        self,
        bot_key: str,
        timestamp: float,
        *,
        chat_id: int | None = None,
    ) -> tuple[int, int]:
        """Stop queued/in-flight work whose current write ACL was revoked.

        Callers hold ``BEGIN IMMEDIATE``, so no worker can acquire a lease
        between the authorization change and this quarantine transition.
        Pending work becomes ``dead`` and an in-flight/ambiguous request
        becomes ``uncertain``; both require explicit reauthorization and
        :meth:`requeue_outbox` before another send.
        """

        parameters = (bot_key, chat_id, chat_id)
        rows = await self._fetchall(
            """
            SELECT status, COUNT(*) AS count FROM outbox
            WHERE bot_key = ? AND (? IS NULL OR chat_id = ?)
              AND status IN ('pending', 'retry', 'sending')
              AND NOT EXISTS (
                  SELECT 1 FROM chat_authorizations AS authorization
                  WHERE authorization.bot_key = outbox.bot_key
                    AND authorization.chat_id = outbox.chat_id
                    AND authorization.can_write = 1
              )
            GROUP BY status
            """,
            parameters,
        )
        counts = {str(row["status"]): int(row["count"]) for row in rows}
        dead = counts.get("pending", 0) + counts.get("retry", 0)
        uncertain = counts.get("sending", 0)
        if dead:
            await self._conn().execute(
                """
                UPDATE outbox
                SET status = 'dead', lease_token = NULL, lease_until = NULL,
                    next_attempt_at = ?, updated_at = ?,
                    last_error = 'outbound chat authorization was revoked'
                WHERE bot_key = ? AND (? IS NULL OR chat_id = ?)
                  AND status IN ('pending', 'retry')
                  AND NOT EXISTS (
                      SELECT 1 FROM chat_authorizations AS authorization
                      WHERE authorization.bot_key = outbox.bot_key
                        AND authorization.chat_id = outbox.chat_id
                        AND authorization.can_write = 1
                  )
                """,
                (timestamp, timestamp, *parameters),
            )
            await self._counter(bot_key, "outbox_dead", dead)
        if uncertain:
            await self._conn().execute(
                """
                UPDATE outbox
                SET status = 'uncertain', lease_token = NULL, lease_until = NULL,
                    next_attempt_at = ?, updated_at = ?,
                    last_error = 'outbound chat authorization was revoked during send'
                WHERE bot_key = ? AND (? IS NULL OR chat_id = ?)
                  AND status = 'sending'
                  AND NOT EXISTS (
                      SELECT 1 FROM chat_authorizations AS authorization
                      WHERE authorization.bot_key = outbox.bot_key
                        AND authorization.chat_id = outbox.chat_id
                        AND authorization.can_write = 1
                  )
                """,
                (timestamp, timestamp, *parameters),
            )
            await self._counter(bot_key, "outbox_uncertain", uncertain)
        return dead, uncertain

    def _prepare_update(
        self,
        update: Mapping[str, Any] | str | bytes,
        *,
        update_id: int | None,
        chat_id: int | None,
        received_at: float,
    ) -> _PreparedUpdate:
        _reject_oversized_raw_payload(update, self.max_payload_bytes, "Telegram update")
        value, payload_json, fingerprint = _json_object(update)
        if len(payload_json.encode("utf-8")) > self.max_payload_bytes:
            raise ValueError("Telegram update exceeds the configured storage byte limit")
        payload_update_id = value.get("update_id")
        if update_id is None:
            update_id = _validate_integer(payload_update_id, "update_id", non_negative=True)
        else:
            update_id = _validate_integer(update_id, "update_id", non_negative=True)
            if payload_update_id is not None:
                validated_payload_update_id = _validate_integer(
                    payload_update_id, "payload.update_id", non_negative=True
                )
                if validated_payload_update_id != update_id:
                    raise ValueError("update_id argument does not match payload.update_id")
        extracted_chat_id = _extract_chat_id(value)
        if chat_id is None:
            chat_id = extracted_chat_id
        else:
            chat_id = _validate_integer(chat_id, "chat_id")
            if extracted_chat_id is not None and extracted_chat_id != chat_id:
                raise ValueError("chat_id argument does not match the chat encoded in the payload")
        return _PreparedUpdate(update_id, payload_json, fingerprint, chat_id, received_at)

    async def enqueue_update(
        self,
        bot_key: str,
        update: Mapping[str, Any] | str | bytes,
        *,
        update_id: int | None = None,
        chat_id: int | None = None,
        received_at: float | None = None,
    ) -> EnqueueOutcome:
        result = await self.ingest_poll_batch(
            bot_key,
            (update,),
            update_ids=(update_id,),
            chat_ids=(chat_id,),
            received_at=received_at,
            advance_offset=False,
        )
        return result.outcomes[0]

    async def ingest_poll_batch(
        self,
        bot_key: str,
        updates: Sequence[Mapping[str, Any] | str | bytes],
        *,
        next_offset: int | None = None,
        update_ids: Sequence[int | None] | None = None,
        chat_ids: Sequence[int | None] | None = None,
        received_at: float | None = None,
        advance_offset: bool = True,
    ) -> BatchIngestResult:
        """Insert one Telegram polling batch and advance its offset atomically.

        A full queue rolls back both inserts and the offset.  Duplicate IDs are
        harmless.  A different fingerprint is reported as ``conflict`` and the
        original payload is never replaced.
        """

        bot_key = _validate_bot_key(bot_key)
        if len(updates) > 500:
            raise ValueError("A polling batch may contain at most 500 updates")
        if update_ids is not None and len(update_ids) != len(updates):
            raise ValueError("update_ids must match updates length")
        if chat_ids is not None and len(chat_ids) != len(updates):
            raise ValueError("chat_ids must match updates length")
        timestamp = self._now(received_at)
        prepared = tuple(
            self._prepare_update(
                update,
                update_id=update_ids[index] if update_ids is not None else None,
                chat_id=chat_ids[index] if chat_ids is not None else None,
                received_at=timestamp,
            )
            for index, update in enumerate(updates)
        )
        if next_offset is not None:
            next_offset = _validate_integer(next_offset, "next_offset", non_negative=True)
        elif advance_offset and prepared:
            next_offset = max(item.update_id for item in prepared) + 1
            if next_offset > _MAX_SQLITE_INTEGER:
                raise ValueError("Computed polling offset exceeds SQLite integer range")

        # The first value for an ID is the one eligible for insertion.  Later
        # values are still represented in the result as duplicate/conflict.
        first_by_id: dict[int, _PreparedUpdate] = {}
        for item in prepared:
            first_by_id.setdefault(item.update_id, item)
        ids = tuple(first_by_id)

        async with self._lock:
            try:
                await self._begin()
                existing_rows: list[aiosqlite.Row] = []
                if ids:
                    existing_rows = await self._fetchall(
                        f"""
                        SELECT event_id, update_id, payload_hash FROM inbox_events
                        WHERE bot_key = ? AND update_id IN ({_placeholders(len(ids))})
                        """,  # noqa: S608
                        (bot_key, *ids),
                    )
                existing = {
                    int(row["update_id"]): (int(row["event_id"]), str(row["payload_hash"]))
                    for row in existing_rows
                }
                new_ids = tuple(update_id for update_id in ids if update_id not in existing)
                if new_ids:
                    row = await self._fetchone(
                        "SELECT COUNT(*) AS count FROM inbox_events WHERE state <> 'acked'"
                    )
                    pending = int(row["count"]) if row is not None else 0
                    if pending + len(new_ids) > self.max_queue_events:
                        raise QueueFullError("Durable inbox queue is full")
                    for update_id_value in new_ids:
                        item = first_by_id[update_id_value]
                        cursor = await self._conn().execute(
                            """
                            INSERT INTO inbox_events(
                                bot_key, update_id, payload_json, payload_hash, chat_id, received_at
                            ) VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (
                                bot_key,
                                item.update_id,
                                item.payload_json,
                                item.fingerprint,
                                item.chat_id,
                                item.received_at,
                            ),
                        )
                        if cursor.lastrowid is None:
                            raise StorageError("SQLite did not return an inbox event ID")
                        existing[update_id_value] = (int(cursor.lastrowid), item.fingerprint)
                        await cursor.close()

                outcomes: list[EnqueueOutcome] = []
                first_seen_in_input: set[int] = set()
                counts: dict[UpdateDisposition, int] = {
                    "inserted": 0,
                    "duplicate": 0,
                    "conflict": 0,
                }
                originally_existing = {int(row["update_id"]) for row in existing_rows}
                for item in prepared:
                    event_id, stored_fingerprint = existing[item.update_id]
                    if (
                        item.update_id not in first_seen_in_input
                        and item.update_id not in originally_existing
                    ):
                        disposition: UpdateDisposition = "inserted"
                    elif item.fingerprint == stored_fingerprint:
                        disposition = "duplicate"
                    else:
                        disposition = "conflict"
                    first_seen_in_input.add(item.update_id)
                    counts[disposition] += 1
                    outcomes.append(
                        EnqueueOutcome(
                            event_id=event_id,
                            update_id=item.update_id,
                            disposition=disposition,
                            incoming_fingerprint=item.fingerprint,
                            stored_fingerprint=stored_fingerprint,
                        )
                    )
                for counter_disposition, count in counts.items():
                    await self._counter(bot_key, f"inbox_{counter_disposition}", count)

                if advance_offset and next_offset is not None:
                    await self._conn().execute(
                        """
                        INSERT INTO poll_offsets(bot_key, next_offset, updated_at) VALUES (?, ?, ?)
                        ON CONFLICT(bot_key) DO UPDATE SET
                            next_offset = excluded.next_offset,
                            updated_at = excluded.updated_at
                        """,
                        (bot_key, next_offset, timestamp),
                    )
                await self._commit()
                return BatchIngestResult(tuple(outcomes), next_offset if advance_offset else None)
            except BaseException:
                await self._rollback()
                raise

    # Name used by ingress code that does not care whether the source is a
    # webhook or polling.
    store_update = enqueue_update

    async def get_poll_offset(self, bot_key: str) -> int | None:
        bot_key = _validate_bot_key(bot_key)
        async with self._lock:
            row = await self._fetchone("SELECT next_offset FROM poll_offsets WHERE bot_key = ?", (bot_key,))
            return int(row["next_offset"]) if row is not None else None

    async def get_event(self, bot_key: str, update_id: int) -> InboxEvent | None:
        """Look up one inbox row by Telegram's per-bot ``update_id``."""

        bot_key = _validate_bot_key(bot_key)
        update_id = _validate_integer(update_id, "update_id", non_negative=True)
        async with self._lock:
            row = await self._fetchone(
                "SELECT * FROM inbox_events WHERE bot_key = ? AND update_id = ?",
                (bot_key, update_id),
            )
            return self._inbox_record(row) if row is not None else None

    async def get_event_by_event_id(self, bot_key: str, event_id: int) -> InboxEvent | None:
        """Look up one inbox row by its local, monotonically increasing ID."""

        bot_key = _validate_bot_key(bot_key)
        event_id = _validate_integer(event_id, "event_id", non_negative=True)
        async with self._lock:
            row = await self._fetchone(
                "SELECT * FROM inbox_events WHERE bot_key = ? AND event_id = ?",
                (bot_key, event_id),
            )
            return self._inbox_record(row) if row is not None else None

    async def peek_events(
        self,
        bot_key: str,
        *,
        after_event_id: int = 0,
        limit: int = 20,
        include_acked: bool = False,
    ) -> tuple[InboxEvent, ...]:
        """Read a bounded local-ID page without acquiring or changing leases."""

        bot_key = _validate_bot_key(bot_key)
        after_event_id = _validate_integer(after_event_id, "after_event_id", non_negative=True)
        if limit <= 0 or limit > 500:
            raise ValueError("limit must be between 1 and 500")
        async with self._lock:
            if include_acked:
                rows = await self._fetchall(
                    """
                    SELECT * FROM inbox_events
                    WHERE bot_key = ? AND event_id > ?
                    ORDER BY event_id LIMIT ?
                    """,
                    (bot_key, after_event_id, limit),
                )
            else:
                rows = await self._fetchall(
                    """
                    SELECT * FROM inbox_events
                    WHERE bot_key = ? AND event_id > ? AND state <> 'acked'
                    ORDER BY event_id LIMIT ?
                    """,
                    (bot_key, after_event_id, limit),
                )
            return tuple(self._inbox_record(row) for row in rows)

    @staticmethod
    def _inbox_record(row: aiosqlite.Row) -> InboxEvent:
        return InboxEvent(
            event_id=int(row["event_id"]),
            bot_key=str(row["bot_key"]),
            update_id=int(row["update_id"]),
            payload_json=str(row["payload_json"]),
            fingerprint=str(row["payload_hash"]),
            chat_id=int(row["chat_id"]) if row["chat_id"] is not None else None,
            state=str(row["state"]),  # type: ignore[arg-type]
            received_at=float(row["received_at"]),
            attempts=int(row["attempts"]),
            lease_token=str(row["lease_token"]) if row["lease_token"] is not None else None,
            lease_until=float(row["lease_until"]) if row["lease_until"] is not None else None,
            acked_at=float(row["acked_at"]) if row["acked_at"] is not None else None,
        )

    async def claim_events(
        self,
        bot_key: str,
        *,
        limit: int | None = None,
        lease_seconds: float = 30.0,
        now: float | None = None,
    ) -> InboxClaim:
        bot_key = _validate_bot_key(bot_key)
        limit = self.max_claim_events if limit is None else limit
        if limit <= 0 or limit > self.max_claim_events:
            raise ValueError(f"limit must be between 1 and {self.max_claim_events}")
        if lease_seconds <= 0 or lease_seconds > MAX_LEASE_SECONDS or not math.isfinite(lease_seconds):
            raise ValueError("lease_seconds must be finite and between 0 and 86400")
        timestamp = self._now(now)
        lease_until = timestamp + lease_seconds
        lease_token = secrets.token_urlsafe(32)
        async with self._lock:
            try:
                await self._begin()
                rows = await self._fetchall(
                    """
                    SELECT event_id FROM inbox_events
                    WHERE bot_key = ?
                      AND state IN ('queued', 'leased')
                      AND (state = 'queued' OR lease_until <= ?)
                    ORDER BY event_id
                    LIMIT ?
                    """,
                    (bot_key, timestamp, limit),
                )
                event_ids = tuple(int(row["event_id"]) for row in rows)
                if not event_ids:
                    await self._commit()
                    return InboxClaim(None, None, ())
                await self._conn().execute(
                    f"""
                    UPDATE inbox_events
                    SET state = 'leased', lease_token = ?, lease_until = ?, attempts = attempts + 1
                    WHERE event_id IN ({_placeholders(len(event_ids))})
                    """,  # noqa: S608
                    (lease_token, lease_until, *event_ids),
                )
                claimed_rows = await self._fetchall(
                    f"""
                    SELECT * FROM inbox_events WHERE event_id IN ({_placeholders(len(event_ids))})
                    ORDER BY event_id
                    """,  # noqa: S608
                    event_ids,
                )
                await self._counter(bot_key, "inbox_claimed", len(event_ids))
                await self._commit()
                return InboxClaim(
                    lease_token,
                    lease_until,
                    tuple(self._inbox_record(row) for row in claimed_rows),
                )
            except BaseException:
                await self._rollback()
                raise

    claim_updates = claim_events

    async def _change_claimed_events(
        self,
        bot_key: str,
        identifiers: Iterable[int],
        lease_token: str,
        *,
        identifier_column: Literal["event_id", "update_id"],
        action: Literal["ack", "release"],
        now: float | None,
    ) -> int:
        bot_key = _validate_bot_key(bot_key)
        ids = _unique_ints(identifiers, identifier_column)
        if not ids:
            return 0
        if not isinstance(lease_token, str) or len(lease_token) < 20:
            raise LeaseConflictError("A valid lease token is required")
        timestamp = self._now(now)
        async with self._lock:
            try:
                await self._begin()
                rows = await self._fetchall(
                    f"""
                    SELECT {identifier_column} FROM inbox_events
                    WHERE bot_key = ? AND {identifier_column} IN ({_placeholders(len(ids))})
                      AND state = 'leased' AND lease_token = ? AND lease_until > ?
                    """,  # noqa: S608
                    (bot_key, *ids, lease_token, timestamp),
                )
                if len(rows) != len(ids):
                    raise LeaseConflictError("Inbox lease is missing, expired, or owned by another consumer")
                if action == "ack":
                    await self._conn().execute(
                        f"""
                        UPDATE inbox_events
                        SET state = 'acked', lease_token = NULL, lease_until = NULL, acked_at = ?
                        WHERE bot_key = ? AND {identifier_column} IN ({_placeholders(len(ids))})
                        """,  # noqa: S608
                        (timestamp, bot_key, *ids),
                    )
                    await self._counter(bot_key, "inbox_acked", len(ids))
                else:
                    await self._conn().execute(
                        f"""
                        UPDATE inbox_events
                        SET state = 'queued', lease_token = NULL, lease_until = NULL
                        WHERE bot_key = ? AND {identifier_column} IN ({_placeholders(len(ids))})
                        """,  # noqa: S608
                        (bot_key, *ids),
                    )
                    await self._counter(bot_key, "inbox_released", len(ids))
                await self._commit()
                return len(ids)
            except BaseException:
                await self._rollback()
                raise

    async def ack_events(
        self,
        bot_key: str,
        event_ids: Iterable[int],
        lease_token: str,
        *,
        now: float | None = None,
    ) -> int:
        return await self._change_claimed_events(
            bot_key,
            event_ids,
            lease_token,
            identifier_column="event_id",
            action="ack",
            now=now,
        )

    async def ack_updates(
        self,
        bot_key: str,
        update_ids: Iterable[int],
        lease_token: str,
        *,
        now: float | None = None,
    ) -> int:
        return await self._change_claimed_events(
            bot_key,
            update_ids,
            lease_token,
            identifier_column="update_id",
            action="ack",
            now=now,
        )

    async def release_events(
        self,
        bot_key: str,
        event_ids: Iterable[int],
        lease_token: str,
        *,
        now: float | None = None,
    ) -> int:
        return await self._change_claimed_events(
            bot_key,
            event_ids,
            lease_token,
            identifier_column="event_id",
            action="release",
            now=now,
        )

    async def release_updates(
        self,
        bot_key: str,
        update_ids: Iterable[int],
        lease_token: str,
        *,
        now: float | None = None,
    ) -> int:
        return await self._change_claimed_events(
            bot_key,
            update_ids,
            lease_token,
            identifier_column="update_id",
            action="release",
            now=now,
        )

    async def recover_inbox_leases(self, *, now: float | None = None) -> int:
        timestamp = self._now(now)
        async with self._lock:
            try:
                await self._begin()
                cursor = await self._conn().execute(
                    """
                    UPDATE inbox_events
                    SET state = 'queued', lease_token = NULL, lease_until = NULL
                    WHERE state = 'leased' AND lease_until <= ?
                    """,
                    (timestamp,),
                )
                changed = max(cursor.rowcount, 0)
                await cursor.close()
                await self._commit()
                return changed
            except BaseException:
                await self._rollback()
                raise

    async def recover_inherited_inbox(self) -> int:
        """Requeue every inbox lease inherited from a stopped process.

        This startup-only operation is safe because inbox delivery is
        explicitly at-least-once and the previous consumer cannot ACK anymore.
        Periodic recovery should use :meth:`recover_inbox_leases` instead.
        """

        async with self._lock:
            try:
                await self._begin()
                cursor = await self._conn().execute(
                    """
                    UPDATE inbox_events
                    SET state = 'queued', lease_token = NULL, lease_until = NULL
                    WHERE state = 'leased'
                    """
                )
                changed = max(cursor.rowcount, 0)
                await cursor.close()
                await self._commit()
                return changed
            except BaseException:
                await self._rollback()
                raise

    async def purge_acked(
        self,
        *,
        acked_before: float,
        bot_key: str | None = None,
        limit: int = 1_000,
    ) -> int:
        """Delete only ACKed inbox rows older than ``acked_before``."""

        cutoff = _validate_time(acked_before, "acked_before")
        if bot_key is not None:
            bot_key = _validate_bot_key(bot_key)
        if limit <= 0 or limit > 100_000:
            raise ValueError("limit must be between 1 and 100000")
        async with self._lock:
            try:
                await self._begin()
                if bot_key is None:
                    cursor = await self._conn().execute(
                        """
                        DELETE FROM inbox_events WHERE event_id IN (
                            SELECT event_id FROM inbox_events
                            WHERE state = 'acked' AND acked_at < ?
                            ORDER BY acked_at, event_id LIMIT ?
                        ) AND state = 'acked'
                        """,
                        (cutoff, limit),
                    )
                else:
                    cursor = await self._conn().execute(
                        """
                        DELETE FROM inbox_events WHERE event_id IN (
                            SELECT event_id FROM inbox_events
                            WHERE state = 'acked' AND acked_at < ? AND bot_key = ?
                            ORDER BY acked_at, event_id LIMIT ?
                        ) AND state = 'acked'
                        """,
                        (cutoff, bot_key, limit),
                    )
                deleted = max(cursor.rowcount, 0)
                await cursor.close()
                await self._commit()
                return deleted
            except BaseException:
                await self._rollback()
                raise

    async def set_chat_authorization(
        self,
        bot_key: str,
        chat_id: int,
        *,
        can_read: bool = True,
        can_write: bool = True,
        alias: str | None = None,
        now: float | None = None,
    ) -> ChatAuthorization:
        bot_key = _validate_bot_key(bot_key)
        chat_id = _validate_integer(chat_id, "chat_id")
        if not isinstance(can_read, bool) or not isinstance(can_write, bool):
            raise ValueError("can_read and can_write must be booleans")
        if alias is not None and not _ALIAS_RE.fullmatch(alias):
            raise ValueError("Chat alias must contain 1-64 letters, digits, '_' or '-'")
        timestamp = self._now(now)
        async with self._lock:
            try:
                await self._begin()
                await self._conn().execute(
                    """
                    INSERT INTO chat_authorizations(
                        bot_key, chat_id, alias, can_read, can_write, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(bot_key, chat_id) DO UPDATE SET
                        alias = excluded.alias,
                        can_read = excluded.can_read,
                        can_write = excluded.can_write,
                        updated_at = excluded.updated_at
                    """,
                    (bot_key, chat_id, alias, int(can_read), int(can_write), timestamp),
                )
                if not can_write:
                    await self._quarantine_unauthorized_outbox(bot_key, timestamp, chat_id=chat_id)
                await self._commit()
                return ChatAuthorization(bot_key, chat_id, alias, can_read, can_write, timestamp)
            except BaseException:
                await self._rollback()
                raise

    async def replace_chat_authorizations(
        self,
        bot_key: str,
        chat_ids: Iterable[int],
        *,
        can_read: bool = True,
        can_write: bool = True,
        now: float | None = None,
    ) -> int:
        """Atomically replace one bot's numeric chat allowlist."""

        bot_key = _validate_bot_key(bot_key)
        if not isinstance(can_read, bool) or not isinstance(can_write, bool):
            raise ValueError("can_read and can_write must be booleans")
        normalized = tuple(dict.fromkeys(_validate_integer(value, "chat_id") for value in chat_ids))
        timestamp = self._now(now)
        async with self._lock:
            try:
                await self._begin()
                await self._conn().execute("DELETE FROM chat_authorizations WHERE bot_key = ?", (bot_key,))
                await self._conn().executemany(
                    """
                    INSERT INTO chat_authorizations(
                        bot_key, chat_id, alias, can_read, can_write, updated_at
                    ) VALUES (?, ?, NULL, ?, ?, ?)
                    """,
                    [(bot_key, chat_id, int(can_read), int(can_write), timestamp) for chat_id in normalized],
                )
                await self._quarantine_unauthorized_outbox(bot_key, timestamp)
                await self._commit()
                return len(normalized)
            except BaseException:
                await self._rollback()
                raise

    async def remove_chat_authorization(self, bot_key: str, chat_id: int) -> bool:
        bot_key = _validate_bot_key(bot_key)
        chat_id = _validate_integer(chat_id, "chat_id")
        timestamp = self._now(None)
        async with self._lock:
            try:
                await self._begin()
                cursor = await self._conn().execute(
                    "DELETE FROM chat_authorizations WHERE bot_key = ? AND chat_id = ?",
                    (bot_key, chat_id),
                )
                removed = cursor.rowcount > 0
                await cursor.close()
                await self._quarantine_unauthorized_outbox(bot_key, timestamp, chat_id=chat_id)
                await self._commit()
                return removed
            except BaseException:
                await self._rollback()
                raise

    async def get_chat_authorization(
        self,
        bot_key: str,
        chat: int | str,
    ) -> ChatAuthorization | None:
        bot_key = _validate_bot_key(bot_key)
        async with self._lock:
            if isinstance(chat, int) and not isinstance(chat, bool):
                chat_id = _validate_integer(chat, "chat_id")
                row = await self._fetchone(
                    "SELECT * FROM chat_authorizations WHERE bot_key = ? AND chat_id = ?",
                    (bot_key, chat_id),
                )
            elif isinstance(chat, str) and _ALIAS_RE.fullmatch(chat):
                row = await self._fetchone(
                    "SELECT * FROM chat_authorizations WHERE bot_key = ? AND alias = ?",
                    (bot_key, chat),
                )
            else:
                raise ValueError("chat must be an integer ID or safe alias")
            if row is None:
                return None
            return ChatAuthorization(
                bot_key=str(row["bot_key"]),
                chat_id=int(row["chat_id"]),
                alias=str(row["alias"]) if row["alias"] is not None else None,
                can_read=bool(row["can_read"]),
                can_write=bool(row["can_write"]),
                updated_at=float(row["updated_at"]),
            )

    async def is_chat_authorized(
        self,
        bot_key: str,
        chat: int | str,
        *,
        permission: Literal["read", "write"] = "read",
    ) -> bool:
        if permission not in {"read", "write"}:
            raise ValueError("permission must be read or write")
        authorization = await self.get_chat_authorization(bot_key, chat)
        if authorization is None:
            return False
        return authorization.can_read if permission == "read" else authorization.can_write

    async def list_authorized_chats(
        self,
        bot_key: str,
        *,
        limit: int = 20,
    ) -> tuple[ChatAuthorization, ...]:
        """Return a bounded diagnostic view of one bot's explicit allowlist."""

        bot_key = _validate_bot_key(bot_key)
        if limit <= 0 or limit > 500:
            raise ValueError("limit must be between 1 and 500")
        async with self._lock:
            rows = await self._fetchall(
                """
                SELECT * FROM chat_authorizations
                WHERE bot_key = ? ORDER BY chat_id LIMIT ?
                """,
                (bot_key, limit),
            )
            return tuple(
                ChatAuthorization(
                    bot_key=str(row["bot_key"]),
                    chat_id=int(row["chat_id"]),
                    alias=str(row["alias"]) if row["alias"] is not None else None,
                    can_read=bool(row["can_read"]),
                    can_write=bool(row["can_write"]),
                    updated_at=float(row["updated_at"]),
                )
                for row in rows
            )

    async def reserve_outbox(
        self,
        bot_key: str,
        idempotency_key: str,
        *,
        chat_id: int,
        method: str,
        payload: Mapping[str, Any] | str | bytes,
        require_authorized: bool = True,
        now: float | None = None,
    ) -> OutboxReservation:
        bot_key = _validate_bot_key(bot_key)
        if not isinstance(idempotency_key, str) or not 1 <= len(idempotency_key) <= 200:
            raise ValueError("idempotency_key must contain 1-200 characters")
        if any(ord(character) < 0x20 or ord(character) == 0x7F for character in idempotency_key):
            raise ValueError("idempotency_key may not contain control characters")
        chat_id = _validate_integer(chat_id, "chat_id")
        if not isinstance(method, str) or not _METHOD_RE.fullmatch(method):
            raise ValueError("method must be a safe Telegram method identifier")
        _reject_oversized_raw_payload(payload, self.max_payload_bytes, "Outbox payload")
        value, payload_json, _ = _json_object(payload)
        if len(payload_json.encode("utf-8")) > self.max_payload_bytes:
            raise ValueError("Outbox payload exceeds the configured storage byte limit")
        if "chat_id" in value:
            embedded_chat_id = _validate_integer(value["chat_id"], "payload.chat_id")
            if embedded_chat_id != chat_id:
                raise ValueError("payload.chat_id does not match the authorized target")
        fingerprint_input = json.dumps(
            {"chat_id": chat_id, "method": method, "payload": value},
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        fingerprint = hashlib.sha256(fingerprint_input).hexdigest()
        timestamp = self._now(now)
        if require_authorized is not True:
            raise AuthorizationError("Outbox authorization checks cannot be bypassed")

        async with self._lock:
            try:
                await self._begin()
                authorization = await self._fetchone(
                    """
                    SELECT can_write FROM chat_authorizations
                    WHERE bot_key = ? AND chat_id = ?
                    """,
                    (bot_key, chat_id),
                )
                if authorization is None or not bool(authorization["can_write"]):
                    raise AuthorizationError("Outbound Telegram chat is not authorized")
                existing = await self._fetchone(
                    "SELECT * FROM outbox WHERE bot_key = ? AND idempotency_key = ?",
                    (bot_key, idempotency_key),
                )
                if existing is not None:
                    if str(existing["fingerprint"]) != fingerprint:
                        await self._counter(bot_key, "outbox_conflict")
                        await self._commit()
                        raise IdempotencyConflictError(
                            "Idempotency key was already reserved for different logical input"
                        )
                    await self._counter(bot_key, "outbox_reused")
                    await self._commit()
                    return OutboxReservation(False, self._outbox_record(existing))
                row = await self._fetchone(
                    "SELECT COUNT(*) AS count FROM outbox WHERE status NOT IN ('sent', 'dead')"
                )
                pending = int(row["count"]) if row is not None else 0
                if pending >= self.max_outbox_events:
                    raise QueueFullError("Durable outbox queue is full")
                cursor = await self._conn().execute(
                    """
                    INSERT INTO outbox(
                        bot_key, idempotency_key, fingerprint, chat_id, method, payload_json,
                        status, next_attempt_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                    """,
                    (
                        bot_key,
                        idempotency_key,
                        fingerprint,
                        chat_id,
                        method,
                        payload_json,
                        timestamp,
                        timestamp,
                        timestamp,
                    ),
                )
                if cursor.lastrowid is None:
                    raise StorageError("SQLite did not return an outbox ID")
                outbox_id = int(cursor.lastrowid)
                await cursor.close()
                created_row = await self._fetchone("SELECT * FROM outbox WHERE outbox_id = ?", (outbox_id,))
                if created_row is None:
                    raise StorageError("Reserved outbox row disappeared")
                await self._counter(bot_key, "outbox_reserved")
                await self._commit()
                return OutboxReservation(True, self._outbox_record(created_row))
            except IdempotencyConflictError:
                # The conflict counter is intentionally durable.
                raise
            except BaseException:
                await self._rollback()
                raise

    @staticmethod
    def _outbox_record(row: aiosqlite.Row) -> OutboxRecord:
        return OutboxRecord(
            outbox_id=int(row["outbox_id"]),
            bot_key=str(row["bot_key"]),
            idempotency_key=str(row["idempotency_key"]),
            fingerprint=str(row["fingerprint"]),
            chat_id=int(row["chat_id"]),
            method=str(row["method"]),
            payload_json=str(row["payload_json"]),
            status=str(row["status"]),  # type: ignore[arg-type]
            attempts=int(row["attempts"]),
            next_attempt_at=float(row["next_attempt_at"]),
            lease_token=str(row["lease_token"]) if row["lease_token"] is not None else None,
            lease_until=float(row["lease_until"]) if row["lease_until"] is not None else None,
            telegram_message_id=(
                int(row["telegram_message_id"]) if row["telegram_message_id"] is not None else None
            ),
            last_error=str(row["last_error"]) if row["last_error"] is not None else None,
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
            sent_at=float(row["sent_at"]) if row["sent_at"] is not None else None,
        )

    async def get_outbox(
        self,
        bot_key: str,
        *,
        outbox_id: int | None = None,
        idempotency_key: str | None = None,
    ) -> OutboxRecord | None:
        bot_key = _validate_bot_key(bot_key)
        if (outbox_id is None) == (idempotency_key is None):
            raise ValueError("Specify exactly one of outbox_id or idempotency_key")
        async with self._lock:
            if outbox_id is not None:
                outbox_id = _validate_integer(outbox_id, "outbox_id", non_negative=True)
                row = await self._fetchone(
                    "SELECT * FROM outbox WHERE bot_key = ? AND outbox_id = ?",
                    (bot_key, outbox_id),
                )
            else:
                row = await self._fetchone(
                    "SELECT * FROM outbox WHERE bot_key = ? AND idempotency_key = ?",
                    (bot_key, idempotency_key),
                )
            return self._outbox_record(row) if row is not None else None

    async def claim_outbox(
        self,
        *,
        bot_key: str | None = None,
        limit: int | None = None,
        lease_seconds: float = 30.0,
        now: float | None = None,
    ) -> OutboxClaim:
        if bot_key is not None:
            bot_key = _validate_bot_key(bot_key)
        limit = self.max_claim_events if limit is None else limit
        if limit <= 0 or limit > self.max_claim_events:
            raise ValueError(f"limit must be between 1 and {self.max_claim_events}")
        if lease_seconds <= 0 or lease_seconds > MAX_LEASE_SECONDS or not math.isfinite(lease_seconds):
            raise ValueError("lease_seconds must be finite and between 0 and 86400")
        timestamp = self._now(now)
        lease_until = timestamp + lease_seconds
        lease_token = secrets.token_urlsafe(32)
        async with self._lock:
            try:
                await self._begin()
                if bot_key is None:
                    rows = await self._fetchall(
                        """
                        SELECT outbox.outbox_id FROM outbox
                        JOIN chat_authorizations AS authorization
                          ON authorization.bot_key = outbox.bot_key
                         AND authorization.chat_id = outbox.chat_id
                         AND authorization.can_write = 1
                        WHERE outbox.status IN ('pending', 'retry')
                          AND outbox.next_attempt_at <= ?
                        ORDER BY outbox.next_attempt_at, outbox.created_at, outbox.outbox_id
                        LIMIT ?
                        """,
                        (timestamp, limit),
                    )
                else:
                    rows = await self._fetchall(
                        """
                        SELECT outbox.outbox_id FROM outbox
                        JOIN chat_authorizations AS authorization
                          ON authorization.bot_key = outbox.bot_key
                         AND authorization.chat_id = outbox.chat_id
                         AND authorization.can_write = 1
                        WHERE outbox.status IN ('pending', 'retry')
                          AND outbox.next_attempt_at <= ? AND outbox.bot_key = ?
                        ORDER BY outbox.next_attempt_at, outbox.created_at, outbox.outbox_id
                        LIMIT ?
                        """,
                        (timestamp, bot_key, limit),
                    )
                outbox_ids = tuple(int(row["outbox_id"]) for row in rows)
                if not outbox_ids:
                    await self._commit()
                    return OutboxClaim(None, None, ())
                await self._conn().execute(
                    f"""
                    UPDATE outbox
                    SET status = 'sending', lease_token = ?, lease_until = ?,
                        attempts = attempts + 1, updated_at = ?
                    WHERE outbox_id IN ({_placeholders(len(outbox_ids))})
                    """,  # noqa: S608
                    (lease_token, lease_until, timestamp, *outbox_ids),
                )
                claimed = await self._fetchall(
                    f"""
                    SELECT * FROM outbox WHERE outbox_id IN ({_placeholders(len(outbox_ids))})
                    ORDER BY next_attempt_at, created_at, outbox_id
                    """,  # noqa: S608
                    outbox_ids,
                )
                await self._commit()
                return OutboxClaim(
                    lease_token,
                    lease_until,
                    tuple(self._outbox_record(row) for row in claimed),
                )
            except BaseException:
                await self._rollback()
                raise

    async def claim_outbox_id(
        self,
        outbox_id: int,
        *,
        lease_seconds: float = 30.0,
        now: float | None = None,
    ) -> OutboxClaim:
        """Atomically lease one specific due outbox record.

        This is the synchronous-send counterpart to :meth:`claim_outbox`:
        reserve a durable record, claim that exact ID, perform the Telegram
        request, then call one of the ``mark_outbox_*`` methods.
        """

        outbox_id = _validate_integer(outbox_id, "outbox_id", non_negative=True)
        if lease_seconds <= 0 or lease_seconds > MAX_LEASE_SECONDS or not math.isfinite(lease_seconds):
            raise ValueError("lease_seconds must be finite and between 0 and 86400")
        timestamp = self._now(now)
        lease_until = timestamp + lease_seconds
        lease_token = secrets.token_urlsafe(32)
        async with self._lock:
            try:
                await self._begin()
                row = await self._fetchone("SELECT * FROM outbox WHERE outbox_id = ?", (outbox_id,))
                if row is None:
                    raise OutboxStateError("Outbox record does not exist")
                if str(row["status"]) not in {"pending", "retry"}:
                    # A competing synchronous caller or background worker won
                    # the lease. Returning an empty claim lets the caller wait
                    # for and replay that durable result without sending twice.
                    await self._commit()
                    return OutboxClaim(None, None, ())
                if float(row["next_attempt_at"]) > timestamp:
                    await self._commit()
                    return OutboxClaim(None, None, ())
                authorization = await self._fetchone(
                    """
                    SELECT can_write FROM chat_authorizations
                    WHERE bot_key = ? AND chat_id = ?
                    """,
                    (str(row["bot_key"]), int(row["chat_id"])),
                )
                if authorization is None or not bool(authorization["can_write"]):
                    raise AuthorizationError("Outbound Telegram chat authorization was revoked")
                await self._conn().execute(
                    """
                    UPDATE outbox
                    SET status = 'sending', lease_token = ?, lease_until = ?,
                        attempts = attempts + 1, updated_at = ?
                    WHERE outbox_id = ?
                    """,
                    (lease_token, lease_until, timestamp, outbox_id),
                )
                claimed = await self._fetchone("SELECT * FROM outbox WHERE outbox_id = ?", (outbox_id,))
                if claimed is None:
                    raise StorageError("Outbox record disappeared while being claimed")
                await self._commit()
                return OutboxClaim(lease_token, lease_until, (self._outbox_record(claimed),))
            except BaseException:
                await self._rollback()
                raise

    async def _transition_outbox(
        self,
        outbox_id: int,
        lease_token: str,
        *,
        status: OutboxStatus,
        now: float | None,
        retry_at: float | None = None,
        telegram_message_id: int | None = None,
        error: str | None = None,
    ) -> OutboxRecord:
        outbox_id = _validate_integer(outbox_id, "outbox_id", non_negative=True)
        if not isinstance(lease_token, str) or len(lease_token) < 20:
            raise LeaseConflictError("A valid outbox lease token is required")
        if status not in {"retry", "uncertain", "sent", "dead"}:
            raise ValueError("Invalid terminal/progress status")
        timestamp = self._now(now)
        if retry_at is None:
            retry_at = timestamp
        else:
            retry_at = _validate_time(retry_at, "retry_at")
        if status == "retry" and retry_at < timestamp:
            raise ValueError("retry_at may not be earlier than now")
        if telegram_message_id is not None:
            telegram_message_id = _validate_integer(
                telegram_message_id, "telegram_message_id", non_negative=True
            )
        if status == "sent" and telegram_message_id is None:
            raise ValueError("telegram_message_id is required when marking an outbox record sent")
        if error is not None:
            error = error.replace("\x00", "")[:1_024]

        async with self._lock:
            try:
                await self._begin()
                row = await self._fetchone("SELECT * FROM outbox WHERE outbox_id = ?", (outbox_id,))
                if row is None:
                    raise OutboxStateError("Outbox record does not exist")
                # Expiry is only a recovery eligibility timestamp; it does not
                # mutate ownership by itself.  If this exact token is still on
                # a `sending` row, recovery/reclaim has not won the race and
                # the worker may durably record the Telegram result.  Once
                # recovery changes status/token, this same check rejects the
                # stale worker regardless of wall-clock timing.
                if str(row["status"]) != "sending" or str(row["lease_token"]) != lease_token:
                    raise LeaseConflictError("Outbox lease is missing or owned by another worker")
                sent_at = timestamp if status == "sent" else None
                await self._conn().execute(
                    """
                    UPDATE outbox
                    SET status = ?, next_attempt_at = ?, lease_token = NULL, lease_until = NULL,
                        telegram_message_id = ?, last_error = ?, updated_at = ?, sent_at = ?
                    WHERE outbox_id = ?
                    """,
                    (
                        status,
                        retry_at,
                        telegram_message_id,
                        error,
                        timestamp,
                        sent_at,
                        outbox_id,
                    ),
                )
                updated = await self._fetchone("SELECT * FROM outbox WHERE outbox_id = ?", (outbox_id,))
                if updated is None:
                    raise StorageError("Outbox record disappeared during transition")
                await self._counter(str(row["bot_key"]), f"outbox_{status}")
                await self._commit()
                return self._outbox_record(updated)
            except BaseException:
                await self._rollback()
                raise

    async def mark_outbox_sent(
        self,
        outbox_id: int,
        lease_token: str,
        telegram_message_id: int,
        *,
        now: float | None = None,
    ) -> OutboxRecord:
        return await self._transition_outbox(
            outbox_id,
            lease_token,
            status="sent",
            telegram_message_id=telegram_message_id,
            now=now,
        )

    async def mark_outbox_retry(
        self,
        outbox_id: int,
        lease_token: str,
        *,
        retry_at: float,
        error: str,
        now: float | None = None,
    ) -> OutboxRecord:
        return await self._transition_outbox(
            outbox_id,
            lease_token,
            status="retry",
            retry_at=retry_at,
            error=error,
            now=now,
        )

    async def mark_outbox_uncertain(
        self,
        outbox_id: int,
        lease_token: str,
        *,
        error: str,
        now: float | None = None,
    ) -> OutboxRecord:
        return await self._transition_outbox(
            outbox_id,
            lease_token,
            status="uncertain",
            error=error,
            now=now,
        )

    async def mark_outbox_dead(
        self,
        outbox_id: int,
        lease_token: str,
        *,
        error: str,
        now: float | None = None,
    ) -> OutboxRecord:
        return await self._transition_outbox(
            outbox_id,
            lease_token,
            status="dead",
            error=error,
            now=now,
        )

    async def recover_outbox(
        self,
        *,
        now: float | None = None,
        retry_expired: bool = False,
    ) -> int:
        """Recover expired sends.

        The safe default is ``uncertain`` because Telegram may have accepted a
        request before the bridge crashed.  ``retry_expired=True`` opts into
        at-least-once delivery and its possible duplicate messages.
        """

        timestamp = self._now(now)
        destination = "retry" if retry_expired else "uncertain"
        async with self._lock:
            try:
                await self._begin()
                cursor = await self._conn().execute(
                    """
                    UPDATE outbox
                    SET status = ?, lease_token = NULL, lease_until = NULL,
                        next_attempt_at = ?, updated_at = ?,
                        last_error = COALESCE(last_error, 'worker lease expired during send')
                    WHERE status = 'sending' AND lease_until <= ?
                    """,
                    (destination, timestamp, timestamp, timestamp),
                )
                changed = max(cursor.rowcount, 0)
                await cursor.close()
                await self._commit()
                return changed
            except BaseException:
                await self._rollback()
                raise

    async def recover_inherited_outbox(self, *, now: float | None = None) -> int:
        """Quarantine every send inherited from a previous process.

        Invoke this only during startup, before starting any local outbox
        workers.  The previous process is gone, so even an unexpired lease can
        no longer complete its durable transition.  Telegram may nevertheless
        have accepted the request, therefore automatic retry would be unsafe.
        """

        timestamp = self._now(now)
        async with self._lock:
            try:
                await self._begin()
                cursor = await self._conn().execute(
                    """
                    UPDATE outbox
                    SET status = 'uncertain', lease_token = NULL, lease_until = NULL,
                        next_attempt_at = ?, updated_at = ?,
                        last_error = COALESCE(
                            last_error,
                            'send was in progress when the previous bridge process stopped'
                        )
                    WHERE status = 'sending'
                    """,
                    (timestamp, timestamp),
                )
                changed = max(cursor.rowcount, 0)
                await cursor.close()
                await self._commit()
                return changed
            except BaseException:
                await self._rollback()
                raise

    async def requeue_outbox(self, outbox_id: int, *, now: float | None = None) -> OutboxRecord:
        """Explicitly retry an uncertain/dead result after operator policy."""

        outbox_id = _validate_integer(outbox_id, "outbox_id", non_negative=True)
        timestamp = self._now(now)
        async with self._lock:
            try:
                await self._begin()
                row = await self._fetchone("SELECT * FROM outbox WHERE outbox_id = ?", (outbox_id,))
                if row is None or str(row["status"]) not in {"uncertain", "dead"}:
                    raise OutboxStateError("Only uncertain or dead outbox records may be manually requeued")
                authorization = await self._fetchone(
                    """
                    SELECT can_write FROM chat_authorizations
                    WHERE bot_key = ? AND chat_id = ?
                    """,
                    (str(row["bot_key"]), int(row["chat_id"])),
                )
                if authorization is None or not bool(authorization["can_write"]):
                    raise AuthorizationError("Outbound Telegram chat authorization was revoked")
                if str(row["status"]) == "dead":
                    capacity = await self._fetchone(
                        "SELECT COUNT(*) AS count FROM outbox WHERE status NOT IN ('sent', 'dead')"
                    )
                    pending = int(capacity["count"]) if capacity is not None else 0
                    if pending >= self.max_outbox_events:
                        raise QueueFullError("Durable outbox queue is full")
                await self._conn().execute(
                    """
                    UPDATE outbox
                    SET status = 'retry', next_attempt_at = ?, updated_at = ?, last_error = NULL
                    WHERE outbox_id = ?
                    """,
                    (timestamp, timestamp, outbox_id),
                )
                updated = await self._fetchone("SELECT * FROM outbox WHERE outbox_id = ?", (outbox_id,))
                if updated is None:
                    raise StorageError("Outbox record disappeared while requeueing")
                await self._commit()
                return self._outbox_record(updated)
            except BaseException:
                await self._rollback()
                raise

    async def recover(
        self,
        *,
        now: float | None = None,
        retry_expired_outbox: bool = False,
    ) -> RecoveryResult:
        """Run startup recovery before any local inbox/outbox worker starts.

        Unlike periodic :meth:`recover_outbox`, this quarantines *all*
        inherited sends, including leases whose wall-clock expiry is still in
        the future.  Retrying an ambiguous Telegram request can duplicate a
        message, so startup recovery never offers an automatic-retry mode.
        """

        if retry_expired_outbox:
            raise ValueError("Startup recovery cannot retry inherited Telegram sends")
        # The two recovery operations are each atomic.  A crash between them
        # is harmless because both operations are idempotent.
        timestamp = self._now(now)
        inbox = await self.recover_inherited_inbox()
        outbox = await self.recover_inherited_outbox(now=timestamp)
        return RecoveryResult(inbox, outbox, "uncertain")

    async def metrics(self, *, bot_key: str | None = None, now: float | None = None) -> StorageMetrics:
        if bot_key is not None:
            bot_key = _validate_bot_key(bot_key)
        timestamp = self._now(now)
        async with self._lock:
            if bot_key is None:
                inbox_rows = await self._fetchall(
                    "SELECT state, COUNT(*) AS count FROM inbox_events GROUP BY state"
                )
                outbox_rows = await self._fetchall(
                    "SELECT status, COUNT(*) AS count FROM outbox GROUP BY status"
                )
                counter_rows = await self._fetchall(
                    "SELECT name, SUM(value) AS value FROM storage_counters GROUP BY name"
                )
                oldest_inbox = await self._fetchone(
                    "SELECT MIN(received_at) AS oldest FROM inbox_events WHERE state <> 'acked'"
                )
                oldest_outbox = await self._fetchone(
                    "SELECT MIN(created_at) AS oldest FROM outbox WHERE status NOT IN ('sent', 'dead')"
                )
            else:
                inbox_rows = await self._fetchall(
                    """
                    SELECT state, COUNT(*) AS count FROM inbox_events
                    WHERE bot_key = ? GROUP BY state
                    """,
                    (bot_key,),
                )
                outbox_rows = await self._fetchall(
                    """
                    SELECT status, COUNT(*) AS count FROM outbox
                    WHERE bot_key = ? GROUP BY status
                    """,
                    (bot_key,),
                )
                counter_rows = await self._fetchall(
                    "SELECT name, value FROM storage_counters WHERE bot_key = ?",
                    (bot_key,),
                )
                oldest_inbox = await self._fetchone(
                    """
                    SELECT MIN(received_at) AS oldest FROM inbox_events
                    WHERE bot_key = ? AND state <> 'acked'
                    """,
                    (bot_key,),
                )
                oldest_outbox = await self._fetchone(
                    """
                    SELECT MIN(created_at) AS oldest FROM outbox
                    WHERE bot_key = ? AND status NOT IN ('sent', 'dead')
                    """,
                    (bot_key,),
                )
            inbox_states = {str(row["state"]): int(row["count"]) for row in inbox_rows}
            outbox_states = {str(row["status"]): int(row["count"]) for row in outbox_rows}
            counters = {str(row["name"]): int(row["value"]) for row in counter_rows}
            inbox_oldest_value = oldest_inbox["oldest"] if oldest_inbox is not None else None
            outbox_oldest_value = oldest_outbox["oldest"] if oldest_outbox is not None else None
            return StorageMetrics(
                inbox_states=inbox_states,
                outbox_states=outbox_states,
                counters=counters,
                inbox_pending=inbox_states.get("queued", 0) + inbox_states.get("leased", 0),
                outbox_pending=sum(
                    count for state, count in outbox_states.items() if state not in _TERMINAL_OUTBOX
                ),
                oldest_inbox_age_seconds=(
                    max(0.0, timestamp - float(inbox_oldest_value))
                    if inbox_oldest_value is not None
                    else None
                ),
                oldest_outbox_age_seconds=(
                    max(0.0, timestamp - float(outbox_oldest_value))
                    if outbox_oldest_value is not None
                    else None
                ),
            )


# A concise name for application wiring while keeping the backend explicit in
# tests and documentation.
Storage = SQLiteStorage


__all__ = [
    "MAX_LEASE_SECONDS",
    "BatchIngestResult",
    "ChatAuthorization",
    "EnqueueOutcome",
    "InboxClaim",
    "InboxEvent",
    "LeaseConflictError",
    "OutboxClaim",
    "OutboxRecord",
    "OutboxReservation",
    "OutboxStateError",
    "RecoveryResult",
    "SQLiteStorage",
    "Storage",
    "StorageError",
    "StorageMetrics",
    "UpdateFingerprintConflictError",
]

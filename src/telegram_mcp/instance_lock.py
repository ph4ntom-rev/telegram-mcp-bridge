"""Cross-platform, non-blocking singleton lock for one SQLite bridge worker."""

from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import Any, BinaryIO

from telegram_mcp.errors import BridgeError


class InstanceAlreadyRunningError(BridgeError):
    """Another bridge process owns the database worker lock."""


class InstanceLock:
    """Hold an advisory OS lock without deleting a race-prone lock file."""

    def __init__(self, database_path: Path) -> None:
        self.path = Path(f"{database_path}.lock")
        self._handle: BinaryIO | None = None

    @property
    def acquired(self) -> bool:
        return self._handle is not None

    def acquire(self) -> None:
        if self._handle is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        handle = os.fdopen(descriptor, "r+b", buffering=0)
        try:
            if os.fstat(descriptor).st_size == 0:
                os.write(descriptor, b"\0")
            os.lseek(descriptor, 0, os.SEEK_SET)
            if os.name == "nt":
                platform_lock: Any = importlib.import_module("msvcrt")
                platform_lock.locking(descriptor, platform_lock.LK_NBLCK, 1)
            else:
                platform_lock = importlib.import_module("fcntl")
                platform_lock.flock(
                    descriptor,
                    platform_lock.LOCK_EX | platform_lock.LOCK_NB,
                )
        except (OSError, ImportError) as exc:
            handle.close()
            raise InstanceAlreadyRunningError(
                "Another telegram-mcp serve process is already using this database"
            ) from exc
        self._handle = handle

    def release(self) -> None:
        handle, self._handle = self._handle, None
        if handle is None:
            return
        descriptor = handle.fileno()
        try:
            os.lseek(descriptor, 0, os.SEEK_SET)
            if os.name == "nt":
                platform_lock: Any = importlib.import_module("msvcrt")
                platform_lock.locking(descriptor, platform_lock.LK_UNLCK, 1)
            else:
                platform_lock = importlib.import_module("fcntl")
                platform_lock.flock(descriptor, platform_lock.LOCK_UN)
        finally:
            handle.close()

    def __enter__(self) -> InstanceLock:
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        self.release()

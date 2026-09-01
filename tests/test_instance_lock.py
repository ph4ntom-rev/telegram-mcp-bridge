from __future__ import annotations

from pathlib import Path

import pytest

from telegram_mcp.instance_lock import InstanceAlreadyRunningError, InstanceLock


def test_instance_lock_rejects_second_owner_and_can_be_reacquired(tmp_path: Path) -> None:
    database = tmp_path / "bridge.sqlite3"
    first = InstanceLock(database)
    second = InstanceLock(database)

    first.acquire()
    first.acquire()
    assert first.acquired
    try:
        with pytest.raises(InstanceAlreadyRunningError):
            second.acquire()
    finally:
        first.release()
        first.release()

    second.acquire()
    assert second.acquired
    second.release()
    assert not second.acquired

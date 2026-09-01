"""Repeatable local latency benchmark; it never contacts Telegram."""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

from telegram_mcp.config import Settings
from telegram_mcp.service import BridgeService
from telegram_mcp.storage import SQLiteStorage


class OfflineTelegram:
    started = True


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * fraction)))
    return ordered[index]


def summary(values: list[float]) -> dict[str, float]:
    return {
        "min_ms": round(min(values), 3),
        "p50_ms": round(statistics.median(values), 3),
        "p95_ms": round(percentile(values, 0.95), 3),
        "p99_ms": round(percentile(values, 0.99), 3),
        "max_ms": round(max(values), 3),
        "mean_ms": round(statistics.fmean(values), 3),
    }


def update(update_id: int) -> dict[str, Any]:
    return {
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "chat": {"id": 42, "type": "private"},
            "from": {"id": 42, "is_bot": False},
            "text": "latency probe",
        },
    }


async def run(iterations: int, synchronous: str) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="telegram-mcp-bench-") as directory:
        database = Path(directory) / "bridge.sqlite3"
        settings = Settings(
            telegram_bot_token="1:benchmark",  # noqa: S106 - synthetic non-secret benchmark value
            database_path=database,
            allowed_chat_ids=frozenset({42}),
            allowed_user_ids=frozenset({42}),
            sqlite_synchronous=synchronous,
            max_claim_events=1,
        )
        storage = SQLiteStorage(
            database,
            max_claim_events=1,
            synchronous=synchronous,  # type: ignore[arg-type]
        )
        await storage.open()
        service = BridgeService(
            settings=settings,
            storage=storage,
            telegram=OfflineTelegram(),  # type: ignore[arg-type]
        )
        await service.bootstrap_authorizations()
        commit_ms: list[float] = []
        claim_ack_ms: list[float] = []
        end_to_end_ms: list[float] = []
        try:
            for number in range(iterations):
                started = time.perf_counter_ns()
                await service.ingest_update(update(number))
                committed = time.perf_counter_ns()
                delivery = await service.wait_updates(
                    consumer_id="benchmark",
                    limit=1,
                    wait_seconds=0,
                    lease_seconds=30,
                    include_raw=False,
                )
                token = delivery["lease_token"]
                assert isinstance(token, str)
                await service.ack_updates(lease_token=token, event_ids=None)
                finished = time.perf_counter_ns()
                commit_ms.append((committed - started) / 1_000_000)
                claim_ack_ms.append((finished - committed) / 1_000_000)
                end_to_end_ms.append((finished - started) / 1_000_000)
        finally:
            await storage.close()
    return {
        "iterations": iterations,
        "sqlite_synchronous": synchronous,
        "durable_ingest": summary(commit_ms),
        "claim_plus_ack": summary(claim_ack_ms),
        "durable_round_trip": summary(end_to_end_ms),
        "scope": "local SQLite/MCP service path only; excludes Telegram and model latency",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=500)
    parser.add_argument("--synchronous", choices=("FULL", "EXTRA", "NORMAL"), default="FULL")
    args = parser.parse_args()
    if not 10 <= args.iterations <= 100_000:
        parser.error("--iterations must be between 10 and 100000")
    print(json.dumps(asyncio.run(run(args.iterations, args.synchronous)), indent=2))


if __name__ == "__main__":
    main()

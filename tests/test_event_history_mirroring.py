# pyright: basic
"""Tests for mirroring event history snapshots via middleware."""

from __future__ import annotations

import asyncio
import multiprocessing
import sqlite3
from pathlib import Path
from typing import Any
from collections.abc import Sequence

import pytest

from bubus import BaseEvent, EventBus, SQLiteHistoryMirrorMiddleware


class HistoryTestEvent(BaseEvent):
    """Event for verifying middleware mirroring behaviour."""

    payload: str
    should_fail: bool = False


def _summarize_history(history: dict[str, BaseEvent[Any]]) -> list[dict[str, Any]]:
    """Collect comparable information about events stored in history."""
    summary: list[dict[str, Any]] = []
    for event in history.values():
        handler_results = [
            {
                'handler_name': result.handler_name.rsplit('.', 1)[-1],
                'status': result.status,
                'result': result.result,
                'error': repr(result.error) if result.error else None,
            }
            for result in sorted(event.event_results.values(), key=lambda r: r.handler_name)
        ]
        summary.append(
            {
                'event_type': event.event_type,
                'event_status': event.event_status,
                'event_path_length': len(event.event_path),
                'children': sorted(child.event_type for child in event.event_children),
                'handler_results': handler_results,
            }
        )
    return sorted(summary, key=lambda record: record['event_type'])


async def _run_scenario(
    *,
    middlewares: Sequence[Any] = (),
    should_fail: bool = False,
) -> list[dict[str, Any]]:
    """Execute a simple scenario and return the history summary."""
    bus = EventBus(middlewares=list(middlewares))

    async def ok_handler(event: HistoryTestEvent) -> str:
        return f'ok-{event.payload}'

    async def conditional_handler(event: HistoryTestEvent) -> str:
        if event.should_fail:
            raise RuntimeError('boom')
        return 'fine'

    bus.on('HistoryTestEvent', ok_handler)
    bus.on('HistoryTestEvent', conditional_handler)

    try:
        await bus.dispatch(HistoryTestEvent(payload='payload', should_fail=should_fail))
        await bus.wait_until_idle()
    finally:
        summary = _summarize_history(bus.event_history)
        await bus.stop()

    return summary


@pytest.mark.asyncio
async def test_sqlite_mirror_matches_inmemory_success(tmp_path: Path) -> None:
    db_path = tmp_path / 'events_success.sqlite'
    in_memory_result = await _run_scenario()
    sqlite_result = await _run_scenario(middlewares=[SQLiteHistoryMirrorMiddleware(db_path)])
    assert sqlite_result == in_memory_result

    conn = sqlite3.connect(db_path)
    event_phases = conn.execute('SELECT phase FROM events_log ORDER BY id').fetchall()
    conn.close()
    assert {phase for (phase,) in event_phases} >= {'pending', 'started', 'completed'}


@pytest.mark.asyncio
async def test_sqlite_mirror_matches_inmemory_error(tmp_path: Path) -> None:
    db_path = tmp_path / 'events_error.sqlite'
    in_memory_result = await _run_scenario(should_fail=True)
    sqlite_result = await _run_scenario(
        middlewares=[SQLiteHistoryMirrorMiddleware(db_path)],
        should_fail=True,
    )
    assert sqlite_result == in_memory_result

    conn = sqlite3.connect(db_path)
    phases = conn.execute('SELECT DISTINCT phase FROM events_log').fetchall()
    conn.close()
    assert {phase for (phase,) in phases} >= {'pending', 'started', 'completed'}


def _worker_dispatch(db_path: str, worker_id: int) -> None:
    """Process entrypoint for exercising concurrent writes."""

    async def run() -> None:
        middleware = SQLiteHistoryMirrorMiddleware(Path(db_path))
        bus = EventBus(name=f'WorkerBus{worker_id}', middlewares=[middleware])

        async def handler(event: HistoryTestEvent) -> str:
            return f'worker-{worker_id}'

        bus.on('HistoryTestEvent', handler)
        try:
            await bus.dispatch(HistoryTestEvent(payload=f'worker-{worker_id}'))
            await bus.wait_until_idle()
        finally:
            await bus.stop()

    asyncio.run(run())


def test_sqlite_mirror_supports_concurrent_processes(tmp_path: Path) -> None:
    db_path = tmp_path / 'shared_history.sqlite'
    ctx = multiprocessing.get_context('spawn')
    processes = [ctx.Process(target=_worker_dispatch, args=(str(db_path), idx)) for idx in range(3)]
    for proc in processes:
        proc.start()
    for proc in processes:
        proc.join(timeout=20)
        assert proc.exitcode == 0

    conn = sqlite3.connect(db_path)
    events = conn.execute('SELECT DISTINCT eventbus_name FROM events_log').fetchall()
    results_count = conn.execute('SELECT COUNT(*) FROM event_results_log').fetchone()
    conn.close()

    bus_labels = {name for (name,) in events}
    assert len(bus_labels) == 3
    for idx in range(3):
        assert any(label.startswith(f'WorkerBus{idx}#') and len(label.rsplit('#', 1)[-1]) == 4 for label in bus_labels)
    assert results_count is not None
    # Each worker records pending/started/completed for its single handler
    assert results_count[0] == 9

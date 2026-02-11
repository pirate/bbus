"""SQLite table bridge for forwarding events between runtimes.

Uses Python stdlib sqlite3 and polling for new rows.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

from uuid_extensions import uuid7str

from bubus.models import BaseEvent
from bubus.service import EventBus, EventPatternType, inside_handler_context


class SQLiteEventBridge:
    def __init__(
        self,
        path: str,
        *,
        table: str = 'bubus_events',
        poll_interval: float = 0.25,
        name: str | None = None,
    ):
        self.path = Path(path)
        self.table = table
        self.poll_interval = poll_interval
        self._inbound_bus = EventBus(name=name or f'SQLiteEventBridge_{uuid7str()[-8:]}')

        self._running = False
        self._listener_task: asyncio.Task[None] | None = None
        self._last_row_id = 0

    def on(self, event_pattern: EventPatternType, handler: Callable[[BaseEvent[Any]], Any]) -> None:
        self._ensure_started()
        self._inbound_bus.on(event_pattern, handler)

    async def dispatch(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        self._ensure_started()
        payload = event.model_dump(mode='json')
        await asyncio.to_thread(self._insert_payload, json.dumps(payload, separators=(',', ':')))

        if inside_handler_context.get():
            return None
        return event

    async def emit(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        return await self.dispatch(event)

    async def start(self) -> None:
        if self._running:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._init_db)
        self._last_row_id = await asyncio.to_thread(self._max_row_id)
        self._running = True
        self._listener_task = asyncio.create_task(self._listen_loop())

    async def close(self, *, clear: bool = True) -> None:
        self._running = False
        if self._listener_task is not None:
            self._listener_task.cancel()
            await asyncio.gather(self._listener_task, return_exceptions=True)
            self._listener_task = None
        await self._inbound_bus.stop(clear=clear)

    def _ensure_started(self) -> None:
        if self._running:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        self._listener_task = asyncio.create_task(self.start())

    async def _listen_loop(self) -> None:
        while self._running:
            try:
                rows = await asyncio.to_thread(self._fetch_new_rows, self._last_row_id)
                for row_id, payload in rows:
                    self._last_row_id = max(self._last_row_id, row_id)
                    try:
                        parsed = json.loads(payload)
                    except Exception:
                        continue
                    await self._dispatch_inbound_payload(parsed)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            await asyncio.sleep(self.poll_interval)

    async def _dispatch_inbound_payload(self, payload: Any) -> None:
        event = BaseEvent[Any].model_validate(payload)
        for bus in list(EventBus.all_instances):
            if not bus:
                continue
            existing = bus.event_history.get(event.event_id)
            if existing is not None:
                event = existing
                break
        self._inbound_bus.dispatch(event)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.execute('PRAGMA journal_mode=WAL')
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                f'''
                CREATE TABLE IF NOT EXISTS {self.table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                '''
            )
            conn.commit()

    def _insert_payload(self, payload: str) -> None:
        with self._connect() as conn:
            conn.execute(f'INSERT INTO {self.table} (payload) VALUES (?)', (payload,))
            conn.commit()

    def _max_row_id(self) -> int:
        with self._connect() as conn:
            row = conn.execute(f'SELECT COALESCE(MAX(id), 0) FROM {self.table}').fetchone()
            return int(row[0] if row else 0)

    def _fetch_new_rows(self, last_row_id: int) -> list[tuple[int, str]]:
        with self._connect() as conn:
            rows = conn.execute(
                f'SELECT id, payload FROM {self.table} WHERE id > ? ORDER BY id ASC',
                (last_row_id,),
            ).fetchall()
            return [(int(row[0]), str(row[1])) for row in rows]

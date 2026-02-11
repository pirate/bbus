"""SQLite flat-table bridge for forwarding events between runtimes.

Uses Python stdlib sqlite3 and polling for new rows.
Schema mirrors Postgres bridge shape:
- event_id (PRIMARY KEY)
- event_created_at (indexed)
- event_type (indexed)
- one TEXT column per event field storing JSON-serialized values
"""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

from uuid_extensions import uuid7str

from bubus.models import BaseEvent
from bubus.service import EventBus, EventPatternType, inside_handler_context

_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _validate_identifier(identifier: str, *, label: str) -> str:
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f'Invalid {label}: {identifier!r}. Use only [A-Za-z0-9_] and start with a letter/_')
    return identifier


class SQLiteEventBridge:
    def __init__(
        self,
        path: str,
        table: str = 'bubus_events',
        *,
        poll_interval: float = 0.25,
        name: str | None = None,
    ):
        self.path = Path(path)
        self.table = _validate_identifier(table, label='table name')
        self.poll_interval = poll_interval
        self._inbound_bus = EventBus(name=name or f'SQLiteEventBridge_{uuid7str()[-8:]}')

        self._running = False
        self._listener_task: asyncio.Task[None] | None = None
        self._last_seen_event_created_at = ''
        self._last_seen_event_id = ''
        self._table_columns: set[str] = {'event_id', 'event_created_at', 'event_type'}

    def on(self, event_pattern: EventPatternType, handler: Callable[[BaseEvent[Any]], Any]) -> None:
        self._ensure_started()
        self._inbound_bus.on(event_pattern, handler)

    async def dispatch(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        self._ensure_started()
        if not self._running:
            await self.start()

        payload = event.model_dump(mode='json')
        payload_keys = sorted(payload.keys())

        await asyncio.to_thread(self._ensure_columns, payload_keys)
        await asyncio.to_thread(self._upsert_payload, payload, payload_keys)

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
        await asyncio.to_thread(self._refresh_column_cache)
        await asyncio.to_thread(self._ensure_columns, ['event_id', 'event_created_at', 'event_type'])
        await asyncio.to_thread(self._ensure_base_indexes)
        await asyncio.to_thread(self._set_cursor_to_latest_row)
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
                rows = await asyncio.to_thread(
                    self._fetch_new_rows,
                    self._last_seen_event_created_at,
                    self._last_seen_event_id,
                )
                for row in rows:
                    event_created_at = str(row.get('event_created_at') or '')
                    event_id = str(row.get('event_id') or '')
                    if event_created_at or event_id:
                        self._last_seen_event_created_at = event_created_at
                        self._last_seen_event_id = event_id

                    payload: dict[str, Any] = {}
                    for key, raw_value in row.items():
                        if raw_value is None:
                            continue
                        if isinstance(raw_value, str):
                            try:
                                payload[key] = json.loads(raw_value)
                            except Exception:
                                payload[key] = raw_value
                        else:
                            payload[key] = raw_value

                    await self._dispatch_inbound_payload(payload)
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
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                f'''
                CREATE TABLE IF NOT EXISTS "{self.table}" (
                    "event_id" TEXT PRIMARY KEY,
                    "event_created_at" TEXT,
                    "event_type" TEXT
                )
                '''
            )
            conn.commit()

    def _refresh_column_cache(self) -> None:
        with self._connect() as conn:
            rows = conn.execute(f'PRAGMA table_info("{self.table}")').fetchall()
            self._table_columns = {str(row['name']) for row in rows}

    def _ensure_columns(self, keys: list[str]) -> None:
        for key in keys:
            _validate_identifier(key, label='event field name')

        missing_columns = [key for key in keys if key not in self._table_columns]
        if not missing_columns:
            return

        with self._connect() as conn:
            for key in missing_columns:
                conn.execute(f'ALTER TABLE "{self.table}" ADD COLUMN "{key}" TEXT')
                self._table_columns.add(key)
            conn.commit()

    def _ensure_base_indexes(self) -> None:
        event_created_at_index = f'{self.table}_event_created_at_idx'
        event_type_index = f'{self.table}_event_type_idx'

        with self._connect() as conn:
            conn.execute(f'CREATE INDEX IF NOT EXISTS "{event_created_at_index}" ON "{self.table}" ("event_created_at")')
            conn.execute(f'CREATE INDEX IF NOT EXISTS "{event_type_index}" ON "{self.table}" ("event_type")')
            conn.commit()

    def _upsert_payload(self, payload: dict[str, Any], payload_keys: list[str]) -> None:
        columns_sql = ', '.join(f'"{key}"' for key in payload_keys)
        placeholders_sql = ', '.join('?' for _ in payload_keys)
        values = [json.dumps(payload[key], separators=(',', ':')) if payload[key] is not None else None for key in payload_keys]

        update_fields = [key for key in payload_keys if key != 'event_id']
        if update_fields:
            updates_sql = ', '.join(f'"{key}" = excluded."{key}"' for key in update_fields)
            upsert_sql = (
                f'INSERT INTO "{self.table}" ({columns_sql}) VALUES ({placeholders_sql}) '
                f'ON CONFLICT("event_id") DO UPDATE SET {updates_sql}'
            )
        else:
            upsert_sql = (
                f'INSERT INTO "{self.table}" ({columns_sql}) VALUES ({placeholders_sql}) ON CONFLICT("event_id") DO NOTHING'
            )

        with self._connect() as conn:
            conn.execute(upsert_sql, values)
            conn.commit()

    def _set_cursor_to_latest_row(self) -> None:
        with self._connect() as conn:
            row = conn.execute(
                f'''
                SELECT
                    COALESCE("event_created_at", '') AS event_created_at,
                    COALESCE("event_id", '') AS event_id
                FROM "{self.table}"
                ORDER BY COALESCE("event_created_at", '') DESC, COALESCE("event_id", '') DESC
                LIMIT 1
                '''
            ).fetchone()
            if row is None:
                self._last_seen_event_created_at = ''
                self._last_seen_event_id = ''
                return
            self._last_seen_event_created_at = str(row['event_created_at'] or '')
            self._last_seen_event_id = str(row['event_id'] or '')

    def _fetch_new_rows(self, last_event_created_at: str, last_event_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                f'''
                SELECT *
                FROM "{self.table}"
                WHERE
                    COALESCE("event_created_at", '') > ?
                    OR (
                        COALESCE("event_created_at", '') = ?
                        AND COALESCE("event_id", '') > ?
                    )
                ORDER BY COALESCE("event_created_at", '') ASC, COALESCE("event_id", '') ASC
                ''',
                (last_event_created_at, last_event_created_at, last_event_id),
            ).fetchall()
            return [dict(row) for row in rows]

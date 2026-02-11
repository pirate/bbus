"""PostgreSQL LISTEN/NOTIFY + flat-table bridge for forwarding events.

Optional dependency: asyncpg

Usage:
    # table and channel both default to "bubus_events"
    bridge = PostgresEventBridge('postgresql://user:pass@localhost:5432/mydb')

    # explicit channel override
    bridge = PostgresEventBridge(
        'postgresql://user:pass@localhost:5432/mydb/events_table',
        channel='events_custom',
    )

Connection URL format:
    postgresql://user:pass@host:5432/dbname[/tablename]?sslmode=require

The optional trailing path segment is treated as the table name (defaults to
"bubus_events"). The bridge auto-creates
that table and auto-adds columns for new event fields as TEXT columns.
Each field value is stored as JSON text in its own column (flat row, no payload
JSON column).
"""

from __future__ import annotations

import asyncio
import importlib
import json
import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from uuid_extensions import uuid7str

from bubus.models import BaseEvent
from bubus.service import EventBus, EventPatternType, inside_handler_context

_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_INTERNAL_COLUMNS = {'row_id', 'inserted_at'}
_DEFAULT_POSTGRES_TABLE = 'bubus_events'
_DEFAULT_POSTGRES_CHANNEL = 'bubus_events'


def _validate_identifier(identifier: str, *, label: str) -> str:
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f'Invalid {label}: {identifier!r}. Use only [A-Za-z0-9_] and start with a letter/_')
    return identifier


def _parse_table_url(table_url: str) -> tuple[str, str]:
    """Split a postgres URL into (dsn_without_table, table_name).

    Example:
      postgresql://u:p@h:5432/mydb/mytable?sslmode=require
        -> (postgresql://u:p@h:5432/mydb?sslmode=require, mytable)
      postgresql://u:p@h:5432/mydb?sslmode=require
        -> (postgresql://u:p@h:5432/mydb?sslmode=require, bubus_events)
    """
    parsed = urlsplit(table_url)
    segments = [segment for segment in parsed.path.split('/') if segment]
    if len(segments) < 1:
        raise ValueError(
            'PostgresEventBridge URL must include at least database in path, e.g. '
            'postgresql://user:pass@host:5432/dbname[/tablename]'
        )

    db_name = segments[0]
    table_name = _validate_identifier(segments[1], label='table name') if len(segments) >= 2 else _DEFAULT_POSTGRES_TABLE

    dsn_path = f'/{db_name}'
    dsn = urlunsplit((parsed.scheme, parsed.netloc, dsn_path, parsed.query, parsed.fragment))
    return dsn, table_name


class PostgresEventBridge:
    def __init__(self, table_url: str, channel: str | None = None, *, name: str | None = None):
        self.table_url = table_url
        self.dsn, self.table = _parse_table_url(table_url)
        derived_channel = channel or _DEFAULT_POSTGRES_CHANNEL
        self.channel = _validate_identifier(derived_channel[:63], label='channel name')
        self._inbound_bus = EventBus(name=name or f'PostgresEventBridge_{uuid7str()[-8:]}')

        self._running = False
        self._conn: Any | None = None
        self._listener_callback: Any | None = None
        self._table_columns: set[str] = {'event_id'}

    def on(self, event_pattern: EventPatternType, handler: Callable[[BaseEvent[Any]], Any]) -> None:
        self._ensure_started()
        self._inbound_bus.on(event_pattern, handler)

    async def dispatch(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        self._ensure_started()
        if self._conn is None:
            await self.start()

        payload = event.model_dump(mode='json')
        payload_keys = sorted(payload.keys())
        await self._ensure_columns(payload_keys)

        columns_sql = ', '.join(f'"{key}"' for key in payload_keys)
        placeholders_sql = ', '.join(f'${index}' for index in range(1, len(payload_keys) + 1))
        values = [json.dumps(payload[key], separators=(',', ':')) if payload[key] is not None else None for key in payload_keys]

        update_fields = [key for key in payload_keys if key != 'event_id']
        if update_fields:
            updates_sql = ', '.join(f'"{key}" = EXCLUDED."{key}"' for key in update_fields)
            upsert_sql = (
                f'INSERT INTO "{self.table}" ({columns_sql}) VALUES ({placeholders_sql}) '
                f'ON CONFLICT ("event_id") DO UPDATE SET {updates_sql}'
            )
        else:
            upsert_sql = (
                f'INSERT INTO "{self.table}" ({columns_sql}) VALUES ({placeholders_sql}) '
                'ON CONFLICT ("event_id") DO NOTHING'
            )

        assert self._conn is not None
        await self._conn.execute(upsert_sql, *values)
        await self._conn.execute('SELECT pg_notify($1, $2)', self.channel, event.event_id)

        if inside_handler_context.get():
            return None
        return event

    async def emit(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        return await self.dispatch(event)

    async def start(self) -> None:
        if self._running:
            return

        asyncpg = self._load_asyncpg()
        self._conn = await asyncpg.connect(self.dsn)
        await self._ensure_table_exists()
        await self._refresh_column_cache()

        async def _dispatch_event_id(event_id: str) -> None:
            try:
                await self._dispatch_by_event_id(event_id)
            except Exception:
                return

        def _listener(_connection: Any, _pid: int, _channel: str, payload: str) -> None:
            asyncio.create_task(_dispatch_event_id(payload))

        self._listener_callback = _listener
        await self._conn.add_listener(self.channel, _listener)
        self._running = True

    async def close(self, *, clear: bool = True) -> None:
        self._running = False
        if self._conn is not None:
            if self._listener_callback is not None:
                try:
                    await self._conn.remove_listener(self.channel, self._listener_callback)
                except Exception:
                    pass
                self._listener_callback = None
            await self._conn.close()
            self._conn = None
        await self._inbound_bus.stop(clear=clear)

    def _ensure_started(self) -> None:
        if self._running:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        asyncio.create_task(self.start())

    async def _dispatch_by_event_id(self, event_id: str) -> None:
        assert self._conn is not None
        row = await self._conn.fetchrow(f'SELECT * FROM "{self.table}" WHERE "event_id" = $1', event_id)
        if row is None:
            return

        payload: dict[str, Any] = {}
        for key, raw_value in dict(row).items():
            if key in _INTERNAL_COLUMNS or raw_value is None:
                continue
            try:
                payload[key] = json.loads(raw_value)
            except Exception:
                payload[key] = raw_value

        await self._dispatch_inbound_payload(payload)

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

    async def _ensure_table_exists(self) -> None:
        assert self._conn is not None
        await self._conn.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{self.table}" (
                "row_id" BIGSERIAL PRIMARY KEY,
                "inserted_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                "event_id" TEXT NOT NULL UNIQUE
            )
            '''
        )

    async def _refresh_column_cache(self) -> None:
        assert self._conn is not None
        rows = await self._conn.fetch(
            '''
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1
            ''',
            self.table,
        )
        self._table_columns = {str(row['column_name']) for row in rows}

    async def _ensure_columns(self, keys: list[str]) -> None:
        for key in keys:
            _validate_identifier(key, label='event field name')

        missing_columns = [key for key in keys if key not in self._table_columns]
        if not missing_columns:
            return

        assert self._conn is not None
        for key in missing_columns:
            await self._conn.execute(f'ALTER TABLE "{self.table}" ADD COLUMN IF NOT EXISTS "{key}" TEXT')
            self._table_columns.add(key)

    @staticmethod
    def _load_asyncpg() -> Any:
        try:
            return importlib.import_module('asyncpg')
        except ModuleNotFoundError as exc:
            raise RuntimeError('PostgresEventBridge requires optional dependency: pip install asyncpg') from exc

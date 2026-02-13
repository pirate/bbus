"""Async helpers for reading the SQLite event history."""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from typing import Any

from .config import resolve_db_path


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(resolve_db_path(), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


@dataclass
class HistorySchemaStatus:
    events_table_exists: bool
    results_table_exists: bool


async def fetch_schema_status() -> HistorySchemaStatus:
    return await asyncio.to_thread(_fetch_schema_status_sync)


def _fetch_schema_status_sync() -> HistorySchemaStatus:
    conn = _connect()
    try:
        return HistorySchemaStatus(
            events_table_exists=_table_exists(conn, 'events_log'),
            results_table_exists=_table_exists(conn, 'event_results_log'),
        )
    finally:
        conn.close()


async def fetch_events(limit: int = 50) -> list[dict[str, Any]]:
    return await asyncio.to_thread(_fetch_events_sync, limit)


def _fetch_events_sync(limit: int) -> list[dict[str, Any]]:
    conn = _connect()
    try:
        if not _table_exists(conn, 'events_log'):
            return []
        rows = conn.execute(
            """
            SELECT id, event_id, event_type, event_status, eventbus_id, eventbus_name, phase, event_json, inserted_at
            FROM events_log
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


async def fetch_results(limit: int = 50) -> list[dict[str, Any]]:
    return await asyncio.to_thread(_fetch_results_sync, limit)


def _fetch_results_sync(limit: int) -> list[dict[str, Any]]:
    conn = _connect()
    try:
        if not _table_exists(conn, 'event_results_log'):
            return []
        rows = conn.execute(
            """
            SELECT id, event_id, event_result_id, handler_name, status, phase, result_repr, error_repr,
                   eventbus_id, eventbus_name, event_type, event_result_json, inserted_at
            FROM event_results_log
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


@dataclass
class HistoryStreamState:
    last_event_id: int = 0
    last_result_id: int = 0


async def stream_new_rows(state: HistoryStreamState) -> dict[str, list[dict[str, Any]]]:
    """Return new rows added since the last call."""
    updates = await asyncio.to_thread(_stream_new_rows_sync, state)
    return updates


def _stream_new_rows_sync(state: HistoryStreamState) -> dict[str, list[dict[str, Any]]]:
    conn = _connect()
    try:
        events: list[sqlite3.Row] = []
        results: list[sqlite3.Row] = []

        if _table_exists(conn, 'events_log'):
            events = conn.execute(
                """
                SELECT id, event_id, event_type, event_status, eventbus_id, eventbus_name, phase, event_json, inserted_at
                FROM events_log
                WHERE id > ?
                ORDER BY id ASC
                """,
                (state.last_event_id,),
            ).fetchall()

        if _table_exists(conn, 'event_results_log'):
            results = conn.execute(
                """
                SELECT id, event_id, event_result_id, handler_name, status, phase, result_repr, error_repr,
                       eventbus_id, eventbus_name, event_type, event_result_json, inserted_at
                FROM event_results_log
                WHERE id > ?
                ORDER BY id ASC
                """,
                (state.last_result_id,),
            ).fetchall()

        if events:
            state.last_event_id = int(events[-1]['id'])
        if results:
            state.last_result_id = int(results[-1]['id'])

        return {
            'events': [dict(row) for row in events],
            'results': [dict(row) for row in results],
        }
    finally:
        conn.close()

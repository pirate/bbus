"""Reusable EventBus middleware helpers."""

from __future__ import annotations

import asyncio
import importlib
import logging
import sqlite3
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

from bubus.event_handler import EventHandler
from bubus.event_result import EventResult
from bubus.logging import log_eventbus_tree
from bubus.base_event import BaseEvent, EventStatus

if TYPE_CHECKING:
    from bubus.event_bus import EventBus

__all__ = [
    'EventBusMiddleware',
    'OtelTracingMiddleware',
    'BusHandlerRegisteredEvent',
    'BusHandlerUnregisteredEvent',
    'WALEventBusMiddleware',
    'LoggerEventBusMiddleware',
    'SQLiteHistoryMirrorMiddleware',
    'SyntheticErrorEventMiddleware',
    'SyntheticReturnEventMiddleware',
    'SyntheticHandlerChangeEventMiddleware',
]

logger = logging.getLogger('bubus.middleware')
_SYNTHETIC_EVENT_SUFFIXES = ('ErrorEvent', 'ResultEvent')


class EventBusMiddleware:
    """Hookable lifecycle interface for observing or extending EventBus execution.

    Hooks:
        on_event_change(eventbus, event, status): Called on event state transitions
        on_event_result_change(eventbus, event, event_result, status): Called on EventResult state transitions
        on_handler_change(eventbus, handler, registered): Called when handlers are added/removed via on()/off()

    Status values: EventStatus.PENDING, STARTED, COMPLETED, ERROR
    """

    async def on_event_change(self, eventbus: 'EventBus', event: BaseEvent[Any], status: EventStatus) -> None:
        """Called on event state transitions (pending, started, completed, error)."""

    async def on_event_result_change(
        self,
        eventbus: 'EventBus',
        event: BaseEvent[Any],
        event_result: EventResult[Any],
        status: EventStatus,
    ) -> None:
        """Called on EventResult state transitions (pending, started, completed, error)."""

    async def on_handler_change(self, eventbus: 'EventBus', handler: EventHandler, registered: bool) -> None:
        """Called when handlers are added (registered=True) or removed (registered=False)."""


class OtelTracingMiddleware(EventBusMiddleware):
    """Emit OpenTelemetry spans for events/handlers.

    Setup example (with optional Sentry export):

    ```python
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    import sentry_sdk

    provider = TracerProvider()
    # provider.add_span_processor(BatchSpanProcessor(...your OTLP exporter...))
    # provider.add_span_processor(sentry_sdk.integrations.opentelemetry.SentrySpanProcessor())  # optional
    trace.set_tracer_provider(provider)

    bus = EventBus(middlewares=[OtelTracingMiddleware()])
    ```
    """

    def __init__(self, tracer: Any | None = None, trace_api: Any | None = None):
        self._trace_api = trace_api
        self._status_cls = None
        self._status_code = None
        if self._trace_api is None:
            try:
                self._trace_api = importlib.import_module('opentelemetry.trace')
            except Exception:
                self._trace_api = None
        if tracer is None:
            if self._trace_api is None:
                raise RuntimeError(
                    'OtelTracingMiddleware requires "opentelemetry-api". Install it with: pip install opentelemetry-api'
                )
            tracer = self._trace_api.get_tracer('bubus.middleware.otel')
        try:
            status_mod = importlib.import_module('opentelemetry.trace.status')
            self._status_cls = getattr(status_mod, 'Status', None)
            self._status_code = getattr(status_mod, 'StatusCode', None)
        except Exception:
            pass
        if tracer is None:
            raise ImportError('OpenTelemetry tracer unavailable')
        self._tracer = tracer
        self._event_spans: dict[tuple[str, str], Any] = {}
        self._handler_spans: dict[tuple[str, str, str], Any] = {}

    @staticmethod
    def _event_key(eventbus: EventBus, event: BaseEvent[Any]) -> tuple[str, str]:
        return (eventbus.id, event.event_id)

    @staticmethod
    def _handler_key(eventbus: EventBus, event: BaseEvent[Any], event_result: EventResult[Any]) -> tuple[str, str, str]:
        return (eventbus.id, event.event_id, event_result.handler_id)

    def _start_span(self, name: str, parent_span: Any | None = None) -> Any:
        if parent_span is not None and self._trace_api is not None:
            try:
                return self._tracer.start_span(name, context=self._trace_api.set_span_in_context(parent_span))
            except Exception:
                pass
        return self._tracer.start_span(name)

    def _find_parent_span(self, event: BaseEvent[Any]) -> Any | None:
        if not event.event_parent_id:
            return None
        from bubus.event_bus import EventBus

        for bus in list(EventBus.all_instances):
            if not bus or event.event_parent_id not in bus.event_history:
                continue
            parent_event = bus.event_history[event.event_parent_id]
            for parent_result in parent_event.event_results.values():
                if any(child.event_id == event.event_id for child in parent_result.event_children):
                    parent_handler_span = self._handler_spans.get((bus.id, parent_event.event_id, parent_result.handler_id))
                    if parent_handler_span is not None:
                        return parent_handler_span
            return self._event_spans.get((bus.id, parent_event.event_id))
        return None

    def _ensure_event_span(self, eventbus: EventBus, event: BaseEvent[Any]) -> Any:
        key = self._event_key(eventbus, event)
        existing = self._event_spans.get(key)
        if existing is not None:
            return existing
        span = self._start_span(f'bubus.event.{event.event_type}', parent_span=self._find_parent_span(event))
        span.set_attribute('bubus.kind', 'event')
        span.set_attribute('bubus.event_id', event.event_id)
        span.set_attribute('bubus.event_type', event.event_type)
        span.set_attribute('bubus.bus_id', eventbus.id)
        span.set_attribute('bubus.bus_name', eventbus.label)
        if event.event_parent_id:
            span.set_attribute('bubus.event_parent_id', event.event_parent_id)
        self._event_spans[key] = span
        return span

    async def on_event_change(self, eventbus: EventBus, event: BaseEvent[Any], status: EventStatus) -> None:
        if status == EventStatus.STARTED:
            self._ensure_event_span(eventbus, event)
            return
        if status == EventStatus.COMPLETED:
            key = self._event_key(eventbus, event)
            span = self._event_spans.pop(key, None)
            if span is None:
                span = self._ensure_event_span(eventbus, event)
                self._event_spans.pop(key, None)
            span.end()

    async def on_event_result_change(
        self,
        eventbus: EventBus,
        event: BaseEvent[Any],
        event_result: EventResult[Any],
        status: EventStatus,
    ) -> None:
        key = self._handler_key(eventbus, event, event_result)
        if status == EventStatus.STARTED:
            if key in self._handler_spans:
                return
            parent_event_span = self._ensure_event_span(eventbus, event)
            span = self._start_span(f'bubus.handler.{event_result.handler_name}', parent_span=parent_event_span)
            span.set_attribute('bubus.kind', 'handler')
            span.set_attribute('bubus.event_id', event.event_id)
            span.set_attribute('bubus.event_type', event.event_type)
            span.set_attribute('bubus.handler_id', event_result.handler_id)
            span.set_attribute('bubus.handler_name', event_result.handler_name)
            span.set_attribute('bubus.bus_id', eventbus.id)
            span.set_attribute('bubus.bus_name', eventbus.label)
            self._handler_spans[key] = span
            return
        if status != EventStatus.COMPLETED:
            return
        span = self._handler_spans.pop(key, None)
        if span is None:
            return
        error = event_result.error
        if error is not None:
            span.record_exception(error)
            if self._status_cls and self._status_code and hasattr(span, 'set_status'):
                span.set_status(self._status_cls(self._status_code.ERROR, str(error)))
        span.end()


class BusHandlerRegisteredEvent(BaseEvent):
    """Synthetic event emitted when a handler is added with EventBus.on()."""

    handler: EventHandler


class BusHandlerUnregisteredEvent(BaseEvent):
    """Synthetic event emitted when a handler is removed with EventBus.off()."""

    handler: EventHandler


class SyntheticErrorEvent(BaseEvent):
    """Synthetic event payload used by SyntheticErrorEventMiddleware."""

    error: Any
    error_type: str


class SyntheticReturnEvent(BaseEvent):
    """Synthetic event payload used by SyntheticReturnEventMiddleware."""

    data: Any


class SyntheticErrorEventMiddleware(EventBusMiddleware):
    """Use in `EventBus(middlewares=[...])` to emit `{OriginalEventType}ErrorEvent` on handler failures."""

    async def on_event_result_change(
        self,
        eventbus: EventBus,
        event: BaseEvent[Any],
        event_result: EventResult[Any],
        status: EventStatus,
    ) -> None:
        if status != EventStatus.COMPLETED or event_result.error is None or event.event_type.endswith(_SYNTHETIC_EVENT_SUFFIXES):
            return
        try:
            eventbus.dispatch(
                SyntheticErrorEvent(
                    event_type=f'{event.event_type}ErrorEvent',
                    error=event_result.error,
                    error_type=type(event_result.error).__name__,
                )
            )
        except Exception as exc:  # pragma: no cover
            logger.error('❌ %s Failed to emit synthetic error event for %s: %s', eventbus, event.event_id, exc)


class SyntheticReturnEventMiddleware(EventBusMiddleware):
    """Use in `EventBus(middlewares=[...])` to emit `{OriginalEventType}ResultEvent` for non-None returns."""

    async def on_event_result_change(
        self,
        eventbus: EventBus,
        event: BaseEvent[Any],
        event_result: EventResult[Any],
        status: EventStatus,
    ) -> None:
        result_value = event_result.result
        if (
            status != EventStatus.COMPLETED
            or event_result.error is not None
            or result_value is None
            or isinstance(result_value, BaseEvent)
            or event.event_type.endswith(_SYNTHETIC_EVENT_SUFFIXES)
        ):
            return
        try:
            eventbus.dispatch(SyntheticReturnEvent(event_type=f'{event.event_type}ResultEvent', data=result_value))
        except Exception as exc:  # pragma: no cover
            logger.error('❌ %s Failed to emit synthetic result event for %s: %s', eventbus, event.event_id, exc)


class SyntheticHandlerChangeEventMiddleware(EventBusMiddleware):
    """Use in `EventBus(middlewares=[...])` to emit handler metadata events on .on() and .off()."""

    async def on_handler_change(self, eventbus: EventBus, handler: EventHandler, registered: bool) -> None:
        try:
            if registered:
                eventbus.dispatch(BusHandlerRegisteredEvent(handler=handler.model_copy(deep=True)))
            else:
                eventbus.dispatch(BusHandlerUnregisteredEvent(handler=handler.model_copy(deep=True)))
        except Exception as exc:  # pragma: no cover
            logger.error('❌ %s Failed to emit synthetic handler change event for handler %s: %s', eventbus, handler.id, exc)


class WALEventBusMiddleware(EventBusMiddleware):
    """Persist completed events to a JSONL write-ahead log."""

    def __init__(self, wal_path: Path | str):
        self.wal_path = Path(wal_path)
        self.wal_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    async def on_event_change(self, eventbus: EventBus, event: BaseEvent[Any], status: EventStatus) -> None:
        if status != EventStatus.COMPLETED:
            return
        try:
            event_json = event.model_dump_json()  # pyright: ignore[reportUnknownMemberType]
            await asyncio.to_thread(self._write_line, event_json + '\n')
        except Exception as exc:  # pragma: no cover
            logger.error('❌ %s Failed to save event %s to WAL: %s', eventbus, event.event_id, exc)

    def _write_line(self, line: str) -> None:
        with self._lock:
            with self.wal_path.open('a', encoding='utf-8') as fp:
                fp.write(line)


class LoggerEventBusMiddleware(EventBusMiddleware):
    """Log completed events to stdout and optionally to a file."""

    def __init__(self, log_path: Path | str | None = None):
        self.log_path = Path(log_path) if log_path is not None else None
        if self.log_path is not None:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)

    async def on_event_change(self, eventbus: EventBus, event: BaseEvent[Any], status: EventStatus) -> None:
        if status != EventStatus.COMPLETED:
            return

        summary = event.event_log_safe_summary()
        logger.info('✅ %s completed event %s', eventbus, summary)
        line = f'[{eventbus.label}] {summary}\n'

        if self.log_path is not None:
            await asyncio.to_thread(self._write_line, line)
        print(line.rstrip('\n'), flush=True)

        if logger.isEnabledFor(logging.DEBUG):
            log_eventbus_tree(eventbus)

    def _write_line(self, line: str) -> None:
        with self.log_path.open('a', encoding='utf-8') as fp:  # type: ignore[union-attr]
            fp.write(line)


class SQLiteHistoryMirrorMiddleware(EventBusMiddleware):
    """Mirror event and handler snapshots into append-only SQLite tables."""

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
        self._init_db()

    def __del__(self):
        try:
            self._conn.close()
        except Exception:
            pass

    async def on_event_change(self, eventbus: EventBus, event: BaseEvent[Any], status: EventStatus) -> None:
        event_json = event.model_dump_json()
        await asyncio.to_thread(
            self._insert_event_snapshot,
            eventbus,
            event.event_id,
            event.event_type,
            str(event.event_status),
            str(status),
            event_json,
        )

    async def on_event_result_change(
        self,
        eventbus: EventBus,
        event: BaseEvent[Any],
        event_result: EventResult[Any],
        status: EventStatus,
    ) -> None:
        error_repr = repr(event_result.error) if event_result.error is not None else None
        result_repr: str | None = None
        if event_result.result is not None and event_result.error is None:
            try:
                result_repr = repr(event_result.result)
            except Exception:
                result_repr = '<unrepr-able>'

        try:
            event_result_json = event_result.model_dump_json()
        except Exception:
            event_result_json = None

        await asyncio.to_thread(
            self._insert_event_result_snapshot,
            event_result.id,
            event_result.event_id,
            event_result.handler_id,
            event_result.handler_name,
            eventbus.id,
            eventbus.label,
            event.event_type,
            event_result.status,
            str(status),
            result_repr,
            error_repr,
            event_result_json,
        )

    def _init_db(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_status TEXT NOT NULL,
                    eventbus_id TEXT NOT NULL,
                    eventbus_name TEXT NOT NULL,
                    phase TEXT,
                    event_json TEXT NOT NULL,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_results_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_result_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    handler_id TEXT NOT NULL,
                    handler_name TEXT NOT NULL,
                    eventbus_id TEXT NOT NULL,
                    eventbus_name TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    phase TEXT,
                    result_repr TEXT,
                    error_repr TEXT,
                    event_result_json TEXT,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.execute('PRAGMA journal_mode=WAL')
            self._conn.execute('PRAGMA synchronous=NORMAL')

    def _insert_event_snapshot(
        self,
        eventbus: EventBus,
        event_id: str,
        event_type: str,
        event_status: str,
        phase: str | None,
        event_json: str,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO events_log (
                    event_id,
                    event_type,
                    event_status,
                    eventbus_id,
                    eventbus_name,
                    phase,
                    event_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    event_type,
                    event_status,
                    eventbus.id,
                    eventbus.label,
                    phase,
                    event_json,
                ),
            )
            self._conn.commit()

    def _insert_event_result_snapshot(
        self,
        event_result_id: str,
        event_id: str,
        handler_id: str,
        handler_name: str,
        eventbus_id: str,
        eventbus_name: str,
        event_type: str,
        status: str,
        phase: str | None,
        result_repr: str | None,
        error_repr: str | None,
        event_result_json: str | None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO event_results_log (
                    event_result_id,
                    event_id,
                    handler_id,
                    handler_name,
                    eventbus_id,
                    eventbus_name,
                    event_type,
                    status,
                    phase,
                    result_repr,
                    error_repr,
                    event_result_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_result_id,
                    event_id,
                    handler_id,
                    handler_name,
                    eventbus_id,
                    eventbus_name,
                    event_type,
                    status,
                    phase,
                    result_repr,
                    error_repr,
                    event_result_json,
                ),
            )
            self._conn.commit()

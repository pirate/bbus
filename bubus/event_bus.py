import asyncio
import contextvars
import inspect
import logging
import warnings
import weakref
from collections import defaultdict, deque
from collections.abc import Callable, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, TypeVar, cast, overload
from uuid import UUID

from uuid_extensions import uuid7str  # pyright: ignore[reportMissingImports, reportUnknownVariableType]

uuid7str: Callable[[], str] = uuid7str  # pyright: ignore

from bubus.base_event import (
    BUBUS_LOGGING_LEVEL,
    BaseEvent,
    EventConcurrencyMode,
    EventHandlerCompletionMode,
    EventHandlerConcurrencyMode,
    EventStatus,
    PythonIdentifierStr,
    PythonIdStr,
    T_Event,
    T_EventResultType,
    UUIDStr,
)
from bubus.event_handler import (
    AsyncEventHandlerClassMethod,
    AsyncEventHandlerFunc,
    AsyncEventHandlerMethod,
    EventHandler,
    EventHandlerCallable,
    EventHandlerClassMethod,
    EventHandlerFunc,
    EventHandlerMethod,
)
from bubus.event_history import EventHistory
from bubus.event_result import EventResult
from bubus.helpers import CleanShutdownQueue, QueueShutDown, log_filtered_traceback
from bubus.lock_manager import LockManager, ReentrantLock
from bubus.middlewares import EventBusMiddleware

logger = logging.getLogger('bubus')
logger.setLevel(BUBUS_LOGGING_LEVEL)

T_ExpectedEvent = TypeVar('T_ExpectedEvent', bound=BaseEvent[Any])

EventPatternType = PythonIdentifierStr | Literal['*'] | type[BaseEvent[Any]]


@dataclass(slots=True, eq=False)
class _FindWaiter:
    event_key: str
    matches: Callable[[BaseEvent[Any]], bool]
    future: asyncio.Future[BaseEvent[Any] | None]
    timeout_handle: asyncio.TimerHandle | None = None


# Context variable to track the current event being processed (for setting event_parent_id from inside a child event)
_current_event_context: ContextVar[BaseEvent[Any] | None] = ContextVar('current_event', default=None)
# Context variable to track the current handler ID (for tracking child events)
_current_handler_id_context: ContextVar[str | None] = ContextVar('current_handler_id', default=None)


def get_current_event() -> BaseEvent[Any] | None:
    """Return the currently active event in this async context, if any."""
    return _current_event_context.get()


def get_current_handler_id() -> str | None:
    """Return the currently active handler id in this async context, if any."""
    return _current_handler_id_context.get()


def in_handler_context() -> bool:
    """Return True when called from inside an executing handler context."""
    return get_current_handler_id() is not None


class EventBus:
    """
    Async event bus with write-ahead logging and guaranteed FIFO processing.

    Features:
    - Enqueue events synchronously, await their results using 'await Event()'
    - FIFP Write-ahead logging with UUIDs and timestamps,
    - Serial event processing, configurable handler concurrency per event ('serial' | 'parallel')
    """

    # Track all EventBus instances (using weakrefs to allow garbage collection)
    all_instances: weakref.WeakSet['EventBus'] = weakref.WeakSet()

    # Class Attributes
    name: PythonIdentifierStr = 'EventBus'
    event_concurrency: EventConcurrencyMode = EventConcurrencyMode.BUS_SERIAL
    event_timeout: float | None = 60.0
    event_slow_timeout: float | None = 300.0
    event_handler_concurrency: EventHandlerConcurrencyMode = EventHandlerConcurrencyMode.SERIAL
    event_handler_completion: EventHandlerCompletionMode = EventHandlerCompletionMode.ALL
    event_handler_slow_timeout: float | None = 30.0
    event_handler_detect_file_paths: bool = True
    max_history_size: int | None = 100
    max_history_drop: bool = False

    # Runtime State
    id: UUIDStr = '00000000-0000-0000-0000-000000000000'
    handlers: dict[PythonIdStr, EventHandler]
    handlers_by_key: dict[str, list[PythonIdStr]]
    pending_event_queue: CleanShutdownQueue[BaseEvent[Any]] | None
    event_history: EventHistory[BaseEvent[Any]]

    _is_running: bool = False
    _runloop_task: asyncio.Task[None] | None = None
    _parallel_event_tasks: set[asyncio.Task[None]]
    _on_idle: asyncio.Event | None = None
    _active_event_ids: set[str]
    _processing_event_ids: set[str]
    _warned_about_dropping_uncompleted_events: bool
    _duplicate_handler_name_check_limit: int = 256
    _pending_handler_changes: list[tuple[EventHandler, bool]]
    _find_waiters: set[_FindWaiter]
    _lock_for_event_bus_serial: ReentrantLock
    locks: LockManager

    def __init__(
        self,
        name: PythonIdentifierStr | None = None,
        event_concurrency: EventConcurrencyMode | str | None = None,
        event_handler_concurrency: EventHandlerConcurrencyMode | str = EventHandlerConcurrencyMode.SERIAL,
        event_handler_completion: EventHandlerCompletionMode | str = EventHandlerCompletionMode.ALL,
        max_history_size: int | None = 50,  # Keep only 50 events in history
        max_history_drop: bool = False,
        event_timeout: float | None = 60.0,
        event_slow_timeout: float | None = 300.0,
        event_handler_slow_timeout: float | None = 30.0,
        event_handler_detect_file_paths: bool = True,
        middlewares: Sequence[EventBusMiddleware] | None = None,
        id: UUIDStr | str | None = None,
    ):
        self.id = str(UUID(str(id))) if id is not None else uuid7str()
        self.name = name or f'{self.__class__.__name__}_{self.id[-8:]}'
        assert self.name.isidentifier(), f'EventBus name must be a unique identifier string, got: {self.name}'

        # Force garbage collection to clean up any dead EventBus instances in the WeakSet
        # gc.collect()  # Commented out - this is expensive and causes 5s delays when creating many EventBus instances

        # Check for name uniqueness among existing instances
        # We'll collect potential conflicts and check if they're still alive
        original_name = self.name
        conflicting_buses: list[EventBus] = []

        for existing_bus in list(EventBus.all_instances):  # Make a list copy to avoid modification during iteration
            if existing_bus is not self and existing_bus.name == self.name:
                # Since stop() renames buses to _stopped_{id}, any bus with a matching
                # user-specified name is either running or never-started - both should
                # be considered conflicts. This makes name conflict detection deterministic.
                conflicting_buses.append(existing_bus)

        # If we found conflicting buses, auto-generate a unique suffix
        if conflicting_buses:
            # Generate a unique suffix using the last 8 chars of a UUID
            unique_suffix = uuid7str()[-8:]
            self.name = f'{original_name}_{unique_suffix}'

            warnings.warn(
                f'⚠️ EventBus with name "{original_name}" already exists. '
                f'Auto-generated unique name: "{self.name}" to avoid conflicts. '
                f'Consider using unique names or stop(clear=True) on unused buses.',
                UserWarning,
                stacklevel=2,
            )

        self.pending_event_queue = None
        self.event_history = EventHistory()
        self.handlers = {}
        self.handlers_by_key = defaultdict(list)
        self._lock_for_event_bus_serial = ReentrantLock()
        self.locks = LockManager()
        self._parallel_event_tasks = set()
        try:
            self.event_concurrency = EventConcurrencyMode(event_concurrency or EventConcurrencyMode.BUS_SERIAL)
        except ValueError as exc:
            raise AssertionError(
                f'event_concurrency must be "global-serial", "bus-serial", or "parallel", got: {event_concurrency!r}'
            ) from exc
        try:
            self.event_handler_concurrency = EventHandlerConcurrencyMode(
                event_handler_concurrency or EventHandlerConcurrencyMode.SERIAL
            )
        except ValueError as exc:
            raise AssertionError(
                f'event_handler_concurrency must be "serial" or "parallel", got: {event_handler_concurrency!r}'
            ) from exc
        try:
            self.event_handler_completion = EventHandlerCompletionMode(event_handler_completion or EventHandlerCompletionMode.ALL)
        except ValueError as exc:
            raise AssertionError(f'event_handler_completion must be "all" or "first", got: {event_handler_completion!r}') from exc
        self.event_timeout = event_timeout
        self.event_slow_timeout = event_slow_timeout
        self.event_handler_slow_timeout = event_handler_slow_timeout
        self.event_handler_detect_file_paths = bool(event_handler_detect_file_paths)
        assert self.event_timeout is None or self.event_timeout > 0, (
            f'event_timeout must be > 0 or None, got: {self.event_timeout!r}'
        )
        assert self.event_slow_timeout is None or self.event_slow_timeout > 0, (
            f'event_slow_timeout must be > 0 or None, got: {self.event_slow_timeout!r}'
        )
        assert self.event_handler_slow_timeout is None or self.event_handler_slow_timeout > 0, (
            f'event_handler_slow_timeout must be > 0 or None, got: {self.event_handler_slow_timeout!r}'
        )
        self._on_idle = None
        self.middlewares: list[EventBusMiddleware] = list(middlewares or [])
        self._active_event_ids = set()
        self._processing_event_ids = set()
        self._warned_about_dropping_uncompleted_events = False
        self._pending_handler_changes = []
        self._find_waiters = set()

        # Memory leak prevention settings
        self.max_history_size = max_history_size
        self.max_history_drop = max_history_drop

        # Register this instance
        EventBus.all_instances.add(self)

    def __del__(self):
        """Auto-cleanup on garbage collection"""
        # Most cleanup should have been done by the event loop close hook
        # This is just a fallback for any remaining cleanup

        # Signal the run loop to stop
        self._is_running = False

        # Our custom queue handles cleanup properly in shutdown()
        # No need for manual cleanup here

        # Check total memory usage across all EventBus instances
        try:
            self._check_total_memory_usage()
        except Exception:
            # Don't let memory check errors prevent cleanup
            pass

    def __str__(self) -> str:
        icon = '🟢' if self._is_running else '🔴'
        queue_size = self.pending_event_queue.qsize() if self.pending_event_queue else 0
        return f'{self.label}{icon}(queue={queue_size} active={len(self._active_event_ids)} history={len(self.event_history)} handlers={len(self.handlers)})'

    @property
    def label(self) -> str:
        return f'{self.name}#{self.id[-4:]}'

    def __repr__(self) -> str:
        return str(self)

    @property
    def event_bus_serial_lock(self) -> ReentrantLock:
        """Public accessor for the bus-serial event lock used by LockManager."""
        return self._lock_for_event_bus_serial

    async def _on_event_change(self, event: BaseEvent[Any], status: EventStatus) -> None:
        if not self.middlewares:
            return
        for middleware in self.middlewares:
            await middleware.on_event_change(self, event, status)

    async def _on_event_result_change(self, event: BaseEvent[Any], event_result: EventResult[Any], status: EventStatus) -> None:
        if not self.middlewares:
            return
        for middleware in self.middlewares:
            await middleware.on_event_result_change(self, event, event_result, status)

    async def _on_handler_change(self, handler: EventHandler, registered: bool) -> None:
        if not self.middlewares:
            return
        for middleware in self.middlewares:
            await middleware.on_handler_change(self, handler, registered)

    def _notify_handler_change(self, handler: EventHandler, registered: bool) -> None:
        if not self.middlewares:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Preserve .on()/.off() notifications registered before an event loop starts.
            self._pending_handler_changes.append((handler.model_copy(deep=True), registered))
            return
        loop.create_task(self._on_handler_change(handler, registered))

    def _flush_pending_handler_changes(self) -> None:
        if not self._pending_handler_changes or not self.middlewares:
            return
        loop = asyncio.get_running_loop()
        queued = list(self._pending_handler_changes)
        self._pending_handler_changes.clear()
        for handler, registered in queued:
            loop.create_task(self._on_handler_change(handler, registered))

    @staticmethod
    def _event_field_is_defined(event: BaseEvent[Any], field_name: str) -> bool:
        if field_name in event.model_fields_set:
            return True
        extras = event.model_extra
        if isinstance(extras, dict) and field_name in extras:
            return True
        event_field = event.__class__.model_fields.get(field_name)
        base_field = BaseEvent.model_fields.get(field_name)
        if event_field is None or base_field is None:
            return False
        return event_field.default != base_field.default

    @staticmethod
    def _resolve_event_slow_timeout(event: BaseEvent[Any], eventbus: 'EventBus') -> float | None:
        event_slow_timeout = getattr(event, 'event_slow_timeout', None)
        if event_slow_timeout is not None:
            return cast(float, event_slow_timeout)
        slow_timeout = getattr(event, 'slow_timeout', None)
        if slow_timeout is not None:
            return cast(float, slow_timeout)
        return eventbus.event_slow_timeout

    @staticmethod
    def _resolve_handler_slow_timeout(event: BaseEvent[Any], handler: EventHandler, eventbus: 'EventBus') -> float | None:
        if 'handler_slow_timeout' in handler.model_fields_set:
            return handler.handler_slow_timeout
        if EventBus._event_field_is_defined(event, 'event_handler_slow_timeout'):
            return event.event_handler_slow_timeout
        if EventBus._event_field_is_defined(event, 'event_slow_timeout'):
            return cast(float | None, getattr(event, 'event_slow_timeout', None))
        if EventBus._event_field_is_defined(event, 'slow_timeout'):
            return cast(float | None, getattr(event, 'slow_timeout', None))
        if hasattr(eventbus, 'event_handler_slow_timeout'):
            return eventbus.event_handler_slow_timeout
        return eventbus.event_slow_timeout

    @staticmethod
    def _resolve_handler_timeout(
        event: BaseEvent[Any],
        handler: EventHandler,
        eventbus: 'EventBus',
        timeout_override: float | None = None,
    ) -> float | None:
        if 'handler_timeout' in handler.model_fields_set:
            resolved_handler_timeout = handler.handler_timeout
        elif EventBus._event_field_is_defined(event, 'event_handler_timeout'):
            resolved_handler_timeout = event.event_handler_timeout
        else:
            resolved_handler_timeout = eventbus.event_timeout

        resolved_event_timeout = event.event_timeout

        if resolved_handler_timeout is None and resolved_event_timeout is None:
            resolved_timeout = None
        elif resolved_handler_timeout is None:
            resolved_timeout = resolved_event_timeout
        elif resolved_event_timeout is None:
            resolved_timeout = resolved_handler_timeout
        else:
            resolved_timeout = min(resolved_handler_timeout, resolved_event_timeout)

        if timeout_override is None:
            return resolved_timeout
        if resolved_timeout is None:
            return timeout_override
        return min(resolved_timeout, timeout_override)

    async def _slow_event_warning_monitor(self, event: BaseEvent[Any], slow_timeout: float) -> None:
        await asyncio.sleep(slow_timeout)
        if self._is_event_complete_fast(event):
            return
        running_handler_count = sum(1 for result in event.event_results.values() if result.status == 'started')
        started_at = event.event_started_at or event.event_created_at
        elapsed_seconds = max(0.0, (datetime.now(UTC) - started_at).total_seconds())
        logger.warning(
            '⚠️ Slow event processing: %s.on(%s#%s, %d handlers) still running after %.2fs',
            self.label,
            event.event_type,
            event.event_id[-4:],
            running_handler_count,
            elapsed_seconds,
        )

    @staticmethod
    def _is_event_complete_fast(event: BaseEvent[Any]) -> bool:
        signal = event._event_completed_signal  # pyright: ignore[reportPrivateUsage]
        if signal is not None:
            return signal.is_set()
        if getattr(event, '_event_is_complete_flag', False):  # pyright: ignore[reportPrivateUsage]
            return True
        return event.event_completed_at is not None

    @staticmethod
    def _is_event_started_fast(event: BaseEvent[Any]) -> bool:
        for result in event.event_results.values():
            if result.started_at is not None or result.status == 'started':
                return True
        return False

    def _has_inflight_events_fast(self) -> bool:
        return bool(self._active_event_ids)

    @staticmethod
    def _mark_event_complete_on_all_buses(event: BaseEvent[Any]) -> None:
        event_id = event.event_id
        for bus in list(EventBus.all_instances):
            if bus:
                bus._active_event_ids.discard(event_id)
                if bus.max_history_size == 0:
                    # max_history_size=0 means "keep only in-flight events".
                    # As soon as an event is completed, drop it from history.
                    bus.event_history.pop(event_id, None)

    @property
    def events_pending(self) -> list[BaseEvent[Any]]:
        """Get events that haven't started processing yet (does not include events that have not even finished dispatching yet in self.event_queue)"""
        return [
            event
            for event in self.event_history.values()
            if not self._is_event_complete_fast(event) and not self._is_event_started_fast(event)
        ]

    @property
    def events_started(self) -> list[BaseEvent[Any]]:
        """Get events currently being processed"""
        return [
            event
            for event in self.event_history.values()
            if not self._is_event_complete_fast(event) and self._is_event_started_fast(event)
        ]

    @property
    def events_completed(self) -> list[BaseEvent[Any]]:
        """Get events that have completed processing"""
        return [event for event in self.event_history.values() if self._is_event_complete_fast(event)]

    # Overloads for typed event patterns with specific handler signatures
    # Order matters - more specific types must come before general ones

    # Class pattern registration keeps strict event typing.
    @overload
    def on(self, event_pattern: type[T_Event], handler: EventHandlerFunc[T_Event]) -> EventHandler: ...

    @overload
    def on(self, event_pattern: type[T_Event], handler: AsyncEventHandlerFunc[T_Event]) -> EventHandler: ...

    @overload
    def on(self, event_pattern: type[T_Event], handler: EventHandlerMethod[T_Event]) -> EventHandler: ...

    @overload
    def on(self, event_pattern: type[T_Event], handler: AsyncEventHandlerMethod[T_Event]) -> EventHandler: ...

    @overload
    def on(self, event_pattern: type[T_Event], handler: EventHandlerClassMethod[T_Event]) -> EventHandler: ...

    @overload
    def on(self, event_pattern: type[T_Event], handler: AsyncEventHandlerClassMethod[T_Event]) -> EventHandler: ...

    # String and wildcard registration is looser: any BaseEvent subclass handler is allowed.
    @overload
    def on(self, event_pattern: PythonIdentifierStr | Literal['*'], handler: EventHandlerFunc[T_Event]) -> EventHandler: ...

    @overload
    def on(self, event_pattern: PythonIdentifierStr | Literal['*'], handler: AsyncEventHandlerFunc[T_Event]) -> EventHandler: ...

    @overload
    def on(self, event_pattern: PythonIdentifierStr | Literal['*'], handler: EventHandlerMethod[T_Event]) -> EventHandler: ...

    @overload
    def on(
        self,
        event_pattern: PythonIdentifierStr | Literal['*'],
        handler: AsyncEventHandlerMethod[T_Event],
    ) -> EventHandler: ...

    @overload
    def on(
        self,
        event_pattern: PythonIdentifierStr | Literal['*'],
        handler: EventHandlerClassMethod[T_Event],
    ) -> EventHandler: ...

    @overload
    def on(
        self,
        event_pattern: PythonIdentifierStr | Literal['*'],
        handler: AsyncEventHandlerClassMethod[T_Event],
    ) -> EventHandler: ...

    # I dont think this is needed, but leaving it here for now
    # 9. Coroutine[Any, Any, Any] - direct coroutine
    # @overload
    # def on(self, event_pattern: EventPatternType, handler: Coroutine[Any, Any, Any]) -> None: ...

    def on(
        self,
        event_pattern: EventPatternType,
        handler: Any,
    ) -> EventHandler:
        """
        Subscribe to events matching a pattern, event type name, or event model class.
        Use event_pattern='*' to subscribe to all events. Handler can be sync or async function or method.

        Examples:
                eventbus.on('TaskStartedEvent', handler)  # Specific event type
                eventbus.on(TaskStartedEvent, handler)  # Event model class
                eventbus.on('*', handler)  # Subscribe to all events
                eventbus.on('*', other_eventbus.dispatch)  # Forward all events to another EventBus

        Note: When forwarding events between buses, all handler results are automatically
        flattened into the original event's results, so EventResults sees all handlers
        from all buses as a single flat collection.
        """
        assert isinstance(event_pattern, str) or isinstance(event_pattern, type), (
            f'Invalid event pattern: {event_pattern}, must be a string event type or subclass of BaseEvent'
        )
        assert inspect.isfunction(handler) or inspect.ismethod(handler) or inspect.iscoroutinefunction(handler), (
            f'Invalid handler: {handler}, must be a sync or async function or method'
        )

        # Normalize event key to string event_type or wildcard.
        event_key = self._normalize_event_pattern(event_pattern)

        # Ensure event_key is definitely a string at this point
        assert isinstance(event_key, str)

        # Check for duplicate handler names. Keep this bounded so large handler
        # registrations (e.g. perf scenarios with tens of thousands of handlers)
        # do not degrade into O(n^2) registration time.
        new_handler_name = EventHandler.get_callable_handler_name(handler)
        existing_handler_ids = self.handlers_by_key.get(event_key, [])
        if existing_handler_ids and len(existing_handler_ids) <= self._duplicate_handler_name_check_limit:
            for existing_handler_id in existing_handler_ids:
                existing_handler = self.handlers.get(existing_handler_id)
                if existing_handler and existing_handler.handler_name == new_handler_name:
                    warnings.warn(
                        f"⚠️ {self} Handler {new_handler_name} already registered for event '{event_key}'. "
                        f'This may make it difficult to filter event results by handler name. '
                        f'Consider using unique function names.',
                        UserWarning,
                        stacklevel=2,
                    )
                    break

        # Register handler entry and index it by event key.
        handler_entry = EventHandler.from_callable(
            handler=cast(EventHandlerCallable, handler),
            event_pattern=event_key,
            eventbus_name=self.name,
            eventbus_id=self.id,
            detect_handler_file_path=self.event_handler_detect_file_paths,
        )
        assert handler_entry.id is not None
        self.handlers[handler_entry.id] = handler_entry
        self.handlers_by_key[event_key].append(handler_entry.id)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                '👂 %s.on(%s, %s) Registered event handler #%s',
                self,
                event_key,
                handler_entry.handler_name,
                handler_entry.id[-4:],
            )
        self._notify_handler_change(handler_entry, registered=True)
        return handler_entry

    @overload
    def off(
        self, event_pattern: type[T_Event], handler: EventHandlerCallable | PythonIdStr | EventHandler | None = None
    ) -> None: ...

    @overload
    def off(
        self,
        event_pattern: PythonIdentifierStr | Literal['*'],
        handler: EventHandlerCallable | PythonIdStr | EventHandler | None = None,
    ) -> None: ...

    def off(
        self,
        event_pattern: EventPatternType,
        handler: EventHandlerCallable | PythonIdStr | EventHandler | None = None,
    ) -> None:
        """Deregister handlers for an event pattern by id, callable, EventHandler, or all."""
        event_key = self._normalize_event_pattern(event_pattern)
        indexed_ids = list(self.handlers_by_key.get(event_key, []))
        if not indexed_ids:
            return

        requested_id: str | None = None
        requested_callable: EventHandlerCallable | None = None
        if isinstance(handler, EventHandler):
            requested_id = handler.id
        elif isinstance(handler, str):
            requested_id = handler
        elif handler is not None:
            requested_callable = handler

        for handler_id in indexed_ids:
            entry = self.handlers.get(handler_id)
            if entry is None:
                self._remove_indexed_handler(event_key, handler_id)
                continue

            should_remove = False
            if handler is None:
                should_remove = True
            elif requested_id is not None and entry.id == requested_id:
                should_remove = True
            elif requested_callable is not None and entry.handler is requested_callable:
                should_remove = True

            if should_remove:
                self.handlers.pop(handler_id, None)
                self._remove_indexed_handler(event_key, handler_id)
                self._notify_handler_change(entry, registered=False)

    def dispatch(self, event: T_ExpectedEvent) -> T_ExpectedEvent:
        """
        Enqueue an event for processing and immediately return an Event(status='pending') version (synchronous).
        You can await the returned Event(status='pending') object to block until it is done being executed aka Event(status='completed'),
        or you can interact with the unawaited Event(status='pending') before its handlers have finished.

        (The first EventBus.dispatch() call will auto-start a bus's async _run_loop() if it's not already running)

        >>> completed_event = await eventbus.dispatch(SomeEvent())
                # 1. enqueues the event synchronously
                # 2. returns an awaitable SomeEvent() with pending results in .event_results
                # 3. awaits the SomeEvent() which waits until all pending results are complete and returns the completed SomeEvent()

        >>> result_value = await eventbus.dispatch(SomeEvent()).event_result()
                # 1. enqueues the event synchronously
                # 2. returns a pending SomeEvent() with pending results in .event_results
                # 3. awaiting .event_result() waits until all pending results are complete, and returns the raw result value of the first one
        """

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(f'{self}.dispatch() called but no event loop is running! Event not queued: {event.event_type}')

        assert event.event_id, 'Missing event.event_id: UUIDStr = uuid7str()'
        assert event.event_created_at, 'Missing event.event_created_at: datetime = datetime.now(UTC)'
        assert event.event_type and event.event_type.isidentifier(), 'Missing event.event_type: str'

        # Apply bus default timeout only when event timeout is not explicitly set.
        if event.event_timeout is None and not self._event_field_is_defined(event, 'event_timeout'):
            event.event_timeout = self.event_timeout

        # Copy bus-level slow timeout defaults only when the event has no own overrides.
        has_event_slow_override = self._event_field_is_defined(event, 'event_slow_timeout') or self._event_field_is_defined(
            event, 'slow_timeout'
        )
        if not has_event_slow_override:
            setattr(event, 'event_slow_timeout', self.event_slow_timeout)

        has_handler_slow_override = self._event_field_is_defined(event, 'event_handler_slow_timeout')
        if not has_handler_slow_override and not has_event_slow_override:
            event.event_handler_slow_timeout = self.event_handler_slow_timeout

        # Default per-event event concurrency from the bus when absent or None.
        if event.event_concurrency is None:
            event.event_concurrency = self.event_concurrency

        # Default per-event handler concurrency from the bus when absent.
        if event.event_handler_concurrency is None:
            event.event_handler_concurrency = self.event_handler_concurrency

        # Default per-event completion mode from the bus when absent.
        if event.event_handler_completion is None:
            event.event_handler_completion = self.event_handler_completion

        # Automatically set event_parent_id from context if not already set
        if event.event_parent_id is None:
            current_event: 'BaseEvent[Any] | None' = _current_event_context.get()
            if current_event is not None:
                event.event_parent_id = current_event.event_id

        # Capture dispatch-time context for propagation to handlers (GitHub issue #20)
        # This ensures ContextVars set before dispatch() are accessible in handlers
        if event._event_dispatch_context is None:  # pyright: ignore[reportPrivateUsage]
            event._event_dispatch_context = contextvars.copy_context()  # pyright: ignore[reportPrivateUsage]

        # Track child events - if we're inside a handler, add this event to the handler's event_children list
        # Only track if this is a NEW event (not forwarding an existing event)
        current_handler_id = _current_handler_id_context.get()
        if current_handler_id is not None:
            current_event = _current_event_context.get()
            if current_event is not None and current_handler_id in current_event.event_results:
                # Only add as child if it's a different event (not forwarding the same event)
                if event.event_id != current_event.event_id:
                    current_event.event_results[current_handler_id].event_children.append(event)

        # Add this EventBus label to the event_path if not already there
        if self.label not in event.event_path:
            # preserve identity of the original object instead of creating a new one, so that the original object remains awaitable to get the result
            # NOT: event = event.model_copy(update={'event_path': event.event_path + [self.name]})
            event.event_path.append(self.label)
        else:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    '⚠️ %s.dispatch(%s) - Bus already in path, not adding again. Path: %s',
                    self,
                    event.event_type,
                    event.event_path,
                )

        assert event.event_path, 'Missing event.event_path: list[str] (with at least one bus label recorded in it)'
        assert all(
            '#' in entry
            and entry.rsplit('#', 1)[0].isidentifier()
            and entry.rsplit('#', 1)[1].isalnum()
            and len(entry.rsplit('#', 1)[1]) == 4
            for entry in event.event_path
        ), f'Event.event_path must be a list of EventBus labels BusName#abcd, got: {event.event_path}'

        # NOTE:
        # dispatch() is intentionally synchronous and runs on the same event-loop
        # thread as the runloop task. Blocking here for "pressure" would deadlock
        # naive flood loops because the runloop cannot progress until dispatch() returns.
        # So pressure is handled by policy:
        #   - max_history_drop=True  -> absorb and trim oldest history entries
        #   - max_history_drop=False -> reject new dispatches at max_history_size
        if (
            self.max_history_size is not None
            and self.max_history_size > 0
            and not self.max_history_drop
            and len(self.event_history) >= self.max_history_size
        ):
            raise RuntimeError(
                f'{self} history limit reached ({len(self.event_history)}/{self.max_history_size}); '
                'set max_history_drop=True to drop old history instead of rejecting new events'
            )

        # Auto-start if needed
        self._flush_pending_handler_changes()
        self._start()
        # Ensure every dispatched event has a completion signal tied to this loop.
        # Completion logic always sets this signal; consumers like event_results_* await it.
        _ = event.event_completed_signal

        # Put event in queue synchronously using put_nowait
        if self.pending_event_queue:
            try:
                self.pending_event_queue.put_nowait(event)
                # Only add to history after successfully queuing
                self.event_history[event.event_id] = event
                self._active_event_ids.add(event.event_id)
                if self._find_waiters:
                    # Resolve future find waiters immediately on dispatch so callers
                    # don't wait for queue position or handler execution.
                    for waiter in tuple(self._find_waiters):
                        if waiter.event_key != '*' and event.event_type != waiter.event_key:
                            continue
                        if not waiter.matches(event):
                            continue
                        if waiter.timeout_handle is not None:
                            waiter.timeout_handle.cancel()
                        self._find_waiters.discard(waiter)
                        if not waiter.future.done():
                            waiter.future.set_result(event)
                if self.middlewares:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._on_event_change(event, EventStatus.PENDING))
                if logger.isEnabledFor(logging.INFO):
                    logger.info(
                        '🗣️ %s.dispatch(%s) ➡️ %s#%s (#%d %s)',
                        self,
                        event.event_type,
                        event.event_type,
                        event.event_id[-4:],
                        self.pending_event_queue.qsize(),
                        event.event_status,
                    )
            except asyncio.QueueFull:
                # Don't add to history if we can't queue it
                logger.error(
                    f'⚠️ {self} Event queue is full! Dropping event and aborting {event.event_type}:\n{event.model_dump_json()}'  # pyright: ignore[reportUnknownMemberType]
                )
                raise  # could also block indefinitely until queue has space, but dont drop silently or delete events
        else:
            logger.warning('⚠️ %s.dispatch() called but event_queue is None! Event not queued: %s', self, event.event_type)

        # Note: We do NOT pre-create EventResults here anymore.
        # EventResults are created only when handlers actually start executing.
        # This avoids "orphaned" pending results for handlers that get filtered out later.

        # Amortize cleanup work by trimming only after a soft overage; this keeps
        # hot dispatch fast under large naive floods while still bounding memory.
        if self.max_history_size is not None and self.max_history_size > 0 and self.max_history_drop:
            soft_limit = max(self.max_history_size, int(self.max_history_size * 1.2))
            if len(self.event_history) > soft_limit:
                self.cleanup_event_history()

        return event

    def emit(self, event: T_ExpectedEvent) -> T_ExpectedEvent:
        """Alias for dispatch(), mirroring EventEmitter-style APIs."""
        return self.dispatch(event)

    @staticmethod
    def _normalize_event_pattern(event_pattern: object) -> str:
        if event_pattern == '*':
            return '*'
        if isinstance(event_pattern, str):
            return event_pattern
        if isinstance(event_pattern, type) and issubclass(event_pattern, BaseEvent):
            # Respect explicit event_type defaults on model classes first.
            event_type_field = event_pattern.model_fields.get('event_type')
            event_type_default = event_type_field.default if event_type_field is not None else None
            if isinstance(event_type_default, str) and event_type_default not in ('', 'UndefinedEvent'):
                return event_type_default
            return event_pattern.__name__
        raise ValueError(f'Invalid event pattern: {event_pattern}, must be a string event type, "*", or subclass of BaseEvent')

    def _event_matches_pattern(self, event: BaseEvent[Any], pattern: EventPatternType) -> bool:
        pattern_key = self._normalize_event_pattern(pattern)
        if pattern_key == '*':
            return True
        return event.event_type == pattern_key

    def _remove_indexed_handler(self, event_pattern: str, handler_id: PythonIdStr) -> None:
        ids = self.handlers_by_key.get(event_pattern)
        if not ids:
            return
        try:
            ids.remove(handler_id)
        except ValueError:
            return
        if not ids:
            self.handlers_by_key.pop(event_pattern, None)

    @overload
    async def find(
        self,
        event_type: type[T_ExpectedEvent],
        where: None = None,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        **event_fields: Any,
    ) -> T_ExpectedEvent | None: ...

    @overload
    async def find(
        self,
        event_type: type[T_ExpectedEvent],
        where: Callable[[T_ExpectedEvent], bool],
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        **event_fields: Any,
    ) -> T_ExpectedEvent | None: ...

    @overload
    async def find(
        self,
        event_type: PythonIdentifierStr | Literal['*'],
        where: Callable[[BaseEvent[Any]], bool] | None = None,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        **event_fields: Any,
    ) -> BaseEvent[Any] | None: ...

    async def find(
        self,
        event_type: EventPatternType,
        where: Callable[[Any], bool] | None = None,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        **event_fields: Any,
    ) -> BaseEvent[Any] | None:
        """
        Find an event matching criteria in history and/or future.

        Mirrors TS `EventBus.find` behavior:
        - Default behavior with no options: `past=True`, `future=False`
        - Search history and return the most recent match
        - Optionally wait for future dispatches
        - Supports exact-match equality filters via keyword args for any event field

        Args:
            event_type: The event type string or model class to find
            where: Predicate function for filtering (default: lambda _: True)
            child_of: Only match events that are descendants of this parent event
            past: Controls history search behavior:
                - True: search all history
                - False: skip history search
                - float: search events from last N seconds only
                - timedelta: search events from last N seconds
            future: Controls future wait behavior:
                - True: wait forever for matching event
                - False: don't wait for future events
                - float: wait up to N seconds for matching event
            **event_fields: Optional exact-match filters for any event field
                (for example `event_status='completed'`, `user_id='u-1'`)

        Returns:
            Matching event or None if not found/timeout
        """
        resolved_past_input = True if past is None else past
        if isinstance(resolved_past_input, timedelta):
            resolved_past: bool | float = max(0.0, resolved_past_input.total_seconds())
        elif isinstance(resolved_past_input, bool):
            resolved_past = resolved_past_input
        else:
            resolved_past = max(0.0, float(resolved_past_input))

        resolved_future_input = False if future is None else future
        if isinstance(resolved_future_input, bool):
            resolved_future: bool | float = resolved_future_input
        else:
            resolved_future = max(0.0, float(resolved_future_input))

        if resolved_past is False and resolved_future is False:
            return None

        event_key = self._normalize_event_pattern(event_type)
        where_predicate: Callable[[BaseEvent[Any]], bool]
        if where is None:
            where_predicate = lambda _: True
        else:
            where_predicate = where

        def matches(event: BaseEvent[Any]) -> bool:
            if event_key != '*' and event.event_type != event_key:
                return False
            if child_of is not None and not self.event_is_child_of(event, child_of):
                return False
            for field_name, expected_value in event_fields.items():
                if not hasattr(event, field_name):
                    return False
                if getattr(event, field_name) != expected_value:
                    return False
            if not where_predicate(event):
                return False
            return True

        if resolved_past is not False:
            cutoff: datetime | None = None
            if resolved_past is not True:
                cutoff = datetime.now(UTC) - timedelta(seconds=float(resolved_past))

            events = list(self.event_history.values())
            for event in reversed(events):
                if cutoff is not None and event.event_created_at < cutoff:
                    continue
                if matches(event):
                    return event

        if resolved_future is False:
            return None

        event_match_future: asyncio.Future[BaseEvent[Any] | None] = asyncio.get_running_loop().create_future()
        waiter = _FindWaiter(event_key=event_key, matches=matches, future=event_match_future)
        if resolved_future is not True:
            timeout_seconds = float(resolved_future)

            def _on_wait_timeout() -> None:
                self._find_waiters.discard(waiter)
                if waiter.timeout_handle is not None:
                    waiter.timeout_handle.cancel()
                    waiter.timeout_handle = None
                if not event_match_future.done():
                    event_match_future.set_result(None)

            waiter.timeout_handle = asyncio.get_running_loop().call_later(timeout_seconds, _on_wait_timeout)
        self._find_waiters.add(waiter)

        try:
            return await event_match_future
        finally:
            self._find_waiters.discard(waiter)
            if waiter.timeout_handle is not None:
                waiter.timeout_handle.cancel()
                waiter.timeout_handle = None

    def event_is_child_of(self, event: BaseEvent[Any], ancestor: BaseEvent[Any]) -> bool:
        """
        Check if event is a descendant of ancestor (child, grandchild, etc.).

        Walks up the parent chain from event looking for ancestor.
        Returns True if ancestor is found in the chain, False otherwise.

        Args:
            event: The potential descendant event
            ancestor: The potential ancestor event

        Returns:
            True if event is a descendant of ancestor, False otherwise
        """
        current_id = event.event_parent_id
        visited: set[str] = set()

        while current_id and current_id not in visited:
            if current_id == ancestor.event_id:
                return True
            visited.add(current_id)

            # Find parent event in any bus's history
            parent = self.event_history.get(current_id)
            if parent is None:
                # Check other buses
                for bus in list(EventBus.all_instances):
                    if bus is not self and current_id in bus.event_history:
                        parent = bus.event_history[current_id]
                        break
            if parent is None:
                break
            current_id = parent.event_parent_id

        return False

    def event_is_parent_of(self, event: BaseEvent[Any], descendant: BaseEvent[Any]) -> bool:
        return self.event_is_child_of(descendant, event)

    def _start(self) -> None:
        """Start the event bus if not already running"""
        if not self._is_running:
            try:
                loop = asyncio.get_running_loop()

                # Hook into the event loop's close method to cleanup before it closes
                # this is necessary to silence "RuntimeError: no running event loop" and "event loop is closed" errors on shutdown
                if not hasattr(loop, '_eventbus_close_hooked'):
                    original_close = loop.close
                    registered_eventbuses: weakref.WeakSet[EventBus] = weakref.WeakSet()

                    def close_with_cleanup() -> None:
                        # Clean up all registered EventBuses before closing the loop
                        for eventbus in list(registered_eventbuses):
                            try:
                                # Stop the eventbus while loop is still running
                                if eventbus._is_running:
                                    eventbus._is_running = False

                                    # Shutdown the queue properly - our custom queue will handle cleanup
                                    if eventbus.pending_event_queue:
                                        eventbus.pending_event_queue.shutdown(immediate=True)

                                    if eventbus._runloop_task and not eventbus._runloop_task.done():
                                        # Suppress warning before cancelling
                                        if hasattr(eventbus._runloop_task, '_log_destroy_pending'):
                                            eventbus._runloop_task._log_destroy_pending = False  # type: ignore
                                        eventbus._runloop_task.cancel()
                            except Exception:
                                pass

                        # Now close the loop
                        original_close()

                    loop_any = cast(Any, loop)
                    loop_any.close = close_with_cleanup
                    loop_any._eventbus_close_hooked = True
                    loop_any._eventbus_instances = registered_eventbuses

                # Register this EventBus instance in the WeakSet of all EventBuses on the loop
                if hasattr(loop, '_eventbus_instances'):
                    cast(Any, loop)._eventbus_instances.add(self)

                # Create async objects if needed
                if self.pending_event_queue is None:
                    # Keep queue unbounded so naive dispatch floods can enqueue without
                    # artificial queue caps; queue stores event object references.
                    self.pending_event_queue = CleanShutdownQueue[BaseEvent[Any]](maxsize=0)
                    self._on_idle = asyncio.Event()
                    self._on_idle.clear()  # Start in a busy state unless we confirm queue is empty by running step() at least once

                # Create and start the run loop task.
                # Use a weakref-based runner so an unreferenced EventBus can be GC'd
                # without requiring explicit stop(clear=True) by callers.
                # Run loops must start with a clean context. If dispatch() is called
                # from inside a handler, lock-depth ContextVars would otherwise leak
                # into the new task and bypass event lock acquisition.
                self._runloop_task = loop.create_task(
                    EventBus._run_loop_weak(weakref.ref(self)),
                    name=f'{self}._run_loop',
                    context=contextvars.Context(),
                )
                self._is_running = True
            except RuntimeError:
                # No event loop - will start when one becomes available
                pass

    async def stop(self, timeout: float | None = None, clear: bool = False) -> None:
        """Stop the event bus, optionally waiting for events to complete

        Args:
            timeout: Maximum time to wait for pending events to complete
            clear: If True, clear event history and remove from global tracking to free memory
        """
        if not self._is_running and not self._parallel_event_tasks:
            for waiter in tuple(self._find_waiters):
                self._find_waiters.discard(waiter)
                if waiter.timeout_handle is not None:
                    waiter.timeout_handle.cancel()
                if not waiter.future.done():
                    waiter.future.set_result(None)
            return

        # Wait for completion if timeout specified and > 0
        # timeout=0 means "don't wait", so skip the wait entirely
        if timeout is not None and timeout > 0:
            try:
                await self.wait_until_idle(timeout=timeout)
            except TimeoutError:
                pass

        queue_size = self.pending_event_queue.qsize() if self.pending_event_queue else 0
        has_inflight = self._has_inflight_events_fast()
        if queue_size or has_inflight:
            logger.debug(
                '⚠️ %s stopping with pending events: queue=%d inflight=%s history=%d',
                self,
                queue_size,
                has_inflight,
                len(self.event_history),
            )

        # Signal shutdown
        self._is_running = False

        # Shutdown the queue to unblock any pending get() operations
        if self.pending_event_queue:
            self.pending_event_queue.shutdown()

        # print('STOPPING', self.event_history)

        # Wait for the run loop task to finish / force-cancel it if it's hanging
        if self._runloop_task and not self._runloop_task.done():
            await asyncio.wait({self._runloop_task}, timeout=0.1)
            try:
                self._runloop_task.cancel()
            except Exception:
                pass

        if self._parallel_event_tasks:
            for task in list(self._parallel_event_tasks):
                if not task.done():
                    task.cancel()
            await asyncio.gather(*list(self._parallel_event_tasks), return_exceptions=True)
            self._parallel_event_tasks.clear()

        # Clear references
        self._runloop_task = None
        self._active_event_ids.clear()
        self._processing_event_ids.clear()
        for waiter in tuple(self._find_waiters):
            self._find_waiters.discard(waiter)
            if waiter.timeout_handle is not None:
                waiter.timeout_handle.cancel()
            if not waiter.future.done():
                waiter.future.set_result(None)
        if self._on_idle:
            self._on_idle.set()

        # Rename the bus to release the name. This ensures stopped buses don't
        # cause name conflicts with new buses using the same name. This makes
        # name conflict detection deterministic (not dependent on GC timing).
        self.name = f'_stopped_{self.id[-8:]}'

        # Clear event history and handlers if requested (for memory cleanup)
        if clear:
            self.event_history.clear()
            self.handlers.clear()
            self.handlers_by_key.clear()
            self._active_event_ids.clear()

            # Remove from global instance tracking
            if self in EventBus.all_instances:
                EventBus.all_instances.discard(self)

            # Remove from event loop's tracking if present
            try:
                loop = asyncio.get_running_loop()
                if hasattr(loop, '_eventbus_instances'):
                    cast(Any, loop)._eventbus_instances.discard(self)
            except RuntimeError:
                # No running loop, that's fine
                pass

            logger.debug('🧹 %s cleared event history and removed from global tracking', self)

        logger.debug('🛑 %s shut down %s', self, 'gracefully' if timeout is not None else 'immediately')

        # Check total memory usage across all instances
        try:
            self._check_total_memory_usage()
        except Exception:
            # Don't let memory check errors prevent shutdown
            pass

    async def wait_until_idle(self, timeout: float | None = None) -> None:
        """Wait until the event bus is idle (no events being processed and all handlers completed)"""

        self._start()
        assert self._on_idle and self.pending_event_queue, 'EventBus._start() must be called before wait_until_idle() is reached'

        start_time = asyncio.get_event_loop().time()
        remaining_timeout = timeout

        try:
            # First wait for the queue to be empty
            join_task = asyncio.create_task(self.pending_event_queue.join())
            await asyncio.wait_for(join_task, timeout=remaining_timeout)

            # Update remaining timeout
            if timeout is not None:
                elapsed = asyncio.get_event_loop().time() - start_time
                remaining_timeout = max(0, timeout - elapsed)

            # Wait for idle state
            idle_task = asyncio.create_task(self._on_idle.wait())
            await asyncio.wait_for(idle_task, timeout=remaining_timeout)

            # Critical: Ensure the runloop has settled by yielding control
            # This allows the runloop to complete any in-flight operations
            # and prevents race conditions with event_history access
            await asyncio.sleep(0)  # Yield to event loop

            # Double-check we're truly idle - if new events came in, wait again
            while not self._on_idle.is_set() or self._has_inflight_events_fast():
                if timeout is not None:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    remaining_timeout = max(0, timeout - elapsed)
                    if remaining_timeout <= 0:
                        raise TimeoutError()

                # Clear and wait again
                self._on_idle.clear()
                idle_task = asyncio.create_task(self._on_idle.wait())
                await asyncio.wait_for(idle_task, timeout=remaining_timeout)
                await asyncio.sleep(0)  # Yield again

        except TimeoutError:
            logger.warning(
                '⌛️ %s Timeout waiting for event bus to be idle after %ss (history=%d)',
                self,
                timeout,
                len(self.event_history),
            )

    async def _run_loop(self) -> None:
        """Main event processing loop"""
        try:
            while self._is_running:
                try:
                    _processed_event = await self.step()
                    # Check if we should set idle state after processing
                    if self._on_idle and self.pending_event_queue:
                        if not self._has_inflight_events_fast() and self.pending_event_queue.qsize() == 0:
                            self._on_idle.set()
                except QueueShutDown:
                    # Queue was shut down, exit cleanly
                    break
                except RuntimeError as e:
                    # Event loop is closing
                    if 'Event loop is closed' in str(e) or 'no running event loop' in str(e):
                        break
                    else:
                        logger.exception('❌ %s Runtime error in event loop: %s %s', self, type(e).__name__, e, exc_info=True)
                        # Continue running even if there's an error
                except Exception as e:
                    logger.exception('❌ %s Error in event loop: %s %s', self, type(e).__name__, e, exc_info=True)
                    # Continue running even if there's an error
        except asyncio.CancelledError:
            # Task was cancelled, clean exit
            # logger.debug(f'🛑 {self} Event loop task cancelled')
            pass
        finally:
            # Don't call stop() here as it might create new tasks
            self._is_running = False

    @staticmethod
    async def _run_loop_weak(bus_ref: 'weakref.ReferenceType[EventBus]') -> None:
        """
        Weakref-based run loop.

        Unlike a bound coroutine (self._run_loop), this runner avoids holding a
        strong EventBus reference while idle, allowing unreferenced buses to be
        garbage-collected naturally without an explicit stop().
        """
        try:
            while True:
                bus = bus_ref()
                if bus is None or not bus._is_running:
                    break

                queue = bus.pending_event_queue
                on_idle = bus._on_idle
                del bus

                if queue is None or on_idle is None:
                    await asyncio.sleep(0.01)
                    continue

                event: BaseEvent[Any] | None = None
                try:
                    get_next_queued_event = asyncio.create_task(queue.get())
                    if hasattr(get_next_queued_event, '_log_destroy_pending'):
                        get_next_queued_event._log_destroy_pending = False  # type: ignore[attr-defined]
                    has_next_event, _pending = await asyncio.wait({get_next_queued_event}, timeout=0.1)
                    if not has_next_event:
                        get_next_queued_event.cancel()
                        bus = bus_ref()
                        if bus is None:
                            break
                        if bus._on_idle and bus.pending_event_queue:
                            if not bus._has_inflight_events_fast() and bus.pending_event_queue.qsize() == 0:
                                bus._on_idle.set()
                        del bus
                        continue

                    event = await get_next_queued_event
                except QueueShutDown:
                    break
                except asyncio.CancelledError:
                    break
                except RuntimeError as e:
                    if 'Event loop is closed' in str(e) or 'no running event loop' in str(e):
                        break
                    logger.exception(f'❌ Weak run loop runtime error: {type(e).__name__} {e}', exc_info=True)
                    continue
                except Exception as e:
                    logger.exception(f'❌ Weak run loop error: {type(e).__name__} {e}', exc_info=True)
                    continue

                bus = bus_ref()
                if bus is None:
                    try:
                        queue.task_done()
                    except Exception:
                        pass
                    break

                try:
                    if bus._on_idle:
                        bus._on_idle.clear()

                    bus._processing_event_ids.add(event.event_id)

                    event_lock = bus.locks.get_lock_for_event(bus, event)
                    if event_lock is None:

                        async def process_parallel_event(
                            bus: 'EventBus' = bus,
                            event: BaseEvent[Any] = event,
                            queue: CleanShutdownQueue[BaseEvent[Any]] = queue,
                        ) -> None:
                            try:
                                await bus.step(event=event)
                            finally:
                                try:
                                    queue.task_done()
                                except ValueError:
                                    pass

                        task = asyncio.create_task(
                            process_parallel_event(),
                            name=f'{bus}.process_event({event.event_id[-4:]})',
                        )
                        bus._parallel_event_tasks.add(task)

                        def _on_done(
                            done_task: asyncio.Task[None], *, bus_ref: 'weakref.ReferenceType[EventBus]' = weakref.ref(bus)
                        ):
                            live_bus = bus_ref()
                            if live_bus is not None:
                                live_bus._parallel_event_tasks.discard(done_task)
                            if done_task.cancelled():
                                return
                            try:
                                exc = done_task.exception()
                            except asyncio.CancelledError:
                                return
                            if exc is not None:
                                logger.exception('❌ Weak run loop parallel event task error: %s %s', type(exc).__name__, exc)

                        task.add_done_callback(_on_done)
                    else:
                        try:
                            await bus.step(event=event)
                        finally:
                            try:
                                queue.task_done()
                            except ValueError:
                                pass
                except QueueShutDown:
                    break
                except asyncio.CancelledError:
                    break
                except RuntimeError as e:
                    if 'Event loop is closed' in str(e) or 'no running event loop' in str(e):
                        break
                    logger.exception(f'❌ Weak run loop runtime error: {type(e).__name__} {e}', exc_info=True)
                except Exception as e:
                    logger.exception(f'❌ Weak run loop error: {type(e).__name__} {e}', exc_info=True)
                finally:
                    del bus
        finally:
            bus = bus_ref()
            if bus is not None:
                bus._is_running = False

    async def _get_next_event(self, wait_for_timeout: float = 0.1) -> 'BaseEvent[Any] | None':
        """Get the next event from the queue"""

        assert self._on_idle and self.pending_event_queue, 'EventBus._start() must be called before _get_next_event()'
        if not self._is_running:
            return None

        try:
            # Create a task for queue.get() so we can cancel it cleanly
            get_next_queued_event = asyncio.create_task(self.pending_event_queue.get())
            if hasattr(get_next_queued_event, '_log_destroy_pending'):
                get_next_queued_event._log_destroy_pending = False  # type: ignore  # Suppress warnings on this task in case of cleanup

            # Wait for next event with timeout
            has_next_event, _pending = await asyncio.wait({get_next_queued_event}, timeout=wait_for_timeout)
            if has_next_event:
                # Check if we're still running before returning the event
                if not self._is_running:
                    get_next_queued_event.cancel()
                    return None
                return await get_next_queued_event  # await to actually resolve it to the next event
            else:
                # Get task timed out, cancel it cleanly to suppress warnings
                get_next_queued_event.cancel()

                # Check if we're idle, if so, set the idle flag
                if not self._has_inflight_events_fast() and self.pending_event_queue.qsize() == 0:
                    self._on_idle.set()
                return None

        except (asyncio.CancelledError, RuntimeError, QueueShutDown):
            # Clean cancellation during shutdown or queue was shut down
            return None

    async def _finalize_local_event_processing(self, event: BaseEvent[Any]) -> None:
        """
        Clear local in-flight markers and run completion propagation exactly once.

        This is shared by both `step()` and the weak runloop path so completion
        semantics stay identical regardless of which runner consumed the event.
        """
        self._processing_event_ids.discard(event.event_id)
        # Local bus consumed this event instance (or observed completion), so it
        # should not remain in this bus's active set.
        self._active_event_ids.discard(event.event_id)

        newly_completed_events = self._mark_event_tree_complete_if_ready(event)
        for completed_event in newly_completed_events:
            await self._on_event_change(completed_event, EventStatus.COMPLETED)

    def _mark_event_tree_complete_if_ready(self, root_event: BaseEvent[Any]) -> list[BaseEvent[Any]]:
        """
        Re-check completion for `root_event` and descendants in post-order.

        Timeout/cancellation paths can update child result statuses after an
        earlier completion check. Running this post-order pass ensures children
        are marked complete before their parents are re-evaluated.
        """
        newly_completed: list[BaseEvent[Any]] = []
        visited_event_ids: set[str] = set()

        def visit(event: BaseEvent[Any]) -> None:
            if event.event_id in visited_event_ids:
                return
            visited_event_ids.add(event.event_id)

            for child_event in event.event_children:
                visit(child_event)

            was_complete = self._is_event_complete_fast(event)
            # Only the root event may still appear "in-flight" on this bus during finalization.
            # Descendants are not currently being processed in this frame, so they must consider
            # queues on this bus too (otherwise queued children can be marked complete too early).
            current_bus = self if event.event_id == root_event.event_id else None
            event.event_mark_complete_if_all_handlers_completed(current_bus=current_bus)
            just_completed = (not was_complete) and self._is_event_complete_fast(event)
            if just_completed:
                self._mark_event_complete_on_all_buses(event)
                newly_completed.append(event)

        visit(root_event)
        return newly_completed

    async def step(
        self, event: 'BaseEvent[Any] | None' = None, timeout: float | None = None, wait_for_timeout: float = 0.1
    ) -> 'BaseEvent[Any] | None':
        """
        Consume and process a single event from the queue (one iteration of the run loop).

        This is the high-level "consumer" method that:
        1. Dequeues the next event (or uses one passed in)
        2. Acquires the event lock selected by concurrency mode
        3. Calls handle_event() to execute handlers
        4. Marks the queue task as done (only if event came from queue)
        5. Manages idle state signaling

        Use this method when manually driving the event loop (e.g., in tests).
        For automatic processing, use dispatch() which queues events for the run loop.

        Args:
            event: Optional event to process directly (bypasses queue if provided)
            timeout: Handler execution timeout in seconds
            wait_for_timeout: How long to wait for next event from queue (default: 0.1s)

        Returns:
            The processed event, or None if queue was empty/shutdown

        Warning:
            Passing an event directly (bypassing the queue) is for advanced use only, be aware if:

            - **Event not in queue**: Works fine, handlers execute normally.
            - **Event already completed**: Handlers will run AGAIN, overwriting previous
              results. No guard against double-processing.
            - **Event in queue but not next**: Event processes immediately, but STAYS
              in queue. The run loop will process it again later (double-processing).

        See Also:
            dispatch: Queues an event for normal async processing by the bus's existing run loop (recommended)
            handle_event: Lower-level method that executes handlers (called by step)
        """
        assert self._on_idle and self.pending_event_queue, 'EventBus._start() must be called before step()'

        # Track if we got the event from the queue
        from_queue = False

        # Wait for next event with timeout to periodically check idle state
        if event is None:
            event = await self._get_next_event(wait_for_timeout=wait_for_timeout)
            from_queue = True
        if event is None:
            return None

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('🏃 %s.step(%s) STARTING', self, event)

        # Clear idle state when we get an event
        self._on_idle.clear()

        # Acquire the event lock selected by event/bus concurrency policy.
        self._processing_event_ids.add(event.event_id)
        try:
            async with self.locks.lock_for_event(self, event):
                # Process the event
                if not self._is_event_complete_fast(event):
                    await self.handle_event(event, timeout=timeout)

                # Mark task as done only if we got it from the queue
                if from_queue:
                    self.pending_event_queue.task_done()
        finally:
            await self._finalize_local_event_processing(event)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('✅ %s.step(%s) COMPLETE', self, event)
        return event

    async def handle_event(self, event: BaseEvent[Any], timeout: float | None = None) -> None:
        """
        Execute all applicable handlers for an event (low-level, assumes lock is held).

        This is the core event handling method that:
        1. Finds all applicable handlers (type-specific + wildcard)
        2. Creates pending EventResult placeholders
        3. Executes handlers (serially or in parallel based on bus config)
        4. Marks the event as complete when all handlers finish
        5. Propagates completion status up the parent event chain
        6. Cleans up event history if over size limit

        IMPORTANT: This method assumes the caller already applied `locks.lock_for_event(...)`
        for the event execution.
        For safe external use, call step() instead which handles locking.

        Args:
            event: The event to handle
            timeout: Handler execution timeout in seconds (defaults to event.event_timeout)

        Warning:
            This is a low-level method with no safety guards. Behavior in edge cases:

            - **Event not in queue**: Works fine, handlers execute normally. This method
              does not interact with the queue at all.
            - **Event already completed**: Handlers run AGAIN, ``event_create_pending_results()``
              overwrites previous results. No guard against double-processing.
            - **Event in queue but not next**: Works fine for this call, but event stays
              in queue and will be processed again later by the run loop.
            - **Another event being processed (lock held elsewhere)**: If called without
              holding the lock, concurrent handler execution may cause race conditions.
              If called from within a handler (lock is re-entrant), causes nested processing.
            - **This exact event already being processed**: Recursive/re-entrant processing.
              Handlers run again while already running, results overwritten mid-execution.
              Likely to cause undefined behavior.

        See Also:
            step: High-level method that acquires lock and calls handle_event
            dispatch: Queues an event for async processing (recommended)
        """
        # Get applicable handlers
        applicable_handlers = self._get_applicable_handlers(event)

        event_slow_timeout = self._resolve_event_slow_timeout(event, self)
        slow_event_warning_task: asyncio.Task[None] | None = None
        if event_slow_timeout is not None:
            slow_event_warning_task = asyncio.create_task(
                self._slow_event_warning_monitor(event, event_slow_timeout),
                name=f'{self}.slow_event_monitor({event})',
            )

        # Execute handlers
        try:
            await self._execute_handlers(event, handlers=applicable_handlers, timeout=timeout)
        finally:
            if slow_event_warning_task is not None:
                slow_event_warning_task.cancel()
                try:
                    await slow_event_warning_task
                except asyncio.CancelledError:
                    pass

        # Mark event as complete and emit change if it just completed
        was_complete = self._is_event_complete_fast(event)
        event.event_mark_complete_if_all_handlers_completed(current_bus=self)
        just_completed = (not was_complete) and self._is_event_complete_fast(event)
        if just_completed:
            self._mark_event_complete_on_all_buses(event)
            await self._on_event_change(event, EventStatus.COMPLETED)

        # After processing this event, check if any parent events can now be marked complete
        # We do this by walking up the parent chain
        current = event
        checked_ids: set[str] = set()

        while current.event_parent_id and current.event_parent_id not in checked_ids:
            checked_ids.add(current.event_parent_id)

            # Find parent event in any bus's history
            parent_event = None
            parent_bus: EventBus | None = None
            # Create a list copy to avoid "Set changed size during iteration" error
            for bus in list(EventBus.all_instances):
                if bus and current.event_parent_id in bus.event_history:
                    parent_event = bus.event_history[current.event_parent_id]
                    parent_bus = bus
                    break

            if not parent_event:
                break

            # Check if parent can be marked complete
            was_complete = self._is_event_complete_fast(parent_event)
            if not was_complete:
                parent_event.event_mark_complete_if_all_handlers_completed(current_bus=parent_bus)
            just_completed = (not was_complete) and self._is_event_complete_fast(parent_event)
            if parent_bus and just_completed:
                self._mark_event_complete_on_all_buses(parent_event)
                await parent_bus._on_event_change(parent_event, EventStatus.COMPLETED)

            # Move up the chain
            current = parent_event

        # Clean up excess events to prevent memory leaks
        if (
            self.max_history_size is not None
            and self.max_history_size > 0
            and self.max_history_drop
            and len(self.event_history) > self.max_history_size
        ):
            self.cleanup_event_history()

    def _get_applicable_handlers(self, event: BaseEvent[Any]) -> dict[PythonIdStr, EventHandler]:
        """Get all handlers that should process the given event, filtering out those that would create loops"""
        applicable_handlers: list[EventHandler] = []

        for key in (event.event_type, '*'):
            indexed_ids = self.handlers_by_key.get(key, [])
            if not indexed_ids:
                continue
            for handler_id in indexed_ids:
                handler_entry = self.handlers.get(handler_id)
                if handler_entry:
                    applicable_handlers.append(handler_entry)

        # Filter out handlers that would create loops and build id->handler mapping
        # Use handler id as key to preserve all handlers even with duplicate names
        filtered_handlers: dict[PythonIdStr, EventHandler] = {}
        for handler_entry in applicable_handlers:
            if self._would_create_loop(event, handler_entry):
                continue
            else:
                assert handler_entry.id is not None
                filtered_handlers[handler_entry.id] = handler_entry
                # logger.debug(f'  Found handler {EventHandler._get_callable_handler_name(cast(Any, handler))}#{handler_id[-4:]}()')

        return filtered_handlers

    def _enter_handler_execution_context(
        self, event: BaseEvent[Any], handler_id: str
    ) -> tuple[contextvars.Token[Any], contextvars.Token[str | None]]:
        event_token = _current_event_context.set(event)
        current_handler_token = _current_handler_id_context.set(handler_id)
        return event_token, current_handler_token

    def _exit_handler_execution_context(
        self,
        handler_context_tokens: tuple[contextvars.Token[Any], contextvars.Token[str | None]],
    ) -> None:
        event_token, current_handler_token = handler_context_tokens
        _current_event_context.reset(event_token)
        _current_handler_id_context.reset(current_handler_token)

    @staticmethod
    def _first_mode_result_is_winner(event_result: EventResult[Any]) -> bool:
        if event_result.status != 'completed':
            return False
        if event_result.error is not None:
            return False
        if event_result.result is None:
            return False
        if isinstance(event_result.result, BaseEvent):
            return False
        return True

    async def _mark_remaining_first_mode_result_cancelled(
        self,
        event: BaseEvent[Any],
        event_result: EventResult[Any],
    ) -> None:
        if event_result.status in ('completed', 'error'):
            return
        event_result.update(error=asyncio.CancelledError('Cancelled: first() resolved'))
        await self._on_event_result_change(event, event_result, EventStatus.COMPLETED)

    async def _execute_handlers(
        self,
        event: BaseEvent[Any],
        handlers: dict[PythonIdStr, EventHandler] | None = None,
        timeout: float | None = None,
    ) -> None:
        """Execute all handlers for an event using the configured concurrency mode."""
        applicable_handlers = handlers if (handlers is not None) else self._get_applicable_handlers(event)
        if not applicable_handlers:
            return  # handle_event will mark complete

        pending_handler_map: dict[PythonIdStr, EventHandler | EventHandlerCallable] = dict(applicable_handlers)
        pending_results = event.event_create_pending_results(
            pending_handler_map,
            eventbus=self,
            timeout=timeout if timeout is not None else event.event_timeout,
        )
        # Resolve future find waiters after pending handler results exist, so
        # callers can observe in-flight handler state on the returned event.
        if self._find_waiters:
            for waiter in tuple(self._find_waiters):
                if waiter.event_key != '*' and event.event_type != waiter.event_key:
                    continue
                if not waiter.matches(event):
                    continue
                if waiter.timeout_handle is not None:
                    waiter.timeout_handle.cancel()
                self._find_waiters.discard(waiter)
                if not waiter.future.done():
                    waiter.future.set_result(event)
        if self.middlewares:
            for pending_result in pending_results.values():
                await self._on_event_result_change(event, pending_result, EventStatus.PENDING)

        # Execute handlers in the configured mode.
        completion_mode = event.event_handler_completion or self.event_handler_completion

        handler_items = list(applicable_handlers.items())

        concurrency_mode = event.event_handler_concurrency or self.event_handler_concurrency

        if concurrency_mode == EventHandlerConcurrencyMode.PARALLEL:
            if completion_mode == EventHandlerCompletionMode.FIRST:
                handler_tasks: dict[asyncio.Task[Any], PythonIdStr] = {}
                local_handler_ids: set[PythonIdStr] = set(applicable_handlers.keys())
                for handler_id, handler_entry in applicable_handlers.items():
                    handler_tasks[asyncio.create_task(self.execute_handler(event, handler_entry, timeout=timeout))] = handler_id

                pending_tasks: set[asyncio.Task[Any]] = set(handler_tasks.keys())
                winner_handler_id: PythonIdStr | None = None

                while pending_tasks:
                    done_tasks, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
                    for done_task in done_tasks:
                        try:
                            await done_task
                        except Exception:
                            # Error already logged and recorded in execute_handler
                            pass

                        done_handler_id = handler_tasks[done_task]
                        completed_result = event.event_results.get(done_handler_id)
                        if completed_result is not None and self._first_mode_result_is_winner(completed_result):
                            winner_handler_id = done_handler_id
                            break

                    if winner_handler_id is not None:
                        break

                if winner_handler_id is not None:
                    for pending_task in pending_tasks:
                        pending_task.cancel()
                    if pending_tasks:
                        await asyncio.gather(*pending_tasks, return_exceptions=True)

                    for handler_id, event_result in event.event_results.items():
                        if handler_id not in local_handler_ids or handler_id == winner_handler_id:
                            continue
                        await self._mark_remaining_first_mode_result_cancelled(event, event_result)
                else:
                    if pending_tasks:
                        await asyncio.gather(*pending_tasks, return_exceptions=True)
                return

            parallel_tasks = [
                asyncio.create_task(self.execute_handler(event, handler_entry, timeout=timeout))
                for _, handler_entry in handler_items
            ]
            for task in parallel_tasks:
                try:
                    await task
                except Exception:
                    # Error already logged and recorded in execute_handler
                    pass
            return

        for index, (handler_id, handler_entry) in enumerate(handler_items):
            try:
                await self.execute_handler(event, handler_entry, timeout=timeout)
            except Exception as e:
                # Error already logged and recorded in execute_handler
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        '❌ %s Handler %s#%s(%s) failed with %s: %s',
                        self,
                        handler_entry.handler_name,
                        handler_entry.id[-4:] if handler_entry.id else '----',
                        event,
                        type(e).__name__,
                        e,
                    )

            if completion_mode != EventHandlerCompletionMode.FIRST:
                continue

            completed_result = event.event_results.get(handler_id)
            if completed_result is None or not self._first_mode_result_is_winner(completed_result):
                continue

            for remaining_handler_id, _ in handler_items[index + 1 :]:
                remaining_result = event.event_results.get(remaining_handler_id)
                if remaining_result is None:
                    continue
                await self._mark_remaining_first_mode_result_cancelled(event, remaining_result)
            break

        # print('FINSIHED EXECUTING ALL HANDLERS')

    async def execute_handler(
        self,
        event: 'BaseEvent[T_EventResultType]',
        handler_entry: EventHandler,
        timeout: float | None = None,
    ) -> Any:
        """Safely execute a single handler with middleware support and EventResult orchestration."""

        handler_id = handler_entry.id or handler_entry.compute_handler_id()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                ' ↳ %s.execute_handler(%s, handler=%s#%s)',
                self,
                event,
                handler_entry.handler_name,
                handler_id[-4:],
            )

        resolved_timeout = self._resolve_handler_timeout(event, handler_entry, self, timeout_override=timeout)
        resolved_slow_timeout = self._resolve_handler_slow_timeout(event, handler_entry, self)

        if handler_id not in event.event_results:
            new_results = event.event_create_pending_results({handler_id: handler_entry}, eventbus=self, timeout=resolved_timeout)
            for pending_result in new_results.values():
                await self._on_event_result_change(event, pending_result, EventStatus.PENDING)

        event_result = event.event_results[handler_id]

        # Check if this is the first handler to start (before updating status)
        is_first_handler = not any(r.started_at for r in event.event_results.values())

        event_result.update(status='started', timeout=resolved_timeout)
        await self._on_event_result_change(event, event_result, EventStatus.STARTED)

        # Emit event STARTED once (when first handler starts)
        if is_first_handler:
            await self._on_event_change(event, EventStatus.STARTED)

        try:
            async with self.locks.lock_for_event_handler(self, event, event_result):
                result_value = await event_result.execute(
                    event,
                    eventbus=self,
                    timeout=resolved_timeout,
                    slow_timeout=resolved_slow_timeout,
                    enter_handler_context=self._enter_handler_execution_context,
                    exit_handler_context=self._exit_handler_execution_context,
                    format_exception_for_log=log_filtered_traceback,
                )

            result_type_name = type(result_value).__name__ if result_value is not None else 'None'
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    '    ↳ Handler %s#%s returned: %s',
                    handler_entry.handler_name,
                    handler_id[-4:],
                    result_type_name,
                )

            await self._on_event_result_change(event, event_result, EventStatus.COMPLETED)
            return cast(T_EventResultType, result_value)

        except asyncio.CancelledError:
            await self._on_event_result_change(event, event_result, EventStatus.COMPLETED)
            raise
        except Exception:
            await self._on_event_result_change(event, event_result, EventStatus.COMPLETED)
            raise

    def _would_create_loop(self, event: BaseEvent[Any], handler_entry: EventHandler) -> bool:
        """Check if calling this handler would create a loop"""
        handler = handler_entry.handler
        if handler is None:
            return False

        # First check: If handler is another EventBus.dispatch method, check if we're forwarding to another bus that it's already been processed by
        bound_self = getattr(handler, '__self__', None)
        bound_name = getattr(handler, '__name__', None)
        if isinstance(bound_self, EventBus) and bound_name == 'dispatch':
            target_bus = bound_self
            if target_bus.label in event.event_path:
                logger.debug(
                    f'⚠️ {self} handler {handler_entry.label}({event}) skipped to prevent infinite forwarding loop with {target_bus.label}'
                )
                return True

        # Second check: Check if there's already a result (pending or completed) for this handler on THIS bus
        # We use a combination of bus ID and handler ID to allow the same handler function
        # to run on different buses (important for forwarding)
        handler_id = handler_entry.id or handler_entry.compute_handler_id()
        if handler_id in event.event_results:
            existing_result = event.event_results[handler_id]
            if existing_result.status == 'pending' or existing_result.status == 'started':
                logger.debug(
                    f'⚠️ {self} handler {handler_entry.label}({event}) is already {existing_result.status} for event {event.event_id} (preventing recursive call)'
                )
                return True
            elif existing_result.completed_at is not None:
                logger.debug(
                    f'⚠️ {self} handler {handler_entry.label}({event}) already completed @ {existing_result.completed_at} for event {event.event_id} (will not re-run)'
                )
                return True

        # Third check: For non-forwarding handlers, check recursion depth
        # Forwarding handlers (EventBus.dispatch) are allowed to forward at any depth
        is_forwarding_handler = (
            inspect.ismethod(handler) and isinstance(handler.__self__, EventBus) and handler.__name__ == 'dispatch'
        )

        if not is_forwarding_handler:
            # Only check recursion for regular handlers, not forwarding
            recursion_depth = self._handler_dispatched_ancestor(event, handler_id)
            if recursion_depth > 2:
                raise RuntimeError(
                    f'Infinite loop detected: Handler {handler_entry.label} '
                    f'has recursively processed {recursion_depth} levels of events. '
                    f'Current event: {event}, Handler: {handler_id}'
                )
            elif recursion_depth == 2:
                logger.warning(
                    f'⚠️ {self} handler {handler_entry.label} '
                    f'at maximum recursion depth (2 levels) - next level will raise exception'
                )

        return False

    def _handler_dispatched_ancestor(
        self, event: BaseEvent[Any], handler_id: str, visited: set[str] | None = None, depth: int = 0
    ) -> int:
        """Check how many times this handler appears in the ancestry chain. Returns the depth count."""
        # Prevent infinite recursion in case of circular parent references
        if visited is None:
            visited = set()
        if event.event_id in visited:
            return depth
        visited.add(event.event_id)

        # If this event has no parent, it's a root event - no ancestry to check
        if not event.event_parent_id:
            return depth

        # Find parent event in any bus's history
        parent_event = None
        # Create a list copy to avoid "Set changed size during iteration" error
        for bus in list(EventBus.all_instances):
            if event.event_parent_id in bus.event_history:
                parent_event = bus.event_history[event.event_parent_id]
                break

        if not parent_event:
            return depth

        # Check if this handler processed the parent event
        if handler_id in parent_event.event_results:
            result = parent_event.event_results[handler_id]
            if result.status in ('pending', 'started', 'completed'):
                # This handler processed the parent event, increment depth
                depth += 1

        # Recursively check the parent's ancestry
        return self._handler_dispatched_ancestor(parent_event, handler_id, visited, depth)

    def cleanup_excess_events(self) -> int:
        """
        Clean up excess events from event_history based on max_history_size.

        Returns:
            Number of events removed from history
        """
        if self.max_history_size is None:
            return 0
        if self.max_history_size == 0:
            return self.cleanup_event_history()
        if len(self.event_history) <= self.max_history_size:
            return 0

        # event_history preserves insertion order, so oldest dispatched events are first.
        # Avoid per-cleanup O(n log n) sorting by timestamp in this hot-path helper.
        total_events = len(self.event_history)
        remove_count = total_events - self.max_history_size
        event_ids_to_remove = list(self.event_history.keys())[:remove_count]

        for event_id in event_ids_to_remove:
            del self.event_history[event_id]

        if event_ids_to_remove:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('🧹 %s Cleaned up %d excess events from history', self, len(event_ids_to_remove))

        return len(event_ids_to_remove)

    def cleanup_event_history(self) -> int:
        """
        Clean up event history to maintain max_history_size limit.
        Prioritizes keeping pending/started events over completed ones.

        Returns:
            Total number of events removed from history
        """
        if self.max_history_size is None:
            return 0
        if self.max_history_size == 0:
            completed_event_ids = [
                event_id for event_id, event in self.event_history.items() if self._is_event_complete_fast(event)
            ]
            for event_id in completed_event_ids:
                del self.event_history[event_id]
            return len(completed_event_ids)
        if len(self.event_history) <= self.max_history_size:
            return 0

        # Separate events by status
        pending_events: list[tuple[str, BaseEvent[Any]]] = []
        started_events: list[tuple[str, BaseEvent[Any]]] = []
        completed_events: list[tuple[str, BaseEvent[Any]]] = []

        for event_id, event in self.event_history.items():
            if self._is_event_complete_fast(event):
                completed_events.append((event_id, event))
            elif self._is_event_started_fast(event):
                started_events.append((event_id, event))
            else:
                pending_events.append((event_id, event))

        # Calculate how many to remove
        total_events = len(self.event_history)
        events_to_remove_count = total_events - self.max_history_size

        events_to_remove: list[str] = []

        # First remove completed events (oldest first)
        if completed_events and events_to_remove_count > 0:
            remove_from_completed = min(len(completed_events), events_to_remove_count)
            events_to_remove.extend([event_id for event_id, _ in completed_events[:remove_from_completed]])
            events_to_remove_count -= remove_from_completed

        # If still need to remove more, remove oldest started events
        if events_to_remove_count > 0 and started_events:
            remove_from_started = min(len(started_events), events_to_remove_count)
            events_to_remove.extend([event_id for event_id, _ in started_events[:remove_from_started]])
            events_to_remove_count -= remove_from_started

        # If still need to remove more, remove oldest pending events
        if events_to_remove_count > 0 and pending_events:
            events_to_remove.extend([event_id for event_id, _ in pending_events[:events_to_remove_count]])

        # Remove the events
        for event_id in events_to_remove:
            del self.event_history[event_id]

        if events_to_remove:
            completed_event_ids = {event_id for event_id, _ in completed_events}
            dropped_uncompleted = sum(1 for event_id in events_to_remove if event_id not in completed_event_ids)
            logger.debug(
                f'🧹 {self} Cleaned up {len(events_to_remove)} events from history (kept {len(self.event_history)}/{self.max_history_size})'
            )
            if dropped_uncompleted > 0 and not self._warned_about_dropping_uncompleted_events:
                self._warned_about_dropping_uncompleted_events = True
                logger.warning(
                    '[bubus] ⚠️ Bus %s has exceeded max_history_size=%s and is dropping oldest history entries '
                    '(even uncompleted events). Increase max_history_size or set max_history_drop=False to reject.',
                    self,
                    self.max_history_size,
                )

        return len(events_to_remove)

    def log_tree(self) -> str:
        """Print a nice pretty formatted tree view of all events in the history including their results and child events recursively"""
        from bubus.logging import log_eventbus_tree

        return log_eventbus_tree(self)

    def _check_total_memory_usage(self) -> None:
        """Check total memory usage across all EventBus instances and warn if >50MB"""
        import sys

        total_bytes = 0
        bus_details: list[tuple[str, int, int, int]] = []

        # Iterate through all EventBus instances
        # Create a list copy to avoid "Set changed size during iteration" error
        for bus in list(EventBus.all_instances):
            try:
                bus_bytes = 0

                # Count events in history
                for event in bus.event_history.values():
                    bus_bytes += sys.getsizeof(event)
                    # Also count the event's data
                    if hasattr(event, '__dict__'):
                        for attr_value in event.__dict__.values():
                            if isinstance(attr_value, (str, bytes, list, dict)):
                                bus_bytes += sys.getsizeof(attr_value)  # pyright: ignore[reportUnknownArgumentType]

                # Count events in queue
                if bus.pending_event_queue:
                    # Access internal queue storage
                    if hasattr(bus.pending_event_queue, '_queue'):
                        queue: deque[BaseEvent] = bus.pending_event_queue._queue  # type: ignore[attr-defined]
                        for event in queue:  # pyright: ignore[reportUnknownVariableType]
                            bus_bytes += sys.getsizeof(event)  # pyright: ignore[reportUnknownArgumentType]
                            if hasattr(event, '__dict__'):  # pyright: ignore[reportUnknownArgumentType]
                                for attr_value in event.__dict__.values():  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                                    if isinstance(attr_value, (str, bytes, list, dict)):
                                        bus_bytes += sys.getsizeof(attr_value)  # pyright: ignore[reportUnknownArgumentType]

                total_bytes += bus_bytes
                bus_details.append(
                    (
                        bus.label,
                        bus_bytes,
                        len(bus.event_history),
                        bus.pending_event_queue.qsize() if bus.pending_event_queue else 0,
                    )
                )
            except Exception:
                # Skip buses that can't be measured
                continue

        total_mb = total_bytes / (1024 * 1024)

        if total_mb > 50:
            # Build detailed breakdown
            details: list[str] = []
            for name, bytes_used, history_size, queue_size in sorted(bus_details, key=lambda x: x[1], reverse=True):  # pyright: ignore[reportUnknownLambdaType]
                mb = bytes_used / (1024 * 1024)
                if mb > 0.1:  # Only show buses using >0.1MB
                    details.append(f'  - {name}: {mb:.1f}MB (history={history_size}, queue={queue_size})')

            warning_msg = (
                f'\n⚠️  WARNING: Total EventBus memory usage is {total_mb:.1f}MB (>50MB limit)\n'
                f'Active EventBus instances: {len(EventBus.all_instances)}\n'
            )
            if details:
                warning_msg += 'Memory breakdown:\n' + '\n'.join(details[:5])  # Show top 5
                if len(details) > 5:
                    warning_msg += f'\n  ... and {len(details) - 5} more'

            warning_msg += '\nConsider:\n'
            warning_msg += '  - Reducing max_history_size\n'
            warning_msg += '  - Clearing completed EventBus instances with stop(clear=True)\n'
            warning_msg += '  - Reducing event payload sizes\n'

            logger.warning(warning_msg)

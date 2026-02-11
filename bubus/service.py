import asyncio
import contextvars
import inspect
import logging
import traceback
import warnings
import weakref
from collections import defaultdict, deque
from collections.abc import Callable, Sequence
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, TypeVar, cast, overload

from uuid_extensions import uuid7str  # pyright: ignore[reportMissingImports, reportUnknownVariableType]

uuid7str: Callable[[], str] = uuid7str  # pyright: ignore

from bubus.event_history import EventHistory
from bubus.models import (
    BUBUS_LOGGING_LEVEL,
    AsyncEventHandlerClassMethod,
    AsyncEventHandlerFunc,
    AsyncEventHandlerMethod,
    BaseEvent,
    ContravariantEventHandler,
    EventHandler,
    EventHandlerClassMethod,
    EventHandlerFunc,
    EventHandlerMethod,
    EventResult,
    EventStatus,
    PythonIdentifierStr,
    PythonIdStr,
    T_Event,
    T_EventResultType,
    UUIDStr,
    get_handler_id,
    get_handler_name,
)

logger = logging.getLogger('bubus')
logger.setLevel(BUBUS_LOGGING_LEVEL)


# Define our own QueueShutDown exception
class QueueShutDown(Exception):
    """Raised when putting on to or getting from a shut-down Queue."""

    pass


QueueEntryType = TypeVar('QueueEntryType', bound=BaseEvent[Any])
T_ExpectedEvent = TypeVar('T_ExpectedEvent', bound=BaseEvent[Any])
T_QueryEvent = TypeVar('T_QueryEvent', bound=BaseEvent[Any])
T_QueryEvent = TypeVar('T_QueryEvent', bound=BaseEvent[Any])

EventPatternType = PythonIdentifierStr | Literal['*'] | type[BaseEvent[Any]]


class EventBusMiddleware:
    """Hookable lifecycle interface for observing or extending EventBus execution.

    Hooks:
        on_event_change(eventbus, event, status): Called on event state transitions
        on_event_result_change(eventbus, event, event_result, status): Called on EventResult state transitions

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


class CleanShutdownQueue(asyncio.Queue[QueueEntryType]):
    """asyncio.Queue subclass that handles shutdown cleanly without warnings."""

    _is_shutdown: bool = False
    _getters: deque[asyncio.Future[QueueEntryType]]
    _putters: deque[asyncio.Future[QueueEntryType]]

    def shutdown(self, immediate: bool = True):
        """Shutdown the queue and clean up all pending futures."""
        self._is_shutdown = True

        # Cancel all waiting getters without triggering warnings
        while self._getters:
            getter = self._getters.popleft()
            if not getter.done():
                # Set exception instead of cancelling to avoid "Event loop is closed" errors
                getter.set_exception(QueueShutDown())

        # Cancel all waiting putters
        while self._putters:
            putter = self._putters.popleft()
            if not putter.done():
                putter.set_exception(QueueShutDown())

    async def get(self) -> QueueEntryType:
        """Remove and return an item from the queue, with shutdown support."""
        while self.empty():
            if self._is_shutdown:
                raise QueueShutDown

            getter: asyncio.Future[QueueEntryType] = self._get_loop().create_future()  # type: ignore
            assert isinstance(getter, asyncio.Future)
            self._getters.append(getter)  # type: ignore[arg-type]
            try:
                await getter
            except:
                # Clean up the getter if we're cancelled
                getter.cancel()  # Just in case getter is not done yet.
                try:
                    self._getters.remove(getter)  # type: ignore[arg-type]
                except ValueError:
                    pass
                # Re-raise the exception
                raise

        return self.get_nowait()

    async def put(self, item: QueueEntryType) -> None:
        """Put an item into the queue, with shutdown support."""
        while self.full():
            if self._is_shutdown:
                raise QueueShutDown

            putter: asyncio.Future[QueueEntryType] = self._get_loop().create_future()  # type: ignore
            assert isinstance(putter, asyncio.Future)
            self._putters.append(putter)  # type: ignore[arg-type]
            try:
                await putter
            except:
                putter.cancel()  # Just in case putter is not done yet.
                try:
                    self._putters.remove(putter)  # type: ignore[arg-type]
                except ValueError:
                    pass
                raise

        return self.put_nowait(item)

    def put_nowait(self, item: QueueEntryType) -> None:
        """Put an item into the queue without blocking, with shutdown support."""
        if self._is_shutdown:
            raise QueueShutDown
        return super().put_nowait(item)

    def get_nowait(self) -> QueueEntryType:
        """Remove and return an item if one is immediately available, with shutdown support."""
        if self._is_shutdown and self.empty():
            raise QueueShutDown
        return super().get_nowait()


# Context variable to track the current event being processed (for setting event_parent_id from inside a child event)
_current_event_context: ContextVar[BaseEvent[Any] | None] = ContextVar('current_event', default=None)
# Context variable to track if we're inside a handler (for nested event detection)
inside_handler_context: ContextVar[bool] = ContextVar('inside_handler', default=False)
# Context variable to track if we hold the global lock (for re-entrancy across tasks)
holds_global_lock: ContextVar[bool] = ContextVar('holds_global_lock', default=False)
# Context variable to track the current handler ID (for tracking child events)
_current_handler_id_context: ContextVar[str | None] = ContextVar('current_handler_id', default=None)


class ReentrantLock:
    """A re-entrant lock that works across different asyncio tasks using ContextVar."""

    def __init__(self):
        self._semaphore: asyncio.Semaphore | None = None
        self._depth = 0  # Track re-entrance depth
        self._loop: asyncio.AbstractEventLoop | None = None

    def _get_semaphore(self) -> asyncio.Semaphore:
        """Get or create the semaphore for the current event loop."""
        current_loop = asyncio.get_running_loop()
        if self._semaphore is None or self._loop != current_loop:
            # Create new semaphore for this event loop
            self._semaphore = asyncio.Semaphore(1)
            self._loop = current_loop
        return self._semaphore

    async def __aenter__(self):
        if holds_global_lock.get():
            # We already hold the lock in this context, increment depth
            self._depth += 1
            return self

        # Acquire the lock
        await self._get_semaphore().acquire()
        holds_global_lock.set(True)
        self._depth = 1
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        if not holds_global_lock.get():
            # We don't hold the lock, nothing to do
            return

        self._depth -= 1
        if self._depth == 0:
            # Last exit, release the lock
            holds_global_lock.set(False)
            self._get_semaphore().release()

    def locked(self) -> bool:
        """Check if the lock is currently held."""
        # If semaphore doesn't exist yet or is from a different loop, it's not locked
        try:
            current_loop = asyncio.get_running_loop()
            if self._semaphore is None or self._loop != current_loop:
                return False
            return self._semaphore.locked()
        except RuntimeError:
            # No running loop, can't check
            return False


# Global re-entrant lock shared by all EventBus instances
_global_eventbus_lock: ReentrantLock | None = None


def _get_global_lock() -> ReentrantLock:
    """Get or create the global EventBus lock."""
    global _global_eventbus_lock
    if _global_eventbus_lock is None:
        _global_eventbus_lock = ReentrantLock()
    return _global_eventbus_lock


def _log_pretty_path(path: Path | str | None) -> str:
    """Pretty-print a path, shorten home dir to ~ and cwd to ."""

    if not path or not str(path).strip():
        return ''  # always falsy in -> falsy out so it can be used in ternaries

    # dont print anything thats not a path
    if not isinstance(path, (str, Path)):  # type: ignore
        # no other types are safe to just str(path) and log to terminal unless we know what they are
        # e.g. what if we get storage_date=dict | Path and the dict version could contain real cookies
        return f'<{type(path).__name__}>'

    # replace home dir and cwd with ~ and .
    pretty_path = str(path).replace(str(Path.home()), '~').replace(str(Path.cwd().resolve()), '.')

    # wrap in quotes if it contains spaces
    if pretty_path.strip() and ' ' in pretty_path:
        pretty_path = f'"{pretty_path}"'

    return pretty_path


def _log_filtered_traceback(exc: BaseException) -> str:
    trace_exc = traceback.TracebackException.from_exception(exc, capture_locals=False)

    def _filter(_: traceback.TracebackException):
        trace_exc.stack = traceback.StackSummary.from_list(
            [f for f in trace_exc.stack if 'asyncio/tasks.py' not in f.filename and 'lib/python' not in f.filename]
        )
        if trace_exc.__cause__:
            _filter(trace_exc.__cause__)
        if trace_exc.__context__:
            _filter(trace_exc.__context__)

    _filter(trace_exc)
    return ''.join(trace_exc.format())


class EventBus:
    """
    Async event bus with write-ahead logging and guaranteed FIFO processing.

    Features:
    - Enqueue events synchronously, await their results using 'await Event()'
    - FIFP Write-ahead logging with UUIDs and timestamps,
    - Serial event processing, parallel handler execution per event
    """

    # Track all EventBus instances (using weakrefs to allow garbage collection)
    all_instances: weakref.WeakSet['EventBus'] = weakref.WeakSet()

    # Class Attributes
    name: PythonIdentifierStr = 'EventBus'
    parallel_handlers: bool = False
    max_history_drop: bool = True

    # Runtime State
    id: UUIDStr = '00000000-0000-0000-0000-000000000000'
    handlers: dict[PythonIdStr, list[ContravariantEventHandler[BaseEvent[Any]]]]
    event_queue: CleanShutdownQueue[BaseEvent[Any]] | None
    event_history: EventHistory[BaseEvent[Any]]

    _is_running: bool = False
    _runloop_task: asyncio.Task[None] | None = None
    _on_idle: asyncio.Event | None = None
    _active_event_ids: set[str]
    _processing_event_ids: set[str]
    _warned_about_dropping_uncompleted_events: bool
    _duplicate_handler_name_check_limit: int = 256

    def __init__(
        self,
        name: PythonIdentifierStr | None = None,
        parallel_handlers: bool = False,
        max_history_size: int | None = 50,  # Keep only 50 events in history
        max_history_drop: bool = True,
        middlewares: Sequence[EventBusMiddleware] | None = None,
    ):
        self.id = uuid7str()
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

        self.event_queue = None
        self.event_history = EventHistory()
        self.handlers = defaultdict(list)
        self.parallel_handlers = parallel_handlers
        self._on_idle = None
        self.middlewares: list[EventBusMiddleware] = list(middlewares or [])
        self._active_event_ids = set()
        self._processing_event_ids = set()
        self._warned_about_dropping_uncompleted_events = False

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
        queue_size = self.event_queue.qsize() if self.event_queue else 0
        return f'{self.name}{icon}(queue={queue_size} active={len(self._active_event_ids)} history={len(self.event_history)} handlers={len(self.handlers)})'

    @property
    def label(self) -> str:
        return f'{self.name}#{self.id[-4:]}'

    def __repr__(self) -> str:
        return str(self)

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

    # 1. EventHandlerFunc[T_Event] - sync function taking event
    @overload
    def on(self, event_pattern: EventPatternType, handler: EventHandlerFunc[T_Event]) -> None: ...

    # 2. AsyncEventHandlerFunc[T_Event] - async function taking event
    @overload
    def on(self, event_pattern: EventPatternType, handler: AsyncEventHandlerFunc[T_Event]) -> None: ...

    # 3. EventHandlerMethod[T_Event] - sync method taking self and event
    @overload
    def on(self, event_pattern: EventPatternType, handler: EventHandlerMethod[T_Event]) -> None: ...

    # 4. AsyncEventHandlerMethod[T_Event] - async method taking self and event
    @overload
    def on(self, event_pattern: EventPatternType, handler: AsyncEventHandlerMethod[T_Event]) -> None: ...

    # 5. EventHandlerClassMethod[BaseEvent] - sync classmethod taking cls and event
    @overload
    def on(self, event_pattern: EventPatternType, handler: EventHandlerClassMethod[BaseEvent[Any]]) -> None: ...

    # 6. AsyncEventHandlerClassMethod[BaseEvent] - async classmethod taking cls and event
    @overload
    def on(self, event_pattern: EventPatternType, handler: AsyncEventHandlerClassMethod[BaseEvent[Any]]) -> None: ...

    # I dont think this is needed, but leaving it here for now
    # 9. Coroutine[Any, Any, Any] - direct coroutine
    # @overload # type: ignore[reportUnknownReturnType]
    # def on(self, event_pattern: EventPatternType, handler: Coroutine[Any, Any, Any]) -> None: ...

    def on(
        self,
        event_pattern: EventPatternType,
        handler: (  # TypeAlias with args doesn't work on overloaded signature as of 2025, has to be defined inline!
            EventHandlerFunc[T_Event]
            | AsyncEventHandlerFunc[BaseEvent[Any]]
            | EventHandlerMethod[T_Event]
            | AsyncEventHandlerMethod[BaseEvent[Any]]
            | EventHandlerClassMethod[BaseEvent[Any]]
            | AsyncEventHandlerClassMethod[BaseEvent[Any]]
        ),
    ) -> None:
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
        assert isinstance(event_pattern, str) or issubclass(event_pattern, BaseEvent), (
            f'Invalid event pattern: {event_pattern}, must be a string event type or subclass of BaseEvent'
        )
        assert inspect.isfunction(handler) or inspect.ismethod(handler) or inspect.iscoroutinefunction(handler), (
            f'Invalid handler: {handler}, must be a sync or async function or method'
        )

        # Determine event key
        event_key: str
        if event_pattern == '*':
            event_key = '*'
        elif isinstance(event_pattern, type) and issubclass(event_pattern, BaseEvent):  # pyright: ignore[reportUnnecessaryIsInstance]
            event_key = event_pattern.__name__  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        else:
            event_key = str(event_pattern)

        # Ensure event_key is definitely a string at this point
        assert isinstance(event_key, str)

        # Check for duplicate handler names. Keep this bounded so large handler
        # registrations (e.g. perf scenarios with tens of thousands of handlers)
        # do not degrade into O(n^2) registration time.
        new_handler_name = get_handler_name(handler)
        existing_handlers = self.handlers.get(event_key, [])
        if existing_handlers and len(existing_handlers) <= self._duplicate_handler_name_check_limit:
            for existing_handler in existing_handlers:
                if get_handler_name(existing_handler) == new_handler_name:
                    warnings.warn(
                        f"⚠️ {self} Handler {new_handler_name} already registered for event '{event_key}'. "
                        f'This may make it difficult to filter event results by handler name. '
                        f'Consider using unique function names.',
                        UserWarning,
                        stacklevel=2,
                    )
                    break

        # Register handler
        self.handlers[event_key].append(handler)  # type: ignore
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                '👂 %s.on(%s, %s) Registered event handler',
                self,
                event_key,
                get_handler_name(handler),
            )

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
        assert event.event_schema and '@' in event.event_schema, 'Missing event.event_schema: str (with @version)'

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
        if current_handler_id is not None and inside_handler_context.get():
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
        if self.max_history_size is not None and not self.max_history_drop and len(self.event_history) >= self.max_history_size:
            raise RuntimeError(
                f'{self} history limit reached ({len(self.event_history)}/{self.max_history_size}); '
                'set max_history_drop=True to drop old history instead of rejecting new events'
            )

        # Auto-start if needed
        self._start()

        # Put event in queue synchronously using put_nowait
        if self.event_queue:
            try:
                self.event_queue.put_nowait(event)
                # Only add to history after successfully queuing
                self.event_history[event.event_id] = event
                self._active_event_ids.add(event.event_id)
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
                        self.event_queue.qsize(),
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
        if self.max_history_size and self.max_history_drop:
            soft_limit = max(self.max_history_size, int(self.max_history_size * 1.2))
            if len(self.event_history) > soft_limit:
                self.cleanup_event_history()

        return event

    def emit(self, event: T_ExpectedEvent) -> T_ExpectedEvent:
        """Alias for dispatch(), mirroring EventEmitter-style APIs."""
        return self.dispatch(event)

    def _event_matches_pattern(self, event: BaseEvent[Any], pattern: EventPatternType) -> bool:
        if pattern == '*':
            return True
        if isinstance(pattern, str):
            return event.event_type == pattern
        return isinstance(event, pattern)

    @overload
    async def find(
        self,
        event_type: type[T_ExpectedEvent],
        where: Callable[[BaseEvent[Any] | T_ExpectedEvent], bool] = lambda _: True,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float = True,
        future: bool | float = True,
    ) -> T_ExpectedEvent | None: ...

    @overload
    async def find(
        self,
        event_type: PythonIdentifierStr,
        where: Callable[[BaseEvent[Any]], bool] = lambda _: True,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float = True,
        future: bool | float = True,
    ) -> BaseEvent[Any] | None: ...

    async def find(
        self,
        event_type: PythonIdentifierStr | type[T_ExpectedEvent],
        where: Callable[[BaseEvent[Any]], bool] = lambda _: True,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float = True,
        future: bool | float = True,
    ) -> BaseEvent[Any] | T_ExpectedEvent | None:
        """
        Find an event matching criteria in history and/or future.

        This is a unified method that can search past event_history, wait for future
        events, or both. Use this instead of separate query() and expect() calls.

        Args:
            event_type: The event type string or model class to find
            where: Predicate function for filtering (default: lambda _: True)
            child_of: Only match events that are descendants of this parent event
            past: Controls history search behavior:
                - True: search all history
                - False: skip history search
                - float: search events from last N seconds only
            future: Controls future wait behavior:
                - True: wait forever for matching event
                - False: don't wait for future events
                - float: wait up to N seconds for matching event

        Returns:
            Matching event or None if not found/timeout

        Examples:
            # Search all history, wait up to 5s for future
            event = await bus.find(EventType, past=True, future=5)

            # Search last 5s of history, wait forever
            event = await bus.find(EventType, past=5, future=True)

            # Search last 5s of history, wait up to 5s
            event = await bus.find(EventType, past=5, future=5)

            # Search all history instantly, don't wait (debouncing)
            event = await bus.find(EventType, past=True, future=False)

            # Wait up to 5s for future only (like old expect)
            event = await bus.find(EventType, past=False, future=5)

            # Find child event that may have already fired
            nav_event = await bus.dispatch(NavigateToUrlEvent(...))
            new_tab = await bus.find(TabCreatedEvent, child_of=nav_event, past=True, future=5)
        """
        # If neither past nor future, return None immediately
        if past is False and future is False:
            return None

        # Build combined predicate including child_of check
        def matches(event: BaseEvent[Any]) -> bool:
            if not where(event):
                return False
            if child_of is not None and not self.event_is_child_of(event, child_of):
                return False
            return True

        # Search past history if enabled
        if past is not False:
            # Calculate cutoff time if past is a float (time window in seconds)
            cutoff: datetime | None = None
            if past is not True:  # past is a float/int specifying time window
                cutoff = datetime.now(UTC) - timedelta(seconds=float(past))

            events = list(self.event_history.values())
            for event in reversed(events):
                # Only match completed events in history
                if event.event_completed_at is None:
                    continue
                # Skip events older than cutoff (dispatched before the time window)
                if cutoff is not None and event.event_created_at < cutoff:
                    continue
                if not self._event_matches_pattern(event, event_type):
                    continue
                if matches(event):
                    return event

        # If not searching future, return None
        if future is False:
            return None

        # Wait for future events using expect-like pattern
        future_result: asyncio.Future[BaseEvent[Any]] = asyncio.Future()

        def notify_find_handler(event: BaseEvent[Any]) -> None:
            """Handler that resolves the future when a matching event is found"""
            if not future_result.done() and matches(event):
                future_result.set_result(event)

        # Add debugging info to handler name
        current_frame = inspect.currentframe()
        assert current_frame
        notify_find_handler.__name__ = f'{self}.find({event_type}, past={past}, future={future})@{_log_pretty_path(current_frame.f_code.co_filename)}:{current_frame.f_lineno}'

        # Register temporary listener
        self.on(event_type, notify_find_handler)

        # Ensure the temporary handler runs before user handlers
        event_key = event_type.__name__ if isinstance(event_type, type) else str(event_type)
        handlers_for_key = self.handlers.get(event_key)
        if handlers_for_key and handlers_for_key[-1] is notify_find_handler:
            handlers_for_key.insert(0, handlers_for_key.pop())

        try:
            # Wait forever if future is True, otherwise wait up to N seconds
            if future is True:
                return await future_result
            else:
                return await asyncio.wait_for(future_result, timeout=float(future))
        except asyncio.TimeoutError:
            return None
        finally:
            # Clean up handler
            event_key = event_type.__name__ if isinstance(event_type, type) else str(event_type)
            if event_key in self.handlers and notify_find_handler in self.handlers[event_key]:
                self.handlers[event_key].remove(notify_find_handler)

    @overload
    async def expect(
        self,
        event_type: type[T_ExpectedEvent],
        include: Callable[[BaseEvent[Any] | T_ExpectedEvent], bool] = lambda _: True,
        exclude: Callable[[BaseEvent[Any] | T_ExpectedEvent], bool] = lambda _: False,
        predicate: Callable[[BaseEvent[Any] | T_ExpectedEvent], bool] = lambda _: True,
        timeout: float | None = None,
        past: bool | float = False,
        child_of: BaseEvent[Any] | None = None,
    ) -> T_ExpectedEvent | None: ...

    @overload
    async def expect(
        self,
        event_type: PythonIdentifierStr,
        include: Callable[[BaseEvent[Any]], bool] = lambda _: True,
        exclude: Callable[[BaseEvent[Any]], bool] = lambda _: False,
        predicate: Callable[[BaseEvent[Any]], bool] = lambda _: True,
        timeout: float | None = None,
        past: bool | float = False,
        child_of: BaseEvent[Any] | None = None,
    ) -> BaseEvent[Any] | None: ...

    async def expect(
        self,
        event_type: PythonIdentifierStr | type[T_ExpectedEvent],
        include: Callable[[BaseEvent[Any]], bool] = lambda _: True,
        exclude: Callable[[BaseEvent[Any]], bool] = lambda _: False,
        predicate: Callable[[BaseEvent[Any]], bool] = lambda _: True,
        timeout: float | None = None,
        past: bool | float = False,
        child_of: BaseEvent[Any] | None = None,
    ) -> BaseEvent[Any] | T_ExpectedEvent | None:
        """
        Wait for an event matching the given type/pattern with optional filters.

        This is a backwards-compatible wrapper around find(). For new code, consider
        using find() directly for clearer semantics.

        Args:
                event_type: The event type string or model class to wait for
                include: Filter function that must return True for the event to match (default: lambda e: True)
                exclude: Filter function that must return False for the event to match (default: lambda e: False)
                predicate: Deprecated name, alias for include (default: lambda e: True)
                timeout: Maximum time to wait in seconds as a float (None = wait forever)
                past: Controls history search (default: False):
                    - True: search all history first
                    - False: skip history search
                    - float: search events from last N seconds
                child_of: Only match events that are descendants of this parent event

        Returns:
                The first matching event, or None if no match arrives before the timeout

        Example:
                # Wait for any response event
                response = await eventbus.expect('ResponseEvent', timeout=30)

                # Wait for specific response with include filter
                response = await eventbus.expect(
                        'ResponseEvent',
                        include=lambda e: e.request_id == my_request_id,
                        timeout=30
                )

                # Wait for response excluding certain types
                response = await eventbus.expect(
                        'ResponseEvent',
                        exclude=lambda e: e.error_code is not None,
                        timeout=30
                )

                # Search history first, then wait for future
                response = await eventbus.expect(
                        'ResponseEvent',
                        past=True,
                        timeout=30
                )

        .. deprecated::
            Use find() instead for clearer semantics:
            ``await bus.find(EventType, where=..., past=False, future=timeout)``
        """
        warnings.warn(
            'expect() is deprecated, use find() instead. '
            'Example: await bus.find(EventType, where=lambda e: ..., past=False, future=30)',
            DeprecationWarning,
            stacklevel=2,
        )

        # Merge include/exclude/predicate into single where function for find()
        def where(event: BaseEvent[Any]) -> bool:
            if predicate is not None and not predicate(event):  # type: ignore[truthy-function]
                return False
            if not include(event):
                return False
            if exclude(event):
                return False
            return True

        # Map timeout to future parameter: None -> True (wait forever), float -> float (wait N seconds)
        future_param: bool | float = True if timeout is None else timeout

        # Delegate to find()
        return await self.find(
            event_type,
            where=where,
            child_of=child_of,
            past=past,
            future=future_param,
        )

    @overload
    async def query(
        self,
        event_type: type[T_QueryEvent],
        include: Callable[[BaseEvent[Any] | T_QueryEvent], bool] = lambda _: True,
        exclude: Callable[[BaseEvent[Any] | T_QueryEvent], bool] = lambda _: False,
        predicate: Callable[[BaseEvent[Any] | T_QueryEvent], bool] = lambda _: True,
        since: timedelta | float | int | None = None,
    ) -> T_QueryEvent | None: ...

    @overload
    async def query(
        self,
        event_type: PythonIdentifierStr | Literal['*'],
        include: Callable[[BaseEvent[Any]], bool] = lambda _: True,
        exclude: Callable[[BaseEvent[Any]], bool] = lambda _: False,
        predicate: Callable[[BaseEvent[Any]], bool] = lambda _: True,
        since: timedelta | float | int | None = None,
    ) -> BaseEvent[Any] | None: ...

    async def query(
        self,
        event_type: PythonIdentifierStr | Literal['*'] | type[T_QueryEvent],
        include: Callable[[BaseEvent[Any]], bool] = lambda _: True,
        exclude: Callable[[BaseEvent[Any]], bool] = lambda _: False,
        predicate: Callable[[BaseEvent[Any]], bool] = lambda _: True,
        since: timedelta | float | int | None = None,
    ) -> BaseEvent[Any] | T_QueryEvent | None:
        """
        Return the most recent completed event matching the filters, or None if not found.

        This is a convenience wrapper around find() for searching history only.

        Args:
            event_type: The event type string or model class to find
            include: Filter function that must return True for the event to match
            exclude: Filter function that must return False for the event to match
            predicate: Deprecated alias for include
            since: Only search events from the last N seconds (timedelta, float, or int)

        Returns:
            The most recent matching event, or None if not found

        .. deprecated::
            Use find() instead for clearer semantics:
            ``await bus.find(EventType, where=..., past=since, future=False)``
        """
        warnings.warn(
            'query() is deprecated, use find() instead. '
            'Example: await bus.find(EventType, where=lambda e: ..., past=True, future=False)',
            DeprecationWarning,
            stacklevel=2,
        )

        # Merge include/exclude/predicate into single where function
        def where(event: BaseEvent[Any]) -> bool:
            if predicate is not None and not predicate(event):  # type: ignore[truthy-function]
                return False
            if not include(event):
                return False
            if exclude(event):
                return False
            return True

        # Convert since to past parameter for find()
        past_param: bool | float
        if since is None:
            past_param = True  # Search all history
        elif isinstance(since, timedelta):
            if since < timedelta(0):
                raise ValueError('since must be non-negative')
            past_param = since.total_seconds()
        else:
            if since < 0:
                raise ValueError('since must be non-negative')
            past_param = float(since)

        # Delegate to find() with future=False (no waiting)
        return await self.find(
            event_type,
            where=where,
            past=past_param,
            future=False,
        )

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
                                    if eventbus.event_queue:
                                        eventbus.event_queue.shutdown(immediate=True)

                                    if eventbus._runloop_task and not eventbus._runloop_task.done():
                                        # Suppress warning before cancelling
                                        if hasattr(eventbus._runloop_task, '_log_destroy_pending'):
                                            eventbus._runloop_task._log_destroy_pending = False  # type: ignore
                                        eventbus._runloop_task.cancel()
                            except Exception:
                                pass

                        # Now close the loop
                        original_close()

                    loop.close = close_with_cleanup
                    loop._eventbus_close_hooked = True  # type: ignore
                    loop._eventbus_instances = registered_eventbuses  # type: ignore

                # Register this EventBus instance in the WeakSet of all EventBuses on the loop
                if hasattr(loop, '_eventbus_instances'):
                    loop._eventbus_instances.add(self)  # type: ignore

                # Create async objects if needed
                if self.event_queue is None:
                    # Keep queue unbounded so naive dispatch floods can enqueue without
                    # artificial queue caps; queue stores event object references.
                    self.event_queue = CleanShutdownQueue[BaseEvent[Any]](maxsize=0)
                    self._on_idle = asyncio.Event()
                    self._on_idle.clear()  # Start in a busy state unless we confirm queue is empty by running step() at least once

                # Create and start the run loop task.
                # Use a weakref-based runner so an unreferenced EventBus can be GC'd
                # without requiring explicit stop(clear=True) by callers.
                # Run loops must start with a clean context. If dispatch() is called
                # from inside a handler, ContextVars like holds_global_lock=True would
                # otherwise leak into the new task and bypass global lock acquisition.
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
        if not self._is_running:
            return

        # Wait for completion if timeout specified and > 0
        # timeout=0 means "don't wait", so skip the wait entirely
        if timeout is not None and timeout > 0:
            try:
                await self.wait_until_idle(timeout=timeout)
            except TimeoutError:
                pass

        queue_size = self.event_queue.qsize() if self.event_queue else 0
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
        if self.event_queue:
            self.event_queue.shutdown()

        # print('STOPPING', self.event_history)

        # Wait for the run loop task to finish / force-cancel it if it's hanging
        if self._runloop_task and not self._runloop_task.done():
            await asyncio.wait({self._runloop_task}, timeout=0.1)
            try:
                self._runloop_task.cancel()
            except Exception:
                pass

        # Clear references
        self._runloop_task = None
        self._active_event_ids.clear()
        self._processing_event_ids.clear()
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
            self._active_event_ids.clear()

            # Remove from global instance tracking
            if self in EventBus.all_instances:
                EventBus.all_instances.discard(self)

            # Remove from event loop's tracking if present
            try:
                loop = asyncio.get_running_loop()
                if hasattr(loop, '_eventbus_instances'):
                    loop._eventbus_instances.discard(self)  # type: ignore
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
        assert self._on_idle and self.event_queue, 'EventBus._start() must be called before wait_until_idle() is reached'

        start_time = asyncio.get_event_loop().time()
        remaining_timeout = timeout

        try:
            # First wait for the queue to be empty
            join_task = asyncio.create_task(self.event_queue.join())
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
                    if self._on_idle and self.event_queue:
                        if not self._has_inflight_events_fast() and self.event_queue.qsize() == 0:
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

                queue = bus.event_queue
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
                        if bus._on_idle and bus.event_queue:
                            if not bus._has_inflight_events_fast() and bus.event_queue.qsize() == 0:
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
                    async with _get_global_lock():
                        # If a competing path already completed this claimed queue item,
                        # skip duplicate handler execution and just drain queue bookkeeping.
                        if not bus._is_event_complete_fast(event):
                            await bus.handle_event(event)
                        queue.task_done()

                    if bus._on_idle and bus.event_queue:
                        if not bus._has_inflight_events_fast() and bus.event_queue.qsize() == 0:
                            bus._on_idle.set()
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
                    await bus._finalize_local_event_processing(event)
                    del bus
        finally:
            bus = bus_ref()
            if bus is not None:
                bus._is_running = False

    async def _get_next_event(self, wait_for_timeout: float = 0.1) -> 'BaseEvent[Any] | None':
        """Get the next event from the queue"""

        assert self._on_idle and self.event_queue, 'EventBus._start() must be called before _get_next_event()'
        if not self._is_running:
            return None

        try:
            # Create a task for queue.get() so we can cancel it cleanly
            get_next_queued_event = asyncio.create_task(self.event_queue.get())
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
                if not self._has_inflight_events_fast() and self.event_queue.qsize() == 0:
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
        2. Acquires the global processing lock
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
        assert self._on_idle and self.event_queue, 'EventBus._start() must be called before step()'

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

        # Always acquire the global lock (it's re-entrant across tasks)
        self._processing_event_ids.add(event.event_id)
        try:
            async with _get_global_lock():
                # Process the event
                if not self._is_event_complete_fast(event):
                    await self.handle_event(event, timeout=timeout)

                # Mark task as done only if we got it from the queue
                if from_queue:
                    self.event_queue.task_done()
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

        IMPORTANT: This method assumes the global processing lock is already held.
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

        # Execute handlers
        await self._execute_handlers(event, handlers=applicable_handlers, timeout=timeout)

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
        if self.max_history_size and self.max_history_drop and len(self.event_history) > self.max_history_size:
            self.cleanup_event_history()

    def _get_applicable_handlers(self, event: BaseEvent[Any]) -> dict[str, EventHandler]:
        """Get all handlers that should process the given event, filtering out those that would create loops"""
        applicable_handlers: list[EventHandler] = []

        # Add event-type-specific handlers
        applicable_handlers.extend(self.handlers.get(event.event_type, []))

        # Add wildcard handlers (handlers registered for '*')
        applicable_handlers.extend(self.handlers.get('*', []))

        # Filter out handlers that would create loops and build id->handler mapping
        # Use handler id as key to preserve all handlers even with duplicate names
        filtered_handlers: dict[PythonIdStr, EventHandler] = {}
        for handler in applicable_handlers:
            if self._would_create_loop(event, handler):
                continue
            else:
                handler_id = get_handler_id(handler, self)
                filtered_handlers[handler_id] = handler
                # logger.debug(f'  Found handler {get_handler_name(handler)}#{handler_id[-4:]}()')

        return filtered_handlers

    def _enter_handler_execution_context(
        self, event: BaseEvent[Any], handler_id: str
    ) -> tuple[contextvars.Token[Any], contextvars.Token[bool], contextvars.Token[str | None]]:
        event_token = _current_event_context.set(event)
        inside_handler_token = inside_handler_context.set(True)
        current_handler_token = _current_handler_id_context.set(handler_id)
        return event_token, inside_handler_token, current_handler_token

    def _exit_handler_execution_context(
        self,
        handler_context_tokens: tuple[contextvars.Token[Any], contextvars.Token[bool], contextvars.Token[str | None]],
    ) -> None:
        event_token, inside_handler_token, current_handler_token = handler_context_tokens
        _current_event_context.reset(event_token)
        inside_handler_context.reset(inside_handler_token)
        _current_handler_id_context.reset(current_handler_token)

    async def _execute_handlers(
        self,
        event: BaseEvent[Any],
        handlers: dict[PythonIdStr, EventHandler] | None = None,
        timeout: float | None = None,
    ) -> None:
        """Execute all handlers for an event in parallel"""
        applicable_handlers = handlers if (handlers is not None) else self._get_applicable_handlers(event)
        if not applicable_handlers:
            return  # handle_event will mark complete

        pending_results = event.event_create_pending_results(
            applicable_handlers, eventbus=self, timeout=timeout or event.event_timeout
        )
        if self.middlewares:
            for pending_result in pending_results.values():
                await self._on_event_result_change(event, pending_result, EventStatus.PENDING)

        # Execute all handlers in parallel
        if self.parallel_handlers:
            handler_tasks: list[asyncio.Task[Any]] = []
            for handler in applicable_handlers.values():
                handler_tasks.append(asyncio.create_task(self.execute_handler(event, handler, timeout=timeout)))

            # Wait for all handlers to complete.
            for task in handler_tasks:
                try:
                    await task
                except Exception:
                    # Error already logged and recorded in execute_handler
                    pass
        else:
            # otherwise, execute handlers serially, wait until each one completes before moving on to the next
            for handler in applicable_handlers.values():
                try:
                    await self.execute_handler(event, handler, timeout=timeout)
                except Exception as e:
                    # Error already logged and recorded in execute_handler
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            '❌ %s Handler %s#%s(%s) failed with %s: %s',
                            self,
                            get_handler_name(handler),
                            str(id(handler))[-4:],
                            event,
                            type(e).__name__,
                            e,
                        )
                    pass

        # print('FINSIHED EXECUTING ALL HANDLERS')

    async def execute_handler(
        self,
        event: 'BaseEvent[T_EventResultType]',
        handler: EventHandler,
        timeout: float | None = None,
    ) -> Any:
        """Safely execute a single handler with middleware support and EventResult orchestration."""

        handler_id = get_handler_id(handler, self)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                ' ↳ %s.execute_handler(%s, handler=%s#%s)',
                self,
                event,
                get_handler_name(handler),
                handler_id[-4:],
            )

        if handler_id not in event.event_results:
            new_results = event.event_create_pending_results(
                {handler_id: handler}, eventbus=self, timeout=timeout or event.event_timeout
            )
            for pending_result in new_results.values():
                await self._on_event_result_change(event, pending_result, EventStatus.PENDING)

        event_result = event.event_results[handler_id]

        # Check if this is the first handler to start (before updating status)
        is_first_handler = not any(r.started_at for r in event.event_results.values())

        event_result.update(status='started', timeout=timeout or event.event_timeout)
        await self._on_event_result_change(event, event_result, EventStatus.STARTED)

        # Emit event STARTED once (when first handler starts)
        if is_first_handler:
            await self._on_event_change(event, EventStatus.STARTED)

        try:
            result_value = await event_result.execute(
                event,
                handler,
                eventbus=self,
                timeout=timeout or event.event_timeout,
                enter_handler_context=self._enter_handler_execution_context,
                exit_handler_context=self._exit_handler_execution_context,
                format_exception_for_log=_log_filtered_traceback,
            )

            result_type_name = type(result_value).__name__ if result_value is not None else 'None'
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    '    ↳ Handler %s#%s returned: %s',
                    get_handler_name(handler),
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

    def _would_create_loop(self, event: BaseEvent[Any], handler: EventHandler) -> bool:
        """Check if calling this handler would create a loop"""

        assert inspect.isfunction(handler) or inspect.iscoroutinefunction(handler) or inspect.ismethod(handler), (
            f'Handler {get_handler_name(handler)} must be a sync or async function, got: {type(handler)}'
        )

        # First check: If handler is another EventBus.dispatch method, check if we're forwarding to another bus that it's already been processed by
        if hasattr(handler, '__self__') and isinstance(handler.__self__, EventBus) and handler.__name__ == 'dispatch':  # pyright: ignore[reportFunctionMemberAccess]  # type: ignore
            target_bus = handler.__self__  # pyright: ignore[reportFunctionMemberAccess]  # type: ignore
            if target_bus.label in event.event_path:
                logger.debug(
                    f'⚠️ {self} handler {get_handler_name(handler)}#{str(id(handler))[-4:]}({event}) skipped to prevent infinite forwarding loop with {target_bus.label}'
                )
                return True

        # Second check: Check if there's already a result (pending or completed) for this handler on THIS bus
        # We use a combination of bus ID and handler ID to allow the same handler function
        # to run on different buses (important for forwarding)
        handler_id = get_handler_id(handler, self)
        if handler_id in event.event_results:
            existing_result = event.event_results[handler_id]
            if existing_result.status == 'pending' or existing_result.status == 'started':
                logger.debug(
                    f'⚠️ {self} handler {get_handler_name(handler)}#{str(id(handler))[-4:]}({event}) is already {existing_result.status} for event {event.event_id} (preventing recursive call)'
                )
                return True
            elif existing_result.completed_at is not None:
                logger.debug(
                    f'⚠️ {self} handler {get_handler_name(handler)}#{str(id(handler))[-4:]}({event}) already completed @ {existing_result.completed_at} for event {event.event_id} (will not re-run)'
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
                    f'Infinite loop detected: Handler {get_handler_name(handler)}#{str(id(handler))[-4:]} '
                    f'has recursively processed {recursion_depth} levels of events. '
                    f'Current event: {event}, Handler: {handler_id}'
                )
            elif recursion_depth == 2:
                logger.warning(
                    f'⚠️ {self} handler {get_handler_name(handler)}#{str(id(handler))[-4:]} '
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
        if not self.max_history_size or len(self.event_history) <= self.max_history_size:
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
        if not self.max_history_size or len(self.event_history) <= self.max_history_size:
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
                if bus.event_queue:
                    # Access internal queue storage
                    if hasattr(bus.event_queue, '_queue'):
                        queue: deque[BaseEvent] = bus.event_queue._queue  # type: ignore[attr-defined]
                        for event in queue:  # pyright: ignore[reportUnknownVariableType]
                            bus_bytes += sys.getsizeof(event)  # pyright: ignore[reportUnknownArgumentType]
                            if hasattr(event, '__dict__'):  # pyright: ignore[reportUnknownArgumentType]
                                for attr_value in event.__dict__.values():  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                                    if isinstance(attr_value, (str, bytes, list, dict)):
                                        bus_bytes += sys.getsizeof(attr_value)  # pyright: ignore[reportUnknownArgumentType]

                total_bytes += bus_bytes
                bus_details.append(
                    (bus.name, bus_bytes, len(bus.event_history), bus.event_queue.qsize() if bus.event_queue else 0)
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

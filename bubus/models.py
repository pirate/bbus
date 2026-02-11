import asyncio
import contextvars
import inspect
import logging
import os
from collections import deque
from collections.abc import Awaitable, Callable, Generator
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Generic, Literal, Protocol, Self, TypeAlias, cast, runtime_checkable
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    TypeAdapter,
    field_serializer,
    model_validator,
)
from typing_extensions import TypeVar  # needed to get TypeVar(default=...) above python 3.11
from uuid_extensions import uuid7str

if TYPE_CHECKING:
    from bubus.service import EventBus


logger = logging.getLogger('bubus')

BUBUS_LOGGING_LEVEL = os.getenv('BUBUS_LOGGING_LEVEL', 'WARNING').upper()  # WARNING normally, otherwise DEBUG when testing
LIBRARY_VERSION = os.getenv('LIBRARY_VERSION', '1.0.0')

logger.setLevel(BUBUS_LOGGING_LEVEL)


class EventStatus(StrEnum):
    """Status of an event or handler in the EventBus lifecycle.

    Using StrEnum ensures backwards compatibility - comparisons like
    `status == 'pending'` still work since EventStatus.PENDING == 'pending'.
    """

    PENDING = 'pending'
    STARTED = 'started'
    COMPLETED = 'completed'  # errored events are also considered completed


def validate_event_name(s: str) -> str:
    assert str(s).isidentifier() and not str(s).startswith('_'), f'Invalid event name: {s}'
    return str(s)


def validate_python_id_str(s: str) -> str:
    assert str(s).replace('.', '').isdigit(), f'Invalid Python ID: {s}'
    return str(s)


def validate_event_path_entry_str(s: str) -> str:
    entry = str(s)
    assert '#' in entry, f'Invalid event_path entry: {entry} (expected BusName#abcd)'
    bus_name, short_id = entry.rsplit('#', 1)
    assert bus_name.isidentifier() and short_id.isalnum() and len(short_id) == 4, (
        f'Invalid event_path entry: {entry} (expected BusName#abcd)'
    )
    return entry


def validate_uuid_str(s: str) -> str:
    uuid = UUID(str(s))
    return str(uuid)


UUIDStr: TypeAlias = Annotated[str, AfterValidator(validate_uuid_str)]
PythonIdStr: TypeAlias = Annotated[str, AfterValidator(validate_python_id_str)]
PythonIdentifierStr: TypeAlias = Annotated[str, AfterValidator(validate_event_name)]
EventPathEntryStr: TypeAlias = Annotated[str, AfterValidator(validate_event_path_entry_str)]
T_EventResultType = TypeVar('T_EventResultType', bound=Any, default=None)
# TypeVar for BaseEvent and its subclasses
# We use contravariant=True because if a handler accepts BaseEvent,
# it can also handle any subclass of BaseEvent
T_Event = TypeVar('T_Event', bound='BaseEvent[Any]', contravariant=True, default='BaseEvent[Any]')

# For protocols with __func__ attributes, we need an invariant TypeVar
T_EventInvariant = TypeVar('T_EventInvariant', bound='BaseEvent[Any]', default='BaseEvent[Any]')

# For handlers, we need to be flexible about the signature since:
# 1. Functions take just the event: handler(event)
# 2. Methods take self + event: handler(self, event)
# 3. Classmethods take cls + event: handler(cls, event)
# 4. Handlers can accept BaseEvent subclasses (contravariance)
#
# Python's type system doesn't handle this well, so we define specific protocols


@runtime_checkable
class EventHandlerFunc(Protocol[T_Event]):
    """Protocol for sync event handler functions"""

    def __call__(self, event: T_Event, /) -> Any: ...


@runtime_checkable
class AsyncEventHandlerFunc(Protocol[T_Event]):
    """Protocol for async event handler functions"""

    async def __call__(self, event: T_Event, /) -> Any: ...


@runtime_checkable
class EventHandlerMethod(Protocol[T_Event]):
    """Protocol for instance method event handlers"""

    def __call__(self, self_: Any, event: T_Event, /) -> Any: ...

    __self__: Any
    __name__: str


@runtime_checkable
class AsyncEventHandlerMethod(Protocol[T_Event]):
    """Protocol for async instance method event handlers"""

    async def __call__(self, self_: Any, event: T_Event, /) -> Any: ...

    __self__: Any
    __name__: str


@runtime_checkable
class EventHandlerClassMethod(Protocol[T_EventInvariant]):
    """Protocol for class method event handlers"""

    def __call__(self, cls: type[Any], event: T_EventInvariant, /) -> Any: ...

    __self__: type[Any]
    __name__: str
    __func__: Callable[[type[Any], T_EventInvariant], Any]


@runtime_checkable
class AsyncEventHandlerClassMethod(Protocol[T_EventInvariant]):
    """Protocol for async class method event handlers"""

    async def __call__(self, cls: type[Any], event: T_EventInvariant, /) -> Any: ...

    __self__: type[Any]
    __name__: str
    __func__: Callable[[type[Any], T_EventInvariant], Awaitable[Any]]


# Event handlers can be sync/async functions, methods, class methods, or coroutines
# The protocols are parameterized with BaseEvent but due to contravariance,
# they also accept handlers that take any BaseEvent subclass
EventHandler: TypeAlias = (
    EventHandlerFunc['BaseEvent[Any]']
    | AsyncEventHandlerFunc['BaseEvent[Any]']
    | EventHandlerMethod['BaseEvent[Any]']
    | AsyncEventHandlerMethod['BaseEvent[Any]']
    | EventHandlerClassMethod['BaseEvent[Any]']
    | AsyncEventHandlerClassMethod['BaseEvent[Any]']
    # | Callable[['BaseEvent'], Any]  # Simple sync callable
    # | Callable[['BaseEvent'], Awaitable[Any]]  # Simple async callable
    # | Coroutine[Any, Any, Any]  # Direct coroutine
)

# ContravariantEventHandler is needed to allow handlers to accept any BaseEvent subclass in some signatures
ContravariantEventHandler: TypeAlias = (
    EventHandlerFunc[T_Event]  # cannot be BaseEvent or type checker will complain
    | AsyncEventHandlerFunc['BaseEvent[Any]']
    | EventHandlerMethod['BaseEvent[Any]']
    | AsyncEventHandlerMethod[T_Event]  # cannot be 'BaseEvent' or type checker will complain
    | EventHandlerClassMethod['BaseEvent[Any]']
    | AsyncEventHandlerClassMethod['BaseEvent[Any]']
)

EventResultFilter = Callable[['EventResult[Any]'], bool]


def get_handler_name(handler: ContravariantEventHandler[T_Event]) -> str:
    assert hasattr(handler, '__name__'), f'Handler {handler} has no __name__ attribute!'
    if inspect.ismethod(handler):
        return f'{type(handler.__self__).__name__}.{handler.__name__}'
    elif callable(handler):
        return f'{handler.__module__}.{handler.__name__}'  # type: ignore
    else:
        raise ValueError(f'Invalid handler: {handler} {type(handler)}, expected a function, coroutine, or method')


def get_handler_id(handler: EventHandler, eventbus: Any = None) -> str:
    """Generate a unique handler ID based on the bus and handler instance."""
    if eventbus is None:
        return str(id(handler))
    return f'{id(eventbus)}.{id(handler)}'


def _extract_basemodel_generic_arg(cls: type) -> Any:
    """
    Extract T_EventResultType Generic arg from BaseModel[T_EventResultType] subclasses using pydantic generic metadata.
    Needed because pydantic messes with the mro and obscures the Generic from the bases list.
    https://github.com/pydantic/pydantic/issues/8410
    """
    # Direct check first for speed - most subclasses will have it directly
    if hasattr(cls, '__pydantic_generic_metadata__'):
        metadata: dict[str, Any] = cls.__pydantic_generic_metadata__  # type: ignore
        origin = metadata.get('origin')  # type: ignore
        args: tuple[Any, ...] = metadata.get('args')  # type: ignore
        if origin is BaseEvent and args and len(args) > 0:  # type: ignore
            return args[0]

    # Only check MRO if direct check failed
    # Skip first element (cls itself) since we already checked it
    for parent in cls.__mro__[1:]:
        if hasattr(parent, '__pydantic_generic_metadata__'):
            metadata = parent.__pydantic_generic_metadata__  # type: ignore
            # Check if this is a parameterized BaseEvent
            origin = metadata.get('origin')  # type: ignore
            args: tuple[Any, ...] = metadata.get('args')  # type: ignore
            if origin is BaseEvent and args and len(args) > 0:  # type: ignore
                return args[0]

    return None


def _to_result_type_json_schema(result_type: Any) -> dict[str, Any] | None:
    """Best-effort conversion of a Python result type into JSON Schema."""
    if result_type is None:
        return None
    if isinstance(result_type, dict):
        return cast(dict[str, Any], result_type)
    if isinstance(result_type, str):
        return None

    try:
        if inspect.isclass(result_type) and issubclass(result_type, BaseModel):
            return result_type.model_json_schema()
    except TypeError:
        pass

    try:
        return TypeAdapter(result_type).json_schema()
    except Exception:
        return None


class BaseEvent(BaseModel, Generic[T_EventResultType]):
    """
    The base model used for all Events that flow through the EventBus system.
    """

    model_config = ConfigDict(
        extra='allow',
        arbitrary_types_allowed=True,
        # Allow ergonomic subclass defaults like `class MyEvent(BaseEvent): event_version = '1.2.3'`
        # without requiring repetitive type annotations on every override.
        ignored_types=(str,),
        validate_assignment=True,
        validate_default=True,
        revalidate_instances='always',
    )

    # Class-level cache for auto-extracted event_result_type
    _event_result_type_cache: ClassVar[Any | None] = None

    event_type: PythonIdentifierStr = Field(default='UndefinedEvent', description='Event type name', max_length=64)
    event_version: str = Field(default='0.0.1', description='Event payload version tag')
    event_schema: str = Field(
        default=f'UndefinedEvent@{LIBRARY_VERSION}',
        description='Event schema version in format ClassName@version',
        max_length=250,
    )  # long because it can include long function names / module paths
    event_timeout: float | None = Field(default=300.0, description='Timeout in seconds for event to finish processing')
    event_result_type: Any = Field(
        default=None, description='Type to cast/validate handler return values (e.g. int, str, bytes, BaseModel subclass)'
    )
    event_result_schema: dict[str, Any] | None = Field(
        default=None, description='JSONSchema describing the expected handler return value shape'
    )

    @field_serializer('event_result_type')
    def event_result_type_serializer(self, value: Any) -> str | None:
        """Serialize event_result_type to a string representation"""
        if value is None:
            return None
        # Use str() to get full representation: 'int', 'str', 'list[int]', etc.
        return str(value)

    @field_serializer('event_result_schema', when_used='json')
    def event_result_schema_serializer(self, value: Any) -> dict[str, Any] | None:
        """Serialize event_result_schema, deriving from event_result_type when possible."""
        if isinstance(value, dict):
            return cast(dict[str, Any], value)
        derived_schema = _to_result_type_json_schema(value)
        if derived_schema is not None:
            return derived_schema
        return _to_result_type_json_schema(self.event_result_type)

    # Runtime metadata
    event_id: UUIDStr = Field(default_factory=uuid7str, max_length=36)
    event_path: list[EventPathEntryStr] = Field(default_factory=list, description='Path tracking for event routing')
    event_parent_id: UUIDStr | None = Field(
        default=None, description='ID of the parent event that triggered this event', max_length=36
    )

    # Completion tracking fields
    event_created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description='Timestamp when event was first dispatched to an EventBus aka marked pending',
    )
    event_processed_at: datetime | None = Field(
        default=None,
        description='Timestamp when event was first processed by any handler',
    )

    event_results: dict[PythonIdStr, 'EventResult[T_EventResultType]'] = Field(
        default_factory=dict, exclude=True
    )  # Results indexed by str(id(handler_func))

    # Completion signal
    _event_completed_signal: asyncio.Event | None = PrivateAttr(default=None)
    _event_is_complete_flag: bool = PrivateAttr(default=False)

    # Dispatch-time context for ContextVar propagation to handlers
    # Captured when dispatch() is called, used when executing handlers via ctx.run()
    _event_dispatch_context: contextvars.Context | None = PrivateAttr(default=None)

    def __hash__(self) -> int:
        """Make events hashable using their unique event_id"""
        return hash(self.event_id)

    def __str__(self) -> str:
        """Compact O(1) summary for hot-path logging."""
        completed_signal = self._event_completed_signal
        is_complete = self._event_is_complete_flag or (completed_signal is not None and completed_signal.is_set())
        if is_complete:
            icon = '✅'
        elif self.event_processed_at is not None:
            icon = '🏃'
        else:
            icon = '⏳'

        bus_hint = self.event_path[-1] if self.event_path else '?'
        return f'{bus_hint}▶ {self.event_type}#{self.event_id[-4:]} {icon}'

    def _remove_self_from_queue(self, bus: 'EventBus') -> bool:
        """Remove this event from the bus's queue if present. Returns True if removed."""
        if bus and bus.event_queue and hasattr(bus.event_queue, '_queue'):
            # Access internal deque of asyncio.Queue (implementation detail)
            queue = cast(deque[BaseEvent[Any]], bus.event_queue._queue)  # type: ignore[attr-defined]
            if self in queue:
                queue.remove(self)
                return True
        return False

    def _is_queued_on_any_bus(self, ignore_bus: 'EventBus | None' = None) -> bool:
        """
        Check whether this event is currently queued on any live EventBus.

        This prevents premature completion when an event has been forwarded to
        another bus but that bus hasn't processed it yet.
        """
        from bubus.service import EventBus

        empty_event_ids: set[str] = set()
        for bus in list(EventBus.all_instances):
            if not bus:
                continue
            if ignore_bus is not None and bus is ignore_bus:
                continue
            active_event_ids = cast(set[str], getattr(bus, '_active_event_ids', empty_event_ids))
            processing_event_ids = cast(set[str], getattr(bus, '_processing_event_ids', empty_event_ids))
            # Another bus can claim queue.get() before marking processing.
            # `_active_event_ids` bridges that handoff gap for completion checks.
            if self.event_id in active_event_ids:
                return True
            if self.event_id in processing_event_ids:
                return True
            if not bus.event_queue or not hasattr(bus.event_queue, '_queue'):
                continue
            queue = cast(deque[BaseEvent[Any]], bus.event_queue._queue)  # type: ignore[attr-defined]
            for queued_event in queue:
                if queued_event.event_id == self.event_id:
                    return True
        return False

    async def _process_self_on_all_buses(self) -> None:
        """
        Process this specific event on all buses where it's queued.

        This handles the case where an event is forwarded to multiple buses -
        we need to process it on each bus, but we only process THIS event,
        not other events in the queues (to avoid overshoot).

        The loop continues until the event's completion signal is set, which
        happens after all handlers on all buses have completed.
        """
        from bubus.service import EventBus

        max_iterations = 1000  # Prevent infinite loops
        iterations = 0

        # Cache the signal - in async context it will always be created
        completed_signal = self.event_completed_signal
        assert completed_signal is not None, 'event_completed_signal should exist in async context'
        claimed_processed_bus_ids: set[int] = set()
        empty_event_ids: set[str] = set()

        try:
            while not completed_signal.is_set() and iterations < max_iterations:
                iterations += 1
                processed_any = False

                # Look for this specific event in all bus queues and process it
                for bus in list(EventBus.all_instances):
                    if not bus or not bus.event_queue:
                        continue
                    processed_on_bus = False

                    if self._remove_self_from_queue(bus):
                        # Fast path: event is still in the queue, claim and process it via EventBus.step
                        # so completion/finalization uses the same logic as the runloop.
                        try:
                            await bus.step(event=self)
                            bus.event_queue.task_done()
                        except ValueError:
                            # Queue bookkeeping can already be drained by competing paths.
                            pass
                        processed_on_bus = True
                    else:
                        # Slow path: another task already claimed queue.get() and set
                        # processing state, but may be blocked on the global lock held
                        # by the awaiting parent handler. Process once here to make progress.
                        bus_key = id(bus)
                        if (
                            self.event_id in cast(set[str], getattr(bus, '_processing_event_ids', empty_event_ids))
                            and bus_key not in claimed_processed_bus_ids
                        ):
                            await bus.step(event=self)
                            claimed_processed_bus_ids.add(bus_key)
                            processed_on_bus = True

                    if processed_on_bus:
                        processed_any = True
                        if completed_signal.is_set():
                            break

                if completed_signal.is_set():
                    break

                if not processed_any:
                    # Event not in any queue, yield control and wait
                    await asyncio.sleep(0)

        except asyncio.CancelledError:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Polling loop cancelled for %s', self)
            raise

    async def _wait_for_completion_inside_handler(self) -> None:
        """
        Wait for this event to complete when called from inside a handler.

        Processes this specific event on all buses where it appears (handling
        the forwarding case), but doesn't process other events (avoiding overshoot).
        """
        await self._process_self_on_all_buses()

    async def _wait_for_completion_outside_handler(self) -> None:
        """
        Wait for this event to complete when called from outside a handler.

        Simply waits on the completion signal - the event loop's normal
        processing will handle the event.
        """
        if self._event_is_complete_flag:
            return
        assert self.event_completed_signal is not None
        await self.event_completed_signal.wait()

    def __await__(self) -> Generator[Self, Any, Any]:
        """Wait for event to complete and return self"""

        async def wait_for_handlers_to_complete_then_return_event():
            if self._event_is_complete_flag:
                return self
            assert self.event_completed_signal is not None
            from bubus.service import holds_global_lock, inside_handler_context

            is_inside_handler = inside_handler_context.get() and holds_global_lock.get()
            is_not_yet_complete = not self._event_is_complete_flag and not self.event_completed_signal.is_set()

            if is_not_yet_complete and is_inside_handler:
                await self._wait_for_completion_inside_handler()
            else:
                await self._wait_for_completion_outside_handler()

            return self

        return wait_for_handlers_to_complete_then_return_event().__await__()

    @model_validator(mode='before')
    @classmethod
    def _set_event_type_from_class_name(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Automatically set event_type to the class name if not provided"""
        is_class_default_unchanged = cls.model_fields['event_type'].default == 'UndefinedEvent'
        is_event_type_not_provided = 'event_type' not in data or data['event_type'] == 'UndefinedEvent'
        if is_class_default_unchanged and is_event_type_not_provided:
            data['event_type'] = cls.__name__
        return data

    @model_validator(mode='before')
    @classmethod
    def _set_event_schema_from_class_name(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Append the library version number to the event schema so we know what version was used to create any JSON dump"""
        is_class_default_unchanged = cls.model_fields['event_schema'].default == f'UndefinedEvent@{LIBRARY_VERSION}'
        is_event_schema_not_provided = 'event_schema' not in data or data['event_schema'] == f'UndefinedEvent@{LIBRARY_VERSION}'
        if is_class_default_unchanged and is_event_schema_not_provided:
            data['event_schema'] = f'{cls.__module__}.{cls.__qualname__}@{LIBRARY_VERSION}'
        return data

    @model_validator(mode='before')
    @classmethod
    def _set_event_result_type_from_generic_arg(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Automatically set event_result_type from Generic type parameter if not explicitly provided."""
        if not isinstance(data, dict):  # type: ignore
            return data

        # Fast path: if event_result_type is already in the data, skip all checks
        if 'event_result_type' in data:
            return data

        # Check if class explicitly defines event_result_type in model_fields
        # This handles cases where user explicitly sets event_result_type in class definition
        if 'event_result_type' in cls.model_fields:
            field = cls.model_fields['event_result_type']
            if field.default is not None and field.default != BaseEvent.model_fields['event_result_type'].default:
                # Explicitly set, use the default value
                data['event_result_type'] = field.default
                return data

        # Fast path: check if class has cached the result type
        if cls._event_result_type_cache is not None:
            data['event_result_type'] = cls._event_result_type_cache
            return data

        # Extract the generic type from BaseEvent[T]
        extracted_type = _extract_basemodel_generic_arg(cls)

        # Cache the result on the class
        cls._event_result_type_cache = extracted_type

        # Set the type if we successfully resolved it
        if extracted_type is not None:
            data['event_result_type'] = extracted_type

        return cast(dict[str, Any], data)  # type: ignore

    @property
    def event_completed_signal(self) -> asyncio.Event | None:
        """Lazily create asyncio.Event when accessed"""
        if self._event_completed_signal is None:
            try:
                asyncio.get_running_loop()
                self._event_completed_signal = asyncio.Event()
            except RuntimeError:
                pass  # Keep it None if no event loop
        return self._event_completed_signal

    @property
    def event_status(self) -> EventStatus:
        """Current status of this event in the lifecycle."""
        if self._event_is_complete_flag:
            return EventStatus.COMPLETED
        if self._event_completed_signal is not None and self._event_completed_signal.is_set():
            return EventStatus.COMPLETED
        if self.event_started_at is not None:
            return EventStatus.STARTED
        return EventStatus.PENDING

    @property
    def event_children(self) -> list['BaseEvent[Any]']:
        """Get all child events dispatched from within this event's handlers"""
        children: list[BaseEvent[Any]] = []
        for event_result in self.event_results.values():
            children.extend(event_result.event_children)
        return children

    @property
    def event_started_at(self) -> datetime | None:
        """Timestamp when event first started being processed by any handler"""
        earliest_started: datetime | None = None
        for result in self.event_results.values():
            started_at = result.started_at
            if started_at is None:
                continue
            if earliest_started is None or started_at < earliest_started:
                earliest_started = started_at
        # If no handlers but event was processed, use the processed timestamp.
        if earliest_started is None and self.event_processed_at:
            return self.event_processed_at
        return earliest_started

    @property
    def event_completed_at(self) -> datetime | None:
        """Timestamp when event was completed by all handlers"""
        # If no handlers at all but event was processed, use the processed timestamp.
        # This supports manually deserialized/updated events in tests and tooling.
        if not self.event_results and self.event_processed_at:
            return self.event_processed_at

        if not self._event_is_complete_flag and not (
            self._event_completed_signal is not None and self._event_completed_signal.is_set()
        ):
            # Fast negative path for in-flight events
            return None

        if not self.event_results:
            return self.event_processed_at

        latest_completed: datetime | None = None
        for result in self.event_results.values():
            if result.status not in ('completed', 'error'):
                return None
            completed_at = result.completed_at
            if completed_at is None:
                continue
            if latest_completed is None or completed_at > latest_completed:
                latest_completed = completed_at
        return latest_completed or self.event_processed_at

    def event_create_pending_results(
        self,
        handlers: dict[PythonIdStr, EventHandler],
        *,
        eventbus: 'EventBus | None' = None,
        timeout: float | None = None,
    ) -> 'dict[PythonIdStr, EventResult[T_EventResultType]]':
        """Ensure EventResult placeholders exist for provided handlers before execution.

        Any stale timing/error data from prior runs is cleared so consumers immediately see a fresh pending state.
        """
        pending_results: dict[PythonIdStr, 'EventResult[T_EventResultType]'] = {}
        self._event_is_complete_flag = False
        for handler_id, handler in handlers.items():
            event_result = self.event_result_update(
                handler=handler,
                eventbus=eventbus,
                status='pending',
            )
            # Reset runtime fields so we never reuse stale data
            event_result.result = None
            event_result.error = None
            event_result.started_at = None
            event_result.completed_at = None
            event_result.status = 'pending'
            event_result.timeout = timeout if timeout is not None else self.event_timeout
            event_result.result_type = self.event_result_type
            pending_results[handler_id] = event_result

        if self.event_completed_signal and not self.event_completed_signal.is_set():
            self.event_processed_at = self.event_processed_at or datetime.now(UTC)
        return pending_results

    @staticmethod
    def _event_result_is_truthy(event_result: 'EventResult[T_EventResultType]') -> bool:
        if event_result.status != 'completed':
            return False
        if event_result.result is None:
            return False
        if isinstance(event_result.result, BaseException) or event_result.error:
            return False
        if isinstance(
            event_result.result, BaseEvent
        ):  # omit if result is a BaseEvent, it's a forwarded event not an actual return value
            return False
        return True

    async def event_results_filtered(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = True,
    ) -> 'dict[PythonIdStr, EventResult[T_EventResultType]]':
        """Get all results filtered by the include function"""

        # wait for all handlers to finish processing
        assert self.event_completed_signal is not None, 'EventResult cannot be awaited outside of an async context'
        await asyncio.wait_for(self.event_completed_signal.wait(), timeout=timeout or self.event_timeout)

        # Wait for each result to complete, but don't raise errors yet
        for event_result in self.event_results.values():
            try:
                await event_result
            except Exception:
                # Ignore exceptions here - we'll handle them based on raise_if_any below
                pass

        event_results: dict[PythonIdStr, EventResult[T_EventResultType]] = {
            handler_key: event_result for handler_key, event_result in self.event_results.items()
        }
        included_results: dict[PythonIdStr, EventResult[T_EventResultType]] = {
            handler_key: event_result for handler_key, event_result in event_results.items() if include(event_result)
        }
        error_results: dict[PythonIdStr, EventResult[T_EventResultType]] = {
            handler_key: event_result
            for handler_key, event_result in event_results.items()
            if event_result.error or isinstance(event_result.result, BaseException)
        }

        if raise_if_any and error_results:
            if len(error_results) == 1:
                single_result = next(iter(error_results.values()))
                single_error = single_result.error or cast(Any, single_result.result)
                if isinstance(single_error, BaseException):
                    raise single_error
                raise Exception(str(single_error))

            collected_errors = self._collect_handler_errors(include_cancelled=True)
            raise ExceptionGroup(
                f'Event {self.event_type}#{self.event_id[-4:]} had {len(collected_errors)} handler error(s)',
                collected_errors,
            )

        if raise_if_none and not included_results:
            raise ValueError(
                f'Expected at least one handler to return a non-None result, but none did! {self} -> {self.event_results}'
            )

        event_results_by_handler_id: dict[PythonIdStr, EventResult[T_EventResultType]] = {
            handler_key: result for handler_key, result in included_results.items()
        }
        for event_result in event_results_by_handler_id.values():
            assert event_result.result is not None, f'EventResult {event_result} has no result'

        return event_results_by_handler_id

    async def raise_if_errors(
        self,
        timeout: float | None = None,
        include_cancelled: bool = False,
    ) -> None:
        """
        Raise an ExceptionGroup containing all handler errors for this event.

        This waits for event completion, then aggregates handler failures from
        event_results. By default, asyncio.CancelledError entries are ignored.
        """
        assert self.event_completed_signal is not None, 'Event cannot be awaited outside of an async context'
        await asyncio.wait_for(self.event_completed_signal.wait(), timeout=timeout or self.event_timeout)

        collected_errors = self._collect_handler_errors(include_cancelled=include_cancelled)

        if collected_errors:
            raise ExceptionGroup(
                f'Event {self.event_type}#{self.event_id[-4:]} had {len(collected_errors)} handler error(s)',
                collected_errors,
            )

    def _collect_handler_errors(self, include_cancelled: bool) -> list[Exception]:
        """Collect handler errors as Exception instances for aggregation."""
        collected_errors: list[Exception] = []
        for event_result in self.event_results.values():
            original_error = event_result.error
            if original_error is None and isinstance(event_result.result, BaseException):
                original_error = event_result.result

            if original_error is None:
                continue

            if isinstance(original_error, asyncio.CancelledError) and not include_cancelled:
                continue

            if isinstance(original_error, Exception):
                collected_errors.append(original_error)
                continue

            wrapped = RuntimeError(
                f'Non-Exception handler error from {event_result.eventbus_name}.{event_result.handler_name}: '
                f'{type(original_error).__name__}: {original_error}'
            )
            wrapped.__cause__ = original_error
            collected_errors.append(wrapped)
        return collected_errors

    async def event_results_by_handler_id(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = True,
    ) -> dict[PythonIdStr, T_EventResultType | None]:
        """Get all raw result values organized by handler id {handler1_id: handler1_result, handler2_id: handler2_result, ...}"""
        included_results = await self.event_results_filtered(
            timeout=timeout, include=include, raise_if_any=raise_if_any, raise_if_none=raise_if_none
        )
        return {
            handler_id: cast(T_EventResultType | None, event_result.result)
            for handler_id, event_result in included_results.items()
        }

    async def event_results_by_handler_name(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = True,
    ) -> dict[PythonIdentifierStr, T_EventResultType | None]:
        """Get all raw result values organized by handler name {handler1_name: handler1_result, handler2_name: handler2_result, ...}"""
        included_results = await self.event_results_filtered(
            timeout=timeout, include=include, raise_if_any=raise_if_any, raise_if_none=raise_if_none
        )
        return {
            event_result.handler_name: cast(T_EventResultType | None, event_result.result)
            for event_result in included_results.values()
        }

    async def event_result(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = True,
    ) -> T_EventResultType | None:
        """Get the first non-None result from the event handlers"""
        valid_results = await self.event_results_filtered(
            timeout=timeout, include=include, raise_if_any=raise_if_any, raise_if_none=raise_if_none
        )
        results = list(valid_results.values())
        return cast(T_EventResultType | None, results[0].result) if results else None

    async def event_results_list(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = True,
    ) -> list[T_EventResultType | None]:
        """Get all result values in a list [handler1_result, handler2_result, ...]"""
        valid_results = await self.event_results_filtered(
            timeout=timeout, include=include, raise_if_any=raise_if_any, raise_if_none=raise_if_none
        )
        return [cast(T_EventResultType | None, event_result.result) for event_result in valid_results.values()]

    async def event_results_flat_dict(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = False,
        raise_if_conflicts: bool = True,
    ) -> dict[str, Any]:
        """Assuming all handlers return dicts, merge all the returned dicts into a single flat dict {**handler1_result, **handler2_result, ...}"""

        valid_results = await self.event_results_filtered(
            timeout=timeout,
            include=lambda event_result: isinstance(event_result.result, dict) and include(event_result),
            raise_if_any=raise_if_any,
            raise_if_none=raise_if_none,
        )

        merged_results: dict[str, Any] = {}
        for event_result in valid_results.values():
            if not event_result.result:
                continue

            # check for event results trampling each other / conflicting
            overlapping_keys: set[str] = merged_results.keys() & event_result.result.keys()  # type: ignore
            if raise_if_conflicts and overlapping_keys:  # type: ignore
                raise ValueError(
                    f'Event handler {event_result.handler_name} returned a dict with keys that would overwrite values from previous handlers: {overlapping_keys} (pass raise_if_conflicts=False to merge with last-handler-wins)'
                )  # type: ignore

            merged_results.update(
                event_result.result  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
            )  # update the merged dict with the contents of the result dict
        return merged_results

    async def event_results_flat_list(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = True,
    ) -> list[Any]:
        """Assuming all handlers return lists, merge all the returned lists into a single flat list [*handler1_result, *handler2_result, ...]"""
        valid_results = await self.event_results_filtered(
            timeout=timeout,
            include=lambda event_result: isinstance(event_result.result, list) and include(event_result),
            raise_if_any=raise_if_any,
            raise_if_none=raise_if_none,
        )
        merged_results: list[T_EventResultType | None] = []
        for event_result in valid_results.values():
            merged_results.extend(
                cast(list[T_EventResultType | None], event_result.result)
            )  # append the contents of the list to the merged list
        return merged_results

    def event_result_update(
        self, handler: EventHandler, eventbus: 'EventBus | None' = None, **kwargs: Any
    ) -> 'EventResult[T_EventResultType]':
        """Create or update an EventResult for a handler"""

        from bubus.service import EventBus

        assert eventbus is None or isinstance(eventbus, EventBus)
        if eventbus is None and handler and inspect.ismethod(handler) and isinstance(handler.__self__, EventBus):
            eventbus = handler.__self__

        handler_name: str = get_handler_name(handler) if handler else 'unknown_handler'
        eventbus_id: PythonIdStr = str(id(eventbus) if eventbus is not None else '000000000000')
        eventbus_name: PythonIdentifierStr = str(eventbus and eventbus.name or 'EventBus')

        # Use bus+handler combination for unique ID
        handler_id: PythonIdStr = get_handler_id(handler, eventbus)

        # Get or create EventResult
        if handler_id not in self.event_results:
            self.event_results[handler_id] = cast(
                EventResult[T_EventResultType],
                EventResult(
                    event_id=self.event_id,
                    handler_id=handler_id,
                    handler_name=handler_name,
                    eventbus_id=eventbus_id,
                    eventbus_name=eventbus_name,
                    status=kwargs.get('status', 'pending'),
                    timeout=self.event_timeout,
                    result_type=self.event_result_type,
                ),
            )
            # logger.debug(f'Created EventResult for handler {handler_id}: {handler and get_handler_name(handler)}')

        # Update the EventResult with provided kwargs
        self.event_results[handler_id].update(**kwargs)
        if 'timeout' in kwargs:
            self.event_results[handler_id].timeout = kwargs['timeout']
        if kwargs.get('status') == 'started' and hasattr(self, 'event_processed_at'):
            self.event_processed_at = self.event_processed_at or datetime.now(UTC)
        # logger.debug(
        #     f'Updated EventResult for handler {handler_id}: status={self.event_results[handler_id].status}, total_results={len(self.event_results)}'
        # )
        # Don't mark complete here - let the EventBus do it after all handlers are done
        return self.event_results[handler_id]

    def event_mark_complete_if_all_handlers_completed(self, current_bus: 'EventBus | None' = None) -> None:
        """Check if all handlers are done and signal completion"""
        completed_signal = self._event_completed_signal
        if completed_signal is not None and completed_signal.is_set():
            self._event_is_complete_flag = True
            return

        # If there are no results at all, the event is complete.
        if not self.event_results:
            # Even with no local handlers, forwarded copies may still be queued elsewhere.
            if self._is_queued_on_any_bus(ignore_bus=current_bus):
                return
            if not self.event_are_all_children_complete():
                return
            if hasattr(self, 'event_processed_at'):
                self.event_processed_at = datetime.now(UTC)
            self._event_is_complete_flag = True
            if completed_signal is not None:
                completed_signal.set()
            self._event_dispatch_context = None
            return

        # Check if all handler results are done.
        for result in self.event_results.values():
            if result.status not in ('completed', 'error'):
                return

        # Forwarded events may still be waiting in another bus queue.
        # Don't mark complete until all queue copies have been consumed.
        if self._is_queued_on_any_bus(ignore_bus=current_bus):
            return

        # Recursively check if all child events are also complete
        if not self.event_are_all_children_complete():
            return

        # All handlers and all child events are done.
        if hasattr(self, 'event_processed_at'):
            self.event_processed_at = datetime.now(UTC)
        self._event_is_complete_flag = True
        if completed_signal is not None:
            completed_signal.set()
        # Clear dispatch context to avoid memory leaks (it holds references to ContextVars)
        self._event_dispatch_context = None

    def event_mark_pending(self) -> Self:
        """Reset mutable runtime state so this event can be dispatched again as pending."""
        self._event_is_complete_flag = False
        self.event_processed_at = None
        self.event_results.clear()
        self._event_dispatch_context = None
        try:
            asyncio.get_running_loop()
            self._event_completed_signal = asyncio.Event()
        except RuntimeError:
            self._event_completed_signal = None
        return self

    def reset(self) -> Self:
        """Return a fresh copy of this event with pending runtime state."""
        fresh_event = self.__class__.model_validate(self.model_dump(mode='python'))
        fresh_event.event_id = uuid7str()
        return fresh_event.event_mark_pending()

    def event_are_all_children_complete(self, _visited: set[str] | None = None) -> bool:
        """Recursively check if all child events and their descendants are complete"""
        if _visited is None:
            _visited = set()

        # Prevent infinite recursion on circular references
        if self.event_id in _visited:
            return True
        _visited.add(self.event_id)

        for child_event in self.event_children:
            if child_event.event_status != 'completed':
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('Event %s has incomplete child %s', self, child_event)
                return False
            # Recursively check child's children
            if not child_event.event_are_all_children_complete(_visited):
                return False
        return True

    def event_cancel_pending_child_processing(self, error: BaseException) -> None:
        """Cancel any pending child events that were dispatched during handler execution"""
        if not isinstance(error, asyncio.CancelledError):
            error = asyncio.CancelledError(
                f'Cancelled pending handler as a result of parent error {error}'
            )  # keep the word "pending" in the error, checked by print_handler_line()
        for child_event in self.event_children:
            for result in child_event.event_results.values():
                if result.status == 'pending':
                    # print('CANCELLING CHILD HANDLER', result, 'due to', error)
                    result.update(error=error)
            child_event.event_cancel_pending_child_processing(error)

    def event_log_safe_summary(self) -> dict[str, Any]:
        """only event metadata without contents, avoid potentially sensitive event contents in logs"""
        return {k: v for k, v in self.model_dump(mode='json').items() if k.startswith('event_') and 'results' not in k}

    def event_log_tree(
        self,
        indent: str = '',
        is_last: bool = True,
        event_children_by_parent: 'dict[str | None, list[BaseEvent[Any]]] | None' = None,
    ) -> None:
        """Print this event and its results with proper tree formatting"""
        from bubus.logging import log_event_tree

        log_event_tree(self, indent, is_last, event_children_by_parent)

    @property
    def event_bus(self) -> 'EventBus':
        """Get the EventBus that is currently processing this event"""
        from bubus.service import EventBus, inside_handler_context

        if not inside_handler_context.get():
            raise AttributeError('event_bus property can only be accessed from within an event handler')

        # The event_path contains all buses this event has passed through
        # The last one in the path is the one currently processing
        if not self.event_path:
            raise RuntimeError('Event has no event_path - was it dispatched?')

        current_bus_label = self.event_path[-1]

        # Find the bus by label (BusName#abcd).
        # Create a list copy to avoid "Set changed size during iteration" error
        for bus in list(EventBus.all_instances):
            if bus and bus.label == current_bus_label:
                return bus

        raise RuntimeError(f'Could not find active EventBus for path entry {current_bus_label}')


def attr_name_allowed(key: str) -> bool:
    allowed_unprefixed_attrs = {'raise_if_errors', 'reset'}
    return key in pydantic_builtin_attrs or key in event_builtin_attrs or key.startswith('_') or key in allowed_unprefixed_attrs


# PSA: All BaseEvent buil-in attrs and methods must be prefixed with "event_" in order to avoid clashing with data contents (which share a namespace with the metadata)
# This is the same approach Pydantic uses for their special `model_*` attrs (and BaseEvent is also a pydantic model, so model_ prefixes are reserved too)
# resist the urge to nest the event data in an inner object unless absolutely necessary, flat simplifies most of the code and makes it easier to read JSON logs with less nesting
pydantic_builtin_attrs = dir(BaseModel)
event_builtin_attrs = {key for key in dir(BaseEvent) if key.startswith('event_')}
illegal_attrs = {key for key in dir(BaseEvent) if not attr_name_allowed(key)}
assert not illegal_attrs, (
    'All BaseEvent attrs and methods must be prefixed with "event_" in order to avoid clashing '
    'with BaseEvent subclass fields used to store event contents (which share a namespace with the event_ metadata). '
    f'not allowed: {illegal_attrs}'
)


class EventResult(BaseModel, Generic[T_EventResultType]):
    """Individual result from a single handler"""

    model_config = ConfigDict(
        extra='forbid',
        arbitrary_types_allowed=True,
        validate_assignment=False,  # Disable to allow flexible result types - validation handled in update()
        validate_default=True,
        revalidate_instances='always',
    )

    # Automatically set fields, setup at Event init and updated by the EventBus.execute_handler() calling event_result.update(...)
    id: UUIDStr = Field(default_factory=uuid7str)
    status: Literal['pending', 'started', 'completed', 'error'] = 'pending'
    event_id: UUIDStr
    handler_id: PythonIdStr
    handler_name: str
    result_type: Any | type[T_EventResultType] | None = None
    eventbus_id: PythonIdStr
    eventbus_name: PythonIdentifierStr
    timeout: float | None = None
    started_at: datetime | None = None

    # Result fields, updated by the EventBus.execute_handler() calling event_result.update(...)
    result: T_EventResultType | BaseEvent[Any] | None = None
    error: BaseException | None = None
    completed_at: datetime | None = None

    # Completion signal
    _handler_completed_signal: asyncio.Event | None = PrivateAttr(default=None)

    # any child events that were emitted during handler execution are captured automatically and stored here to track hierarchy
    # note about why this is BaseEvent[Any] instead of a more specific type:
    #   unfortunately we cant determine child event types statically / it's not worth it to force child event types to be defined at compile-time
    #   so we just allow handlers to emit any BaseEvent subclass/instances with any result types
    #   in theory it's possible to define the entire event tree hierarchy at compile-time with something like ParentEvent[ChildEvent[GrandchildEvent[FinalResultValueType]]],
    #   it's not worth the complexity headache it would incur on users of the library though,
    #   and it would significantly reduce runtime flexibility, e.g. you couldn't define and dispatch arbitrary server-provided event types at runtime
    event_children: list['BaseEvent[Any]'] = Field(default_factory=list)  # pyright: ignore[reportUnknownVariableType]

    @field_serializer('result', when_used='json')
    def _serialize_result(self, value: T_EventResultType | BaseEvent[Any] | None) -> Any:
        """Preserve handler return values when serializing without extra validation."""
        return value

    @property
    def handler_completed_signal(self) -> asyncio.Event | None:
        """Lazily create asyncio.Event when accessed"""
        if self._handler_completed_signal is None:
            try:
                asyncio.get_running_loop()
                self._handler_completed_signal = asyncio.Event()
            except RuntimeError:
                pass  # Keep it None if no event loop
        return self._handler_completed_signal

    def __str__(self) -> str:
        handler_qualname = f'{self.eventbus_name}.{self.handler_name}'
        return f'{handler_qualname}() -> {self.result or self.error or "..."} ({self.status})'

    def __repr__(self) -> str:
        icon = '🏃' if self.status == 'pending' else '✅' if self.status == 'completed' else '❌'
        return f'{self.handler_name}#{self.handler_id[-4:]}() {icon}'

    def __await__(self) -> Generator[Self, Any, T_EventResultType | BaseEvent[Any] | None]:
        """
        Wait for this result to complete and return the result or raise error.
        Does not execute the handler itself, only waits for it to be marked completed by the EventBus.
        EventBus triggers handlers and calls event_result.update() to mark them as started or completed.
        """

        async def wait_for_handler_to_complete_and_return_result() -> T_EventResultType | BaseEvent[Any] | None:
            assert self.handler_completed_signal is not None, 'EventResult cannot be awaited outside of an async context'

            try:
                await asyncio.wait_for(self.handler_completed_signal.wait(), timeout=self.timeout)
            except TimeoutError:
                # self.handler_completed_signal.clear()
                raise TimeoutError(
                    f'Event handler {self.eventbus_name}.{self.handler_name}(#{self.event_id[-4:]}) timed out after {self.timeout}s'
                )

            if self.status == 'error' and self.error:
                raise self.error if isinstance(self.error, BaseException) else Exception(self.error)  # pyright: ignore[reportUnnecessaryIsInstance]

            return self.result

        # do not re-raise exceptions here for now, just return the event in all cases and let the caller handle checking event.error or event.result

        return wait_for_handler_to_complete_and_return_result().__await__()

    def update(self, **kwargs: Any) -> Self:
        """Update the EventResult with provided kwargs, called by EventBus during handler execution."""

        # fix common mistake of returning an exception object instead of marking the event result as an error result
        if 'result' in kwargs and isinstance(kwargs['result'], BaseException):
            logger.warning(
                f'ℹ Event handler {self.handler_name} returned an exception object, auto-converting to EventResult(result=None, status="error", error={kwargs["result"]})'
            )
            kwargs['error'] = kwargs['result']
            kwargs['status'] = 'error'
            kwargs['result'] = None

        if 'result' in kwargs:
            result: Any = kwargs['result']
            self.status = 'completed'
            if self.result_type is not None and result is not None:
                # Always allow BaseEvent results without validation
                # This is needed for event forwarding patterns like bus1.on('*', bus2.dispatch)
                if isinstance(result, BaseEvent):
                    self.result = cast(T_EventResultType, result)
                else:
                    # cast the return value to the expected type using TypeAdapter
                    try:
                        if issubclass(self.result_type, BaseModel):
                            # if expected result type is a pydantic model, validate it with pydantic
                            validated_result = self.result_type.model_validate(result)
                        else:
                            # cast the return value to the expected type e.g. int(result) / str(result) / list(result) / etc.
                            ResultType = TypeAdapter(self.result_type)
                            validated_result = ResultType.validate_python(result)

                        # Normal assignment works, make sure validate_assignment=False otherwise pydantic will attempt to re-validate it a second time
                        self.result = cast(T_EventResultType, validated_result)

                    except Exception as cast_error:
                        self.error = ValueError(
                            f'Event handler returned a value that did not match expected event_result_type: {self.result_type.__name__}({result}) -> {type(cast_error).__name__}: {cast_error}'
                        )
                        self.result = None
                        self.status = 'error'
            else:
                # No result_type specified or result is None - assign directly
                self.result = cast(T_EventResultType, result)

        if 'error' in kwargs:
            assert isinstance(kwargs['error'], (BaseException, str)), (
                f'Invalid error type: {type(kwargs["error"]).__name__} {kwargs["error"]}'
            )
            self.error = kwargs['error'] if isinstance(kwargs['error'], BaseException) else Exception(kwargs['error'])  # pyright: ignore[reportUnnecessaryIsInstance]
            self.status = 'error'

        if 'status' in kwargs:
            assert kwargs['status'] in ('pending', 'started', 'completed', 'error'), f'Invalid status: {kwargs["status"]}'
            self.status = kwargs['status']

        if self.status != 'pending' and not self.started_at:
            self.started_at = datetime.now(UTC)

        if self.status in ('completed', 'error') and not self.completed_at:
            self.completed_at = datetime.now(UTC)
            if self.handler_completed_signal:
                self.handler_completed_signal.set()
        return self

    async def execute(
        self,
        event: 'BaseEvent[T_EventResultType]',
        handler: EventHandler,
        *,
        eventbus: 'EventBus',
        timeout: float | None,
        enter_handler_context: Callable[[BaseEvent[Any], str], tuple[Any, Any, Any]] | None = None,
        exit_handler_context: Callable[[tuple[Any, Any, Any]], None] | None = None,
        format_exception_for_log: Callable[[BaseException], str] | None = None,
    ) -> T_EventResultType | BaseEvent[Any] | None:
        """Execute the handler and update internal state automatically."""

        def _default_enter_handler_context(_: BaseEvent[Any], __: str) -> tuple[None, None, None]:
            return (None, None, None)

        def _default_exit_handler_context(_: tuple[Any, Any, Any]) -> None:
            return None

        def _default_format_exception_for_log(exc: BaseException) -> str:
            from traceback import TracebackException

            return ''.join(TracebackException.from_exception(exc, capture_locals=False).format())

        _enter_handler_context_callable = enter_handler_context or _default_enter_handler_context
        _exit_handler_context_callable = exit_handler_context or _default_exit_handler_context
        _format_exception_for_log_callable = format_exception_for_log or _default_format_exception_for_log

        self.timeout = timeout if timeout is not None else self.timeout or event.event_timeout
        self.result_type = event.event_result_type
        self.update(status='started')
        if hasattr(event, 'event_processed_at'):
            event.event_processed_at = event.event_processed_at or datetime.now(UTC)

        monitor_task: asyncio.Task[None] | None = None
        handler_task: asyncio.Task[Any] | None = None

        # Use dispatch-time context if available (GitHub issue #20)
        # This ensures ContextVars set before dispatch() are accessible in handlers
        # Use getattr to handle stub events that may not have this attribute
        dispatch_context = getattr(event, '_event_dispatch_context', None)

        async def deadlock_monitor() -> None:
            await asyncio.sleep(15.0)
            logger.warning(
                f'⚠️ {eventbus} handler {self.handler_name}() has been running for >15s on event. Possible slow processing or deadlock.\n'
                '(handler could be trying to await its own result or could be blocked by another async task).\n'
                f'{self.handler_name}({event})'
            )

        monitor_task = asyncio.create_task(
            deadlock_monitor(), name=f'{eventbus}.deadlock_monitor({event}, {self.handler_name}#{self.handler_id[-4:]})'
        )

        # For handlers running in dispatch context, we need to set up internal context vars
        # INSIDE that context. Create a wrapper that does setup -> handler -> cleanup.
        # This includes holds_global_lock which is set by ReentrantLock in the parent context.
        async def async_handler_with_context() -> Any:
            """Wrapper that sets up internal context before calling async handler."""
            from bubus.service import holds_global_lock

            # Set holds_global_lock since we're running inside a handler that holds the lock
            # (ReentrantLock set this in the parent context, but dispatch_context is from before that)
            holds_global_lock.set(True)
            tokens = _enter_handler_context_callable(event, self.handler_id)
            try:
                return await handler(event)  # type: ignore
            finally:
                _exit_handler_context_callable(tokens)

        def sync_handler_with_context() -> Any:
            """Wrapper that sets up internal context before calling sync handler."""
            from bubus.service import holds_global_lock

            holds_global_lock.set(True)
            tokens = _enter_handler_context_callable(event, self.handler_id)
            try:
                return handler(event)  # type: ignore[call-arg]  # protocol allows _self param but we dont need it because it's already bound
            finally:
                _exit_handler_context_callable(tokens)

        # If no dispatch context, set up context vars the normal way (outside handler)
        if dispatch_context is None:
            handler_context_tokens = _enter_handler_context_callable(event, self.handler_id)
        else:
            handler_context_tokens = None  # Will be set inside the wrapper

        try:
            if inspect.iscoroutinefunction(handler):
                if dispatch_context is not None:
                    # Run wrapper (which sets internal context) inside dispatch context
                    handler_task = asyncio.create_task(
                        async_handler_with_context(),
                        context=dispatch_context,
                    )
                else:
                    handler_task = asyncio.create_task(handler(event))  # type: ignore
                handler_return_value: Any = await asyncio.wait_for(handler_task, timeout=self.timeout)
            elif inspect.isfunction(handler) or inspect.ismethod(handler):
                if dispatch_context is not None:
                    # Run sync wrapper inside dispatch context
                    handler_return_value = dispatch_context.run(sync_handler_with_context)
                else:
                    handler_return_value = handler(event)
                if isinstance(handler_return_value, BaseEvent):
                    logger.debug(f'Handler {self.handler_name} returned BaseEvent, not awaiting to avoid circular dependency')
            else:
                raise ValueError(f'Handler {get_handler_name(handler)} must be a sync or async function, got: {type(handler)}')

            monitor_task.cancel()
            self.update(result=handler_return_value)
            return self.result

        except asyncio.CancelledError as exc:
            if monitor_task:
                monitor_task.cancel()
            handler_interrupted_error = asyncio.CancelledError(
                f'Event handler {self.handler_name}#{self.handler_id[-4:]}({event}) was interrupted because of a parent timeout'
            )
            self.update(error=handler_interrupted_error)
            raise handler_interrupted_error from exc

        except TimeoutError as exc:
            if monitor_task:
                monitor_task.cancel()
            children = (
                f' and interrupted any processing of {len(event.event_children)} child events' if event.event_children else ''
            )
            timeout_error = TimeoutError(
                f'Event handler {self.handler_name}#{self.handler_id[-4:]}({event}) timed out after {self.timeout}s{children}'
            )
            self.update(error=timeout_error)
            event.event_cancel_pending_child_processing(timeout_error)

            from bubus.logging import log_timeout_tree

            log_timeout_tree(event, self)
            raise timeout_error from exc

        except Exception as exc:
            if monitor_task:
                monitor_task.cancel()
            self.update(error=exc)

            red = '\033[91m'
            reset = '\033[0m'
            logger.error(
                f'❌ {eventbus} Error in event handler {self.handler_name}({event}) -> \n{red}{type(exc).__name__}({exc}){reset}\n{_format_exception_for_log_callable(exc)}',
            )
            raise

        finally:
            if handler_task and not handler_task.done():
                handler_task.cancel()
                try:
                    await asyncio.wait_for(handler_task, timeout=0.1)
                except (asyncio.CancelledError, TimeoutError):
                    pass

            if monitor_task:
                try:
                    if not monitor_task.done():
                        monitor_task.cancel()
                    await monitor_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

            # Only exit context if it was set outside the wrapper (i.e., no dispatch context)
            if handler_context_tokens is not None:
                _exit_handler_context_callable(handler_context_tokens)

    def log_tree(
        self,
        indent: str = '',
        is_last: bool = True,
        event_children_by_parent: dict[str | None, list[BaseEvent[Any]]] | None = None,
    ) -> None:
        """Print this result and its child events with proper tree formatting"""
        from bubus.logging import log_eventresult_tree

        log_eventresult_tree(self, indent, is_last, event_children_by_parent)


# Resolve forward references
BaseEvent.model_rebuild()
EventResult.model_rebuild()

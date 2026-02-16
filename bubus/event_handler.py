import asyncio
import inspect
import os
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, cast, runtime_checkable
from uuid import NAMESPACE_DNS, UUID, uuid5
from weakref import ref as weakref

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing_extensions import TypeVar

if TYPE_CHECKING:
    from bubus.base_event import BaseEvent


# TypeVar for BaseEvent and its subclasses
# We use contravariant=True because if a handler accepts BaseEvent,
# it can also handle any subclass of BaseEvent
T_Event = TypeVar('T_Event', bound='BaseEvent[Any]', contravariant=True, default='BaseEvent[Any]')

# For protocols with __func__ attributes, we need an invariant TypeVar
T_EventInvariant = TypeVar('T_EventInvariant', bound='BaseEvent[Any]', default='BaseEvent[Any]')


class EventHandlerCancelledError(asyncio.CancelledError):
    """Handler was cancelled before starting or before producing a result."""


class EventHandlerTimeoutError(TimeoutError):
    """Handler exceeded its configured handler timeout."""


class EventHandlerAbortedError(asyncio.CancelledError):
    """Handler was interrupted while running (for example by event hard-timeout)."""


class EventHandlerResultSchemaError(ValueError):
    """Handler returned a value incompatible with the event_result_type schema."""


@runtime_checkable
class EventHandlerFunc(Protocol[T_Event]):
    """Protocol for sync event handler functions."""

    def __call__(self, event: T_Event, /) -> Any: ...


@runtime_checkable
class AsyncEventHandlerFunc(Protocol[T_Event]):
    """Protocol for async event handler functions."""

    async def __call__(self, event: T_Event, /) -> Any: ...


@runtime_checkable
class EventHandlerMethod(Protocol[T_Event]):
    """Protocol for instance method event handlers."""

    def __call__(self, self_: Any, event: T_Event, /) -> Any: ...

    __self__: Any
    __name__: str


@runtime_checkable
class AsyncEventHandlerMethod(Protocol[T_Event]):
    """Protocol for async instance method event handlers."""

    async def __call__(self, self_: Any, event: T_Event, /) -> Any: ...

    __self__: Any
    __name__: str


@runtime_checkable
class EventHandlerClassMethod(Protocol[T_EventInvariant]):
    """Protocol for class method event handlers."""

    def __call__(self, cls: type[Any], event: T_EventInvariant, /) -> Any: ...

    __self__: type[Any]
    __name__: str
    __func__: Callable[[type[Any], T_EventInvariant], Any]


@runtime_checkable
class AsyncEventHandlerClassMethod(Protocol[T_EventInvariant]):
    """Protocol for async class method event handlers."""

    async def __call__(self, cls: type[Any], event: T_EventInvariant, /) -> Any: ...

    __self__: type[Any]
    __name__: str
    __func__: Callable[[type[Any], T_EventInvariant], Awaitable[Any]]


# Event handlers can be plain functions, bound methods, class methods, or
# async variants. Runtime validation in EventBus enforces callable shape.
EventHandlerCallable: TypeAlias = Callable[..., Any]
NormalizedEventHandlerCallable: TypeAlias = Callable[[Any], Awaitable[Any]]

# Single-argument callable that accepts one BaseEvent subtype.
ContravariantEventHandlerCallable: TypeAlias = Callable[[T_Event], Any]

HANDLER_ID_NAMESPACE: UUID = uuid5(NAMESPACE_DNS, 'bubus-handler')


def _format_handler_source_path(path: str, line_no: int | None = None) -> str:
    normalized = str(Path(path).expanduser().resolve())
    home = str(Path.home())
    if normalized == home:
        display = '~'
    elif normalized.startswith(home + os.sep):
        display = f'~{normalized[len(home) :]}'
    else:
        display = normalized
    return f'{display}:{line_no}' if line_no else display


class _HandlerCacheKey:
    __slots__ = ('handler_ref', 'handler_id', '_hash')

    def __init__(self, handler: EventHandlerCallable) -> None:
        # Some callables override __eq__ without __hash__ and become unhashable.
        # Use identity-based hashing for a stable cache key without retaining handlers.
        self.handler_ref = weakref(handler)
        self.handler_id = id(handler)
        self._hash = self.handler_id

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _HandlerCacheKey):
            return False
        if self.handler_id != other.handler_id:
            return False
        return self.handler_ref() is other.handler_ref()


@lru_cache(maxsize=100)
def _get_callable_handler_file_path(handler_key: _HandlerCacheKey) -> str | None:
    """Best-effort, low-overhead source location for a handler callable."""
    handler = handler_key.handler_ref()
    if handler is None:
        return None
    target: Any = handler.__func__ if inspect.ismethod(handler) else handler
    target = inspect.unwrap(target)

    code_obj = getattr(target, '__code__', None)
    if code_obj is not None:
        file_path = getattr(code_obj, 'co_filename', None)
        line_no = getattr(code_obj, 'co_firstlineno', None)
        if isinstance(file_path, str) and file_path.strip():
            return _format_handler_source_path(file_path, int(line_no) if isinstance(line_no, int) else None)

    try:
        source_file = inspect.getsourcefile(target) or inspect.getfile(target)
    except (OSError, TypeError):
        source_file = None

    line_no: int | None = None
    try:
        _, line_no = inspect.getsourcelines(target)
    except (OSError, TypeError):
        line_no = None

    if isinstance(source_file, str) and source_file.strip():
        return _format_handler_source_path(source_file, line_no)

    module = inspect.getmodule(target)
    module_file = getattr(module, '__file__', None) if module is not None else None
    if isinstance(module_file, str) and module_file.strip():
        return _format_handler_source_path(module_file, line_no)

    return None


class EventHandler(BaseModel):
    """Serializable metadata wrapper around a registered event handler callable."""

    model_config = ConfigDict(
        extra='forbid',
        arbitrary_types_allowed=True,
        validate_assignment=False,
        validate_default=True,
        revalidate_instances='always',
    )

    id: str = ''
    handler: EventHandlerCallable | None = Field(default=None, exclude=True, repr=False)
    handler_async: NormalizedEventHandlerCallable | None = Field(default=None, exclude=True, repr=False)
    handler_name: str = 'anonymous'
    handler_file_path: str | None = None
    handler_timeout: float | None = None
    handler_slow_timeout: float | None = None
    handler_registered_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    handler_registered_ts: int | float = Field(default_factory=time.time_ns)
    event_pattern: str = '*'
    eventbus_name: str = 'EventBus'
    eventbus_id: str = '00000000-0000-0000-0000-000000000000'

    def __setattr__(self, name: str, value: Any) -> None:
        """Keep normalized async callable in sync when `handler` is reassigned."""
        super().__setattr__(name, value)
        if name != 'handler':
            return
        if value is None:
            super().__setattr__('handler_async', None)
            return
        super().__setattr__('handler_async', self.resolve_async_handler(cast(EventHandlerCallable, value)))

    @field_validator('handler_name', mode='before')
    @classmethod
    def _validate_handler_name_field(cls, value: Any) -> str:
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                return normalized
        return 'anonymous'

    @field_validator('eventbus_name')
    @classmethod
    def _validate_eventbus_name_field(cls, value: str) -> str:
        normalized = str(value)
        assert normalized.isidentifier() and not normalized.startswith('_'), f'Invalid event bus name: {value!r}'
        return normalized

    @property
    def eventbus_label(self) -> str:
        return f'{self.eventbus_name}#{self.eventbus_id[-4:]}'

    @staticmethod
    def get_callable_handler_name(handler: EventHandlerCallable) -> str:
        assert hasattr(handler, '__name__'), f'Handler {handler} has no __name__ attribute!'
        if inspect.ismethod(handler):
            return f'{type(handler.__self__).__name__}.{handler.__name__}'
        if callable(handler):
            handler_module = getattr(handler, '__module__', '<unknown>')
            handler_name = getattr(handler, '__name__', type(handler).__name__)
            return f'{handler_module}.{handler_name}'
        raise ValueError(f'Invalid handler: {handler} {type(handler)}, expected a function, coroutine, or method')

    @model_validator(mode='before')
    @classmethod
    def _populate_handler_name(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        params = cast(dict[str, Any], data)
        if params.get('id') is None:
            params.pop('id', None)
        handler = params.get('handler')
        if handler is not None and not params.get('handler_name'):
            try:
                derived_name = cls.get_callable_handler_name(cast(EventHandlerCallable, handler))
                params['handler_name'] = derived_name.strip() or 'function'
            except Exception:
                params['handler_name'] = 'function'
        return params

    def model_post_init(self, __context: Any) -> None:
        if not self.id:
            self.id = self.compute_handler_id()

    def compute_handler_id(self) -> str:
        """Match TS handler-id algorithm: uuidv5(seed, HANDLER_ID_NAMESPACE)."""
        file_path = self.handler_file_path or 'unknown'
        registered_at = self.handler_registered_at
        if registered_at.tzinfo is None:
            registered_at = registered_at.replace(tzinfo=UTC)
        registered_at_iso = registered_at.astimezone(UTC).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        seed = (
            f'{self.eventbus_id}|{self.handler_name}|{file_path}|'
            f'{registered_at_iso}|{self.handler_registered_ts}|{self.event_pattern}'
        )
        return str(uuid5(HANDLER_ID_NAMESPACE, seed))

    @property
    def label(self) -> str:
        assert self.id, 'EventHandler.id must be set'
        return f'{self.handler_name}#{self.id[-4:]}'

    def __str__(self) -> str:
        has_name = self.handler_name and self.handler_name != 'anonymous'
        assert self.id, 'EventHandler.id must be set'
        display = f'{self.handler_name}()' if has_name else f'function#{self.id[-4:]}()'
        return f'{display} @ {self.handler_file_path}' if self.handler_file_path else display

    def __call__(self, event: 'BaseEvent[Any]') -> Any:
        if self.handler is None:
            raise RuntimeError(f'EventHandler {self.id} has no callable attached')
        handler_callable = cast(Callable[[Any], Any], self.handler)
        return handler_callable(event)

    @staticmethod
    def resolve_async_handler(handler: EventHandlerCallable) -> NormalizedEventHandlerCallable:
        """Normalize one handler callable to a single async call signature.

        Sync handlers are wrapped in an async closure that runs inline on the
        event loop thread. Async handlers are wrapped to preserve a consistent
        callable shape for downstream execution code.
        """
        if inspect.iscoroutinefunction(handler):

            async def normalized_handler(event: Any) -> Any:
                return await handler(event)

            return normalized_handler

        if inspect.isfunction(handler) or inspect.ismethod(handler):

            async def normalized_handler(event: Any) -> Any:
                return handler(event)

            return normalized_handler

        handler_name = EventHandler.get_callable_handler_name(handler)
        raise ValueError(f'Handler {handler_name} must be a sync or async function, got: {type(handler)}')

    def to_json_dict(self) -> dict[str, Any]:
        return self.model_dump(mode='json', exclude={'handler'})

    @classmethod
    def from_json_dict(cls, data: Any, handler: EventHandlerCallable | None = None) -> 'EventHandler':
        entry = cls.model_validate(data)
        if not entry.id:
            entry.id = entry.compute_handler_id()
        handler_name_provided = isinstance(data, dict) and bool(cast(dict[str, Any], data).get('handler_name'))
        if handler is not None:
            entry.handler = handler
            entry.handler_async = cls.resolve_async_handler(handler)
            if not handler_name_provided and entry.handler_name == 'anonymous':
                try:
                    derived_name = cls.get_callable_handler_name(handler)
                    entry.handler_name = derived_name.strip() or 'function'
                except Exception:
                    entry.handler_name = 'function'
        return entry

    @classmethod
    def from_callable(
        cls,
        *,
        handler: EventHandlerCallable,
        event_pattern: str,
        eventbus_name: str,
        eventbus_id: str,
        detect_handler_file_path: bool = True,
        id: str | None = None,
        handler_file_path: str | None = None,
        handler_timeout: float | None = None,
        handler_slow_timeout: float | None = None,
        handler_registered_at: datetime | None = None,
        handler_registered_ts: int | None = None,
    ) -> 'EventHandler':
        resolved_file_path = handler_file_path
        if resolved_file_path is None and detect_handler_file_path:
            resolved_file_path = _get_callable_handler_file_path(_HandlerCacheKey(handler))
        async_handler = cls.resolve_async_handler(handler)

        handler_params: dict[str, Any] = {
            'handler': handler,
            'handler_async': async_handler,
            'handler_file_path': resolved_file_path,
            'handler_registered_at': handler_registered_at or datetime.now(UTC),
            'handler_registered_ts': handler_registered_ts or time.time_ns(),
            'event_pattern': event_pattern,
            'eventbus_name': eventbus_name,
            'eventbus_id': eventbus_id,
        }
        try:
            derived_name = cls.get_callable_handler_name(handler)
            handler_params['handler_name'] = derived_name.strip() or 'function'
        except Exception:
            handler_params['handler_name'] = 'function'
        if id is not None:
            handler_params['id'] = id
        if handler_timeout is not None:
            handler_params['handler_timeout'] = handler_timeout
        if handler_slow_timeout is not None:
            handler_params['handler_slow_timeout'] = handler_slow_timeout

        entry = cls(**handler_params)
        if not entry.id:
            entry.id = entry.compute_handler_id()
        return entry


__all__ = [
    'AsyncEventHandlerClassMethod',
    'AsyncEventHandlerFunc',
    'AsyncEventHandlerMethod',
    'ContravariantEventHandlerCallable',
    'EventHandler',
    'EventHandlerCallable',
    'EventHandlerClassMethod',
    'EventHandlerFunc',
    'EventHandlerMethod',
    'NormalizedEventHandlerCallable',
]

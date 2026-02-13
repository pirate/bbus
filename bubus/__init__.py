"""Event bus library."""

from . import events_suck
from .base_event import (
    BaseEvent,
    EventConcurrencyMode,
    EventHandlerCompletionMode,
    EventHandlerConcurrencyMode,
    EventStatus,
    PythonIdentifierStr,
    PythonIdStr,
    UUIDStr,
)
from .bridges import HTTPEventBridge, SocketEventBridge
from .event_bus import EventBus
from .event_handler import EventHandler
from .event_history import EventHistory, InMemoryEventHistory
from .event_result import EventResult
from .middlewares import (
    AutoErrorEventMiddleware,
    AutoHandlerChangeEventMiddleware,
    AutoReturnEventMiddleware,
    BusHandlerRegisteredEvent,
    BusHandlerUnregisteredEvent,
    EventBusMiddleware,
    LoggerEventBusMiddleware,
    OtelTracingMiddleware,
    SQLiteHistoryMirrorMiddleware,
    WALEventBusMiddleware,
)

__all__ = [
    'EventBus',
    'EventBusMiddleware',
    'BusHandlerRegisteredEvent',
    'BusHandlerUnregisteredEvent',
    'HTTPEventBridge',
    'SocketEventBridge',
    'LoggerEventBusMiddleware',
    'OtelTracingMiddleware',
    'SQLiteHistoryMirrorMiddleware',
    'AutoErrorEventMiddleware',
    'AutoHandlerChangeEventMiddleware',
    'AutoReturnEventMiddleware',
    'WALEventBusMiddleware',
    'EventHistory',
    'InMemoryEventHistory',
    'BaseEvent',
    'EventStatus',
    'EventResult',
    'EventHandler',
    'EventHandlerConcurrencyMode',
    'EventHandlerCompletionMode',
    'EventConcurrencyMode',
    'UUIDStr',
    'PythonIdStr',
    'PythonIdentifierStr',
    'events_suck',
]

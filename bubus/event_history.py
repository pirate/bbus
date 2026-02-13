from __future__ import annotations

from typing import Any, Generic, TypeVar

from .base_event import BaseEvent, UUIDStr

BaseEventT = TypeVar('BaseEventT', bound=BaseEvent[Any])


class EventHistory(dict[UUIDStr, BaseEventT], Generic[BaseEventT]):
    """Backward-compatible in-memory history with plain dict behaviour."""

    __slots__ = ()


# Backwards compatible alias – before refactor this was the default backend.
InMemoryEventHistory = EventHistory

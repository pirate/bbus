from __future__ import annotations

from typing import Any, Generic, TypeVar

from .base_event import BaseEvent, UUIDStr

BaseEventT = TypeVar('BaseEventT', bound=BaseEvent[Any])


class EventHistory(dict[UUIDStr, BaseEventT], Generic[BaseEventT]):
    """In-memory event history map with plain dict behaviour."""

    __slots__ = ()

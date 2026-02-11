"""NATS bridge for forwarding events between runtimes.

Optional dependency: nats-py
"""

from __future__ import annotations

import asyncio
import importlib
import json
from collections.abc import Callable
from typing import Any

from uuid_extensions import uuid7str

from bubus.models import BaseEvent
from bubus.service import EventBus, EventPatternType, inside_handler_context


class NATSEventBridge:
    def __init__(self, server: str, subject: str, *, name: str | None = None):
        self.server = server
        self.subject = subject
        self._inbound_bus = EventBus(name=name or f'NATSEventBridge_{uuid7str()[-8:]}', max_history_size=0)

        self._running = False
        self._nc: Any | None = None

    def on(self, event_pattern: EventPatternType, handler: Callable[[BaseEvent[Any]], Any]) -> None:
        self._ensure_started()
        self._inbound_bus.on(event_pattern, handler)

    async def dispatch(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        self._ensure_started()
        if self._nc is None:
            await self.start()

        payload = event.model_dump(mode='json')
        assert self._nc is not None
        await self._nc.publish(self.subject, json.dumps(payload, separators=(',', ':')).encode('utf-8'))

        if inside_handler_context.get():
            return None
        return event

    async def emit(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        return await self.dispatch(event)

    async def start(self) -> None:
        if self._running:
            return

        nats_module = self._load_nats()
        self._nc = await nats_module.connect(self.server)

        async def _on_msg(msg: Any) -> None:
            try:
                payload = json.loads(msg.data.decode('utf-8'))
            except Exception:
                return
            await self._dispatch_inbound_payload(payload)

        assert self._nc is not None
        await self._nc.subscribe(self.subject, cb=_on_msg)
        self._running = True

    async def close(self, *, clear: bool = True) -> None:
        self._running = False
        if self._nc is not None:
            await self._nc.drain()
            await self._nc.close()
            self._nc = None
        await self._inbound_bus.stop(clear=clear)

    def _ensure_started(self) -> None:
        if self._running:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        asyncio.create_task(self.start())

    async def _dispatch_inbound_payload(self, payload: Any) -> None:
        event = BaseEvent[Any].model_validate(payload).reset()
        self._inbound_bus.dispatch(event)

    @staticmethod
    def _load_nats() -> Any:
        try:
            return importlib.import_module('nats')
        except ModuleNotFoundError as exc:
            raise RuntimeError('NATSEventBridge requires optional dependency: pip install nats-py') from exc

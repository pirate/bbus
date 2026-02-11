"""JSONL bridge for forwarding events between runtimes.

This bridge is intentionally simple:
- emit/dispatch appends one raw event JSON object per line
- listener polls the file and emits any unseen lines
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from uuid_extensions import uuid7str

from bubus.models import BaseEvent
from bubus.service import EventBus, EventPatternType, inside_handler_context


class JSONLEventBridge:
    def __init__(self, path: str, *, poll_interval: float = 0.25, name: str | None = None):
        self.path = Path(path)
        self.poll_interval = poll_interval
        self._inbound_bus = EventBus(name=name or f'JSONLEventBridge_{uuid7str()[-8:]}')

        self._running = False
        self._listener_task: asyncio.Task[None] | None = None
        self._line_offset = 0

    def on(self, event_pattern: EventPatternType, handler: Callable[[BaseEvent[Any]], Any]) -> None:
        self._ensure_started()
        self._inbound_bus.on(event_pattern, handler)

    async def dispatch(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        self._ensure_started()

        payload = event.model_dump(mode='json')
        self.path.parent.mkdir(parents=True, exist_ok=True)

        await asyncio.to_thread(self._append_line, json.dumps(payload, separators=(',', ':')))

        if inside_handler_context.get():
            return None
        return event

    async def emit(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        return await self.dispatch(event)

    async def start(self) -> None:
        if self._running:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)
        self._line_offset = self._count_lines()
        self._running = True
        self._listener_task = asyncio.create_task(self._listen_loop())

    async def close(self, *, clear: bool = True) -> None:
        self._running = False
        if self._listener_task is not None:
            self._listener_task.cancel()
            await asyncio.gather(self._listener_task, return_exceptions=True)
            self._listener_task = None
        await self._inbound_bus.stop(clear=clear)

    def _ensure_started(self) -> None:
        if self._running:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        self._listener_task = asyncio.create_task(self.start())

    async def _listen_loop(self) -> None:
        while self._running:
            try:
                await self._poll_new_lines()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            await asyncio.sleep(self.poll_interval)

    async def _poll_new_lines(self) -> None:
        lines = await asyncio.to_thread(self._read_lines)
        if self._line_offset >= len(lines):
            return
        new_lines = lines[self._line_offset :]
        self._line_offset = len(lines)

        for line in new_lines:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            await self._dispatch_inbound_payload(payload)

    async def _dispatch_inbound_payload(self, payload: Any) -> None:
        event = BaseEvent[Any].model_validate(payload)
        for bus in list(EventBus.all_instances):
            if not bus:
                continue
            existing = bus.event_history.get(event.event_id)
            if existing is not None:
                event = existing
                break
        self._inbound_bus.dispatch(event)

    def _read_lines(self) -> list[str]:
        return self.path.read_text(encoding='utf-8').splitlines()

    def _append_line(self, payload: str) -> None:
        with self.path.open('a', encoding='utf-8') as fp:
            fp.write(payload + '\n')

    def _count_lines(self) -> int:
        try:
            return len(self._read_lines())
        except FileNotFoundError:
            return 0

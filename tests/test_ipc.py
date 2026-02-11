"""Tests for HTTPEventBridge and SocketEventBridge transports."""

from __future__ import annotations

import socket
from pathlib import Path

import pytest
from uuid_extensions import uuid7str

from bubus import BaseEvent, EventBus, HTTPEventBridge, SocketEventBridge


class IPCPingEvent(BaseEvent):
    value: int


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        return int(sock.getsockname()[1])


@pytest.mark.asyncio
async def test_http_event_bridge_send_to_listen_on() -> None:
    port = _free_tcp_port()
    endpoint = f'http://127.0.0.1:{port}/events'

    source_bus = EventBus(name='SourceBus')
    sink_bus = EventBus(name='SinkBus')
    sender = HTTPEventBridge(send_to=endpoint)
    receiver = HTTPEventBridge(listen_on=endpoint)

    seen_values: list[int] = []

    sink_bus.on(IPCPingEvent, lambda event: seen_values.append(event.value))
    source_bus.on('*', sender.emit)
    receiver.on('*', sink_bus.emit)

    await receiver.start()

    try:
        outbound_event = source_bus.emit(IPCPingEvent(value=7))
        await outbound_event
        await sink_bus.wait_until_idle()
        received = await sink_bus.find(IPCPingEvent, past=True, future=False)
        assert received is not None
        assert received.value == 7
        assert seen_values == [7]
    finally:
        await sender.close()
        await receiver.close()
        await source_bus.stop(clear=True)
        await sink_bus.stop(clear=True)


@pytest.mark.asyncio
async def test_socket_event_bridge_unix_send_to_listen_on() -> None:
    socket_path = Path('/tmp') / f'bubus-ipc-{uuid7str()[-8:]}.sock'
    source_bus = EventBus(name='SourceBusUnix')
    sink_bus = EventBus(name='SinkBusUnix')
    sender = SocketEventBridge(path=str(socket_path))
    receiver = SocketEventBridge(path=str(socket_path))

    seen_values: list[int] = []

    sink_bus.on(IPCPingEvent, lambda event: seen_values.append(event.value))
    source_bus.on('*', sender.emit)
    receiver.on('*', sink_bus.emit)

    await receiver.start()

    try:
        outbound_event = source_bus.emit(IPCPingEvent(value=19))
        await outbound_event
        await sink_bus.wait_until_idle()
        received = await sink_bus.find(IPCPingEvent, past=True, future=False)
        assert received is not None
        assert received.value == 19
        assert seen_values == [19]
    finally:
        await sender.close()
        await receiver.close()
        await source_bus.stop(clear=True)
        await sink_bus.stop(clear=True)


def test_socket_event_bridge_rejects_long_socket_paths() -> None:
    long_path = '/tmp/' + ('a' * 100) + '.sock'
    with pytest.raises(ValueError, match='too long'):
        SocketEventBridge(path=long_path)

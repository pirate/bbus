"""Process-isolated roundtrip tests for bridge transports."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from shutil import rmtree
from typing import Any

import pytest
from uuid_extensions import uuid7str

from bubus import BaseEvent, HTTPEventBridge, SocketEventBridge
from bubus.bridge_jsonl import JSONLEventBridge
from bubus.bridge_nats import NATSEventBridge
from bubus.bridge_postgres import PostgresEventBridge
from bubus.bridge_redis import RedisEventBridge
from bubus.bridge_sqlite import SQLiteEventBridge

if os.getenv('GITHUB_ACTIONS', '').lower() == 'true':
    pytestmark = pytest.mark.skip(reason='bridge tests are skipped on GitHub Actions')


class IPCPingEvent(BaseEvent):
    label: str


_TEST_RUN_ID = f'{int(time.time() * 1000)}-{uuid7str()[-8:]}'


def _make_temp_dir(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=f'{prefix}-{_TEST_RUN_ID}-'))


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        return int(sock.getsockname()[1])


def _canonical(payload: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in payload.items():
        if key.endswith('_at') and isinstance(value, str):
            try:
                normalized[key] = datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()
                continue
            except ValueError:
                pass
        normalized[key] = value
    return normalized


def _normalize_roundtrip_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _canonical(payload)
    normalized.pop('event_id', None)
    normalized.pop('event_path', None)
    normalized.pop('event_result_type', None)
    return normalized


@asynccontextmanager
async def _running_process(command: list[str], *, cwd: Path | None = None) -> AsyncIterator[subprocess.Popen[str]]:
    process = subprocess.Popen(
        command,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        yield process
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


async def _wait_for_port(port: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
            writer.close()
            await writer.wait_closed()
            return
        except OSError:
            await asyncio.sleep(0.05)
    raise TimeoutError(f'port did not open in time: {port}')


async def _wait_for_path(path: Path, *, process: subprocess.Popen[str], timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise AssertionError(f'worker exited early ({process.returncode})\nstdout:\n{stdout}\nstderr:\n{stderr}')
        await asyncio.sleep(0.05)
    raise TimeoutError(f'path did not appear in time: {path}')


def _make_sender_bridge(kind: str, config: dict[str, Any], *, low_latency: bool = False) -> Any:
    if kind == 'http':
        return HTTPEventBridge(send_to=str(config['endpoint']))
    if kind == 'socket':
        return SocketEventBridge(path=str(config['path']))
    if kind == 'jsonl':
        return JSONLEventBridge(str(config['path']), poll_interval=0.001 if low_latency else 0.05)
    if kind == 'sqlite':
        return SQLiteEventBridge(
            str(config['path']),
            str(config['table']),
            poll_interval=0.001 if low_latency else 0.05,
        )
    if kind == 'redis':
        return RedisEventBridge(str(config['url']))
    if kind == 'nats':
        return NATSEventBridge(str(config['server']), str(config['subject']))
    if kind == 'postgres':
        return PostgresEventBridge(str(config['url']))
    raise ValueError(f'Unsupported bridge kind: {kind}')


def _make_listener_bridge(kind: str, config: dict[str, Any], *, low_latency: bool = False) -> Any:
    if kind == 'http':
        return HTTPEventBridge(listen_on=str(config['endpoint']))
    if kind == 'socket':
        return SocketEventBridge(path=str(config['path']))
    if kind == 'jsonl':
        return JSONLEventBridge(str(config['path']), poll_interval=0.001 if low_latency else 0.05)
    if kind == 'sqlite':
        return SQLiteEventBridge(
            str(config['path']),
            str(config['table']),
            poll_interval=0.001 if low_latency else 0.05,
        )
    if kind == 'redis':
        return RedisEventBridge(str(config['url']))
    if kind == 'nats':
        return NATSEventBridge(str(config['server']), str(config['subject']))
    if kind == 'postgres':
        return PostgresEventBridge(str(config['url']))
    raise ValueError(f'Unsupported bridge kind: {kind}')


async def _measure_warm_latency_ms(kind: str, config: dict[str, Any]) -> float:
    attempts = 3
    last_error: BaseException | None = None

    for _attempt in range(attempts):
        sender = _make_sender_bridge(kind, config, low_latency=True)
        receiver = _make_listener_bridge(kind, config, low_latency=True)

        run_suffix = uuid7str()[-8:]
        warmup_prefix = f'warmup_{run_suffix}_'
        measured_prefix = f'measured_{run_suffix}_'
        warmup_count_target = 5
        measured_count_target = 1000

        warmup_seen_count = 0
        measured_seen_count = 0
        warmup_seen = asyncio.Event()
        measured_seen = asyncio.Event()

        async def _on_event(event: BaseEvent[Any]) -> None:
            nonlocal warmup_seen_count, measured_seen_count
            label = getattr(event, 'label', '')
            if not isinstance(label, str):
                return
            if label.startswith(warmup_prefix):
                warmup_seen_count += 1
                if warmup_seen_count >= warmup_count_target:
                    warmup_seen.set()
                return
            if label.startswith(measured_prefix):
                measured_seen_count += 1
                if measured_seen_count >= measured_count_target:
                    measured_seen.set()

        try:
            await sender.start()
            await receiver.start()
            receiver.on('IPCPingEvent', _on_event)
            await asyncio.sleep(0.1)

            for index in range(warmup_count_target):
                await sender.emit(
                    IPCPingEvent(
                        label=f'{warmup_prefix}{index}',
                    )
                )
            await asyncio.wait_for(warmup_seen.wait(), timeout=60.0)

            start_ns = time.perf_counter_ns()
            for index in range(measured_count_target):
                await sender.emit(
                    IPCPingEvent(
                        label=f'{measured_prefix}{index}',
                    )
                )
            await asyncio.wait_for(measured_seen.wait(), timeout=600.0)
            elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            return elapsed_ms / measured_count_target
        except asyncio.TimeoutError as exc:
            last_error = exc
        finally:
            await sender.close()
            await receiver.close()

        await asyncio.sleep(0.2)

    raise RuntimeError(f'bridge latency measurement timed out after {attempts} attempts: {kind}') from last_error


async def _assert_roundtrip(kind: str, config: dict[str, Any]) -> None:
    temp_path = _make_temp_dir(f'bubus-bridge-{kind}')
    try:
        worker_config_path = temp_path / 'worker_config.json'
        worker_ready_path = temp_path / 'worker_ready'
        received_event_path = temp_path / 'received_event.json'
        worker_config = {
            **config,
            'kind': kind,
            'ready_path': str(worker_ready_path),
            'output_path': str(received_event_path),
        }
        worker_config_path.write_text(json.dumps(worker_config), encoding='utf-8')

        sender = _make_sender_bridge(kind, config)

        worker = subprocess.Popen(
            [sys.executable, str(Path(__file__).with_name('bridge_listener_worker.py')), str(worker_config_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            await _wait_for_path(worker_ready_path, process=worker)
            if kind == 'postgres':
                await sender.start()
            outbound = IPCPingEvent(label=f'{kind}_ok')
            await sender.emit(outbound)
            await _wait_for_path(received_event_path, process=worker)
            received_payload = json.loads(received_event_path.read_text(encoding='utf-8'))
            assert _normalize_roundtrip_payload(received_payload) == _normalize_roundtrip_payload(
                outbound.model_dump(mode='json')
            )
        finally:
            await sender.close()
            if worker.poll() is None:
                worker.terminate()
                try:
                    worker.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    worker.kill()
                    worker.wait(timeout=5)
    finally:
        rmtree(temp_path, ignore_errors=True)


@pytest.mark.asyncio
async def test_http_event_bridge_roundtrip_between_processes() -> None:
    endpoint = f'http://127.0.0.1:{_free_tcp_port()}/events'
    await _assert_roundtrip('http', {'endpoint': endpoint})
    latency_ms = await _measure_warm_latency_ms('http', {'endpoint': endpoint})
    print(f'LATENCY python http {latency_ms:.3f}ms')


@pytest.mark.asyncio
async def test_socket_event_bridge_roundtrip_between_processes() -> None:
    socket_path = Path('/tmp') / f'bb-{_TEST_RUN_ID}-{uuid7str()[-8:]}.sock'
    await _assert_roundtrip('socket', {'path': str(socket_path)})
    latency_ms = await _measure_warm_latency_ms('socket', {'path': str(socket_path)})
    print(f'LATENCY python socket {latency_ms:.3f}ms')


def test_socket_event_bridge_rejects_long_socket_paths() -> None:
    long_path = '/tmp/' + ('a' * 100) + '.sock'
    with pytest.raises(ValueError, match='too long'):
        SocketEventBridge(path=long_path)


@pytest.mark.asyncio
async def test_jsonl_event_bridge_roundtrip_between_processes() -> None:
    temp_dir = _make_temp_dir('bubus-jsonl')
    try:
        jsonl_path = temp_dir / 'events.jsonl'
        await _assert_roundtrip('jsonl', {'path': str(jsonl_path)})
        latency_ms = await _measure_warm_latency_ms('jsonl', {'path': str(jsonl_path)})
        print(f'LATENCY python jsonl {latency_ms:.3f}ms')
    finally:
        rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_sqlite_event_bridge_roundtrip_between_processes() -> None:
    temp_dir = _make_temp_dir('bubus-sqlite')
    try:
        sqlite_path = temp_dir / 'events.sqlite3'
        await _assert_roundtrip('sqlite', {'path': str(sqlite_path), 'table': 'bubus_events'})
        measure_sqlite_path = temp_dir / 'events.measure.sqlite3'
        latency_ms = await _measure_warm_latency_ms('sqlite', {'path': str(measure_sqlite_path), 'table': 'bubus_events'})
        print(f'LATENCY python sqlite {latency_ms:.3f}ms')
    finally:
        rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_redis_event_bridge_roundtrip_between_processes() -> None:
    temp_dir = _make_temp_dir('bubus-redis')
    try:
        port = _free_tcp_port()
        command = [
            'redis-server',
            '--save',
            '',
            '--appendonly',
            'no',
            '--bind',
            '127.0.0.1',
            '--port',
            str(port),
            '--dir',
            str(temp_dir),
        ]
        async with _running_process(command) as redis_process:
            await _wait_for_port(port)
            await _assert_roundtrip('redis', {'url': f'redis://127.0.0.1:{port}/1/bubus_events'})
            latency_ms = await _measure_warm_latency_ms('redis', {'url': f'redis://127.0.0.1:{port}/1/bubus_events'})
            print(f'LATENCY python redis {latency_ms:.3f}ms')
            assert redis_process.poll() is None
    finally:
        rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_nats_event_bridge_roundtrip_between_processes() -> None:
    port = _free_tcp_port()
    command = ['nats-server', '-a', '127.0.0.1', '-p', str(port)]
    async with _running_process(command) as nats_process:
        await _wait_for_port(port)
        await _assert_roundtrip('nats', {'server': f'nats://127.0.0.1:{port}', 'subject': 'bubus_events'})
        latency_ms = await _measure_warm_latency_ms('nats', {'server': f'nats://127.0.0.1:{port}', 'subject': 'bubus_events'})
        print(f'LATENCY python nats {latency_ms:.3f}ms')
        assert nats_process.poll() is None


@pytest.mark.asyncio
async def test_postgres_event_bridge_roundtrip_between_processes() -> None:
    temp_dir = _make_temp_dir('bubus-postgres')
    try:
        data_dir = temp_dir / 'pgdata'
        initdb = subprocess.run(
            ['initdb', '-D', str(data_dir), '-A', 'trust', '-U', 'postgres'],
            capture_output=True,
            text=True,
            check=False,
        )
        assert initdb.returncode == 0, f'initdb failed\nstdout:\n{initdb.stdout}\nstderr:\n{initdb.stderr}'

        port = _free_tcp_port()
        command = ['postgres', '-D', str(data_dir), '-h', '127.0.0.1', '-p', str(port), '-k', '/tmp']
        async with _running_process(command) as postgres_process:
            await _wait_for_port(port)
            await _assert_roundtrip('postgres', {'url': f'postgresql://postgres@127.0.0.1:{port}/postgres/bubus_events'})
            latency_ms = await _measure_warm_latency_ms(
                'postgres', {'url': f'postgresql://postgres@127.0.0.1:{port}/postgres/bubus_events'}
            )
            print(f'LATENCY python postgres {latency_ms:.3f}ms')
            assert postgres_process.poll() is None
    finally:
        rmtree(temp_dir, ignore_errors=True)

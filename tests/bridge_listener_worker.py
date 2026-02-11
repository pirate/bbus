from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from bubus import HTTPEventBridge, SocketEventBridge
from bubus.bridge_jsonl import JSONLEventBridge
from bubus.bridge_nats import NATSEventBridge
from bubus.bridge_postgres import PostgresEventBridge
from bubus.bridge_redis import RedisEventBridge
from bubus.bridge_sqlite import SQLiteEventBridge


def _make_listener_bridge(config: dict[str, Any]) -> Any:
    kind = str(config['kind'])
    if kind == 'http':
        return HTTPEventBridge(listen_on=str(config['endpoint']))
    if kind == 'socket':
        return SocketEventBridge(path=str(config['path']))
    if kind == 'jsonl':
        return JSONLEventBridge(str(config['path']), poll_interval=0.05)
    if kind == 'sqlite':
        return SQLiteEventBridge(str(config['path']), str(config['table']), poll_interval=0.05)
    if kind == 'redis':
        return RedisEventBridge(str(config['url']))
    if kind == 'nats':
        return NATSEventBridge(str(config['server']), str(config['subject']))
    if kind == 'postgres':
        return PostgresEventBridge(str(config['url']))
    raise ValueError(f'Unsupported bridge kind: {kind}')


async def _main(config_path: str) -> None:
    config = json.loads(Path(config_path).read_text(encoding='utf-8'))
    ready_path = Path(str(config['ready_path']))
    output_path = Path(str(config['output_path']))
    done = asyncio.Event()

    bridge = _make_listener_bridge(config)

    def _on_event(event: Any) -> None:
        output_path.write_text(json.dumps(event.model_dump(mode='json')), encoding='utf-8')
        done.set()

    await bridge.start()
    bridge.on('*', _on_event)
    ready_path.write_text('ready', encoding='utf-8')
    try:
        await asyncio.wait_for(done.wait(), timeout=30.0)
    finally:
        await bridge.close()


if __name__ == '__main__':
    asyncio.run(_main(sys.argv[1]))

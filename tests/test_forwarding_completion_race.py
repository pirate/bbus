import asyncio

import pytest

from bubus import BaseEvent, EventBus


class RelayEvent(BaseEvent[str]):
    """Minimal event used for forwarding completion race regression coverage."""


def _dump_bus_state(buses: list[EventBus]) -> str:
    lines: list[str] = []
    for bus in buses:
        queue_size = bus.event_queue.qsize() if bus.event_queue else 0
        lines.append(
            f'{bus.label} queue={queue_size} active={len(bus._active_event_ids)} '
            f'processing={len(bus._processing_event_ids)} history={len(bus.event_history)}'
        )
    for bus in buses:
        lines.append(f'--- {bus.label}.log_tree() ---')
        lines.append(bus.log_tree())
    return '\n'.join(lines)


@pytest.mark.asyncio
async def test_forwarded_event_does_not_leave_stale_active_ids():
    """
    Regression test for the original forwarding completion race:
    an event could be marked completed while another bus still retained its
    event_id in _active_event_ids, causing wait_until_idle() to hang.
    """
    peer1 = EventBus(name='RacePeer1')
    peer2 = EventBus(name='RacePeer2')
    peer3 = EventBus(name='RacePeer3')
    buses = [peer1, peer2, peer3]

    async def local_handler(_event: BaseEvent[str]) -> str:
        return 'ok'

    peer1.on('*', local_handler)
    peer2.on('*', local_handler)
    peer3.on('*', local_handler)

    # Circular forwarding: peer1 -> peer2 -> peer3 -> peer1
    peer1.on('*', peer2.dispatch)
    peer2.on('*', peer3.dispatch)
    peer3.on('*', peer1.dispatch)

    async def wait_all_idle(timeout: float = 5.0) -> None:
        for bus in buses:
            await asyncio.wait_for(bus.wait_until_idle(), timeout=timeout)

    try:
        # Warm-up propagation (this setup made the original bug deterministic on
        # the immediately-following dispatch from peer2).
        peer1.dispatch(RelayEvent())
        await asyncio.sleep(0.2)
        await wait_all_idle()

        second = peer2.dispatch(RelayEvent())
        await asyncio.sleep(0.2)
        try:
            await wait_all_idle()
        except TimeoutError:
            pytest.fail(
                'Forwarding completion race left bus(es) non-idle.\n'
                f'{_dump_bus_state(buses)}'
            )

        assert second.event_status == 'completed'
        for bus in buses:
            assert second.event_id not in bus._active_event_ids
            assert second.event_id not in bus._processing_event_ids

    finally:
        await peer1.stop(clear=True)
        await peer2.stop(clear=True)
        await peer3.stop(clear=True)

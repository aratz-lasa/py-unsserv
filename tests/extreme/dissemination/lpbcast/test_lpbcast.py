import asyncio
from functools import partial
from typing import Any, Dict

import pytest

from tests.utils import init_extreme_membership
from unsserv.common.gossip.config import GossipConfig
from unsserv.common.structs import Node
from unsserv.extreme.dissemination.many_to_many.lpbcast import Lpbcast

init_extreme_membership = init_extreme_membership  # for flake8 compliance
DISSEMINATION_SERVICE_ID = "many_to_many"

lpbcast_events: Dict[Node, asyncio.Event] = {}


async def dissemination_handler(node: Node, data: Any):
    lpbcast_events[node].set()


@pytest.mark.asyncio
@pytest.fixture
async def init_lpbcast():
    lpbcast = None
    r_lpbcasts = []

    async def _init_lpbcast(newc, r_newcs):
        nonlocal lpbcast, r_lpbcasts
        lpbcast = Lpbcast(newc)
        lpbcast_events[newc.my_node] = asyncio.Event()
        await lpbcast.join(
            DISSEMINATION_SERVICE_ID,
            broadcast_handler=partial(dissemination_handler, lpbcast.my_node),
        )
        for r_newc in r_newcs:
            r_lpbcast = Lpbcast(r_newc)
            lpbcast_events[r_newc.my_node] = asyncio.Event()
            await r_lpbcast.join(
                DISSEMINATION_SERVICE_ID,
                broadcast_handler=partial(dissemination_handler, r_lpbcast.my_node),
            )
            r_lpbcasts.append(r_lpbcast)
        await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 7)
        return lpbcast, r_lpbcasts

    try:
        yield _init_lpbcast
    finally:
        await lpbcast.leave()
        for r_lpbcast in r_lpbcasts:
            await r_lpbcast.leave()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_start_stop(init_extreme_membership, init_lpbcast, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    lpbcast, r_lpbcasts = await init_lpbcast(newc, r_newcs)


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_broadcast(init_extreme_membership, init_lpbcast, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    lpbcast, r_lpbcasts = await init_lpbcast(newc, r_newcs)

    data = b"data"
    await lpbcast.broadcast(data)
    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 15)

    lpbcast_events_received = [
        lpbcast_events[r_lpbcast.my_node].is_set() for r_lpbcast in r_lpbcasts
    ]
    assert int(amount * 0.75) <= sum(lpbcast_events_received)

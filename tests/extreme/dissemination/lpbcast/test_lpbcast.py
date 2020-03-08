import asyncio
from functools import partial
from typing import Any, Dict

import pytest

from tests.utils import init_extreme_membership
from unsserv.common.data_structures import Node
from unsserv.common.gossip.gossip_config import GOSSIPING_FREQUENCY
from unsserv.extreme.dissemination.lpbcast.lpbcast import Lpbcast

init_extreme_membership = init_extreme_membership  # for flake8 compliance
DISS_SERVICE_ID = "lpbcast"

lpbcast_events: Dict[Node, asyncio.Event] = {}


async def dissemination_handler(node: Node, data: Any):
    lpbcast_events[node].set()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_start_stop(init_extreme_membership, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    lpbcast = Lpbcast(newc)
    await lpbcast.join_broadcast(
        DISS_SERVICE_ID, partial(dissemination_handler, lpbcast.my_node)
    )
    r_lpbcasts = []
    for r_newc in r_newcs:
        r_lpbcast = Lpbcast(r_newc)
        await r_lpbcast.join_broadcast(
            DISS_SERVICE_ID, partial(dissemination_handler, r_lpbcast.my_node)
        )
        r_lpbcasts.append(r_lpbcast)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 15)

    await lpbcast.leave_broadcast()
    for r_lpbcast in r_lpbcasts:
        await r_lpbcast.leave_broadcast()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_broadcast(init_extreme_membership, amount):
    newc, r_newcs = await init_extreme_membership(amount)

    lpbcast = Lpbcast(newc)
    lpbcast_events[newc.my_node] = asyncio.Event()
    await lpbcast.join_broadcast(
        DISS_SERVICE_ID, partial(dissemination_handler, lpbcast.my_node)
    )
    r_lpbcasts = []
    for r_newc in r_newcs:
        r_lpbcast = Lpbcast(r_newc)
        lpbcast_events[r_newc.my_node] = asyncio.Event()
        await r_lpbcast.join_broadcast(
            DISS_SERVICE_ID, partial(dissemination_handler, r_lpbcast.my_node)
        )
        r_lpbcasts.append(r_lpbcast)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 15)
    data = b"data"
    await lpbcast.broadcast(data)
    await asyncio.sleep(GOSSIPING_FREQUENCY * 15)

    lpbcast_events_received = [
        lpbcast_events[r_lpbcast.my_node].is_set() for r_lpbcast in r_lpbcasts
    ]
    assert int(amount * 0.75) <= sum(lpbcast_events_received)

    await lpbcast.leave_broadcast()
    for r_lpbcast in r_lpbcasts:
        await r_lpbcast.leave_broadcast()

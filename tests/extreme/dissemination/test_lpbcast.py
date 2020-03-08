import asyncio
from functools import partial
from typing import Any, Dict

import pytest

from tests.utils import get_random_nodes
from unsserv.common.data_structures import Node
from unsserv.common.gossip.gossip_config import GOSSIPING_FREQUENCY
from unsserv.extreme.dissemination.lpbcast.lpbcast import Lpbcast
from unsserv.extreme.membership import newscast

first_port = 7771
node = Node(("127.0.0.1", first_port))

MEMB_SERVICE_ID = "newscast"
DISS_SERVICE_ID = "lpbcast"

lpbcast_events: Dict[Node, asyncio.Event] = {}


async def init_membership(amount):
    newc = newscast.Newscast(node)
    await newc.join(MEMB_SERVICE_ID)
    lpbcast_events[newc.my_node] = asyncio.Event()

    r_newcs = []
    r_nodes = get_random_nodes(amount, first_port=first_port + 1)
    for i, r_node in enumerate(r_nodes):
        r_newc = newscast.Newscast(r_node)
        await r_newc.join(MEMB_SERVICE_ID, [node] + r_nodes[:i])
        r_newcs.append(r_newc)
        lpbcast_events[r_node] = asyncio.Event()
    await asyncio.sleep(GOSSIPING_FREQUENCY * 7)
    return newc, r_newcs


async def dissemination_handler(node: Node, data: Any):
    lpbcast_events[node].set()


@pytest.mark.asyncio
async def test_start_stop():
    neighbour_amounts = [1, 5, 100]
    for amount in neighbour_amounts:
        await start_stop(amount)


async def start_stop(amount):
    newc, r_newcs = await init_membership(amount)
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
    await newc.leave()
    for r_newc in r_newcs:
        await r_newc.leave()


@pytest.mark.asyncio
async def test_broadcast():
    neighbour_amounts = [1, 5, 100]
    for amount in neighbour_amounts:
        await broadcast(amount)


async def broadcast(amount):
    newc, r_newcs = await init_membership(amount)

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
    await newc.leave()
    for r_newc in r_newcs:
        await r_newc.leave()

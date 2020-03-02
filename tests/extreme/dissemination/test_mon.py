import asyncio
from functools import partial
from typing import Any, Dict

import pytest

from tests.utils import get_random_nodes
from unsserv.common.data_structures import Node
from unsserv.common.gossip.gossip_config import GOSSIPING_FREQUENCY
from unsserv.extreme.dissemination.mon.mon import Mon
from unsserv.extreme.membership import newscast

first_port = 7771
node = Node(("127.0.0.1", first_port))

MEMB_SERVICE_ID = "newscast"
DISS_SERVICE_ID = "mon"

mon_events: Dict[Node, asyncio.Event] = {}


async def init_membership(amount):
    newc = newscast.Newscast(node)
    await newc.join(MEMB_SERVICE_ID)
    mon_events[newc.my_node] = asyncio.Event()

    r_newcs = []
    r_nodes = get_random_nodes(amount, first_port=first_port + 1)
    for r_node in r_nodes:
        r_newc = newscast.Newscast(r_node)
        await r_newc.join(MEMB_SERVICE_ID, [node])
        r_newcs.append(r_newc)
        mon_events[r_node] = asyncio.Event()
    await asyncio.sleep(GOSSIPING_FREQUENCY * 7)
    return newc, r_newcs


async def dissemination_handler(node: Node, data: Any):
    mon_events[node].set()


@pytest.mark.asyncio
async def test_start_stop():
    neighbour_amounts = [1, 2, 5, 10, 30, 100]
    for amount in neighbour_amounts:
        await start_stop(amount)


async def start_stop(amount):
    newc, r_newcs = await init_membership(amount)

    mon = Mon(newc)
    await mon.join_broadcast(
        DISS_SERVICE_ID, partial(dissemination_handler, mon.my_node)
    )
    r_mons = []
    for r_newc in r_newcs:
        r_mon = Mon(r_newc)
        await r_mon.join_broadcast(
            DISS_SERVICE_ID, partial(dissemination_handler, r_mon.my_node)
        )
        r_mons.append(r_mon)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 15)

    await mon.leave_broadcast()
    for r_mon in r_mons:
        await r_mon.leave_broadcast()
    await newc.leave()
    for r_newc in r_newcs:
        await r_newc.leave()


@pytest.mark.asyncio
async def test_broadcast():
    neighbour_amounts = [1, 2, 5, 10, 30, 100]
    for amount in neighbour_amounts:
        await broadcast(amount)


async def broadcast(amount):
    newc, r_newcs = await init_membership(amount)

    mon = Mon(newc)
    await mon.join_broadcast(
        DISS_SERVICE_ID, partial(dissemination_handler, mon.my_node)
    )
    r_mons = []
    for r_newc in r_newcs:
        r_mon = Mon(r_newc)
        await r_mon.join_broadcast(
            DISS_SERVICE_ID, partial(dissemination_handler, r_mon.my_node)
        )
        r_mons.append(r_mon)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 15)
    data = ""
    await mon.broadcast(data)
    for r_mon in r_mons:
        assert mon_events[r_mon.my_node].is_set()  # check broadcast was received

    await mon.leave_broadcast()
    for r_mon in r_mons:
        await r_mon.leave_broadcast()
    await newc.leave()
    for r_newc in r_newcs:
        await r_newc.leave()

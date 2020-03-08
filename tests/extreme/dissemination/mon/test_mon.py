import asyncio
from functools import partial
from typing import Any, Dict

import pytest

from unsserv.common.data_structures import Node
from unsserv.common.gossip.gossip_config import GOSSIPING_FREQUENCY
from unsserv.extreme.dissemination.mon.mon import Mon
from tests.utils import init_extreme_membership

init_extreme_membership = init_extreme_membership  # for flake8 compliance

DISS_SERVICE_ID = "mon"

mon_events: Dict[Node, asyncio.Event] = {}


async def dissemination_handler(node: Node, data: Any):
    mon_events[node].set()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_start_stop(init_extreme_membership, amount):
    newc, r_newcs = await init_extreme_membership(amount)
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


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_broadcast(init_extreme_membership, amount):
    newc, r_newcs = await init_extreme_membership(amount)

    mon = Mon(newc)
    mon_events[newc.my_node] = asyncio.Event()
    await mon.join_broadcast(
        DISS_SERVICE_ID, partial(dissemination_handler, mon.my_node)
    )

    r_mons = []
    for r_newc in r_newcs:
        r_mon = Mon(r_newc)
        mon_events[r_newc.my_node] = asyncio.Event()
        await r_mon.join_broadcast(
            DISS_SERVICE_ID, partial(dissemination_handler, r_mon.my_node)
        )
        r_mons.append(r_mon)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 15)
    data = b"data"
    await mon.broadcast(data)
    await asyncio.sleep(GOSSIPING_FREQUENCY * 15)

    mon_events_received = [mon_events[r_mon.my_node].is_set() for r_mon in r_mons]
    assert int(amount * 0.75) <= sum(mon_events_received)

    await mon.leave_broadcast()
    for r_mon in r_mons:
        await r_mon.leave_broadcast()

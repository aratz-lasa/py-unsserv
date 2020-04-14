import asyncio
from functools import partial
from typing import Any, Dict

import pytest

from tests.utils import init_extreme_membership
from unsserv.common.gossip.config import GossipConfig
from unsserv.common.structs import Node
from unsserv.extreme.dissemination.mon.mon import Mon

init_extreme_membership = init_extreme_membership  # for flake8 compliance

DISSEMINATION_SERVICE_ID = "mon"

mon_events: Dict[Node, asyncio.Event] = {}


async def dissemination_handler(node: Node, data: Any):
    mon_events[node].set()


@pytest.mark.asyncio
@pytest.fixture
async def init_mon():
    mon = None
    r_mons = []

    async def _init_mon(newc, r_newcs):
        nonlocal mon, r_mons
        mon = Mon(newc)
        mon_events[newc.my_node] = asyncio.Event()
        await mon.join(
            DISSEMINATION_SERVICE_ID,
            broadcast_handler=partial(dissemination_handler, mon.my_node),
        )
        for r_newc in r_newcs:
            r_mon = Mon(r_newc)
            mon_events[r_newc.my_node] = asyncio.Event()
            await r_mon.join(
                DISSEMINATION_SERVICE_ID,
                broadcast_handler=partial(dissemination_handler, r_mon.my_node),
            )
            r_mons.append(r_mon)
        await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 7)
        return mon, r_mons

    try:
        yield _init_mon
    finally:
        await mon.leave()
        for r_mon in r_mons:
            await r_mon.leave()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_start_stop(init_extreme_membership, init_mon, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    mon, r_mons = await init_mon(newc, r_newcs)


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_broadcast(init_extreme_membership, init_mon, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    mon, r_mons = await init_mon(newc, r_newcs)

    data = b"data"
    await mon.broadcast(data)
    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 15)

    mon_events_received = [mon_events[r_mon.my_node].is_set() for r_mon in r_mons]
    assert int(amount * 0.75) <= sum(mon_events_received)

    await mon.leave()
    for r_mon in r_mons:
        await r_mon.leave()

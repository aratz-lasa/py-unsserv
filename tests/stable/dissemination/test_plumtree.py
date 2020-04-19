import asyncio
from functools import partial
from typing import Any, Dict

import pytest

from tests.utils import init_stable_membership
from unsserv.common.gossip.config import GossipConfig
from unsserv.common.structs import Node
from unsserv.stable.dissemination.plumtree.plumtree import Plumtree

init_stable_membership = init_stable_membership  # for flake8 compliance

DISSEMINATION_SERVICE_ID = "plumtree"

dissemination_data = b"data"
plum_events: Dict[Node, asyncio.Event] = {}


async def dissemination_handler(node: Node, data: Any):
    assert data == dissemination_data
    plum_events[node].set()


@pytest.mark.asyncio
@pytest.fixture
async def init_plum():
    plum = None
    r_plums = []

    async def _init_plum(hypa, r_hypas):
        nonlocal plum, r_plums
        plum = Plumtree(hypa)
        plum_events[hypa.my_node] = asyncio.Event()
        await plum.join(
            DISSEMINATION_SERVICE_ID,
            broadcast_handler=partial(dissemination_handler, plum.my_node),
        )
        for r_hypa in r_hypas:
            r_plum = Plumtree(r_hypa)
            plum_events[r_hypa.my_node] = asyncio.Event()
            await r_plum.join(
                DISSEMINATION_SERVICE_ID,
                broadcast_handler=partial(dissemination_handler, r_plum.my_node),
            )
            r_plums.append(r_plum)
        await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 7)
        return plum, r_plums

    try:
        yield _init_plum
    finally:
        await plum.leave()
        for r_plum in r_plums:
            await r_plum.leave()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_start_stop(init_stable_membership, init_plum, amount):
    hypa, r_hypas = await init_stable_membership(amount)
    plum, r_plums = await init_plum(hypa, r_hypas)


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_broadcast(init_stable_membership, init_plum, amount):
    hypa, r_hypas = await init_stable_membership(amount)
    plum, r_plums = await init_plum(hypa, r_hypas)

    await plum.broadcast(dissemination_data)
    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 15)

    plum_events_received = [plum_events[r_plum.my_node].is_set() for r_plum in r_plums]
    assert int(amount * 0.75) <= sum(plum_events_received)

    await plum.leave()
    for r_plum in r_plums:
        await r_plum.leave()

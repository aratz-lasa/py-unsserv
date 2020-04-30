import asyncio
from functools import partial
from typing import Any, Dict

import pytest

from tests.utils import init_stable_membership
from unsserv.common.gossip.config import GossipConfig
from unsserv.common.structs import Node
from unsserv.stable.dissemination.one_to_many.brisa import Brisa

init_stable_membership = init_stable_membership  # for flake8 compliance

DISSEMINATION_SERVICE_ID = "one_to_many"

dissemination_data = b"data"
brisa_events: Dict[Node, asyncio.Event] = {}


async def dissemination_handler(node: Node, data: Any):
    assert data == dissemination_data
    brisa_events[node].set()


@pytest.mark.asyncio
@pytest.fixture
async def init_brisa():
    brisa = None
    r_brisas = []

    async def _init_brisa(hypa, r_hypas):
        nonlocal brisa, r_brisas
        brisa = Brisa(hypa)
        brisa_events[hypa.my_node] = asyncio.Event()
        await brisa.join(
            DISSEMINATION_SERVICE_ID,
            broadcast_handler=partial(dissemination_handler, brisa.my_node),
            im_root=True,
        )
        for r_hypa in r_hypas:
            r_brisa = Brisa(r_hypa)
            brisa_events[r_hypa.my_node] = asyncio.Event()
            await r_brisa.join(
                DISSEMINATION_SERVICE_ID,
                broadcast_handler=partial(dissemination_handler, r_brisa.my_node),
            )
            r_brisas.append(r_brisa)
        await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 7)
        return brisa, r_brisas

    try:
        yield _init_brisa
    finally:
        await brisa.leave()
        for r_brisa in r_brisas:
            await r_brisa.leave()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_start_stop(init_stable_membership, init_brisa, amount):
    hypa, r_hypas = await init_stable_membership(amount)
    brisa, r_brisas = await init_brisa(hypa, r_hypas)


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_broadcast(init_stable_membership, init_brisa, amount):
    hypa, r_hypas = await init_stable_membership(amount)
    brisa, r_brisas = await init_brisa(hypa, r_hypas)

    await brisa.broadcast(dissemination_data)
    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 15)

    brisa_events_received = [
        brisa_events[r_brisa.my_node].is_set() for r_brisa in r_brisas
    ]
    assert int(amount * 0.75) <= sum(brisa_events_received)

    await brisa.leave()
    for r_brisa in r_brisas:
        await r_brisa.leave()

import asyncio

import pytest

from tests.utils import get_random_nodes
from unsserv.common.data_structures import Node
from unsserv.common.gossip.gossip_config import GOSSIPING_FREQUENCY
from unsserv.extreme.membership import newscast
from unsserv.extreme.sampling.mrwb import MRWB

first_port = 7771
node = Node(("127.0.0.1", first_port))

MEMB_SERVICE_ID = "newscast"
AGGR_SERVICE_ID = "mrwb"


async def init_membership(amount):
    newc = newscast.Newscast(node)
    await newc.join(MEMB_SERVICE_ID)

    r_newcs = []
    r_nodes = get_random_nodes(amount, first_port=first_port + 1)
    for i, r_node in enumerate(r_nodes):
        r_newc = newscast.Newscast(r_node)
        await r_newc.join(MEMB_SERVICE_ID, [node] + r_nodes[:i])
        r_newcs.append(r_newc)
    await asyncio.sleep(GOSSIPING_FREQUENCY * 7)
    return newc, r_newcs


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_start_stop(amount):
    newc, r_newcs = await init_membership(amount)

    sampl = MRWB(newc)
    await sampl.join_sampling(AGGR_SERVICE_ID)
    r_sampls = []
    for r_newc in r_newcs:
        r_sampl = MRWB(r_newc)
        await r_sampl.join_sampling(AGGR_SERVICE_ID)
        r_sampls.append(r_sampl)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 10)

    await sampl.leave_sampling()
    for r_sampl in r_sampls:
        await r_sampl.leave_sampling()
    await newc.leave()
    for r_newc in r_newcs:
        await r_newc.leave()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_sampling(amount):
    newc, r_newcs = await init_membership(amount)

    sampl = MRWB(newc)
    await sampl.join_sampling(AGGR_SERVICE_ID)
    r_sampls = []
    for r_newc in r_newcs:
        r_sampl = MRWB(r_newc)
        await r_sampl.join_sampling(AGGR_SERVICE_ID)
        r_sampls.append(r_sampl)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 10)

    samples = {await sampl.get_sample() for _ in range(amount * 2)}
    assert len({r_newc.my_node for r_newc in r_newcs} - samples) / len(r_newcs) <= 0.45

    await sampl.leave_sampling()
    for r_sampl in r_sampls:
        await r_sampl.leave_sampling()
    await newc.leave()
    for r_newc in r_newcs:
        await r_newc.leave()

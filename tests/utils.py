import asyncio

import pytest

from unsserv.common.gossip.config import GossipConfig
from unsserv.common.structs import Node
from unsserv.extreme.membership import newscast
from unsserv.stable.membership import hyparview

first_port = 7771
node = Node(("127.0.0.1", first_port))

NEWSCAST_SERVICE_ID = "newscast"
HYPARVIEW_SERVICE_ID = "hyparview"


def get_random_nodes(amount, first_port=7772, host="127.0.0.1"):
    return list(map(lambda p: Node((host, p)), range(first_port, first_port + amount)))


@pytest.mark.asyncio
@pytest.fixture
async def init_extreme_membership():
    newc = None
    r_newcs = []

    async def get_memberships(amount):
        nonlocal newc, r_newcs
        newc = newscast.Newscast(node)
        await newc.join(NEWSCAST_SERVICE_ID)
        r_nodes = get_random_nodes(amount, first_port=first_port + 1)
        for i, r_node in enumerate(r_nodes):
            r_newc = newscast.Newscast(r_node)
            await r_newc.join(NEWSCAST_SERVICE_ID, bootstrap_nodes=[node] + r_nodes[:i])
            r_newcs.append(r_newc)
        await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 7)
        return newc, r_newcs

    try:
        yield get_memberships
    finally:
        await newc.leave()
        for r_newc in r_newcs:
            await r_newc.leave()


@pytest.mark.asyncio
@pytest.fixture
async def init_stable_membership():
    hypa = None
    r_hypas = []

    async def get_memberships(amount):
        nonlocal hypa, r_hypas
        hypa = hyparview.HyParView(node)
        await hypa.join(HYPARVIEW_SERVICE_ID)
        r_nodes = get_random_nodes(amount, first_port=first_port + 1)
        for i, r_node in enumerate(r_nodes):
            r_hypa = hyparview.HyParView(r_node)
            await r_hypa.join(
                HYPARVIEW_SERVICE_ID, bootstrap_nodes=[node] + r_nodes[:i]
            )
            r_hypas.append(r_hypa)
        await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 7)
        return hypa, r_hypas

    try:
        yield get_memberships
    finally:
        await hypa.leave()
        for r_hypa in r_hypas:
            await r_hypa.leave()

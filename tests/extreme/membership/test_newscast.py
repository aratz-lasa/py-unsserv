import asyncio
from collections import Counter
from math import ceil

import pytest

from tests.utils import get_random_nodes
from unsserv.common.structs import Node
from unsserv.common.gossip.config import GOSSIPING_FREQUENCY, LOCAL_VIEW_SIZE
from unsserv.extreme.membership import newscast

node = Node(("127.0.0.1", 7771))
MEMBERSHIP_SERVICE_ID = "newscast"


@pytest.mark.asyncio
@pytest.fixture
async def init_newscast():
    newc = None
    r_newcs = []

    async def _init_newscast(amount):
        nonlocal newc, r_newcs
        newc = newscast.Newscast(node)
        await newc.join(MEMBERSHIP_SERVICE_ID)
        r_nodes = get_random_nodes(amount)
        for i, r_node in enumerate(r_nodes):
            r_newc = newscast.Newscast(r_node)
            await r_newc.join(
                MEMBERSHIP_SERVICE_ID, bootstrap_nodes=[node] + r_nodes[:i]
            )
            r_newcs.append(r_newc)
        return newc, r_newcs, r_nodes

    try:
        yield _init_newscast
    finally:
        await newc.leave()
        for r_newc in r_newcs:
            await r_newc.leave()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_newscast_join(init_newscast, amount):
    newc, r_newcs, r_nodes = await init_newscast(amount)
    await asyncio.sleep(GOSSIPING_FREQUENCY * 7)

    all_nodes = set(
        [
            item
            for sublist in map(lambda n: n.get_neighbours(), r_newcs + [newc])
            for item in sublist
        ]
    )
    assert amount * 0.9 < len(all_nodes)

    neighbours = newc.get_neighbours()
    assert min(amount, LOCAL_VIEW_SIZE) <= len(neighbours)
    for neighbour in neighbours:
        assert neighbour in r_nodes

    for r_newc in r_newcs:
        r_neighbours = r_newc.get_neighbours()
        assert min(amount, LOCAL_VIEW_SIZE) <= len(r_neighbours)
        for r_neighbour in r_neighbours:
            assert r_neighbour in r_nodes or r_neighbour == node


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount", [LOCAL_VIEW_SIZE + 1, LOCAL_VIEW_SIZE + 5, LOCAL_VIEW_SIZE + 100]
)
async def test_newscast_leave(init_newscast, amount):
    newc, r_newcs, r_nodes = await init_newscast(amount)
    await asyncio.sleep(GOSSIPING_FREQUENCY * 7)

    await newc.leave()
    await asyncio.sleep(GOSSIPING_FREQUENCY * 40)

    all_nodes = Counter(
        [
            item
            for sublist in map(lambda n: n.get_neighbours(), r_newcs)
            for item in sublist
        ]
    )
    nodes_ten_percent = ceil(amount * 0.2)
    assert node not in all_nodes.keys() or node in set(
        map(lambda p: p[0], all_nodes.most_common()[-nodes_ten_percent:])
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount",
    [(LOCAL_VIEW_SIZE * 2) + 1, (LOCAL_VIEW_SIZE * 2) + 5, (LOCAL_VIEW_SIZE * 2) + 100],
)
async def test_newscast_callback(init_newscast, amount):
    newc, r_newcs, r_nodes = await init_newscast(amount)

    callback_event = asyncio.Event()

    async def callback(neighbours):
        nonlocal callback_event
        callback_event.set()

    newc.add_neighbours_handler(callback)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 7)
    assert callback_event.is_set()

import asyncio
from collections import Counter

import pytest
from tests.utils import get_random_nodes
from unsserv.common.data_structures import Node
from unsserv.common.gossip.gossip_config import GOSSIPING_FREQUENCY, LOCAL_VIEW_SIZE
from unsserv.extreme.membership import newscast

node = Node(("127.0.0.1", 7771))
SERVICE_ID = "newscast"


@pytest.mark.asyncio
async def test_newscast_join():
    neighbour_amounts = [1, 2, 5, 10, 30, 100]
    for amount in neighbour_amounts:
        await newscast_join(amount)


async def newscast_join(neighbours_amount):
    newc = newscast.Newscast(node)
    await newc.join(SERVICE_ID)

    r_newcs = []
    r_nodes = get_random_nodes(neighbours_amount)
    for r_node in r_nodes:
        r_newc = newscast.Newscast(r_node)
        await r_newc.join(SERVICE_ID, [node])
        r_newcs.append(r_newc)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 7)

    all_nodes = set(
        [
            item
            for sublist in map(lambda n: n.get_neighbours(), r_newcs + [newc])
            for item in sublist
        ]
    )
    assert neighbours_amount * 0.9 < len(all_nodes)

    neighbours = newc.get_neighbours()
    assert min(neighbours_amount, LOCAL_VIEW_SIZE) <= len(neighbours)
    for neighbour in neighbours:
        assert neighbour in r_nodes

    for r_newc in r_newcs:
        r_neighbours = r_newc.get_neighbours()
        assert min(neighbours_amount, LOCAL_VIEW_SIZE) <= len(r_neighbours)
        for r_neighbour in r_neighbours:
            assert r_neighbour in r_nodes or r_neighbour == node

    await newc.leave()
    for r_newc in r_newcs:
        await r_newc.leave()


@pytest.mark.asyncio
async def test_newscast_leave():
    neighbour_amounts = [
        LOCAL_VIEW_SIZE + 1,
        LOCAL_VIEW_SIZE + 2,
        LOCAL_VIEW_SIZE + 5,
        LOCAL_VIEW_SIZE + 10,
        LOCAL_VIEW_SIZE + 30,
        LOCAL_VIEW_SIZE + 100,
    ]
    for amount in neighbour_amounts:
        await newscast_leave(amount)


async def newscast_leave(neighbours_amount):
    newc = newscast.Newscast(node)
    await newc.join(SERVICE_ID)

    r_newcs = []
    r_nodes = get_random_nodes(neighbours_amount)
    for i, r_node in enumerate(r_nodes):
        r_newc = newscast.Newscast(r_node)
        await r_newc.join(SERVICE_ID, r_nodes[:i] or [node])
        r_newcs.append(r_newc)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 5)
    await newc.leave()
    await asyncio.sleep(GOSSIPING_FREQUENCY * 25)

    all_nodes = Counter(
        [
            item
            for sublist in map(lambda n: n.get_neighbours(), r_newcs)
            for item in sublist
        ]
    )
    nodes_ten_percent = int(neighbours_amount * 0.1)
    assert node not in all_nodes.keys() or node in set(
        map(lambda p: p[0], all_nodes.most_common()[-nodes_ten_percent:])
    )

    # clean up
    await newc.leave()
    for r_newc in r_newcs:
        await r_newc.leave()


@pytest.mark.asyncio
async def test_newscast_callback():
    neighbour_amounts = [1, 2, 5, 10, 30, 100]
    for amount in neighbour_amounts:
        await newscast_callback(amount)


async def newscast_callback(neighbours_amount):
    callback_event = asyncio.Event()

    newc = newscast.Newscast(node)
    await newc.join(SERVICE_ID)

    async def callback(neighbours):
        nonlocal callback_event
        callback_event.set()

    newc.set_neighbours_callback(callback)

    r_newcs = []
    r_nodes = get_random_nodes(neighbours_amount)
    for r_node in r_nodes:
        r_newc = newscast.Newscast(r_node)
        await r_newc.join(SERVICE_ID, [node])
        r_newcs.append(r_newc)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 5)
    assert callback_event.is_set()

    # clean up
    await newc.leave()
    for r_newc in r_newcs:
        await r_newc.leave()

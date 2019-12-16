import itertools
import time

import pytest

from tests.utils import get_random_nodes
from unsserv.data_structures import Node
from unsserv.extreme.membership import newscast

WAITING_TIME = 0.1  # todo: set a proper waiting time depending on gossiping frequency

node = Node("127.0.0.1", 7771)


@pytest.mark.asyncio
async def test_newcast_join():
    neighbour_amounts = [1, 2, 5, 10, 30, 100]
    for amount in neighbour_amounts:
        await newcast_join(amount)


async def newcast_join(neighbours_amount):
    newc = newscast.Newscast(node)
    await newc.join_membership()

    r_newcs = []
    r_nodes = get_random_nodes(neighbours_amount)
    for r_node in r_nodes:
        r_newc = newscast.Newscast(r_node)
        await r_newc.join_membership([node])
        r_newcs.append(r_newc)

    time.sleep(WAITING_TIME)

    all_nodes = set(itertools.chain(map(lambda n: n.get_neighbours(), r_newcs)))
    assert len(all_nodes) == neighbours_amount + 1  # must count 'node' too

    neighbours = newc.get_neighbours()
    assert 1 < len(neighbours)
    for neighbour in neighbours:
        assert neighbour in r_nodes

    for r_newc in r_newcs:
        r_neighbours = r_newc.get_neighbours()
        assert 1 < len(r_neighbours)
        for r_neighbour in r_neighbours:
            assert r_neighbour in r_nodes or r_neighbour == node


@pytest.mark.asyncio
async def test_newcast_leave():
    neighbour_amounts = [1, 2, 5, 10, 30, 100]
    for amount in neighbour_amounts:
        await newcast_leave(amount)


async def newcast_leave(neighbours_amount):
    newc = newscast.Newscast(node)
    await newc.join_membership()

    r_newcs = []
    r_nodes = get_random_nodes(neighbours_amount)
    for r_node in r_nodes:
        r_newc = newscast.Newscast(r_node)
        await r_newc.join_membership([node])
        r_newcs.append(r_newc)

    time.sleep(WAITING_TIME)
    await newc.leave_membership()
    time.sleep(WAITING_TIME)

    all_nodes = set(itertools.chain(map(lambda n: n.get_neighbours(), r_newcs)))
    assert len(all_nodes) == neighbours_amount
    assert node not in all_nodes


@pytest.mark.asyncio
async def test_newcast_callback():
    neighbour_amounts = [1, 2, 5, 10, 30, 100]
    for amount in neighbour_amounts:
        await newcast_callback(amount)


async def newcast_callback(neighbours_amount):
    newc = newscast.Newscast(node)
    await newc.join_membership()

    callback_amount = 0

    async def callback(neighbours):
        global callback_amount
        callback_amount += 1

    newc.set_neighbours_callback(callback)

    r_newcs = []
    r_nodes = get_random_nodes(neighbours_amount)
    for r_node in r_nodes:
        r_newc = newscast.Newscast(r_node)
        await r_newc.join_membership([node])
        r_newcs.append(r_newc)

    time.sleep(WAITING_TIME)
    assert neighbours_amount <= callback_amount

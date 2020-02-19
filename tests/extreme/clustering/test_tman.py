from collections import Counter
import asyncio

import pytest

from tests.utils import get_random_nodes
from unsserv.common.gossip.config import GOSSIPING_FREQUENCY, LOCAL_VIEW_SIZE
from unsserv.common.utils.data_structures import Node
from unsserv.extreme.clustering.t_man import TMan
from unsserv.extreme.membership import newscast

node = Node(("127.0.0.1", 7771))
M_SERVICE_ID = "newscast"
C_SERVICE_ID = "tman"


def port_distance(ranked_node: Node):
    return abs(node.address_info[1] - ranked_node.address_info[1])


@pytest.mark.asyncio
async def test_join_tman():
    neighbour_amounts = [1, 2, 5, 10, 30, 100]
    for amount in neighbour_amounts:
        await join_tman(amount)


async def init_membership(amount):
    newc = newscast.Newscast(node)
    await newc.join(M_SERVICE_ID)

    r_newcs = []
    r_nodes = get_random_nodes(amount)
    for r_node in r_nodes:
        r_newc = newscast.Newscast(r_node)
        await r_newc.join(M_SERVICE_ID, [node])
        r_newcs.append(r_newc)
    await asyncio.sleep(GOSSIPING_FREQUENCY * 7)
    return newc, r_newcs


async def join_tman(amount):
    newc, r_newcs = await init_membership(amount)

    tman = TMan(newc)
    await tman.join(C_SERVICE_ID, port_distance)
    r_tmans = []
    for r_newc in r_newcs:
        r_tman = TMan(r_newc)
        await r_tman.join(C_SERVICE_ID, port_distance)
        r_tmans.append(r_tman)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 25)

    neighbours_set = set(tman.get_neighbours())
    ideal_neighbours_set = set(
        sorted(map(lambda nc: nc.my_node, r_newcs), key=port_distance)[:LOCAL_VIEW_SIZE]
    )
    assert min(amount, LOCAL_VIEW_SIZE) * 0.65 < len(
        ideal_neighbours_set.intersection(neighbours_set)
    )

    # clean up
    await tman.leave()
    for r_tman in r_tmans:
        await r_tman.leave()

    await newc.leave()
    for r_newc in r_newcs:
        await r_newc.leave()


@pytest.mark.asyncio
async def test_leave_tman():
    neighbour_amounts = [1, 2, 5, 10, 30, 100]
    for amount in neighbour_amounts:
        await leave_tman(amount)


async def leave_tman(amount):
    newc, r_newcs = await init_membership(amount)

    tman = TMan(newc)
    await tman.join(C_SERVICE_ID, port_distance)
    r_tmans = []
    for r_newc in r_newcs:
        r_tman = TMan(r_newc)
        await r_tman.join(C_SERVICE_ID, port_distance)
        r_tmans.append(r_tman)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 7)
    await tman.leave()
    await asyncio.sleep(GOSSIPING_FREQUENCY * 25)

    all_nodes = Counter(
        [
            item
            for sublist in map(lambda n: n.get_neighbours(), r_tmans)
            for item in sublist
        ]
    )
    nodes_ten_percent = int(amount * 0.1)
    assert node not in all_nodes.keys() or node in set(
        map(lambda p: p[0], all_nodes.most_common()[-nodes_ten_percent:])
    )

    # clean up
    await tman.leave()
    for r_tman in r_tmans:
        await r_tman.leave()

    await newc.leave()
    for r_newc in r_newcs:
        await r_newc.leave()


async def newscast_callback(neighbours_amount):
    callback_event = asyncio.Event()

    async def callback(local_view):
        assert isinstance(local_view, Counter)
        nonlocal callback_event
        callback_event.set()

    newc, r_newcs = await init_membership(neighbours_amount)

    tman = TMan(newc)
    await tman.join(C_SERVICE_ID, port_distance)
    tman.set_neighbours_callback(callback, local_view=True)
    r_tmans = []
    for r_newc in r_newcs:
        r_tman = TMan(r_newc)
        await r_tman.join(C_SERVICE_ID, port_distance)
        r_tmans.append(r_tman)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 5)
    assert callback_event.is_set()

    # clean up
    await tman.leave()
    for r_tman in r_tmans:
        await r_tman.leave()

    await newc.leave()
    for r_newc in r_newcs:
        await r_newc.leave()

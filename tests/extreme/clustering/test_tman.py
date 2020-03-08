import asyncio
from collections import Counter
from math import ceil

import pytest

from functools import partial
from unsserv.common.data_structures import Node
from unsserv.common.gossip.gossip_config import GOSSIPING_FREQUENCY, LOCAL_VIEW_SIZE
from unsserv.extreme.clustering.t_man import TMan
from tests.utils import init_extreme_membership

init_extreme_membership = init_extreme_membership  # for flake8 compliance

C_SERVICE_ID = "tman"


def port_distance(my_node: Node, ranked_node: Node):
    return abs(my_node.address_info[1] - ranked_node.address_info[1])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount", [LOCAL_VIEW_SIZE + 1, LOCAL_VIEW_SIZE + 5, LOCAL_VIEW_SIZE + 100]
)
async def test_join_tman(init_extreme_membership, amount):
    newc, r_newcs = await init_extreme_membership(amount)

    tman = TMan(newc)
    await tman.join(C_SERVICE_ID, partial(port_distance, tman.my_node))
    r_tmans = []
    for r_newc in r_newcs:
        r_tman = TMan(r_newc)
        await r_tman.join(C_SERVICE_ID, partial(port_distance, r_tman.my_node))
        r_tmans.append(r_tman)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 30)

    neighbours_set = set(tman.get_neighbours())
    ideal_neighbours_set = set(
        sorted(
            map(lambda nc: nc.my_node, r_newcs),
            key=partial(port_distance, tman.my_node),
        )[:LOCAL_VIEW_SIZE]
    )
    assert min(amount, LOCAL_VIEW_SIZE) * 0.5 < len(
        ideal_neighbours_set.intersection(neighbours_set)
    )

    # clean up
    await tman.leave()
    for r_tman in r_tmans:
        await r_tman.leave()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount", [LOCAL_VIEW_SIZE + 1, LOCAL_VIEW_SIZE + 5, LOCAL_VIEW_SIZE + 100]
)
async def test_leave_tman(init_extreme_membership, amount):
    newc, r_newcs = await init_extreme_membership(amount)

    tman = TMan(newc)
    await tman.join(C_SERVICE_ID, partial(port_distance, tman.my_node))
    r_tmans = []
    for r_newc in r_newcs:
        r_tman = TMan(r_newc)
        await r_tman.join(C_SERVICE_ID, partial(port_distance, r_tman.my_node))
        r_tmans.append(r_tman)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 7)
    await tman.leave()
    await newc.leave()
    await asyncio.sleep(GOSSIPING_FREQUENCY * 30)

    all_nodes = Counter(
        [
            item
            for sublist in map(lambda n: n.get_neighbours(), r_tmans)
            for item in sublist
        ]
    )
    nodes_ten_percent = ceil(amount * 0.1)
    assert newc.my_node not in all_nodes.keys() or newc.my_node in set(
        map(lambda p: p[0], all_nodes.most_common()[-nodes_ten_percent:])
    )

    # clean up
    await tman.leave()
    for r_tman in r_tmans:
        await r_tman.leave()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount",
    [(LOCAL_VIEW_SIZE * 2) + 1, (LOCAL_VIEW_SIZE * 2) + 5, (LOCAL_VIEW_SIZE * 2) + 100],
)  # very high neighbours amount,
# to assure neighbours will change, because it is initailzied by Newscast
async def test_tman_callback(init_extreme_membership, amount):
    callback_event = asyncio.Event()

    async def callback(local_view):
        assert isinstance(local_view, Counter)
        nonlocal callback_event
        callback_event.set()

    newc, r_newcs = await init_extreme_membership(amount)

    tman = TMan(newc)
    await tman.join(C_SERVICE_ID, partial(port_distance, tman.my_node))
    tman.set_neighbours_callback(callback, local_view=True)
    r_tmans = []
    for r_newc in r_newcs:
        r_tman = TMan(r_newc)
        await r_tman.join(C_SERVICE_ID, partial(port_distance, r_tman.my_node))
        r_tmans.append(r_tman)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 5)
    assert callback_event.is_set()

    # clean up
    await tman.leave()
    for r_tman in r_tmans:
        await r_tman.leave()

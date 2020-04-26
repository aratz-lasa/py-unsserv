import asyncio
from collections import Counter
from functools import partial
from math import ceil

import pytest

from tests.utils import init_extreme_membership
from unsserv.common.gossip.config import GossipConfig
from unsserv.common.structs import Node
from unsserv.extreme.clustering.t_man import TMan

init_extreme_membership = init_extreme_membership  # for flake8 compliance

CLUSTERING_SERVICE_ID = "tman"


@pytest.mark.asyncio
@pytest.fixture
async def init_tman():
    tman = None
    r_tmans = []

    async def _init_tman(newc, r_newcs):
        nonlocal tman, r_tmans
        tman = TMan(newc)
        await tman.join(
            CLUSTERING_SERVICE_ID, ranking_function=partial(port_distance, tman.my_node)
        )
        for r_newc in r_newcs:
            r_tman = TMan(r_newc)
            await r_tman.join(
                CLUSTERING_SERVICE_ID,
                ranking_function=partial(port_distance, r_tman.my_node),
            )
            r_tmans.append(r_tman)
        return tman, r_tmans

    try:
        yield _init_tman
    finally:
        await tman.leave()
        for r_tman in r_tmans:
            await r_tman.leave()


def port_distance(my_node: Node, ranked_node: Node):
    return abs(my_node.address_info[1] - ranked_node.address_info[1])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount",
    [GossipConfig.LOCAL_VIEW_SIZE * 2 + 1, GossipConfig.LOCAL_VIEW_SIZE * 2 + 5, 100],
)
async def test_join_tman(init_extreme_membership, init_tman, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    tman, r_tmans = await init_tman(newc, r_newcs)
    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 7)

    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 40)

    cluster_nodes = [tman] + r_tmans
    satisfy_ideal_neighbours = []
    for cluster in cluster_nodes:
        neighbours = set(cluster.get_neighbours())
        key_function = partial(port_distance, cluster.my_node)
        ideal_neighbours = set(
            sorted(map(lambda c_n: c_n.my_node, cluster_nodes), key=key_function)[
                1 : GossipConfig.LOCAL_VIEW_SIZE + 1
            ]
        )
        satisfies_half_ideal_neighbours = min(
            amount, GossipConfig.LOCAL_VIEW_SIZE
        ) * 0.5 <= len(ideal_neighbours.intersection(neighbours))
        satisfy_ideal_neighbours.append(satisfies_half_ideal_neighbours)
    assert sum(satisfy_ideal_neighbours) / (amount + 1) >= 0.5


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount",
    [
        GossipConfig.LOCAL_VIEW_SIZE + 1,
        GossipConfig.LOCAL_VIEW_SIZE + 5,
        GossipConfig.LOCAL_VIEW_SIZE + 100,
    ],
)
async def test_leave_tman(init_extreme_membership, init_tman, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    tman, r_tmans = await init_tman(newc, r_newcs)
    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 7)

    await tman.leave()
    await newc.leave()
    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 40)

    all_nodes = Counter(
        [
            item
            for sublist in map(lambda n: n.get_neighbours(), r_tmans)
            for item in sublist
        ]
    )
    nodes_ten_percent = ceil(amount * 0.2)
    assert newc.my_node not in all_nodes.keys() or newc.my_node in set(
        map(lambda p: p[0], all_nodes.most_common()[-nodes_ten_percent:])
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount",
    [
        (GossipConfig.LOCAL_VIEW_SIZE * 2) + 1,
        (GossipConfig.LOCAL_VIEW_SIZE * 2) + 5,
        (GossipConfig.LOCAL_VIEW_SIZE * 2) + 100,
    ],
)  # very high neighbours amount,
# to assure neighbours will change, because it is initailzied by Newscast
async def test_tman_handler(init_extreme_membership, init_tman, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    tman, r_tmans = await init_tman(newc, r_newcs)

    handler_event = asyncio.Event()

    async def handler(local_view):
        assert isinstance(local_view, list)
        nonlocal handler_event
        handler_event.set()

    tman.add_neighbours_handler(handler)

    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 15)
    assert handler_event.is_set()

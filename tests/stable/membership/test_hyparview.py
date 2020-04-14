import asyncio
from collections import Counter
from math import ceil

import pytest

from tests.utils import get_random_nodes
from unsserv.common.gossip.config import GossipConfig
from unsserv.common.structs import Node
from unsserv.stable.membership.hyparview import HyParView

MEMBERSHIP_SERVICE_ID = "hyparview"
node = Node(("127.0.0.1", 7771))


@pytest.mark.asyncio
@pytest.fixture
async def init_hyparview():
    hyparview = None
    r_hyparviews = []

    async def _init_hyparview(amount):
        nonlocal hyparview, r_hyparviews
        hyparview = HyParView(node)
        await hyparview.join(MEMBERSHIP_SERVICE_ID)
        r_nodes = get_random_nodes(amount)
        for i, r_node in enumerate(r_nodes):
            r_hyparview = HyParView(r_node)
            await r_hyparview.join(
                MEMBERSHIP_SERVICE_ID, bootstrap_nodes=[node] + r_nodes[:i]
            )
            r_hyparviews.append(r_hyparview)
        return hyparview, r_hyparviews

    try:
        yield _init_hyparview
    finally:
        await hyparview.leave()
        for r_hyparview in r_hyparviews:
            await r_hyparview.leave()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_join_hyparview(init_hyparview, amount):
    hyparview, r_hyparviews = await init_hyparview(amount)
    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 15)

    all_nodes = set(
        [
            item
            for sublist in map(lambda n: n.get_neighbours(), r_hyparviews + [hyparview])
            for item in sublist
        ]
    )
    assert amount < len(all_nodes)

    all_neighbours = {}
    for r_hyparview in r_hyparviews + [hyparview]:
        all_neighbours[r_hyparview.my_node] = r_hyparview.get_neighbours()
    for node, neighbours in all_neighbours.items():
        for neighbour in neighbours:
            assert node in all_neighbours[neighbour]  # check symmetry


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount",
    [
        GossipConfig.LOCAL_VIEW_SIZE + 1,
        GossipConfig.LOCAL_VIEW_SIZE + 5,
        GossipConfig.LOCAL_VIEW_SIZE + 100,
    ],
)
async def test_leave_hyparview(init_hyparview, amount):
    hyparview, r_hyparviews = await init_hyparview(amount)
    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 7)

    await hyparview.leave()
    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 40)

    all_nodes = Counter(
        [
            item
            for sublist in map(lambda n: n.get_neighbours(), r_hyparviews)
            for item in sublist
        ]
    )
    nodes_ten_percent = ceil(amount * 0.2)
    assert hyparview.my_node not in all_nodes.keys() or hyparview.my_node in set(
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
async def test_hyparview_handler(init_hyparview, amount):
    hyparview, r_hyparviews = await init_hyparview(amount)

    handler_event = asyncio.Event()

    async def handler(local_view):
        assert isinstance(local_view, list)
        nonlocal handler_event
        handler_event.set()

    hyparview.add_neighbours_handler(handler)

    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 15)
    assert handler_event.is_set()

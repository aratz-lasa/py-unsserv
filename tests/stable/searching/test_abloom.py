import asyncio
import random
from math import ceil
from typing import Dict

import pytest

from tests.utils import init_stable_membership
from unsserv.common.gossip.config import GossipConfig
from unsserv.common.structs import Node
from unsserv.common.utils import get_random_id
from unsserv.stable.searching.abloom import ABloom
from unsserv.stable.searching.config import ABloomConfig
from unsserv.stable.searching.structs import Search

init_stable_membership = init_stable_membership  # for flake8 compliance

SAMPLING_SERVICE_ID = "abloom"


@pytest.mark.asyncio
@pytest.fixture
async def init_abloom():
    abloom = None
    r_ablooms = []

    async def _init_abloom(xbot, r_xbots):
        nonlocal abloom, r_ablooms
        abloom = ABloom(xbot)
        await abloom.join(SAMPLING_SERVICE_ID, ttl=2)
        for r_xbot in r_xbots:
            r_abloom = ABloom(r_xbot)
            await r_abloom.join(SAMPLING_SERVICE_ID, ttl=2)
            r_ablooms.append(r_abloom)
        await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 7)
        return abloom, r_ablooms

    try:
        yield _init_abloom
    finally:
        await abloom.leave()
        for r_abloom in r_ablooms:
            await r_abloom.leave()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_start_stop(init_stable_membership, init_abloom, amount):
    xbot, r_xbots = await init_stable_membership(amount)
    abloom, r_ablooms = await init_abloom(xbot, r_xbots)

    await asyncio.sleep(5)
    assert xbot._active_view == set(abloom._neighbours)


def check_filter(node: Node, abloom_dict: Dict[Node, ABloom], search: Search):
    abloom, checked = abloom_dict[node]
    if search.origin_node == node or checked or search.ttl < 1:
        return

    assert (
        search.data_id
        in abloom._abloom_filters[search.origin_node][ABloomConfig.DEPTH - search.ttl]
    )
    abloom_dict[node][1] = True  # set as 'checked'
    next_search = Search(
        id=search.id,
        origin_node=search.origin_node,
        ttl=search.ttl - 1,
        data_id=search.data_id,
    )
    for neighbour in abloom._neighbours:
        check_filter(neighbour, abloom_dict, next_search)


@pytest.mark.asyncio
async def test_publish_unpublish(init_stable_membership, init_abloom):
    xbot, r_xbots = await init_stable_membership(1)
    abloom, r_ablooms = await init_abloom(xbot, r_xbots)

    data = b"test-data"
    data_id = get_random_id()
    await abloom.publish(data_id, data)

    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 5)

    ablooms_dict = {abl.my_node: [abl, False] for abl in r_ablooms + [abloom]}
    search = Search(
        id="id", origin_node=abloom.my_node, ttl=ABloomConfig.DEPTH, data_id=data_id
    )
    for neighbour in abloom._neighbours:
        check_filter(neighbour, ablooms_dict, search)

    with pytest.raises(KeyError):
        await abloom.publish(data_id, data)

    with pytest.raises(KeyError):
        await abloom.publish(data_id, data + b"more-data")

    with pytest.raises(KeyError):
        await abloom.unpublish(data_id + "wrong-id")

    await abloom.unpublish(data_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount,replication_percent",
    [
        (amount, repl_percent)
        for amount in [
            GossipConfig.LOCAL_VIEW_SIZE + 1,
            GossipConfig.LOCAL_VIEW_SIZE + 5,
            GossipConfig.LOCAL_VIEW_SIZE + 100,
        ]
        for repl_percent in [0.3, 0.6, 0.9]
    ],
)
async def test_search(init_stable_membership, init_abloom, amount, replication_percent):
    xbot, r_xbots = await init_stable_membership(amount)
    abloom, r_ablooms = await init_abloom(xbot, r_xbots)
    data_id = get_random_id()
    data = b"data"
    replication_amount = ceil(amount * replication_percent)

    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 10)
    for r_abloom in random.sample(r_ablooms, replication_amount):
        await r_abloom.publish(data_id, data)
    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 5)

    found_data = await abloom.search(data_id)
    assert found_data


@pytest.mark.asyncio
async def test_search_fail(init_stable_membership, init_abloom):
    xbot, r_xbots = await init_stable_membership(100)
    abloom, r_ablooms = await init_abloom(xbot, r_xbots)
    data_id = get_random_id()
    result = await abloom.search(data_id)
    assert not result

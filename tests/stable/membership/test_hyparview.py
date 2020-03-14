from math import ceil
import asyncio
from collections import Counter

import pytest

from tests.utils import init_extreme_membership
from unsserv.common.gossip.gossip_config import GOSSIPING_FREQUENCY, LOCAL_VIEW_SIZE
from unsserv.stable.membership.hyparview import HyParView
from unsserv.stable.membership.hyparview_config import NEIGHBOURS_AMOUNT

init_extreme_membership = init_extreme_membership  # for flake8 compliance

MEMBERSHIP_SERVICE_ID = "hyparview"


@pytest.mark.asyncio
@pytest.fixture
async def init_hyparview():
    hyparview = None
    r_hyparviews = []

    async def _init_hyparview(newc, r_newcs):
        nonlocal hyparview, r_hyparviews
        hyparview = HyParView(newc)
        await hyparview.join(MEMBERSHIP_SERVICE_ID)
        for r_newc in r_newcs:
            r_hyparview = HyParView(r_newc)
            await r_hyparview.join(MEMBERSHIP_SERVICE_ID)
            r_hyparviews.append(r_hyparview)
        await asyncio.sleep(GOSSIPING_FREQUENCY * 7)
        return hyparview, r_hyparviews

    try:
        yield _init_hyparview
    finally:
        await hyparview.leave()
        for r_hyparview in r_hyparviews:
            await r_hyparview.leave()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_join_hyparview(init_extreme_membership, init_hyparview, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    hyparview, r_hyparviews = await init_hyparview(newc, r_newcs)

    all_nodes = set(
        [
            item
            for sublist in map(lambda n: n.get_neighbours(), r_newcs + [newc])
            for item in sublist
        ]
    )
    assert amount * 0.9 < len(all_nodes)

    neighbours = hyparview.get_neighbours()
    assert min(amount, NEIGHBOURS_AMOUNT) <= len(neighbours)

    for r_hyparview in r_hyparviews:
        r_neighbours = r_hyparview.get_neighbours()
        assert min(amount, NEIGHBOURS_AMOUNT) <= len(r_neighbours)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount", [LOCAL_VIEW_SIZE + 1, LOCAL_VIEW_SIZE + 5, LOCAL_VIEW_SIZE + 100]
)
async def test_leave_hyparview(init_extreme_membership, init_hyparview, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    hyparview, r_hyparviews = await init_hyparview(newc, r_newcs)

    await hyparview.leave()
    await newc.leave()
    await asyncio.sleep(GOSSIPING_FREQUENCY * 40)

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
    [(LOCAL_VIEW_SIZE * 2) + 1, (LOCAL_VIEW_SIZE * 2) + 5, (LOCAL_VIEW_SIZE * 2) + 100],
)  # very high neighbours amount,
# to assure neighbours will change, because it is initailzied by Newscast
async def test_hyparview_callback(init_extreme_membership, init_hyparview, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    hyparview, r_hyparviews = await init_hyparview(newc, r_newcs)

    callback_event = asyncio.Event()

    async def callback(local_view):
        assert isinstance(local_view, Counter)
        nonlocal callback_event
        callback_event.set()

    hyparview.set_neighbours_callback(callback, local_view_format=True)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 15)
    assert callback_event.is_set()

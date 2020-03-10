import asyncio
import random
from math import ceil

import pytest

from unsserv.common.gossip.gossip_config import GOSSIPING_FREQUENCY, LOCAL_VIEW_SIZE
from tests.utils import init_extreme_membership
from unsserv.extreme.searching.k_walker import KWalker
from unsserv.common.utils import get_random_id

init_extreme_membership = init_extreme_membership  # for flake8 compliance

SAMPLING_SERVICE_ID = "kwalker"


@pytest.mark.asyncio
@pytest.fixture
async def init_kwalker():
    kwalker = None
    r_kwalkers = []

    async def _init_kwalker(newc, r_newcs):
        nonlocal kwalker, r_kwalkers
        kwalker = KWalker(newc)
        await kwalker.join_searching(SAMPLING_SERVICE_ID)
        for r_newc in r_newcs:
            r_tman = KWalker(r_newc)
            await r_tman.join_searching(SAMPLING_SERVICE_ID)
            r_kwalkers.append(r_tman)
        await asyncio.sleep(GOSSIPING_FREQUENCY * 7)
        return kwalker, r_kwalkers

    try:
        yield _init_kwalker
    finally:
        await kwalker.leave_searching()
        for r_kwalker in r_kwalkers:
            await r_kwalker.leave_searching()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_start_stop(init_extreme_membership, init_kwalker, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    kwalker, r_kwalkers = await init_kwalker(newc, r_newcs)


@pytest.mark.asyncio
async def test_publish_unpublish(init_extreme_membership, init_kwalker):
    newc, r_newcs = await init_extreme_membership(1)
    kwalker, r_kwalkers = await init_kwalker(newc, r_newcs)

    data = b"test-data"
    data_id = get_random_id()
    await kwalker.publish(data_id, data)

    with pytest.raises(KeyError):
        await kwalker.publish(data_id, data)

    with pytest.raises(KeyError):
        await kwalker.publish(data_id, data + b"more-data")

    with pytest.raises(KeyError):
        await kwalker.unpublish(data_id + "wrong-id")

    await kwalker.unpublish(data_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount,replication_percent",
    [
        (amount, repl_percent)
        for amount in [LOCAL_VIEW_SIZE + 1, LOCAL_VIEW_SIZE + 5, LOCAL_VIEW_SIZE + 100]
        for repl_percent in [0.3, 0.6, 0.9]
    ],
)
async def test_search(
    init_extreme_membership, init_kwalker, amount, replication_percent
):
    newc, r_newcs = await init_extreme_membership(amount)
    kwalker, r_kwalkers = await init_kwalker(newc, r_newcs)
    data_id = get_random_id()
    data = b"data"
    replication_amount = ceil(amount * replication_percent)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 10)
    for r_kwalker in random.sample(r_kwalkers, replication_amount):
        await r_kwalker.publish(data_id, data)
    await asyncio.sleep(GOSSIPING_FREQUENCY * 5)

    found_data = None
    random_walks_amount = 0
    while not found_data and random_walks_amount <= (amount - replication_amount) + 1:
        try:
            found_data = await kwalker.search(data_id)
        finally:
            random_walks_amount += 1
    assert found_data
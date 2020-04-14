import asyncio

import pytest

from tests.utils import init_extreme_membership
from unsserv.common.gossip.config import GossipConfig
from unsserv.extreme.sampling.mrwb import MRWB

init_extreme_membership = init_extreme_membership  # for flake8 compliance

AGGREGATION_SERVICE_ID = "mrwb"


@pytest.mark.asyncio
@pytest.fixture
async def init_mrwb():
    sampl = None
    r_sampls = []

    async def _init_mrwb(newc, r_newcs):
        nonlocal sampl, r_sampls
        sampl = MRWB(newc)
        await sampl.join(AGGREGATION_SERVICE_ID)
        for r_newc in r_newcs:
            r_sampl = MRWB(r_newc)
            await r_sampl.join(AGGREGATION_SERVICE_ID)
            r_sampls.append(r_sampl)
        await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 7)
        return r_sampls, sampl

    try:
        yield _init_mrwb
    finally:
        await sampl.leave()
        for r_sampl in r_sampls:
            await r_sampl.leave()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_start_stop(init_extreme_membership, init_mrwb, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    r_sampls, sampl = await init_mrwb(newc, r_newcs)


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_sampling(init_extreme_membership, init_mrwb, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    r_sampls, sampl = await init_mrwb(newc, r_newcs)

    samples = {await sampl.get_sample() for _ in range(amount * 2)}
    assert len({r_newc.my_node for r_newc in r_newcs} - samples) / len(r_newcs) <= 0.45

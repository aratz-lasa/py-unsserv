import asyncio

import pytest

from unsserv.common.gossip.gossip_config import GOSSIPING_FREQUENCY
from unsserv.extreme.sampling.mrwb import MRWB

from tests.utils import init_extreme_membership

init_extreme_membership = init_extreme_membership  # for flake8 compliance

AGGR_SERVICE_ID = "mrwb"


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_start_stop(init_extreme_membership, amount):
    newc, r_newcs = await init_extreme_membership(amount)

    sampl = MRWB(newc)
    await sampl.join_sampling(AGGR_SERVICE_ID)
    r_sampls = []
    for r_newc in r_newcs:
        r_sampl = MRWB(r_newc)
        await r_sampl.join_sampling(AGGR_SERVICE_ID)
        r_sampls.append(r_sampl)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 10)

    await sampl.leave_sampling()
    for r_sampl in r_sampls:
        await r_sampl.leave_sampling()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_sampling(init_extreme_membership, amount):
    newc, r_newcs = await init_extreme_membership(amount)

    sampl = MRWB(newc)
    await sampl.join_sampling(AGGR_SERVICE_ID)
    r_sampls = []
    for r_newc in r_newcs:
        r_sampl = MRWB(r_newc)
        await r_sampl.join_sampling(AGGR_SERVICE_ID)
        r_sampls.append(r_sampl)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 10)

    samples = {await sampl.get_sample() for _ in range(amount * 2)}
    assert len({r_newc.my_node for r_newc in r_newcs} - samples) / len(r_newcs) <= 0.45

    await sampl.leave_sampling()
    for r_sampl in r_sampls:
        await r_sampl.leave_sampling()

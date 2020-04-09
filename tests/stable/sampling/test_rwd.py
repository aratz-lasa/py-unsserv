import asyncio

import pytest

from unsserv.common.gossip.config import GOSSIPING_FREQUENCY
from unsserv.stable.sampling.rwd import RWD

from tests.utils import init_stable_membership

init_stable_membership = init_stable_membership  # for flake8 compliance

AGGREGATION_SERVICE_ID = "rwd"


@pytest.mark.asyncio
@pytest.fixture
async def init_rwd():
    sampl = None
    r_sampls = []

    async def _init_rwd(newc, r_newcs):
        nonlocal sampl, r_sampls
        sampl = RWD(newc)
        await sampl.join_sampling(AGGREGATION_SERVICE_ID)
        for r_newc in r_newcs:
            r_sampl = RWD(r_newc)
            await r_sampl.join_sampling(AGGREGATION_SERVICE_ID)
            r_sampls.append(r_sampl)
        await asyncio.sleep(GOSSIPING_FREQUENCY * 7)
        return r_sampls, sampl

    try:
        yield _init_rwd
    finally:
        await sampl.leave_sampling()
        for r_sampl in r_sampls:
            await r_sampl.leave_sampling()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_start_stop(init_stable_membership, init_rwd, amount):
    newc, r_newcs = await init_stable_membership(amount)
    r_sampls, sampl = await init_rwd(newc, r_newcs)


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_sampling(init_stable_membership, init_rwd, amount):
    newc, r_newcs = await init_stable_membership(amount)
    r_sampls, sampl = await init_rwd(newc, r_newcs)

    samples = {await sampl.get_sample() for _ in range(amount * 2)}
    assert len({r_newc.my_node for r_newc in r_newcs} - samples) / len(r_newcs) <= 0.45

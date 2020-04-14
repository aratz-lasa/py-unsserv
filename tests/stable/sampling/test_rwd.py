import asyncio

import pytest

from tests.utils import init_stable_membership
from unsserv.common.gossip.config import GossipConfig
from unsserv.stable.sampling.rwd import RWD

init_stable_membership = init_stable_membership  # for flake8 compliance

AGGREGATION_SERVICE_ID = "rwd"


@pytest.mark.asyncio
@pytest.fixture
async def init_rwd():
    sampl = None
    r_sampls = []

    async def _init_rwd(hypa, r_hypas):
        nonlocal sampl, r_sampls
        sampl = RWD(hypa)
        await sampl.join(AGGREGATION_SERVICE_ID)
        for r_hypa in r_hypas:
            r_sampl = RWD(r_hypa)
            await r_sampl.join(AGGREGATION_SERVICE_ID)
            r_sampls.append(r_sampl)
        await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 7)
        return r_sampls, sampl

    try:
        yield _init_rwd
    finally:
        await sampl.leave()
        for r_sampl in r_sampls:
            await r_sampl.leave()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_start_stop(init_stable_membership, init_rwd, amount):
    hypa, r_hypas = await init_stable_membership(amount)
    r_sampls, sampl = await init_rwd(hypa, r_hypas)


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_sampling(init_stable_membership, init_rwd, amount):
    hypa, r_hypas = await init_stable_membership(amount)
    r_sampls, sampl = await init_rwd(hypa, r_hypas)

    samples = {await sampl.get_sample() for _ in range(amount * 2)}
    assert len({r_hypa.my_node for r_hypa in r_hypas} - samples) / len(r_hypas) <= 0.45

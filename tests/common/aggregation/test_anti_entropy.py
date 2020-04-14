import asyncio

import pytest

from tests.utils import init_extreme_membership
from unsserv.common.aggregation.anti_entropy import (
    AntiEntropy,
    aggregate_functions,
)
from unsserv.common.aggregation.config import AggregateType
from unsserv.common.gossip.config import GossipConfig

init_extreme_membership = init_extreme_membership  # for flake8 compliance

AGGR_SERVICE_ID = "tman"


@pytest.mark.asyncio
@pytest.fixture
async def init_anti_entropy():
    anti = None
    r_antis = []

    async def _init_anti_entropy(newc, r_newcs):
        nonlocal anti, r_antis
        anti = AntiEntropy(newc)
        await anti.join(
            AGGR_SERVICE_ID,
            aggregate_type=AggregateType.MEAN,
            aggregate_value=anti.my_node.address_info[1],
        )
        for r_newc in r_newcs:
            r_anti = AntiEntropy(r_newc)
            await r_anti.join(
                AGGR_SERVICE_ID,
                aggregate_type=AggregateType.MEAN,
                aggregate_value=r_newc.my_node.address_info[1],
            )
            r_antis.append(r_anti)
        await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 7)
        return anti, r_antis

    try:
        yield _init_anti_entropy
    finally:
        await anti.leave()
        for r_anti in r_antis:
            await r_anti.leave()


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_start_stop(init_extreme_membership, init_anti_entropy, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    anti, r_antis = await init_anti_entropy(newc, r_newcs)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount,aggregate_type",
    [
        (amount, aggregate_type)
        for amount in [1, 5, 100]
        for aggregate_type in AggregateType
    ],
)
async def test_aggregate(
    init_extreme_membership, init_anti_entropy, amount, aggregate_type
):
    newc, r_newcs = await init_extreme_membership(amount)
    anti, r_antis = await init_anti_entropy(newc, r_newcs)

    first_port = anti.my_node.address_info[1]
    assert (
        abs(
            await anti.get_aggregate()
            - aggregate_functions[aggregate_type](
                [number + first_port for number in range(amount + 1)]
            )
        )
        / await anti.get_aggregate()
        < 0.1
    )
    for r_anti in r_antis:
        assert (
            abs(
                await r_anti.get_aggregate()
                - aggregate_functions[aggregate_type](
                    [number + first_port for number in range(amount + 1)]
                )
            )
            / await r_anti.get_aggregate()
            < 0.1
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount",
    [
        (GossipConfig.LOCAL_VIEW_SIZE * 2) + 1,
        (GossipConfig.LOCAL_VIEW_SIZE * 2) + 5,
        (GossipConfig.LOCAL_VIEW_SIZE * 2) + 100,
    ],
)
async def test_aggregate_handler(init_extreme_membership, init_anti_entropy, amount):
    newc, r_newcs = await init_extreme_membership(amount)
    anti, r_antis = await init_anti_entropy(newc, r_newcs)

    handler_event = asyncio.Event()

    async def handler(neighbours):
        nonlocal handler_event
        handler_event.set()

    anti.add_aggregate_handler(handler)

    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 7)
    assert handler_event.is_set()

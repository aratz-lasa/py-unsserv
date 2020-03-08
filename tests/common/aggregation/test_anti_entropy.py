import asyncio

import pytest
from tests.utils import get_random_nodes
from unsserv.common.aggregation.anti_entropy import (
    AggregateType,
    AntiEntropy,
    aggregate_functions,
)
from unsserv.common.data_structures import Node
from unsserv.common.gossip.gossip_config import GOSSIPING_FREQUENCY
from unsserv.extreme.membership import newscast

first_port = 7771
node = Node(("127.0.0.1", first_port))

MEMB_SERVICE_ID = "newscast"
AGGR_SERVICE_ID = "tman"


async def init_membership(amount):
    newc = newscast.Newscast(node)
    await newc.join(MEMB_SERVICE_ID)

    r_newcs = []
    r_nodes = get_random_nodes(amount, first_port=first_port + 1)
    for i, r_node in enumerate(r_nodes):
        r_newc = newscast.Newscast(r_node)
        await r_newc.join(MEMB_SERVICE_ID, [node] + r_nodes[:i])
        r_newcs.append(r_newc)
    await asyncio.sleep(GOSSIPING_FREQUENCY * 7)
    return newc, r_newcs


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_start_stop(amount):
    newc, r_newcs = await init_membership(amount)

    anti = AntiEntropy(newc)
    await anti.join_aggregation(
        AGGR_SERVICE_ID, (AggregateType.MEAN, node.address_info[1])
    )
    r_antis = []
    for r_newc in r_newcs:
        r_anti = AntiEntropy(r_newc)
        await r_anti.join_aggregation(
            AGGR_SERVICE_ID, (AggregateType.MEAN, r_newc.my_node.address_info[1])
        )
        r_antis.append(r_anti)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 15)

    await anti.leave_aggregation()
    for r_anti in r_antis:
        await r_anti.leave_aggregation()
    await newc.leave()
    for r_newc in r_newcs:
        await r_newc.leave()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount,aggregate_type",
    [
        (amount, aggregate_type)
        for amount in [1, 5, 100]
        for aggregate_type in AggregateType
    ],
)
async def test_aggregate(amount, aggregate_type):
    newc, r_newcs = await init_membership(amount)

    anti = AntiEntropy(newc)
    await anti.join_aggregation(AGGR_SERVICE_ID, (aggregate_type, node.address_info[1]))
    r_antis = []
    for r_newc in r_newcs:
        r_anti = AntiEntropy(r_newc)
        await r_anti.join_aggregation(
            AGGR_SERVICE_ID, (aggregate_type, r_newc.my_node.address_info[1])
        )
        r_antis.append(r_anti)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 15)

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

    await anti.leave_aggregation()
    for r_anti in r_antis:
        await r_anti.leave_aggregation()
    await newc.leave()
    for r_newc in r_newcs:
        await r_newc.leave()

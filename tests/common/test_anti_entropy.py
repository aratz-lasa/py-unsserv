import asyncio

import pytest

from tests.utils import get_random_nodes
from unsserv.common.anti_entropy import AntiEntropy, AggregateType, aggregate_functions
from unsserv.common.gossip.config import GOSSIPING_FREQUENCY
from unsserv.common.utils.data_structures import Node
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
    for r_node in r_nodes:
        r_newc = newscast.Newscast(r_node)
        await r_newc.join(MEMB_SERVICE_ID, [node])
        r_newcs.append(r_newc)
    await asyncio.sleep(GOSSIPING_FREQUENCY * 7)
    return newc, r_newcs


@pytest.mark.asyncio
async def test_start_stop():
    neighbour_amounts = [1, 2, 5, 10, 30, 100]
    for amount in neighbour_amounts:
        await start_stop(amount)


async def start_stop(amount):
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


@pytest.mark.asyncio
async def test_aggregate():
    neighbour_amounts = [1, 2, 5, 10, 30, 100]
    for aggregate_type in AggregateType:
        for amount in neighbour_amounts:
            await aggregate(amount, aggregate_type)


async def aggregate(amount, aggregate_type):
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

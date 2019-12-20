import asyncio
from collections import Counter

import pytest
from typing import List

from tests.utils import get_random_nodes
from unsserv.common.gossip import gossip
from unsserv.common.gossip.config import GOSSIPING_FREQUENCY
from unsserv.common.gossip.gossip import LOCAL_VIEW_SIZE, View
from unsserv.data_structures import Node

node = Node(("127.0.0.1", 7771))


def test_view_selection():
    r_nodes = get_random_nodes(LOCAL_VIEW_SIZE * 2, first_port=7772)
    view = Counter(dict(map(lambda n: (n[1], n[0] + 1), enumerate(r_nodes))))
    gsp = gossip.Gossip(node)

    gsp.view_selection = gossip.ViewSelectionPolicy.HEAD
    assert set(r_nodes[:LOCAL_VIEW_SIZE]) == set(gsp.select_view(view).keys())

    gsp.view_selection = gossip.ViewSelectionPolicy.TAIL
    assert set(r_nodes[LOCAL_VIEW_SIZE:]) == set(gsp.select_view(view).keys())

    def ranking(node: Node):
        return node.address_info[1] % 2

    def selection_ranking(view: View) -> List[Node]:
        return sorted(view.keys(), key=ranking)

    gsp.view_selection = gossip.ViewSelectionPolicy.HEAD
    gsp.custom_selection_ranking = selection_ranking
    assert set(sorted(r_nodes, key=ranking)[:LOCAL_VIEW_SIZE]) == set(
        gsp.select_view(view).keys()
    )


def test_peer_selection():
    r_nodes = get_random_nodes(LOCAL_VIEW_SIZE, first_port=7772)
    view = Counter(dict(map(lambda n: (n[1], n[0] + 1), enumerate(r_nodes))))
    gsp = gossip.Gossip(node)

    gsp.peer_selection = gossip.PeerSelectionPolicy.HEAD
    assert r_nodes[0] == gsp.select_peer(view)

    gsp.peer_selection = gossip.PeerSelectionPolicy.TAIL
    assert r_nodes[-1] == gsp.select_peer(view)


def test_increase_hop_count():
    r_nodes = get_random_nodes(LOCAL_VIEW_SIZE, first_port=7772)
    view = Counter(dict(map(lambda n: (n[1], n[0] + 1), enumerate(r_nodes))))
    gsp = gossip.Gossip(node)

    assert view + Counter(view.keys()) == gsp.increase_hop_count(view)


def test_merge():
    r_nodes = get_random_nodes(LOCAL_VIEW_SIZE, first_port=7772)
    view1 = Counter(dict(map(lambda n: (n[1], n[0] + 1), enumerate(r_nodes))))
    gsp = gossip.Gossip(node)

    view2 = view1.copy()
    view2[r_nodes[0]] = 99

    assert view1 != view2
    assert view1 == gsp.merge(view1, view2)


@pytest.mark.asyncio
async def test_gossiping():
    neighbour_amounts = [1, 2, 5, 10, 30, 100]
    for amount in neighbour_amounts:
        await gossiping(amount)


async def gossiping(amount):
    r_nodes = get_random_nodes(amount, first_port=7772)
    gsp = gossip.Gossip(node)
    await gsp.start()

    r_gsps = []
    for r_node in r_nodes:
        r_gsp = gossip.Gossip(r_node, [node])
        await r_gsp.start()
        r_gsps.append(r_gsp)

    await asyncio.sleep(GOSSIPING_FREQUENCY * 5)

    for r_gsp in r_gsps:
        assert min(amount, LOCAL_VIEW_SIZE) <= len(r_gsp.local_view)

    await gsp.stop()
    for r_gsp in r_gsps:
        await r_gsp.stop()
import asyncio
from collections import Counter
from typing import Any, List, Tuple

import pytest

import unsserv.common.gossip.config
import unsserv.common.gossip.gossip
import unsserv.common.gossip.structs
from tests.utils import get_random_nodes
from unsserv.common.gossip import gossip
from unsserv.common.gossip.config import GossipConfig
from unsserv.common.gossip.gossip import View
from unsserv.common.gossip.typing import Payload
from unsserv.common.structs import Node
from unsserv.common.utils import parse_node

node = Node(("127.0.0.1", 7771))
SERVICE_ID = "gossip"


def test_view_selection():
    r_nodes = get_random_nodes(GossipConfig.LOCAL_VIEW_SIZE * 2, first_port=7772)
    view = Counter(dict(map(lambda n: (n[1], n[0] + 1), enumerate(r_nodes))))
    gsp = gossip.Gossip(node, SERVICE_ID)

    gsp._config.VIEW_SELECTION = unsserv.common.gossip.config.ViewSelectionPolicy.HEAD
    assert set(r_nodes[: GossipConfig.LOCAL_VIEW_SIZE]) == set(
        gsp._select_view(view).keys()
    )

    gsp._config.VIEW_SELECTION = unsserv.common.gossip.config.ViewSelectionPolicy.TAIL
    assert set(r_nodes[GossipConfig.LOCAL_VIEW_SIZE :]) == set(
        gsp._select_view(view).keys()
    )

    def ranking(node: Node):
        return node.address_info[1] % 2

    def selection_ranking(view: View) -> List[Node]:
        return sorted(view.keys(), key=ranking)

    gsp._config.VIEW_SELECTION = unsserv.common.gossip.config.ViewSelectionPolicy.HEAD
    gsp.custom_selection_ranking = selection_ranking
    assert set(sorted(r_nodes, key=ranking)[: GossipConfig.LOCAL_VIEW_SIZE]) == set(
        gsp._select_view(view).keys()
    )


def test_peer_selection():
    r_nodes = get_random_nodes(GossipConfig.LOCAL_VIEW_SIZE, first_port=7772)
    view = Counter(dict(map(lambda n: (n[1], n[0] + 1), enumerate(r_nodes))))
    gsp = gossip.Gossip(node, SERVICE_ID)

    gsp._config.PEER_SELECTION = unsserv.common.gossip.config.PeerSelectionPolicy.HEAD
    assert r_nodes[0] == gsp._select_peer(view)

    gsp._config.PEER_SELECTION = unsserv.common.gossip.config.PeerSelectionPolicy.TAIL
    assert r_nodes[-1] == gsp._select_peer(view)


def test_increase_hop_count():
    r_nodes = get_random_nodes(GossipConfig.LOCAL_VIEW_SIZE, first_port=7772)
    view = Counter(dict(map(lambda n: (n[1], n[0] + 1), enumerate(r_nodes))))
    gsp = gossip.Gossip(node, SERVICE_ID)

    assert view + Counter(view.keys()) == gsp._increase_hop_count(view)


def test_merge():
    r_nodes = get_random_nodes(GossipConfig.LOCAL_VIEW_SIZE, first_port=7772)
    view1 = Counter(dict(map(lambda n: (n[1], n[0] + 1), enumerate(r_nodes))))
    gsp = gossip.Gossip(node, SERVICE_ID)

    view2 = view1.copy()
    view2[r_nodes[0]] = 99

    assert view1 != view2
    assert view1 == gsp._merge_views(view1, view2)


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", [1, 5, 100])
async def test_gossiping(amount):
    r_nodes = get_random_nodes(amount, first_port=7772)
    gsp = gossip.Gossip(node, SERVICE_ID)
    await gsp.start()

    r_gsps = []
    for i, r_node in enumerate(r_nodes):
        r_gsp = gossip.Gossip(r_node, SERVICE_ID, [node] + r_nodes[:i])
        await r_gsp.start()
        r_gsps.append(r_gsp)

    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 15)

    for r_gsp in r_gsps:
        assert min(amount, GossipConfig.LOCAL_VIEW_SIZE) <= len(r_gsp.local_view)

    await gsp.stop()
    for r_gsp in r_gsps:
        await r_gsp.stop()


@pytest.mark.asyncio
async def test_subscriber():
    class Subscriber(unsserv.common.gossip.gossip.IGossipSubscriber):
        service_id = "sub"

        def __init__(self, my_node, expected_node):
            self.my_node = my_node
            self.expected_node = expected_node
            self.is_payload_received = False

        async def receive_payload(self, payload: Payload):
            node = parse_node(payload[Subscriber.service_id])
            assert node == self.expected_node
            self.is_payload_received = True

        async def get_payload(self) -> Tuple[Any, Any]:
            return Subscriber.service_id, self.my_node

    r_node = get_random_nodes(1, first_port=7772)[0]
    gsp = gossip.Gossip(node, SERVICE_ID)
    sub_gsp = Subscriber(node, r_node)
    gsp.subscribe(sub_gsp)

    r_gsp = gossip.Gossip(r_node, SERVICE_ID, [node])
    sub_r_gsp = Subscriber(r_node, node)
    r_gsp.subscribe(sub_r_gsp)

    await gsp.start()
    await r_gsp.start()

    await asyncio.sleep(GossipConfig.GOSSIPING_FREQUENCY * 3)

    assert sub_gsp.is_payload_received
    assert sub_r_gsp.is_payload_received

    await gsp.stop()
    await r_gsp.stop()

from collections import Counter

from tests.utils import get_random_nodes
from unsserv.data_structures import Node
from unsserv.extreme.membership import gossiping
from unsserv.extreme.membership.gossiping import LOCAL_VIEW_SIZE

node = Node("127.0.0.1", 7771)


def test_view_selection():
    r_nodes = get_random_nodes(LOCAL_VIEW_SIZE * 2, first_port=772)
    view = Counter(dict(map(lambda n: (n[1], n[0] + 1), enumerate(r_nodes))))
    gsp = gossiping.Gossiping(node)

    gsp.view_selection = gossiping.ViewSelectionPolicy.HEAD
    assert set(r_nodes[:LOCAL_VIEW_SIZE]) == set(gsp.select_view(view).keys())

    gsp.view_selection = gossiping.ViewSelectionPolicy.TAIL
    assert set(r_nodes[LOCAL_VIEW_SIZE:]) == set(gsp.select_view(view).keys())


def test_peer_selection():
    r_nodes = get_random_nodes(LOCAL_VIEW_SIZE, first_port=772)
    view = Counter(dict(map(lambda n: (n[1], n[0] + 1), enumerate(r_nodes))))
    gsp = gossiping.Gossiping(node)

    gsp.peer_selection = gossiping.PeerSelectionPolicy.HEAD
    assert r_nodes[0] == gsp.select_peer(view)

    gsp.peer_selection = gossiping.PeerSelectionPolicy.TAIL
    assert r_nodes[-1] == gsp.select_peer(view)


def test_increase_hop_count():
    r_nodes = get_random_nodes(LOCAL_VIEW_SIZE, first_port=772)
    view = Counter(dict(map(lambda n: (n[1], n[0] + 1), enumerate(r_nodes))))
    gsp = gossiping.Gossiping(node)

    assert view + Counter(view.keys()) == gsp.increase_hop_count(view)


def test_merge():
    r_nodes = get_random_nodes(LOCAL_VIEW_SIZE, first_port=772)
    view1 = Counter(dict(map(lambda n: (n[1], n[0] + 1), enumerate(r_nodes))))
    gsp = gossiping.Gossiping(node)

    view2 = view1.copy()
    view2[r_nodes[0]] = 99

    assert view1 != view2
    assert view1 == gsp.merge(view1, view2)

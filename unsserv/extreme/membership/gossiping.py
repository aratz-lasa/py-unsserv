import asyncio
import math
import random
from collections import Counter
from enum import Enum, auto
from typing import Tuple

from unsserv.data_structures import Node

LOCAL_VIEW_SIZE = 10  # todo: select a proper size
GOSSIPING_FREQUENCY = 0.2  # todo: select a proper size
View = Counter


class ViewSelectionPolicy(Enum):
    RAND = auto()
    HEAD = auto()
    TAIL = auto()


class PeerSelectionPolicy(Enum):
    RAND = auto()
    HEAD = auto()
    TAIL = auto()


class ViewPropagationPolicy(Enum):
    PUSH = auto()
    PULL = auto()
    PUSHPULL = auto()


class Gossiping:
    my_node: Node
    local_view: View

    def __init__(
        self,
        my_node: Node,
        local_view: View = None,
        view_selection=ViewSelectionPolicy.HEAD,
        peer_selection=PeerSelectionPolicy.RAND,
        view_propagation=ViewPropagationPolicy.PUSHPULL,
        local_view_size: int = LOCAL_VIEW_SIZE,
        gossiping_frequency: float = GOSSIPING_FREQUENCY,
    ):
        self.my_node = my_node
        self.local_view = local_view or Counter()
        self.view_selection = view_selection
        self.peer_selection = peer_selection
        self.view_propagation = view_propagation

        self.local_view_size = local_view_size
        self.gossiping_frequency = gossiping_frequency

    async def active_process(self):
        await asyncio.sleep(self.gossiping_frequency)
        peer = self.select_peer(self.local_view)
        if self.view_propagation is not ViewPropagationPolicy.PULL:
            my_descriptor = Counter({self.my_node: 0})
            buffer = self.merge(my_descriptor, self.local_view)
            await self.send_view(peer, buffer)
        else:
            # Empty view to tigger response
            await self.send_view(peer, Counter())
        if self.view_propagation is not ViewPropagationPolicy.PUSH:
            view = await self.receive_view_from(peer)
            view = self.increase_hop_count(view)
            buffer = self.merge(view, self.local_view)
            self.local_view = self.select_view(buffer)

    async def passive_process(self):
        peer, view = await self.wait_view()
        view = self.increase_hop_count(view)
        if self.view_propagation is not ViewPropagationPolicy.PUSH:
            my_descriptor = Counter({self.my_node: 0})
            buffer = self.merge(my_descriptor, self.local_view)
            await self.send_view(peer, buffer)
        buffer = self.merge(view, self.local_view)
        self.local_view = self.select_view(buffer)

    def select_peer(self, view: View) -> Node:
        if self.peer_selection is ViewSelectionPolicy.RAND:
            return random.choice(list(view.keys()))
        elif self.peer_selection is ViewSelectionPolicy.HEAD:
            return view.most_common()[-1][0]
        elif self.peer_selection is ViewSelectionPolicy.TAIL:
            return view.most_common(1)[0][0]
        raise AttributeError("Invalid Peer Selection policy")

    def select_view(self, view: View) -> View:
        remove_amount = max(len(view) - self.local_view_size, 0)
        if self.view_selection is ViewSelectionPolicy.RAND:
            return view - Counter(dict(random.sample(view.items(), remove_amount)))
        elif self.view_selection is ViewSelectionPolicy.HEAD:
            return view - Counter(dict(view.most_common(remove_amount)))
        elif self.view_selection is ViewSelectionPolicy.TAIL:
            return view - Counter(dict(view.most_common()[-remove_amount:]))
        raise AttributeError("Invalid View Selection policy")

    def increase_hop_count(self, view: View) -> View:
        return view + Counter(view.keys())

    def merge(self, view1: View, view2: View) -> View:
        all_nodes = set(list(view1.keys()) + list(set(view2.keys())))
        merged_view: Counter = Counter()
        for node in all_nodes:
            merged_view[node] = (
                view1[node]
                if view1.get(node, math.inf) < view2.get(node, math.inf)
                else view2[node]
            )
        return merged_view

    async def send_view(self, peer: Node, view: View) -> None:
        pass  # todo

    async def receive_view_from(self, peer: Node) -> View:
        pass  # todo

    async def wait_view(self) -> Tuple[Node, View]:
        pass  # todo

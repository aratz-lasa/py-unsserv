import asyncio
import math
import random
from collections import Counter
from enum import Enum, auto
from typing import Union, List

from unsserv.common.gossip.config import (
    DATA_FIELD_VIEW,
    LOCAL_VIEW_SIZE,
    GOSSIPING_FREQUENCY,
)
from unsserv.common.gossip.rpc import GossipRPC
from unsserv.data_structures import Node, Message

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

    _transport: asyncio.BaseTransport
    _proactive_task: asyncio.Task

    def __init__(
        self,
        my_node: Node,
        local_view_nodes: List[Node] = None,
        view_selection=ViewSelectionPolicy.HEAD,
        peer_selection=PeerSelectionPolicy.RAND,
        view_propagation=ViewPropagationPolicy.PUSHPULL,
        local_view_size: int = LOCAL_VIEW_SIZE,
        gossiping_frequency: float = GOSSIPING_FREQUENCY,
    ):
        self.my_node = my_node
        self.local_view = Counter(local_view_nodes or [])
        self.view_selection = view_selection
        self.peer_selection = peer_selection
        self.view_propagation = view_propagation

        self.local_view_size = local_view_size
        self.gossiping_frequency = gossiping_frequency

        self.rpc = GossipRPC(self.my_node, self.reactive_process)

    async def start(self):
        (
            self._transport,
            protocol,
        ) = await asyncio.get_event_loop().create_datagram_endpoint(
            lambda: self.rpc, self.my_node
        )
        self._proactive_task = asyncio.create_task(self.proactive_process())

    async def stop(self):
        if self._transport is not None:
            self._transport.close()
        if self._proactive_task:
            self._proactive_task.cancel()
            try:
                await self._proactive_task
            except asyncio.CancelledError:
                pass

    async def proactive_process(self):
        while True:
            await asyncio.sleep(self.gossiping_frequency)
            peer = self.select_peer(self.local_view)
            if not peer:  # Empty Local view
                continue
            push_view = Counter()  # if PULL, empty view
            if self.view_propagation is not ViewPropagationPolicy.PULL:
                my_descriptor = Counter({self.my_node: 0})
                push_view = self.merge(my_descriptor, self.local_view)
            data = {DATA_FIELD_VIEW: push_view}
            push_message = Message(self.my_node, data)
            if self.view_propagation is ViewPropagationPolicy.PUSH:
                try:
                    await self.rpc.call_push(peer, push_message)
                except ConnectionError:
                    self.local_view.pop(peer)
                    continue
            else:
                try:
                    push_message = await self.rpc.call_pushpull(
                        peer, push_message
                    )  # rpc.pushpull used for bot PULL and PUSHPULL
                except ConnectionError:
                    self.local_view.pop(peer)
                    continue
                view = Counter(push_message.data[DATA_FIELD_VIEW])
                view = self.increase_hop_count(view)
                buffer = self.merge(view, self.local_view)
                self.local_view = self.select_view(buffer)

    async def reactive_process(self, message: Message) -> Union[None, Message]:
        view = Counter(message.data[DATA_FIELD_VIEW])
        view = self.increase_hop_count(view)
        pull_return_message = None
        if self.view_propagation is not ViewPropagationPolicy.PUSH:
            my_descriptor = Counter({self.my_node: 0})
            data = {DATA_FIELD_VIEW: self.merge(my_descriptor, self.local_view)}
            pull_return_message = Message(self.my_node, data)
        buffer = self.merge(view, self.local_view)
        self.local_view = self.select_view(buffer)
        return pull_return_message

    def select_peer(self, view: View) -> Node:
        if self.peer_selection is PeerSelectionPolicy.RAND:
            return random.choice(list(view.keys())) if view else None
        elif self.peer_selection is PeerSelectionPolicy.HEAD:
            return view.most_common()[-1][0] if view else None
        elif self.peer_selection is PeerSelectionPolicy.TAIL:
            return view.most_common(1)[0][0] if view else None
        raise AttributeError("Invalid Peer Selection policy")

    def select_view(self, view: View) -> View:
        if self.my_node in view:
            view.pop(self.my_node)
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

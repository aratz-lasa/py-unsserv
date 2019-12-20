import asyncio
import math
import random
from collections import Counter
from enum import Enum, auto
from typing import List, Callable, Any, Coroutine, Optional, Union

from unsserv.api import View
from unsserv.common.gossip.config import (
    DATA_FIELD_VIEW,
    LOCAL_VIEW_SIZE,
    GOSSIPING_FREQUENCY,
)
from unsserv.common.gossip.rpc.rpc import RPC
from unsserv.data_structures import Node, Message

LocalViewCallback = Callable[[View], Coroutine[Any, Any, None]]
CustomSelectionRanking = Callable[[View], List[Node]]
ExternalViewSource = Callable


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


class Gossip:
    my_node: Node
    local_view: View

    _transport: asyncio.BaseTransport
    _proactive_task: asyncio.Task

    def __init__(
        self,
        my_node: Node,
        service_id: Any,
        local_view_nodes: List[Node] = None,
        local_view_callback: LocalViewCallback = None,
        external_view_source: ExternalViewSource = None,
        view_selection=ViewSelectionPolicy.HEAD,
        peer_selection=PeerSelectionPolicy.RAND,
        view_propagation=ViewPropagationPolicy.PUSHPULL,
        custom_selection_ranking: CustomSelectionRanking = None,
        local_view_size: int = LOCAL_VIEW_SIZE,
        gossiping_frequency: float = GOSSIPING_FREQUENCY,
        transport_protocol: str = "udp",
        multiplex: bool = True,
    ):
        self.my_node = my_node
        self.service_id = service_id
        self.local_view = Counter(local_view_nodes or [])
        self.local_view_callback = local_view_callback
        self.get_external_view = external_view_source
        self.view_selection = view_selection
        self.peer_selection = peer_selection
        self.view_propagation = view_propagation
        self.custom_selection_ranking = custom_selection_ranking

        self.local_view_size = local_view_size
        self.gossiping_frequency = gossiping_frequency
        self.rpc = RPC.get_rpc(
            self.my_node, type=transport_protocol, multiplex=multiplex
        )

    async def start(self):
        await self.rpc.register_service(self.service_id, self.reactive_process)
        self._proactive_task = asyncio.create_task(self.proactive_process())

    async def stop(self):
        await self.rpc.unregister_service(self.service_id)

        if self._proactive_task:
            self._proactive_task.cancel()
            try:
                await self._proactive_task
            except asyncio.CancelledError:
                pass

    def is_running(self) -> bool:
        return self._proactive_task.cancelled()

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
            push_message = Message(self.my_node, "id", data)
            if self.view_propagation is ViewPropagationPolicy.PUSH:
                try:
                    await self.rpc.call_push(peer, push_message)
                except ConnectionError:
                    try:
                        self.local_view.pop(peer)
                    except KeyError:
                        pass
                    continue
            else:
                try:
                    push_message = await self.rpc.call_pushpull(
                        peer, push_message
                    )  # rpc.pushpull used for bot PULL and PUSHPULL
                except ConnectionError:
                    try:
                        self.local_view.pop(peer)
                    except KeyError:
                        pass
                    continue
                view = _decode_view(push_message.data[DATA_FIELD_VIEW])
                view = self.increase_hop_count(view)
                buffer = self.merge(view, self.local_view)
                if self.get_external_view:
                    buffer = self.merge(view, self.get_external_view())

                new_view = self.select_view(buffer)
                await self.try_call_callback(new_view)
                self.local_view = new_view

    async def reactive_process(self, message: Message) -> Union[None, Message]:
        view = _decode_view(message.data[DATA_FIELD_VIEW])
        view = self.increase_hop_count(view)
        pull_return_message = None
        if self.view_propagation is not ViewPropagationPolicy.PUSH:
            my_descriptor = Counter({self.my_node: 0})
            data = {DATA_FIELD_VIEW: self.merge(my_descriptor, self.local_view)}
            pull_return_message = Message(self.my_node, "id", data)
        buffer = self.merge(view, self.local_view)
        if self.get_external_view:
            buffer = self.merge(view, self.get_external_view())

        new_view = self.select_view(buffer)
        await self.try_call_callback(new_view)
        self.local_view = new_view
        return pull_return_message

    def select_peer(self, view: View) -> Optional[Node]:
        if self.custom_selection_ranking:
            ordered_nodes = self.custom_selection_ranking(view)
        else:
            ordered_nodes = list(map(lambda n: n[0], reversed(view.most_common())))
        if self.peer_selection is PeerSelectionPolicy.RAND:
            return random.choice(ordered_nodes) if view else None
        elif self.peer_selection is PeerSelectionPolicy.HEAD:
            return ordered_nodes[0] if view else None
        elif self.peer_selection is PeerSelectionPolicy.TAIL:
            return ordered_nodes[-1] if view else None
        raise AttributeError("Invalid Peer Selection policy")

    def select_view(self, view: View) -> View:
        if self.my_node in view:
            view.pop(self.my_node)
        remove_amount = max(len(view) - self.local_view_size, 0)
        if remove_amount < 1:
            return view
        if self.custom_selection_ranking:
            ordered_nodes = self.custom_selection_ranking(view)
        else:
            ordered_nodes = list(map(lambda n: n[0], reversed(view.most_common())))

        new_view = view.copy()
        if self.view_selection is ViewSelectionPolicy.RAND:
            for node in random.sample(ordered_nodes, remove_amount):
                new_view.pop(node)
        elif self.view_selection is ViewSelectionPolicy.HEAD:
            for node in ordered_nodes[-remove_amount:]:
                new_view.pop(node)
        elif self.view_selection is ViewSelectionPolicy.TAIL:
            for node in ordered_nodes[:remove_amount]:
                new_view.pop(node)
        else:
            raise AttributeError("Invalid View Selection policy")
        return new_view

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

    async def try_call_callback(self, new_view):
        if set(new_view.keys()) != set(self.local_view.keys()):
            if self.local_view_callback:
                await self.local_view_callback(new_view)


def _decode_view(raw_view: dict) -> View:
    return Counter(dict(map(lambda n: (Node(*n[0]), n[1]), raw_view.items())))

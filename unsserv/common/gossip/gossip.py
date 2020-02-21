import asyncio
import math
import random
from unsserv.common.gossip.gossip_subcriber_interface import IGossipSubscriber
from collections import Counter
from enum import Enum, auto
from typing import List, Callable, Any, Coroutine, Optional, Union, Dict

from unsserv.common.api import View
from unsserv.common.gossip.gossip_config import (
    DATA_FIELD_VIEW,
    LOCAL_VIEW_SIZE,
    GOSSIPING_FREQUENCY,
)
from unsserv.common.rpc.rpc import RPC, RpcBase
from unsserv.common.data_structures import Message
from unsserv.common.data_structures import Node

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


class GossipProtocol(RpcBase):
    async def call_push(self, destination: Node, message: Message) -> None:
        rpc_result = await self.push(destination.address_info, message)
        self._handle_call_response(rpc_result)

    async def call_pushpull(self, destination: Node, message: Message) -> Message:
        rpc_result = await self.pushpull(destination.address_info, message)
        return self._decode_message(self._handle_call_response(rpc_result))

    async def rpc_push(self, node: Node, raw_message: List) -> None:
        message = self._decode_message(raw_message)
        await self.registered_services[message.service_id](message)

    async def rpc_pushpull(self, node: Node, raw_message: List) -> Message:
        message = self._decode_message(raw_message)
        pull_return_message = await self.registered_services[message.service_id](
            message
        )
        assert pull_return_message
        return pull_return_message


class Gossip:
    my_node: Node
    local_view: View
    running: bool = False

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
            self.my_node, ProtocolClass=GossipProtocol, multiplex=multiplex
        )

        self.subscribers: List[IGossipSubscriber] = []

    async def start(self):
        if self.running:
            raise RuntimeError("Already running Gossip")
        await self.rpc.register_service(self.service_id, self._reactive_process)
        self._proactive_task = asyncio.create_task(self._proactive_process())
        self.running = True

    async def stop(self):
        await self.rpc.unregister_service(self.service_id)

        if self._proactive_task:
            self._proactive_task.cancel()
            try:
                await self._proactive_task
            except asyncio.CancelledError:
                pass
        self.running = False

    def subscribe(self, subscriber: IGossipSubscriber):
        self.subscribers.append(subscriber)

    def unsubscribe(self, subscriber: IGossipSubscriber):
        self.subscribers.remove(subscriber)

    async def _proactive_process(self):
        while True:
            await asyncio.sleep(self.gossiping_frequency)
            peer = self._select_peer(self.local_view)
            if not peer:  # Empty Local view
                continue
            push_view = Counter()  # if PULL, empty view
            if self.view_propagation is not ViewPropagationPolicy.PULL:
                my_descriptor = Counter({self.my_node: 0})
                push_view = self._merge(my_descriptor, self.local_view)
            subscribers_data = await self._retrieve_from_subscribers()
            data = {DATA_FIELD_VIEW: push_view, **subscribers_data}
            push_message = Message(self.my_node, self.service_id, data)
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
                view = self._increase_hop_count(view)
                buffer = self._merge(view, self.local_view)
                if self.get_external_view:
                    buffer = self._merge(view, self.get_external_view())

                new_view = self._select_view(buffer)
                await self._try_call_callback(new_view)
                self.local_view = new_view

    async def _reactive_process(self, message: Message) -> Union[None, Message]:
        view = _decode_view(message.data[DATA_FIELD_VIEW])
        view = self._increase_hop_count(view)
        pull_return_message = None
        if self.view_propagation is not ViewPropagationPolicy.PUSH:
            my_descriptor = Counter({self.my_node: 0})
            subscribers_data = await self._retrieve_from_subscribers()
            data = {
                DATA_FIELD_VIEW: self._merge(my_descriptor, self.local_view),
                **subscribers_data,
            }
            pull_return_message = Message(self.my_node, self.service_id, data)
        buffer = self._merge(view, self.local_view)
        if self.get_external_view:
            buffer = self._merge(view, self.get_external_view())

        new_view = self._select_view(buffer)
        await self._try_call_callback(new_view)
        self.local_view = new_view

        await self._deliver_to_subscribers(message)
        return pull_return_message

    def _select_peer(self, view: View) -> Optional[Node]:
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

    def _select_view(self, view: View) -> View:
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

    def _increase_hop_count(self, view: View) -> View:
        return view + Counter(view.keys())

    def _merge(self, view1: View, view2: View) -> View:
        all_nodes = set(list(view1.keys()) + list(set(view2.keys())))
        merged_view: Counter = Counter()
        for node in all_nodes:
            merged_view[node] = (
                view1[node]
                if view1.get(node, math.inf) < view2.get(node, math.inf)
                else view2[node]
            )
        return merged_view

    async def _retrieve_from_subscribers(self):
        data: Dict = {}
        for subscriber in self.subscribers:
            key, value = await subscriber.get_data()
            data[key] = value
        return data

    async def _deliver_to_subscribers(self, message: Message):
        for subscriber in self.subscribers:
            await subscriber.new_message(message)

    async def _try_call_callback(self, new_view):
        if set(new_view.keys()) != set(self.local_view.keys()):
            if self.local_view_callback:
                await self.local_view_callback(new_view)


def _decode_view(raw_view: dict) -> View:
    return Counter(dict(map(lambda n: (Node(*n[0]), n[1]), raw_view.items())))

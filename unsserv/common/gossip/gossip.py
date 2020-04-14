import asyncio
import math
import random
from abc import ABC, abstractmethod
from collections import Counter
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Set, Tuple

from unsserv.common.gossip.config import (
    GOSSIPING_FREQUENCY,
    LOCAL_VIEW_SIZE,
)
from unsserv.common.gossip.protocol import GossipProtocol
from unsserv.common.gossip.structs import (
    PushData,
    ViewSelectionPolicy,
    PeerSelectionPolicy,
    ViewPropagationPolicy,
)
from unsserv.common.gossip.typing import ExternalViewSource, CustomSelectionRanking
from unsserv.common.gossip.typing import Payload, View
from unsserv.common.structs import Node
from unsserv.common.typing import Handler
from unsserv.common.utils import stop_task, HandlerManager


class IGossipSubscriber(ABC):
    @abstractmethod
    async def receive_payload(self, payload: Payload):
        pass

    @abstractmethod
    async def get_payload(self) -> Tuple[Any, Any]:
        pass


class Gossip:
    my_node: Node
    local_view: View
    running: bool = False
    _protocol: GossipProtocol
    _handler_manager: HandlerManager

    def __init__(
        self,
        my_node: Node,
        service_id: Any,
        local_view_nodes: List[Node] = None,
        local_view_handler: Handler = None,
        external_nodes_source: ExternalViewSource = None,
        view_selection=ViewSelectionPolicy.HEAD,
        peer_selection=PeerSelectionPolicy.RAND,
        view_propagation=ViewPropagationPolicy.PUSHPULL,
        custom_selection_ranking: CustomSelectionRanking = None,
        local_view_size: int = LOCAL_VIEW_SIZE,
        gossiping_frequency: float = GOSSIPING_FREQUENCY,
    ):
        self.my_node = my_node
        self.service_id = service_id
        self.local_view = Counter(
            random.sample(local_view_nodes, min(len(local_view_nodes), local_view_size))
            if local_view_nodes
            else []
        )
        self.get_external_nodes = external_nodes_source
        self.view_selection_policy = view_selection
        self.peer_selection_policy = peer_selection
        self.view_propagation_policy = view_propagation
        self.custom_selection_ranking = custom_selection_ranking

        self.local_view_size = local_view_size
        self.gossiping_frequency = gossiping_frequency
        self._protocol = GossipProtocol(self.my_node)

        self.subscribers: List[IGossipSubscriber] = []
        self._handler_manager = HandlerManager()
        if local_view_handler:
            self._handler_manager.add_handler(local_view_handler)

    async def start(self):
        if self.running:
            raise RuntimeError("Already running Gossip")
        await self._initialize_protocol()
        self._gossip_task = asyncio.create_task(self._gossip_loop())
        self.running = True

    async def stop(self):
        if not self.running:
            return
        await self._protocol.stop()
        if self._gossip_task:
            await stop_task(self._gossip_task)
        self.running = False

    def subscribe(self, subscriber: IGossipSubscriber):
        self.subscribers.append(subscriber)

    def unsubscribe(self, subscriber: IGossipSubscriber):
        self.subscribers.remove(subscriber)

    async def _gossip_loop(self):
        while True:
            old_neighbours = set(self.local_view.keys())
            await asyncio.sleep(self.gossiping_frequency)
            await self._exchange_with_peer()
            self._call_handler_if_view_changed(old_neighbours)

    async def _exchange_with_peer(self):
        peer = self._select_peer(self.local_view)
        if not peer:  # Empty Local view
            return
        subscribers_data = await self._get_data_from_subscribers()
        if self.view_propagation_policy is ViewPropagationPolicy.PUSH:
            push_view = self._merge_views(Counter({self.my_node: 0}), self.local_view)
            push_data = PushData(view=push_view, payload=subscribers_data)
            with self._pop_on_connection_error(peer):
                await self._protocol.push(peer, push_data)
        elif self.view_propagation_policy is ViewPropagationPolicy.PULL:
            with self._pop_on_connection_error(peer):
                pull_response = await self._protocol.pull(peer, subscribers_data)
                await self._handler_push(peer, pull_response)
        elif self.view_propagation_policy is ViewPropagationPolicy.PUSHPULL:
            push_view = self._merge_views(Counter({self.my_node: 0}), self.local_view)
            push_data = PushData(view=push_view, payload=subscribers_data)
            with self._pop_on_connection_error(peer):
                pull_response = await self._protocol.pushpull(peer, push_data)
                await self._handler_push(peer, pull_response)

    def _select_peer(self, view: View) -> Optional[Node]:
        if self.custom_selection_ranking:
            ordered_nodes = self.custom_selection_ranking(view)
        else:
            ordered_nodes = list(map(lambda n: n[0], reversed(view.most_common())))
        if self.peer_selection_policy is PeerSelectionPolicy.RAND:
            return random.choice(ordered_nodes) if view else None
        elif self.peer_selection_policy is PeerSelectionPolicy.HEAD:
            return ordered_nodes[0] if view else None
        elif self.peer_selection_policy is PeerSelectionPolicy.TAIL:
            return ordered_nodes[-1] if view else None
        raise AttributeError("Invalid Peer Selection policy")

    def _select_view(self, view: View) -> View:
        # todo: document what happens here
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
        if self.view_selection_policy is ViewSelectionPolicy.RAND:
            for node in random.sample(ordered_nodes, remove_amount):
                new_view.pop(node)
        elif self.view_selection_policy is ViewSelectionPolicy.HEAD:
            for node in ordered_nodes[-remove_amount:]:
                new_view.pop(node)
        elif self.view_selection_policy is ViewSelectionPolicy.TAIL:
            for node in ordered_nodes[:remove_amount]:
                new_view.pop(node)
        else:
            raise AttributeError("Invalid View Selection policy")
        return new_view

    def _increase_hop_count(self, view: View) -> View:
        return view + Counter(view.keys())

    def _merge_views(self, view1: View, view2: View) -> View:
        all_nodes = set(list(view1.keys()) + list(set(view2.keys())))
        merged_view: Counter = Counter()
        for node in all_nodes:
            merged_view[node] = (
                view1[node]
                if view1.get(node, math.inf) < view2.get(node, math.inf)
                else view2[node]
            )
        return merged_view

    async def _get_data_from_subscribers(self):
        data: Dict = {}
        for subscriber in self.subscribers:
            key, value = await subscriber.get_payload()
            data[key] = value
        return data

    async def _deliver_message_to_subscribers(self, gossip_payload: Payload):
        for subscriber in self.subscribers:
            await subscriber.receive_payload(gossip_payload)

    def _call_handler_if_view_changed(self, old_neighbours: Set):
        current_neighbours = set(self.local_view.keys())
        if old_neighbours == current_neighbours:
            return
        self._handler_manager.call_handlers(self.local_view)

    async def _handler_push(self, sender: Node, push_data: PushData):
        view = self._increase_hop_count(push_data.view)
        buffer = self._merge_views(view, self.local_view)
        if self.get_external_nodes:
            buffer = self._merge_views(view, Counter(self.get_external_nodes()))
        new_view = self._select_view(buffer)
        self.local_view = new_view
        await self._deliver_message_to_subscribers(push_data.payload)

    async def _handler_pull(self, sender: Node, payload: Payload) -> PushData:
        my_descriptor = Counter({self.my_node: 0})
        subscribers_data = await self._get_data_from_subscribers()
        # todo: deliver message to subscribers
        view = self._merge_views(my_descriptor, self.local_view)
        return PushData(view=view, payload=subscribers_data)

    async def _handler_pushpull(self, sender: Node, push_data: PushData):
        pull_response = await self._handler_pull(sender, push_data.payload)
        await self._handler_push(sender, push_data)
        return pull_response

    async def _initialize_protocol(self):
        self._protocol.set_handler_push(self._handler_push)
        self._protocol.set_handler_pull(self._handler_pull)
        self._protocol.set_handler_pushpull(self._handler_pushpull)
        await self._protocol.start(self.service_id)

    @contextmanager
    def _pop_on_connection_error(self, node: Node):
        try:
            yield
        except ConnectionError:
            if node in self.local_view:
                self.local_view.pop(node)

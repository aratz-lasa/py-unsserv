import asyncio
import math
import random
from collections import Counter
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

from unsserv.common.gossip.config import (
    GOSSIPING_FREQUENCY,
    LOCAL_VIEW_SIZE,
)
from unsserv.common.gossip.policies import (
    ViewSelectionPolicy,
    PeerSelectionPolicy,
    ViewPropagationPolicy,
)
from unsserv.common.gossip.protocol import GossipProtocol
from unsserv.common.gossip.structs import PushData
from unsserv.common.gossip.subcriber_abc import IGossipSubscriber
from unsserv.common.gossip.typing import (
    ExternalViewSource,
    CustomSelectionRanking,
    LocalViewCallback,
)
from unsserv.common.gossip.typing import Payload
from unsserv.common.services_abc import View
from unsserv.common.structs import Node
from unsserv.common.utils import stop_task


class Gossip:
    _protocol: GossipProtocol
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
    ):
        self.my_node = my_node
        self.service_id = service_id
        self.local_view = Counter(
            random.sample(local_view_nodes, min(len(local_view_nodes), local_view_size))
            if local_view_nodes
            else []
        )
        self.local_view_callback = local_view_callback
        self.get_external_view = external_view_source
        self.view_selection = view_selection
        self.peer_selection = peer_selection
        self.view_propagation = view_propagation
        self.custom_selection_ranking = custom_selection_ranking

        self.local_view_size = local_view_size
        self.gossiping_frequency = gossiping_frequency
        self._protocol = GossipProtocol(self.my_node)

        self.subscribers: List[IGossipSubscriber] = []

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
            await asyncio.sleep(self.gossiping_frequency)
            peer = self._select_peer(self.local_view)
            if not peer:  # Empty Local view
                continue
            subscribers_data = await self._get_data_from_subscribers()
            if self.view_propagation is ViewPropagationPolicy.PUSH:
                push_view = self._merge_views(
                    Counter({self.my_node: 0}), self.local_view
                )
                push_data = PushData(view=push_view, payload=subscribers_data)
                with self._pop_on_connection_error(peer):
                    await self._protocol.push(peer, push_data)
            elif self.view_propagation is ViewPropagationPolicy.PULL:
                with self._pop_on_connection_error(peer):
                    pull_response = await self._protocol.pull(peer, subscribers_data)
                    await self._handler_push(peer, pull_response)
            elif self.view_propagation is ViewPropagationPolicy.PUSHPULL:
                push_view = self._merge_views(
                    Counter({self.my_node: 0}), self.local_view
                )
                push_data = PushData(view=push_view, payload=subscribers_data)
                with self._pop_on_connection_error(peer):
                    pull_response = await self._protocol.pushpull(peer, push_data)
                    await self._handler_push(peer, pull_response)

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

    async def _try_call_callback(self, new_view):
        if set(new_view.keys()) != set(self.local_view.keys()):
            if self.local_view_callback:
                await self.local_view_callback(new_view)

    async def _handler_push(self, sender: Node, push_data: PushData):
        view = self._increase_hop_count(push_data.view)
        buffer = self._merge_views(view, self.local_view)
        if self.get_external_view:
            buffer = self._merge_views(view, self.get_external_view())
        new_view = self._select_view(buffer)
        await self._try_call_callback(new_view)
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

import asyncio
from collections import OrderedDict
from typing import Any, Optional, Set, OrderedDict as OrderedDictType

from unsserv.common.services_abc import IDisseminationService, IMembershipService
from unsserv.common.structs import Property, Node
from unsserv.common.typing import Handler
from unsserv.common.utils import HandlersManager, get_random_id, stop_task
from unsserv.stable.dissemination.many_to_many.config import PlumtreeConfig
from unsserv.stable.dissemination.many_to_many.protocol import PlumtreeProtocol
from unsserv.stable.dissemination.many_to_many.structs import Push
from unsserv.stable.dissemination.many_to_many.typing import (
    PlumDataId,
    PlumData,
    Digest,
)


class Plumtree(IDisseminationService):
    properties = {Property.STABLE, Property.ONE_TO_MANY, Property.SYMMETRIC}
    _protocol: PlumtreeProtocol
    _handlers_manager: HandlersManager
    _config: PlumtreeConfig

    _eager_push_peers: Set[Node]
    _lazy_push_peers: Set[Node]
    _received_data: OrderedDictType[PlumDataId, PlumData]
    _digest: Set[PlumDataId]
    _maintenance_task: asyncio.Task

    def __init__(self, membership: IMembershipService):
        if Property.SYMMETRIC not in membership.properties:
            raise ValueError("Membership must be SYMMETRIC")
        self.membership = membership
        self.my_node = membership.my_node

        self._protocol = PlumtreeProtocol(self.my_node)
        self._handlers_manager = HandlersManager()
        self._config = PlumtreeConfig()

        self._eager_push_peers = set()
        self._lazy_push_peers = set()
        self._received_data = OrderedDict()
        self._digest = set()

    async def join(self, service_id: Any, **configuration: Any):
        if self.running:
            raise RuntimeError("Service already running")
        self.service_id = service_id
        if "broadcast_handler" in configuration:
            self._handlers_manager.add_handler(configuration["broadcast_handler"])
        await self._initialize_protocol()
        self._config.load_from_dict(configuration)
        self._maintenance_task = asyncio.create_task(self._maintenance_loop())
        self.running = True

    async def leave(self):
        if not self.running:
            return
        await stop_task(self._maintenance_task)
        await self._protocol.stop()
        self._handlers_manager.remove_all_handlers()
        self.running = False

    async def broadcast(self, data: bytes):
        data_id = get_random_id()
        push = Push(data=data, data_id=data_id)
        self._received_data[data_id] = data
        await self._forward_push(push, self.my_node)

    def add_broadcast_handler(self, handler: Handler):
        self._handlers_manager.add_handler(handler)

    def remove_broadcast_handler(self, handler: Handler):
        self._handlers_manager.remove_handler(handler)

    async def _maintenance_loop(self):
        while True:
            await asyncio.sleep(self._config.MAINTENANCE_SLEEP)
            while self._config.BUFFER_LIMIT < len(self._received_data):
                self._received_data.popitem(last=False)
            self._update_peers()
            asyncio.create_task(self._forward_digest())

    def _update_peers(self):
        old_peers_set = self._eager_push_peers.union(self._lazy_push_peers)
        current_peers_set = set(self.membership.get_neighbours())
        for peer in old_peers_set - current_peers_set:
            if peer in self._eager_push_peers:
                self._eager_push_peers.remove(peer)
            elif peer in self._lazy_push_peers:
                self._lazy_push_peers.remove(peer)
        self._eager_push_peers.update(current_peers_set - old_peers_set)

    async def _forward_push(self, push: Push, sender: Node):
        for peer in self._eager_push_peers.copy():
            if peer != sender:
                await self._protocol.push(peer, push)

    async def _forward_digest(self):
        for peer in self._lazy_push_peers.copy():
            await self._protocol.ihave(peer, list(self._digest))

    def _add_new_data(self, push: Push, sender: Node):
        self._received_data[push.data_id] = push.data
        self._digest.add(push.data_id)
        self._handlers_manager.call_handlers(push.data)
        asyncio.create_task(self._forward_push(push, sender))

    async def _retrieve_unreceived_data(self, sender: Node, data_id: PlumDataId):
        await asyncio.sleep(self._config.RETRIEVE_TIMEOUT)
        if data_id not in self._digest:
            data = await self._protocol.get_data(sender, data_id)
            if data:
                self._add_new_data(Push(data=data, data_id=data_id), sender)

    def _make_lazy_peer(self, peer: Node):
        if peer in self._eager_push_peers:
            self._eager_push_peers.remove(peer)
        self._lazy_push_peers.add(peer)

    async def _handler_push(self, sender: Node, push: Push):
        if push.data_id in self._digest:
            self._make_lazy_peer(sender)
            asyncio.create_task(self._protocol.prune(sender))
        else:
            self._add_new_data(push, sender)

    async def _handler_ihave(self, sender: Node, digest: Digest):
        for data_id in digest:
            if data_id not in self._received_data:
                asyncio.create_task(self._retrieve_unreceived_data(sender, data_id))

    async def _handler_get_data(
        self, sender: Node, data_id: PlumDataId
    ) -> Optional[PlumData]:
        return self._received_data.get(data_id, None)

    async def _handler_new_neighbour(self, sender: Node):
        self._eager_push_peers.add(sender)

    async def _handler_prune(self, sender: Node):
        self._make_lazy_peer(sender)

    async def _initialize_protocol(self):
        self._protocol.set_handler_push(self._handler_push)
        self._protocol.set_handler_ihave(self._handler_ihave)
        self._protocol.set_handler_get_data(self._handler_get_data)
        self._protocol.set_handler_prune(self._handler_prune)
        await self._protocol.start(self.service_id)

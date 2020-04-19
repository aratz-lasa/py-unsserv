import asyncio
from typing import Any, Optional

from unsserv.common.services_abc import IDisseminationService, IMembershipService
from unsserv.common.structs import Property, Node
from unsserv.common.typing import Handler
from unsserv.common.utils import HandlersManager, stop_task
from unsserv.stable.dissemination.brisa.config import BrisaConfig
from unsserv.stable.dissemination.brisa.protocol import BrisaProtocol
from unsserv.stable.dissemination.brisa.structs import Push
from unsserv.stable.dissemination.brisa.typing import BrisaDataId, BrisaData, Digest


class Brisa(IDisseminationService):
    properties = {Property.STABLE, Property.MANY_TO_MANY}
    _protocol: BrisaProtocol
    _handlers_manager: HandlersManager
    _config: BrisaConfig

    _maintenance_task: asyncio.Task

    def __init__(self, membership: IMembershipService):
        if Property.SYMMETRIC not in membership.properties:
            raise ValueError("Membership must be SYMMETRIC")
        self.membership = membership
        self.my_node = membership.my_node

        self._protocol = BrisaProtocol(self.my_node)
        self._handlers_manager = HandlersManager()
        self._config = BrisaConfig()

    async def join(self, service_id: Any, **configuration: Any):
        if self.running:
            raise RuntimeError("Service already running")
        self.service_id = service_id
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
        pass  # todo

    def add_broadcast_handler(self, handler: Handler):
        self._handlers_manager.add_handler(handler)

    def remove_broadcast_handler(self, handler: Handler):
        self._handlers_manager.remove_handler(handler)

    async def _maintenance_loop(self):
        while True:
            await asyncio.sleep(self._config.MAINTENANCE_SLEEP)
            pass  # todo

    def _update_peers(self):
        pass  # todo

    async def _handler_push(self, sender: Node, push: Push):
        pass  # todo

    async def _handler_ihave(self, sender: Node, digest: Digest):
        pass  # todo

    async def _handler_get_data(
        self, sender: Node, data_id: BrisaDataId
    ) -> Optional[BrisaData]:
        pass  # todo

    async def _handler_new_neighbour(self, sender: Node):
        pass  # todo

    async def _handler_prune(self, sender: Node):
        pass  # todo

    async def _initialize_protocol(self):
        self._protocol.set_handler_push(self._handler_push)
        self._protocol.set_handler_ihave(self._handler_ihave)
        self._protocol.set_handler_get_data(self._handler_get_data)
        self._protocol.set_handler_prune(self._handler_prune)
        await self._protocol.start(self.service_id)

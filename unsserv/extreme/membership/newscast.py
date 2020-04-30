from typing import Any, List, Optional

from unsserv.common.gossip.gossip import Gossip, View
from unsserv.common.services_abc import IMembershipService
from unsserv.common.structs import Node, Property
from unsserv.common.typing import Handler
from unsserv.common.utils import HandlersManager


class Newscast(IMembershipService):
    properties = {Property.EXTREME, Property.HAS_GOSSIP, Property.NON_SYMMETRIC}
    gossip: Optional[Gossip]
    _handlers_manager: HandlersManager

    def __init__(self, node: Node):
        self.my_node = node
        self.gossip = None
        self._handlers_manager = HandlersManager()

    async def join(self, service_id: Any, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Membership")
        self.service_id = service_id
        self.gossip = Gossip(
            my_node=self.my_node,
            service_id=service_id,
            local_view_nodes=configuration.get("bootstrap_nodes", None),
            local_view_handler=self._gossip_local_view_handler,
        )
        await self.gossip.start()
        self.running = True

    async def leave(self):
        if not self.running:
            return
        await self.gossip.stop()
        self.gossip = None
        self.running = False

    def get_neighbours(self) -> List[Node]:
        if not self.running:
            raise RuntimeError("Membership service not running")
        return list(self.gossip.local_view.keys())

    def add_neighbours_handler(self, handler: Handler):
        self._handlers_manager.add_handler(handler)

    def remove_neighbours_handler(self, handler: Handler):
        self._handlers_manager.remove_handler(handler)

    async def _gossip_local_view_handler(self, local_view: View):
        self._handlers_manager.call_handlers(list(local_view.keys()))

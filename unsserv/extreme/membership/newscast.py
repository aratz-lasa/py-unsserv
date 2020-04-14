from typing import Any, List, Union, Optional

from unsserv.common.service_properties import Property
from unsserv.common.services_abc import MembershipService
from unsserv.common.typing import Handler
from unsserv.common.structs import Node
from unsserv.common.gossip.gossip import Gossip, View
from unsserv.common.utils import HandlerManager


class Newscast(MembershipService):
    properties = {Property.EXTREME, Property.HAS_GOSSIP, Property.NON_SYMMETRIC}
    gossip: Optional[Gossip]
    _handler_manager: HandlerManager

    def __init__(self, node: Node):
        self.my_node = node
        self.gossip = None
        self._handler_manager = HandlerManager()

    async def join(self, service_id: Any, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Membership")
        self.service_id = service_id
        self.gossip = Gossip(
            my_node=self.my_node,
            service_id=service_id,
            local_view_nodes=configuration.get("bootstrap_nodes", None),
            local_view_callback=self._local_view_callback,
        )
        await self.gossip.start()
        self.running = True

    async def leave(self):
        if not self.running:
            return
        await self.gossip.stop()
        self.gossip = None
        self.running = False

    def get_neighbours(
        self, local_view_format: bool = False
    ) -> Union[List[Node], View]:
        if not self.running:
            raise RuntimeError("Membership service not running")
        if local_view_format:
            return self.gossip.local_view
        return list(self.gossip.local_view.keys())

    def add_neighbours_handler(self, handler: Handler):
        self._handler_manager.add_handler(handler)

    def remove_neighbours_handler(self, handler: Handler):
        self._handler_manager.remove_handler(handler)

    async def _local_view_callback(self, local_view: View):
        self._handler_manager.call_handlers(list(local_view.keys()))

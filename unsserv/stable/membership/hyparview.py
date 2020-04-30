from typing import List, Any, Optional

from unsserv.common.gossip.gossip import Gossip
from unsserv.common.services_abc import IMembershipService
from unsserv.common.structs import Node, Property
from unsserv.common.typing import Handler
from unsserv.stable.membership.double_layered.double_layered import IDoubleLayered
from unsserv.stable.membership.double_layered.config import DoubleLayeredConfig


class HyParView(IMembershipService, IDoubleLayered):
    properties = {Property.STABLE, Property.SYMMETRIC, Property.HAS_GOSSIP}
    gossip: Optional[Gossip]

    def __init__(self, my_node: Node):
        super().__init__(my_node)
        self.gossip = None
        self._config = DoubleLayeredConfig()

    async def join(self, service_id: Any, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Membership")
        self.service_id = service_id
        self.gossip = Gossip(
            my_node=self.my_node,
            service_id=f"gossip-{service_id}",
            local_view_nodes=configuration.get("bootstrap_nodes", None),
        )
        await self.gossip.start()
        await self._start_two_layered(f"double_layered-{service_id}")
        self.running = True

    async def leave(self):
        if not self.running:
            return
        await self.gossip.stop()
        await self._stop_two_layered()
        self.running = False

    def get_neighbours(self) -> List[Node]:
        return list(self._active_view)

    def add_neighbours_handler(self, handler: Handler):
        if not self.running:
            raise RuntimeError("Membership service not running")
        self._handlers_manager.add_handler(handler)

    def remove_neighbours_handler(self, handler: Handler):
        self._handlers_manager.remove_handler(handler)

    def _get_passive_view_nodes(self):
        return list(self.gossip.local_view.keys())

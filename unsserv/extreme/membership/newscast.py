from typing import Any, List, Union, Optional

from unsserv.common.service_properties import Property
from unsserv.common.services_abc import MembershipService, NeighboursCallback
from unsserv.common.structs import Node
from unsserv.common.gossip.gossip import Gossip, View


class Newscast(MembershipService):
    properties = {Property.EXTREME, Property.HAS_GOSSIP, Property.NON_SYMMETRIC}
    gossip: Optional[Gossip]
    _callbacks: List[NeighboursCallback]

    def __init__(self, node: Node):
        self.my_node = node
        self._callbacks = []
        self.gossip = None

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

    def add_neighbours_callback(self, callback: NeighboursCallback):
        if not self.running:
            raise RuntimeError("Membership service not running")
        self._callbacks.append(callback)

    def remove_neighbours_callback(self, callback: NeighboursCallback):
        if callback not in self._callbacks:
            raise ValueError("Callback not found")
        self._callbacks.remove(callback)

    async def _local_view_callback(self, local_view: View):
        for callback in self._callbacks:
            await callback(list(local_view.keys()))

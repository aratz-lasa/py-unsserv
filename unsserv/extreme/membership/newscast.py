from typing import Union, Any, List

from unsserv.common.api import MembershipService, NeighboursCallback
from unsserv.common.gossip.gossip import Gossip, View
from unsserv.common.data_structures import Node


class Newscast(MembershipService):
    my_node: Node
    _multiplex: bool
    _callback: NeighboursCallback

    def __init__(self, node: Node, multiplex: bool = True):
        self.my_node = node
        self._multiplex = multiplex
        self._callback = None
        self._callback_raw_format = False
        self._gossip: Union[Gossip, None] = None

    async def join(self, service_id: Any, bootstrap_nodes: List[Node] = None):
        if self.running:
            raise RuntimeError("Already running membership")
        self.service_id = service_id
        self._gossip = Gossip(
            my_node=self.my_node,
            service_id=service_id,
            local_view_nodes=bootstrap_nodes,
            local_view_callback=self._local_view_callback,
            multiplex=self._multiplex,
        )
        await self._gossip.start()
        self.running = True

    async def leave(self) -> None:
        await self._gossip.stop()
        self._gossip = None
        self.running = False

    def get_neighbours(self, local_view: bool = False) -> Union[List[Node], View]:
        if not self._gossip:
            raise RuntimeError("Membership not joined")
        if local_view:
            return self._gossip.local_view
        return list(self._gossip.local_view.keys())

    def set_neighbours_callback(
        self, callback: NeighboursCallback, local_view: bool = False
    ) -> None:
        self._callback = callback
        self._callback_raw_format = local_view

    async def _local_view_callback(self, local_view: View):
        if self._callback:
            if self._callback_raw_format:
                await self._callback(local_view)
            else:
                await self._callback(list(local_view.keys()))

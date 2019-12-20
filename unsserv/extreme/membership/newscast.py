from typing import Union, Any, List, Dict

from unsserv.api import MembershipService, NeighboursCallback
from unsserv.common.gossip.gossip import Gossip, View
from unsserv.data_structures import Node


class Newscast(MembershipService):
    _gossip: Gossip
    _registered_gossips: Dict[Any, Gossip]

    def __init__(self, node: Node, multiplex: bool = False):
        super().__init__(node, multiplex)
        self._registered_gossips = {}
        self._callback_raw_format = False

    async def join_membership(
        self, service_id: Any = None, bootstrap_nodes: List[Node] = None
    ):
        if self._gossip.is_running():
            raise RuntimeError("Already joined a membership")
        self._gossip = Gossip(
            my_node=self.my_node,
            service_id=service_id,
            local_view_nodes=bootstrap_nodes,
            local_view_callback=self._local_view_callback,
            multiplex=self.multiplex,
        )
        await self._gossip.start()

    async def leave_membership(self) -> None:
        await self._gossip.stop()

    def get_neighbours(self, local_view: bool = False) -> Union[List[Node], View]:
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

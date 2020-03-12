from typing import Any, List, Union, Optional

from unsserv.common.services_abc import MembershipService, NeighboursCallback
from unsserv.common.data_structures import Node
from unsserv.common.gossip.gossip import Gossip, View


class Newscast(MembershipService):
    _multiplex: bool
    _callback: NeighboursCallback
    _gossip: Optional[Gossip]

    def __init__(self, node: Node, multiplex: bool = True):
        self.my_node = node
        self._multiplex = multiplex
        self._callback = None
        self._callback_raw_format = False
        self._gossip = None

    async def join(self, service_id: Any, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Membership")
        self.service_id = service_id
        self._gossip = Gossip(
            my_node=self.my_node,
            service_id=service_id,
            local_view_nodes=configuration.get("bootstrap_nodes", None),
            local_view_callback=self._local_view_callback,
            multiplex=self._multiplex,
        )
        await self._gossip.start()
        self.running = True

    async def leave(self) -> None:
        if not self.running:
            return
        await self._gossip.stop()
        self._gossip = None
        self.running = False

    def get_neighbours(
        self, local_view_format: bool = False
    ) -> Union[List[Node], View]:
        if not self.running:
            raise RuntimeError("Membership service not running")
        if local_view_format:
            return self._gossip.local_view
        return list(self._gossip.local_view.keys())

    def set_neighbours_callback(
        self, callback: NeighboursCallback, local_view_format: bool = False
    ) -> None:
        if not self.running:
            raise RuntimeError("Membership service not running")
        self._callback = callback
        self._callback_raw_format = local_view_format

    async def _local_view_callback(self, local_view: View):
        if self._callback:
            if self._callback_raw_format:
                await self._callback(local_view)
            else:
                await self._callback(list(local_view.keys()))

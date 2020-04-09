from collections import Counter
from typing import Union, List, Any, Optional

from unsserv.common.gossip.gossip import Gossip
from unsserv.common.services_abc import MembershipService
from unsserv.common.service_properties import Property
from unsserv.common.structs import Node
from unsserv.common.typing import NeighboursCallback, View
from unsserv.stable.membership.double_layered.double_layered import IDoubleLayered


class HyParView(MembershipService, IDoubleLayered):
    properties = {Property.STABLE, Property.SYMMETRIC, Property.HAS_GOSSIP}
    gossip: Optional[Gossip]
    _callback: NeighboursCallback
    _callback_raw_format: bool

    def __init__(self, my_node: Node):
        super().__init__(my_node)
        self.gossip = None
        self._callback = None
        self._callback_raw_format = False

    async def join(self, service_id: Any, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Membership")
        self.service_id = service_id
        self.gossip = Gossip(
            my_node=self.my_node,
            service_id=f"gossip-{service_id}",
            local_view_nodes=configuration.get("bootstrap_nodes", None),
            local_view_callback=self._local_view_callback,
        )
        await self.gossip.start()
        await self._start_two_layered(f"double_layered-{service_id}")
        self.running = True

    async def leave(self) -> None:
        if not self.running:
            return
        await self.gossip.stop()
        await self._stop_two_layered()
        self.running = False

    def get_neighbours(
        self, local_view_format: bool = False
    ) -> Union[List[Node], View]:
        return (
            Counter(self._active_view) if local_view_format else list(self._active_view)
        )

    def set_neighbours_callback(
        self, callback: NeighboursCallback, local_view_format: bool = False
    ) -> None:
        if not self.running:
            raise RuntimeError("Memberhsip service not running")
        self._callback = callback
        self._callback_raw_format = local_view_format

    async def _local_view_callback(self, local_view: View):
        if self._callback:
            if self._callback_raw_format:
                await self._callback(local_view)
            else:
                await self._callback(list(local_view.keys()))

    def _get_passive_view_nodes(self):
        return list(self.gossip.local_view.keys())

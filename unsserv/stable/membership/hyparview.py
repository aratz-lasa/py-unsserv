from typing import Union, List, Any

from unsserv.common.data_structures import Node
from unsserv.common.rpc.rpc import RpcBase, RPC
from unsserv.common.services_abc import MembershipService, ISubService
from unsserv.common.typing import NeighboursCallback, View


class HyParViewProtocol:
    pass  # todo


class HyParViewRPC(RpcBase):
    pass  # todo


class HyParView(MembershipService, ISubService):
    _rpc: HyParViewRPC

    def __init__(self, membership: MembershipService, multiplex: bool = True):
        self.my_node = membership.my_node
        self.membership = membership
        self._rpc = RPC.get_rpc(self.my_node, HyParViewRPC, multiplex)

    async def join(self, service_id: Any, **configuration: Any):
        pass  # todo

    async def leave(self) -> None:
        pass  # todo

    def get_neighbours(
        self, local_view_format: bool = False
    ) -> Union[List[Node], View]:
        pass  # todo

    def set_neighbours_callback(
        self, callback: NeighboursCallback, local_view_format: bool = False
    ) -> None:
        pass  # todo

from typing import Any, Dict

from unsserv.common.data_structures import Message
from unsserv.common.rpc.rpc import RpcBase, RPC
from unsserv.common.services_abc import SearchingService, MembershipService


class KWalkerRPC(RpcBase):
    pass  # todo


class KWalker(SearchingService):
    _search_data: Dict[str, bytes]
    _rpc: KWalkerRPC

    def __init__(self, membership: MembershipService, multiplex: bool = True):
        self.membership = membership
        self.my_node = membership.my_node
        self._search_data = {}
        self._rpc = RPC.get_rpc(
            self.my_node, ProtocolClass=KWalkerRPC, multiplex=multiplex
        )

    async def join_searching(self, service_id: str) -> None:
        if self.running:
            raise RuntimeError("Already running Searching service")
        self._search_data = {}
        self.service_id = service_id
        self._protocol = None  # todo
        # initialize RPC
        await self._rpc.register_service(self.service_id, self._handle_rpc)
        self.running = True

    async def leave_searching(self) -> None:
        if not self.running:
            return
        self._protocol = None
        await self._rpc.unregister_service(self.service_id)
        self.running = False

    async def publish(self, data_id: str, data: bytes) -> None:
        if not self.running:
            raise RuntimeError("Searching service not running")
        if data_id in self._search_data:
            raise KeyError("Data-id already published")
        if not isinstance(data, bytes):
            raise TypeError("Published data must be bytes type")
        if not isinstance(data_id, str):
            raise TypeError("Published data-id must be str type")

        self._search_data[data_id] = data

    async def unpublish(self, data_id: str) -> None:
        if not self.running:
            raise RuntimeError("Searching service not running")
        if data_id not in self._search_data:
            raise KeyError("Data-id not published")
        del self._search_data[data_id]

    async def search(self, id: str) -> Any:
        pass  # todo

    async def _handle_rpc(self, message: Message) -> Any:
        pass  # todo

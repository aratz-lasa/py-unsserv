import inspect
import asyncio
from typing import Callable, Coroutine, Any, Optional

from unsserv.common.api import DisseminationService, MembershipService
from unsserv.common.rpc.rpc import RpcBase, RPC
from unsserv.common.data_structures import Message
from unsserv.extreme.dissemination.mon_config import MON_TIMEOUT

BroadcastHandler = Optional[Callable[[Any], Coroutine[Any, Any, None]]]


class MonRPC(RpcBase):
    pass  # todo


class Mon(DisseminationService):
    _rpc: MonRPC
    _broadcast_handler: BroadcastHandler
    _dissemination_task: asyncio.Task

    def __init__(self, membership: MembershipService, multiplex: bool = True):
        self.my_node = membership.my_node
        self._broadcast_handler = None
        self._rpc = RPC.get_rpc(self.my_node, MonRPC, multiplex=multiplex)

    async def join_broadcast(
        self, service_id: str, *broadcast_configuration: Any
    ) -> None:
        # unpack arguments
        broadcast_handler = broadcast_configuration[0]
        assert inspect.iscoroutinefunction(broadcast_handler)
        timeout = MON_TIMEOUT
        if len(broadcast_handler) == 2:
            timeout = broadcast_configuration[1]
            assert isinstance(timeout, int)
        # initialize dissemination
        if self.running:
            raise RuntimeError("Already running Dissemination")
        self.service_id = service_id
        self._broadcast_handler = broadcast_handler
        await self._rpc.register_service(service_id, self._rpc_handler)
        self._dissemination_task = asyncio.create_task(self._dissemination_process())
        asyncio.create_task(self._dissemination_timeout_process(timeout))
        self.running = True

    async def leave_broadcast(self) -> None:
        if self._dissemination_task:
            self._dissemination_task.cancel()
            try:
                await self._dissemination_task
            except asyncio.CancelledError:
                pass
        await self._rpc.unregister_service(self.service_id)
        self._broadcast_handler = None
        self.running = False

    async def broadcast(self, data: Any) -> None:
        pass  # todo

    async def _rpc_handler(self, message: Message):
        pass  # todo

    async def _dissemination_process(self):
        pass  # todo

    async def _dissemination_timeout_process(self, timeout: int):
        await asyncio.sleep(timeout)
        await self.leave_broadcast()

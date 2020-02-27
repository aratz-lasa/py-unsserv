import inspect
from enum import IntEnum, auto
from typing import Any, List, Dict, Optional
import random

from unsserv.common.errors import ServiceError
from unsserv.common.data_structures import Message, Node
from unsserv.common.rpc.rpc import RPC, RpcBase
from unsserv.common.service_interfaces import DisseminationService, MembershipService
from unsserv.common.utils import get_random_id
from unsserv.extreme.dissemination.mon_config import (
    MON_TIMEOUT,
    DATA_FIELD_COMMAND,
    DATA_FIELD_BROADCAST_ID,
    DATA_FIELD_LEVEL,
    FANOUT,
    DATA_FIELD_BROADCAST_DATA,
)
from unsserv.extreme.dissemination.mon_typing import BroadcastHandler, BroadcastID


class MonCommand(IntEnum):
    SESSION = auto()
    PUSH = auto()


class MonProtocol:
    def __init__(self, my_node: Node, service_id: Any):
        self.my_node = my_node
        self.service_id = service_id

    def make_session_message(self, broadcast_id: str, level: int) -> Message:
        data = {
            DATA_FIELD_COMMAND: MonCommand.SESSION,
            DATA_FIELD_BROADCAST_ID: BroadcastID,
            DATA_FIELD_LEVEL: level,
        }
        return Message(self.my_node, self.service_id, data)

    def make_push_message(self, data: Any) -> Message:
        data = {
            DATA_FIELD_COMMAND: MonCommand.PUSH,
            DATA_FIELD_BROADCAST_ID: BroadcastID,
            DATA_FIELD_BROADCAST_DATA: data,
        }
        return Message(self.my_node, self.service_id, data)


class MonRPC(RpcBase):
    async def call_session(self, destination: Node, message: Message) -> bool:
        rpc_result = await self.session(destination.address_info, message)
        return self._handle_call_response(rpc_result)

    async def call_push(self, destination: Node, message: Message):
        rpc_result = await self.push(destination.address_info, message)
        self._handle_call_response(rpc_result)

    async def rpc_session(self, node: Node, raw_message: List) -> bool:
        message = self._decode_message(raw_message)
        degree = await self.registered_services[message.service_id](message)
        assert isinstance(degree, bool)
        return degree

    async def rpc_push(self, node: Node, raw_message: List):
        message = self._decode_message(raw_message)
        await self.registered_services[message.service_id](message)


class Mon(DisseminationService):
    _rpc: MonRPC
    _broadcast_handler: BroadcastHandler
    _protocol: Optional[MonProtocol]
    _levels: Dict[BroadcastID, int]
    _children: Dict[BroadcastID, List[Node]]
    _parents: Dict[BroadcastID, List[Node]]

    def __init__(self, membership: MembershipService, multiplex: bool = True):
        self.my_node = membership.my_node
        self.membership = membership
        self._broadcast_handler = None
        self._rpc = RPC.get_rpc(self.my_node, MonRPC, multiplex=multiplex)

        self._children = {}
        self._parents = {}
        self._levels = {}

    async def join_broadcast(
        self, service_id: str, *broadcast_configuration: Any
    ) -> None:
        if self.running:
            raise RuntimeError("Already running Dissemination")
        # unpack arguments
        broadcast_handler = broadcast_configuration[0]
        assert inspect.iscoroutinefunction(broadcast_handler)
        root = broadcast_configuration[1]
        assert isinstance(root, bool)
        timeout = MON_TIMEOUT
        if len(broadcast_handler) == 3:
            timeout = broadcast_configuration[2]
            assert isinstance(timeout, int)
        # initialize dissemination
        self.service_id = service_id
        self._broadcast_handler = broadcast_handler
        await self._rpc.register_service(service_id, self._rpc_handler)
        self._protocol = MonProtocol(self.my_node, self.service_id)
        self.running = True

    async def leave_broadcast(self) -> None:
        await self._rpc.unregister_service(self.service_id)
        self._broadcast_handler = None
        self._protocol = None
        self.running = False

    async def broadcast(self, data: Any) -> None:
        if not self.running:
            raise RuntimeError("Dissemination service not running")
        broadcast_id = await self._build_random_tree()
        await self._disseminate(broadcast_id, data)

    async def _rpc_handler(self, message: Message):
        command = message.data[DATA_FIELD_COMMAND]
        if command == MonCommand.SESSION:
            pass  # todo
        elif command == MonCommand.PUSH:
            pass  # todo
        else:
            raise ValueError("Invalid MON protocol value")

    async def _build_random_tree(self) -> str:
        broadcast_id = get_random_id()
        self._levels[broadcast_id] = 0
        neighbours = set(self.membership.get_neighbours())
        assert isinstance(neighbours, list)
        children: List[Node] = []
        fanout = min(len(neighbours), FANOUT)
        message = self._protocol.make_session_message(
            broadcast_id, 0
        )  # only generate once, bc it is the same every time
        while neighbours and len(children) < fanout:
            child = random.choice(neighbours)
            neighbours.remove(child)
            session_ok = await self._rpc.call_session(child, message)
            if not session_ok:
                pass  # todo: what to do
            else:
                children.append(child)
        if len(children) == 0:
            raise ServiceError("Unable to peer with neighbours for disseminating")
        self._children[broadcast_id] = children
        return broadcast_id

    async def _disseminate(self, broadcast_id: str, data: Any):
        message = self._protocol.make_push_message(
            data
        )  # only generate once, bc it is the same every time
        for child in self._children[broadcast_id]:
            await self._rpc.call_push(child, message)

    def _make_message(self, protocol: MonCommand) -> Message:
        pass  # todo

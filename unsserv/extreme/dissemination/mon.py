import asyncio
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
            DATA_FIELD_BROADCAST_ID: broadcast_id,
            DATA_FIELD_LEVEL: level,
        }
        return Message(self.my_node, self.service_id, data)

    def make_push_message(self, broadcast_id: str, data: Any) -> Message:
        data = {
            DATA_FIELD_COMMAND: MonCommand.PUSH,
            DATA_FIELD_BROADCAST_ID: broadcast_id,
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
    _received_data: Dict[
        BroadcastID, Any
    ]  # stores the data received from each broadcast (for avoiding duplicates)
    _children_made_events: Dict[BroadcastID, asyncio.Event]

    def __init__(self, membership: MembershipService, multiplex: bool = True):
        self.my_node = membership.my_node
        self.membership = membership
        self._broadcast_handler = None
        self._rpc = RPC.get_rpc(self.my_node, MonRPC, multiplex=multiplex)

        self._children = {}
        self._parents = {}
        self._levels = {}
        self._received_data = {}

        self._children_made_events = {}

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

    async def _rpc_handler(self, message: Message) -> Any:
        command = message.data[DATA_FIELD_COMMAND]
        broadcast_id = message.data[BroadcastID]
        if command == MonCommand.SESSION:
            first_time = broadcast_id in self._levels
            if first_time:
                self._parents[broadcast_id] = [message.node]
                self._levels[broadcast_id] = message.data[DATA_FIELD_LEVEL] + 1
                candidate_children = list(
                    set(self.membership.get_neighbours()) - {message.node}
                )
                asyncio.create_task(
                    self._make_children(broadcast_id, candidate_children)
                )
                self._children_made_events[broadcast_id] = asyncio.Event()
            else:
                if message.data[DATA_FIELD_LEVEL] < self._levels[broadcast_id]:
                    return False
                if (
                    message.node not in self._parents[broadcast_id]
                ):  # just in case it is a duplicate
                    self._parents[broadcast_id].append(message.node)
            return True
        elif command == MonCommand.PUSH:
            broadcast_data = message.data[DATA_FIELD_BROADCAST_DATA]
            if (
                broadcast_id not in self._received_data
            ):  # if already received data, ignores it
                self._received_data[broadcast_id] = broadcast_data
                asyncio.create_task(self._broadcast_handler(broadcast_data))
                asyncio.create_task(self._disseminate(broadcast_id, broadcast_data))
                # todo: decide how to cleanup all the data from already ended broadcast
        else:
            raise ValueError("Invalid MON protocol value")

    async def _build_random_tree(self) -> str:
        broadcast_id = get_random_id()
        self._levels[broadcast_id] = 0
        candidate_children = self.membership.get_neighbours()
        assert isinstance(candidate_children, list)
        await self._make_children(
            broadcast_id, candidate_children, broadcast_origin=True
        )
        return broadcast_id

    async def _make_children(
        self, broadcast_id: str, neighbours: list, broadcast_origin=False
    ):
        children: List[Node] = []
        fanout = min(len(neighbours), FANOUT)
        message = self._protocol.make_session_message(
            broadcast_id, 0
        )  # only generate once, bc it is the same every time
        while neighbours and len(children) < fanout:
            child = random.choice(neighbours)
            neighbours.remove(child)
            session_ok = await self._rpc.call_session(child, message)
            if session_ok:
                children.append(child)
        if broadcast_origin and len(children) == 0:
            raise ServiceError("Unable to peer with neighbours for disseminating")
        self._children[broadcast_id] = children

    async def _disseminate(self, broadcast_id: str, data: Any):
        await self._children_made_events[
            broadcast_id
        ].wait()  # wait children to initialize
        message = self._protocol.make_push_message(
            broadcast_id, data
        )  # only generate once, bc it is the same every time
        for child in self._children[broadcast_id]:
            await self._rpc.call_push(child, message)

from collections import Counter
import asyncio
import random
from enum import IntEnum, auto
from typing import Union, List, Any, Optional

from unsserv.common.data_structures import Node, Message
from unsserv.common.gossip.gossip import Gossip
from unsserv.common.rpc.rpc import RpcBase, RPC
from unsserv.common.services_abc import MembershipService
from unsserv.common.typing import NeighboursCallback, View
from unsserv.common.utils import parse_node
from unsserv.stable.membership.hyparview_config import (
    DATA_FIELD_COMMAND,
    ACTIVE_VIEW_SIZE,
    DATA_FIELD_TTL,
    DATA_FIELD_ORIGIN_NODE,
    DATA_FIELD_PRIORITY,
    ACTIVE_VIEW_MAINTAIN_FREQUENCY,
    TTL,
)


class HyParViewCommand(IntEnum):
    JOIN = auto()
    FORWARD_JOIN = auto()
    CONNECT = auto()  # it is equivalent to NEIGHBOR in HyParView specification
    DISCONNECT = auto()
    PING = auto()


class HyParViewProtocol:
    def __init__(self, my_node: Node, service_id: Any):
        self.my_node = my_node
        self.service_id = service_id

    def make_join_message(self) -> Message:
        data = {DATA_FIELD_COMMAND: HyParViewCommand.JOIN}
        return Message(self.my_node, self.service_id, data)

    def make_forward_join_message(self, origin_node: Node, ttl: int) -> Message:
        data = {
            DATA_FIELD_COMMAND: HyParViewCommand.FORWARD_JOIN,
            DATA_FIELD_ORIGIN_NODE: origin_node,
            DATA_FIELD_TTL: ttl,
        }
        return Message(self.my_node, self.service_id, data)

    def make_connect_message(self, is_a_priority: bool) -> Message:
        data = {
            DATA_FIELD_COMMAND: HyParViewCommand.CONNECT,
            DATA_FIELD_PRIORITY: is_a_priority,
        }
        return Message(self.my_node, self.service_id, data)

    def make_disconnect_message(self) -> Message:
        data = {DATA_FIELD_COMMAND: HyParViewCommand.DISCONNECT}
        return Message(self.my_node, self.service_id, data)

    def make_ping_message(self) -> Message:
        data = {DATA_FIELD_COMMAND: HyParViewCommand.PING}
        return Message(self.my_node, self.service_id, data)


class HyParViewRPC(RpcBase):
    async def call_join(self, destination: Node, message: Message):
        rpc_result = await self.join(destination.address_info, message)
        self._handle_call_response(rpc_result)

    async def call_forward_join(self, destination: Node, message: Message):
        rpc_result = await self.forward_join(destination.address_info, message)
        self._handle_call_response(rpc_result)

    async def call_connect(self, destination: Node, message: Message) -> int:
        rpc_result = await self.connect(destination.address_info, message)
        return self._handle_call_response(rpc_result)

    async def call_disconnect(self, destination: Node, message: Message):
        rpc_result = await self.disconnect(destination.address_info, message)
        self._handle_call_response(rpc_result)

    async def call_ping(self, destination: Node, message: Message):
        rpc_result = await self.join(destination.address_info, message)
        self._handle_call_response(rpc_result)

    async def rpc_join(self, node: Node, raw_message: List):
        message = self._decode_message(raw_message)
        await self.registered_services[message.service_id](message)

    async def rpc_forward_join(self, node: Node, raw_message: List):
        message = self._decode_message(raw_message)
        await self.registered_services[message.service_id](message)

    async def rpc_connect(self, node: Node, raw_message: List) -> bool:
        message = self._decode_message(raw_message)
        is_connected = await self.registered_services[message.service_id](message)
        assert isinstance(is_connected, bool)
        return is_connected

    async def rpc_disconnect(self, node: Node, raw_message: List):
        message = self._decode_message(raw_message)
        await self.registered_services[message.service_id](message)

    async def rpc_ping(self, node: Node, raw_message: List):
        message = self._decode_message(raw_message)
        await self.registered_services[message.service_id](message)


class HyParView(MembershipService):
    _rpc: HyParViewRPC
    _protocol: Optional[HyParViewProtocol]
    _gossip: Optional[Gossip]
    _active_view: List[Node]
    _multiplex: bool
    _callback: NeighboursCallback
    _callback_raw_format: bool
    _local_view_maintenance_task: asyncio.Task

    def __init__(self, my_node: Node, multiplex: bool = True):
        self.my_node = my_node
        self._multiplex = multiplex
        self._rpc = RPC.get_rpc(self.my_node, HyParViewRPC, multiplex)
        self._active_view = []
        self._callback = None
        self._callback_raw_format = False
        self._gossip = None

    async def join(self, service_id: Any, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Membership")
        self.service_id = service_id
        self._protocol = HyParViewProtocol(self.my_node, self.service_id)
        self._gossip = Gossip(
            my_node=self.my_node,
            service_id=f"gossip-{service_id}",
            local_view_nodes=configuration.get("bootstrap_nodes", None),
            local_view_callback=self._local_view_callback,
            multiplex=self._multiplex,
        )
        await self._gossip.start()
        await self._rpc.register_service(service_id, self._handle_rpc)
        self._local_view_maintenance_task = asyncio.create_task(
            self._maintain_active_view()
        )
        self.running = True

    async def leave(self) -> None:
        if not self.running:
            return
        self._active_view = []
        if self._local_view_maintenance_task:
            self._local_view_maintenance_task.cancel()
            try:
                await self._local_view_maintenance_task
            except asyncio.CancelledError:
                pass
        await self._gossip.stop()
        await self._rpc.unregister_service(self.service_id)
        self._protocol = None
        self.running = False

    def get_neighbours(
        self, local_view_format: bool = False
    ) -> Union[List[Node], View]:
        return Counter(self._active_view) if local_view_format else self._active_view

    def set_neighbours_callback(
        self, callback: NeighboursCallback, local_view_format: bool = False
    ) -> None:
        if not self.running:
            raise RuntimeError("Clustering service not running")
        self._callback = callback
        self._callback_raw_format = local_view_format

    async def _local_view_callback(self, local_view: View):
        if self._callback:
            if self._callback_raw_format:
                await self._callback(local_view)
            else:
                await self._callback(list(local_view.keys()))

    async def _handle_rpc(self, message: Message) -> Any:
        command = message.data[DATA_FIELD_COMMAND]
        if command == HyParViewCommand.JOIN:
            if len(self._active_view) >= ACTIVE_VIEW_SIZE:
                self._active_view.pop(
                    random.randrange(ACTIVE_VIEW_SIZE)
                )  # randomly remove
            self._active_view.append(message.node)
            message = self._protocol.make_forward_join_message(message.node, TTL)
            for neighbour in self._active_view:
                asyncio.create_task(self._rpc.call_join(neighbour, message))
        elif command == HyParViewCommand.FORWARD_JOIN:
            ttl = message.data[DATA_FIELD_TTL]
            origin_node = parse_node(message.data[DATA_FIELD_ORIGIN_NODE])
            if ttl == 0:
                asyncio.create_task(self._connect_to_node(origin_node))
            else:
                neighbour = random.choice(self._active_view)
                message = self._protocol.make_forward_join_message(origin_node, ttl - 1)
                asyncio.create_task(self._rpc.call_forward_join(neighbour, message))
        elif command == HyParViewCommand.CONNECT:
            is_a_priority = message.data[DATA_FIELD_PRIORITY]
            if not is_a_priority and len(self._active_view) >= ACTIVE_VIEW_SIZE:
                return False
            if len(self._active_view) >= ACTIVE_VIEW_SIZE:
                self._active_view.pop(
                    random.randrange(ACTIVE_VIEW_SIZE)
                )  # randomly remove
            self._active_view.append(message.node)
            return True
        elif command == HyParViewCommand.DISCONNECT:
            if message.node in self._active_view:
                self._active_view.remove(message.node)
        elif command == HyParViewCommand.PING:
            return None
        else:
            raise ValueError("Invalid HyParView protocol value")
        return None

    async def _connect_to_node(self, node: Node):
        is_a_priority = len(self._active_view) == 0
        message = self._protocol.make_connect_message(is_a_priority)
        is_connected = await self._rpc.call_connect(node, message)
        if is_connected:
            self._active_view.append(node)

    async def _maintain_active_view(self):
        while True:
            await asyncio.sleep(ACTIVE_VIEW_MAINTAIN_FREQUENCY)
            active_nodes = []
            message = self._protocol.make_ping_message()
            for node in self._active_view:
                try:
                    await self._rpc.call_ping(node, message)
                    active_nodes.append(node)
                except ConnectionError:
                    pass
            self._active_view = active_nodes
            if len(self._active_view) >= ACTIVE_VIEW_SIZE:
                continue
            candidate_neighbours: List[Node] = list(self._gossip.local_view.keys())
            while candidate_neighbours and len(self._active_view) < ACTIVE_VIEW_SIZE:
                candidate_neighbour = candidate_neighbours.pop(
                    random.randrange(len(candidate_neighbours))
                )
                try:
                    await self._rpc.call_ping(candidate_neighbour, message)
                    active_nodes.append(candidate_neighbour)
                except ConnectionError:
                    pass

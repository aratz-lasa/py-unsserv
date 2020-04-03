import asyncio
import random
from collections import Counter
from contextlib import contextmanager
from enum import IntEnum, auto
from typing import Union, List, Any, Optional, Set, Counter as CounterType

from unsserv.common.structs import Node, Message
from unsserv.common.gossip.gossip import Gossip
from unsserv.common.rpc.rpc import RPC, RPCRegister
from unsserv.common.services_abc import MembershipService
from unsserv.common.typing import NeighboursCallback, View
from unsserv.common.utils import parse_node
from unsserv.stable.membership.hyparview_config import (
    FIELD_COMMAND,
    ACTIVE_VIEW_SIZE,
    FIELD_TTL,
    FIELD_ORIGIN_NODE,
    FIELD_PRIORITY,
    ACTIVE_VIEW_MAINTAIN_FREQUENCY,
    TTL,
)


class HyParViewCommand(IntEnum):
    JOIN = auto()
    FORWARD_JOIN = auto()
    CONNECT = auto()  # it is equivalent to NEIGHBOR in HyParView specification
    DISCONNECT = auto()
    STAY_CONNECTED = auto()


class HyParViewProtocol:
    def __init__(self, my_node: Node, service_id: Any):
        self.my_node = my_node
        self.service_id = service_id

    def make_join_message(self) -> Message:
        data = {FIELD_COMMAND: HyParViewCommand.JOIN}
        return Message(self.my_node, self.service_id, data)

    def make_forward_join_message(self, origin_node: Node, ttl: int) -> Message:
        data = {
            FIELD_COMMAND: HyParViewCommand.FORWARD_JOIN,
            FIELD_ORIGIN_NODE: origin_node,
            FIELD_TTL: ttl,
        }
        return Message(self.my_node, self.service_id, data)

    def make_connect_message(self, is_a_priority: bool) -> Message:
        data = {
            FIELD_COMMAND: HyParViewCommand.CONNECT,
            FIELD_PRIORITY: is_a_priority,
        }
        return Message(self.my_node, self.service_id, data)

    def make_disconnect_message(self) -> Message:
        data = {FIELD_COMMAND: HyParViewCommand.DISCONNECT}
        return Message(self.my_node, self.service_id, data)

    def make_stay_connected_message(self) -> Message:
        data = {FIELD_COMMAND: HyParViewCommand.STAY_CONNECTED}
        return Message(self.my_node, self.service_id, data)


class HyParView(MembershipService):
    _rpc: RPC
    _protocol: Optional[HyParViewProtocol]
    _gossip: Optional[Gossip]
    _active_view: Set[Node]
    _callback: NeighboursCallback
    _callback_raw_format: bool
    _local_view_maintenance_task: asyncio.Task
    _candidate_neighbours: CounterType[Node]

    def __init__(self, my_node: Node):
        self.my_node = my_node
        self._rpc = RPCRegister.get_rpc(self.my_node)
        self._gossip = None
        self._callback = None
        self._callback_raw_format = False
        self._active_view = set()
        self._candidate_neighbours = Counter()

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
        self._active_view = set()
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

    async def _handle_rpc(self, message: Message) -> Any:
        command = message.data[FIELD_COMMAND]
        if command == HyParViewCommand.JOIN:
            while len(self._active_view) >= ACTIVE_VIEW_SIZE:
                random_neighbour = random.choice(list(self._active_view))
                self._active_view.remove(random_neighbour)  # randomly remove
                asyncio.create_task(self._try_disconnect(random_neighbour))
            self._active_view.add(message.node)
            message = self._protocol.make_forward_join_message(message.node, TTL)
            for neighbour in list(
                filter(lambda n: n != message.node, self._active_view)
            ):
                asyncio.create_task(self._rpc.call_send_message(neighbour, message))
        elif command == HyParViewCommand.FORWARD_JOIN:
            ttl = message.data[FIELD_TTL]
            origin_node = parse_node(message.data[FIELD_ORIGIN_NODE])
            if ttl == 0:
                asyncio.create_task(self._connect_to_node(origin_node))
            else:
                candidate_neighbours = list(
                    filter(lambda n: n != origin_node, self._active_view)
                ) or [self.my_node]
                neighbour = random.choice(candidate_neighbours)
                message = self._protocol.make_forward_join_message(origin_node, ttl - 1)
                asyncio.create_task(self._rpc.call_send_message(neighbour, message))
        elif command == HyParViewCommand.CONNECT:
            is_a_priority = message.data[FIELD_PRIORITY]
            if not is_a_priority and len(self._active_view) >= ACTIVE_VIEW_SIZE:
                return False
            while len(self._active_view) >= ACTIVE_VIEW_SIZE:
                random_neighbour = random.choice(list(self._active_view))
                self._active_view.remove(random_neighbour)  # randomly remove
                asyncio.create_task(self._try_disconnect(random_neighbour))
            self._active_view.add(message.node)
            return True
        elif command == HyParViewCommand.DISCONNECT:
            if message.node in self._active_view:
                self._active_view.remove(message.node)
        elif command == HyParViewCommand.STAY_CONNECTED:
            return (
                message.node in self._active_view
                or message.node in self._candidate_neighbours
            )
        else:
            raise ValueError("Invalid HyParView protocol value")
        return None

    async def _connect_to_node(self, node: Node):
        with self._create_candidate_neighbour(node):
            is_a_priority = len(self._active_view) == 0
            message = self._protocol.make_connect_message(is_a_priority)
            try:
                is_connected = await self._rpc.call_send_message(node, message)
                if is_connected:
                    self._active_view.add(node)
            except ConnectionError:
                pass

    async def _join_first_time(self):
        message = self._protocol.make_join_message()
        bootstrap_nodes = self._get_passive_view_nodes()
        while bootstrap_nodes and len(self._active_view) < ACTIVE_VIEW_SIZE:
            candidate_node = bootstrap_nodes.pop(random.randrange(len(bootstrap_nodes)))
            with self._create_candidate_neighbour(candidate_node):
                try:
                    await self._rpc.call_send_message(candidate_node, message)
                    self._active_view.add(candidate_node)
                    break
                except ConnectionError:
                    pass

    async def _try_disconnect(self, node: Node):
        message = self._protocol.make_disconnect_message()
        try:
            await self._rpc.call_send_message(node, message)
        except ConnectionError:
            pass

    async def _maintain_active_view(self):
        await self._join_first_time()
        while True:
            await asyncio.sleep(ACTIVE_VIEW_MAINTAIN_FREQUENCY)
            await self._update_active_view()

    async def _update_active_view(self):
        inactive_nodes = set()
        message = self._protocol.make_stay_connected_message()
        for node in self._active_view.copy():
            try:
                is_still_connected = await self._rpc.call_send_message(node, message)
                if not is_still_connected:
                    inactive_nodes.add(node)
            except ConnectionError:
                inactive_nodes.add(node)
        self._active_view = self._active_view - inactive_nodes
        if len(self._active_view) >= ACTIVE_VIEW_SIZE:
            return
        candidate_neighbours: List[Node] = self._get_passive_view_nodes()
        while candidate_neighbours and len(self._active_view) < ACTIVE_VIEW_SIZE:
            candidate_neighbour = candidate_neighbours.pop(
                random.randrange(len(candidate_neighbours))
            )
            await self._connect_to_node(candidate_neighbour)

    @contextmanager
    def _create_candidate_neighbour(self, node: Node):
        self._candidate_neighbours.update([node])
        try:
            yield
        finally:
            self._candidate_neighbours.subtract([node])
            self._candidate_neighbours = (
                +self._candidate_neighbours
            )  # remove zero and negative counts

    def _get_passive_view_nodes(self):
        return list(self._gossip.local_view.keys())

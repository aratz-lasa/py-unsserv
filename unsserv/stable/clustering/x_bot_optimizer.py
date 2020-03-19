from math import ceil
import asyncio
import random
from collections import Counter
from contextlib import contextmanager
from enum import IntEnum, auto
from typing import Union, List, Any, Optional, Set, Counter as CounterType, Callable

from unsserv.common.data_structures import Node, Message
from unsserv.common.rpc.rpc import RpcBase, RPC
from unsserv.common.services_abc import MembershipService, ClusteringService
from unsserv.common.typing import NeighboursCallback, View
from unsserv.common.utils import parse_node
from unsserv.stable.clustering.x_bot_config import (
    DATA_FIELD_COMMAND,
    ACTIVE_VIEW_SIZE,
    DATA_FIELD_TTL,
    DATA_FIELD_ORIGIN_NODE,
    DATA_FIELD_PRIORITY,
    ACTIVE_VIEW_MAINTAIN_FREQUENCY,
    TTL,
    UNBIASED_NODES,
    PASSIVE_SCAN_LENGTH,
)

RankingFunction = Callable[[Node], Any]


class XBotOptimizerCommand(IntEnum):
    JOIN = auto()
    FORWARD_JOIN = auto()
    CONNECT = auto()  # it is equivalent to NEIGHBOR in XBotOptimizer specification
    DISCONNECT = auto()
    STAY_CONNECTED = auto()


class XBotOptimizerProtocol:
    def __init__(self, my_node: Node, service_id: Any):
        self.my_node = my_node
        self.service_id = service_id

    def make_join_message(self) -> Message:
        data = {DATA_FIELD_COMMAND: XBotOptimizerCommand.JOIN}
        return Message(self.my_node, self.service_id, data)

    def make_forward_join_message(self, origin_node: Node, ttl: int) -> Message:
        data = {
            DATA_FIELD_COMMAND: XBotOptimizerCommand.FORWARD_JOIN,
            DATA_FIELD_ORIGIN_NODE: origin_node,
            DATA_FIELD_TTL: ttl,
        }
        return Message(self.my_node, self.service_id, data)

    def make_connect_message(self, is_a_priority: bool) -> Message:
        data = {
            DATA_FIELD_COMMAND: XBotOptimizerCommand.CONNECT,
            DATA_FIELD_PRIORITY: is_a_priority,
        }
        return Message(self.my_node, self.service_id, data)

    def make_disconnect_message(self) -> Message:
        data = {DATA_FIELD_COMMAND: XBotOptimizerCommand.DISCONNECT}
        return Message(self.my_node, self.service_id, data)

    def make_stay_connected_message(self) -> Message:
        data = {DATA_FIELD_COMMAND: XBotOptimizerCommand.STAY_CONNECTED}
        return Message(self.my_node, self.service_id, data)


class XBotOptimizerRPC(RpcBase):
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

    async def call_stay_connected(self, destination: Node, message: Message) -> bool:
        rpc_result = await self.stay_connected(destination.address_info, message)
        return self._handle_call_response(rpc_result)

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

    async def rpc_stay_connected(self, node: Node, raw_message: List) -> bool:
        message = self._decode_message(raw_message)
        is_still_connected = await self.registered_services[message.service_id](message)
        assert isinstance(is_still_connected, bool)
        return is_still_connected


class XBotOptimizer(ClusteringService):
    _rpc: XBotOptimizerRPC
    _protocol: Optional[XBotOptimizerProtocol]
    _active_view: Set[Node]
    _multiplex: bool
    _callback: NeighboursCallback
    _callback_raw_format: bool
    _local_view_maintenance_task: asyncio.Task
    _candidate_neighbours: CounterType[Node]

    def __init__(self, membership: MembershipService, multiplex: bool = True):
        self.membership = membership
        self.my_node = membership.my_node
        self._multiplex = multiplex
        self._rpc = RPC.get_rpc(self.my_node, XBotOptimizerRPC, multiplex)
        self._callback = None
        self._callback_raw_format = False
        self._active_view = set()
        self._candidate_neighbours = Counter()
        self._ranking_function: RankingFunction

    async def join(self, service_id: Any, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Clustering")
        self.service_id = service_id
        self._ranking_function = configuration["ranking_function"]
        self._protocol = XBotOptimizerProtocol(self.my_node, self.service_id)
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
        if command == XBotOptimizerCommand.JOIN:
            while len(self._active_view) >= ACTIVE_VIEW_SIZE:
                random_neighbour = random.choice(list(self._active_view))
                self._active_view.remove(random_neighbour)  # randomly remove
                asyncio.create_task(self._try_disconnect(random_neighbour))
            self._active_view.add(message.node)
            message = self._protocol.make_forward_join_message(message.node, TTL)
            for neighbour in list(
                filter(lambda n: n != message.node, self._active_view)
            ):
                asyncio.create_task(self._rpc.call_forward_join(neighbour, message))
        elif command == XBotOptimizerCommand.FORWARD_JOIN:
            ttl = message.data[DATA_FIELD_TTL]
            origin_node = parse_node(message.data[DATA_FIELD_ORIGIN_NODE])
            if ttl == 0:
                asyncio.create_task(self._connect_to_node(origin_node))
            else:
                candidate_neighbours = list(
                    filter(lambda n: n != origin_node, self._active_view)
                ) or [self.my_node]
                neighbour = random.choice(candidate_neighbours)
                message = self._protocol.make_forward_join_message(origin_node, ttl - 1)
                asyncio.create_task(self._rpc.call_forward_join(neighbour, message))
        elif command == XBotOptimizerCommand.CONNECT:
            is_a_priority = message.data[DATA_FIELD_PRIORITY]
            if not is_a_priority and len(self._active_view) >= ACTIVE_VIEW_SIZE:
                return False
            while len(self._active_view) >= ACTIVE_VIEW_SIZE:
                random_neighbour = random.choice(list(self._active_view))
                self._active_view.remove(random_neighbour)  # randomly remove
                asyncio.create_task(self._try_disconnect(random_neighbour))
            self._active_view.add(message.node)
            return True
        elif command == XBotOptimizerCommand.DISCONNECT:
            if message.node in self._active_view:
                self._active_view.remove(message.node)
        elif command == XBotOptimizerCommand.STAY_CONNECTED:
            return (
                message.node in self._active_view
                or message.node in self._candidate_neighbours
            )
        else:
            raise ValueError("Invalid XBotOptimizer protocol value")
        return None

    async def _connect_to_node(self, node: Node):
        with self._create_candidate_neighbour(node):
            is_a_priority = len(self._active_view) == 0
            message = self._protocol.make_connect_message(is_a_priority)
            try:
                is_connected = await self._rpc.call_connect(node, message)
                if is_connected:
                    self._active_view.add(node)
            except ConnectionError:
                pass

    async def _join_first_time(self):
        message = self._protocol.make_join_message()
        bootstrap_nodes = self.membership.get_neighbours()
        while bootstrap_nodes:
            candidate_node = bootstrap_nodes.pop(random.randrange(len(bootstrap_nodes)))
            with self._create_candidate_neighbour(candidate_node):
                try:
                    await self._rpc.call_join(candidate_node, message)
                    self._active_view.add(candidate_node)
                    break
                except ConnectionError:
                    pass

    async def _try_disconnect(self, node: Node):
        message = self._protocol.make_disconnect_message()
        try:
            await self._rpc.call_disconnect(node, message)
        except ConnectionError:
            pass

    async def _maintain_active_view(self):
        await self._join_first_time()
        while True:
            await asyncio.sleep(ACTIVE_VIEW_MAINTAIN_FREQUENCY)
            inactive_nodes = set()
            message = self._protocol.make_stay_connected_message()
            for node in self._active_view.copy():
                try:
                    is_still_connected = await self._rpc.call_stay_connected(
                        node, message
                    )
                    if not is_still_connected:
                        inactive_nodes.add(node)
                except ConnectionError:
                    inactive_nodes.add(node)
            self._active_view = self._active_view - inactive_nodes
            if len(self._active_view) >= ACTIVE_VIEW_SIZE:
                await self._optimize_active_view()  # todo: create task instead?
                continue
            candidate_neighbours: List[Node] = self.membership.get_neighbours()
            while candidate_neighbours and len(self._active_view) < ACTIVE_VIEW_SIZE:
                candidate_neighbour = candidate_neighbours.pop(
                    random.randrange(len(candidate_neighbours))
                )
                await self._connect_to_node(candidate_neighbour)

    async def _optimize_active_view(self):
        candidate_neighbours = self.membership.get_neighbours()
        if not candidate_neighbours:
            return None
        candidate_neighbours = random.sample(
            candidate_neighbours, min(len(candidate_neighbours), PASSIVE_SCAN_LENGTH)
        )
        biasable_nodes = list(sorted(self._active_view, key=self._ranking_function))[
            ceil(ACTIVE_VIEW_SIZE * UNBIASED_NODES) :
        ]
        for node in biasable_nodes:
            if not candidate_neighbours:
                return None
            candidate = candidate_neighbours.pop()
            if self._get_the_best(candidate, node) == candidate:
                pass  # todo: send optimization

    def _get_the_best(self, node1: Node, node2: Node):
        return (
            node1
            if self._ranking_function(node1) < self._ranking_function(node2)
            else node2
        )

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

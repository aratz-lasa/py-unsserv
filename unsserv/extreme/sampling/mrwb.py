import asyncio
import math
import random
import string
from enum import IntEnum, auto
from typing import Dict, List, Optional

from unsserv.common.api import MembershipService, SamplingService
from unsserv.common.data_structures import Message, Node
from unsserv.common.errors import SamplingError
from unsserv.common.rpc.rpc import RPC, RpcBase
from unsserv.common.utils import parse_node
from unsserv.extreme.sampling.mrwb_config import (
    DATA_FIELD_COMMAND,
    DATA_FIELD_ORIGIN_NODE,
    DATA_FIELD_SAMPLE_ID,
    DATA_FIELD_SAMPLE_RESULT,
    DATA_FIELD_TTL,
    ID_LENGTH,
    MRWB_DEGREE_REFRESH_FREQUENCY,
    SAMPLING_TIMEOUT,
    TTL,
)


class CommandMRWB(IntEnum):
    GET_DEGREE = auto()
    SAMPLE = auto()
    SAMPLE_RESULT = auto()


class MRWBRPC(RpcBase):
    async def call_get_degree(self, destination: Node, message: Message) -> int:
        rpc_result = await self.get_degree(destination.address_info, message)
        return self._handle_call_response(rpc_result)

    async def call_sample(self, destination: Node, message: Message):
        rpc_result = await self.sample(destination.address_info, message)
        self._handle_call_response(rpc_result)

    async def call_sample_result(self, destination: Node, message: Message):
        rpc_result = await self.sample_result(destination.address_info, message)
        self._handle_call_response(rpc_result)

    async def rpc_get_degree(self, node: Node, raw_message: List) -> int:
        message = self._decode_message(raw_message)
        degree = await self.registered_services[message.service_id](message)
        assert isinstance(degree, int)
        return degree

    async def rpc_sample(self, node: Node, raw_message: List):
        message = self._decode_message(raw_message)
        await self.registered_services[message.service_id](message)

    async def rpc_sample_result(self, node: Node, raw_message: List):
        message = self._decode_message(raw_message)
        await self.registered_services[message.service_id](message)


class MRWB(SamplingService):
    _neighbours: List[Node]
    _neighbour_degrees: Dict[Node, int]
    _rpc: MRWBRPC
    _sampling_queue: Dict[str, Node]
    _sampling_events: Dict[str, asyncio.Event]

    def __init__(self, membership: MembershipService, multiplex: bool = True):
        self.my_node = membership.my_node
        self._membership = membership
        self._neighbours = []
        self._neighbour_degrees = {}
        self._rpc = RPC.get_rpc(
            self.my_node, ProtocolClass=MRWBRPC, multiplex=multiplex
        )

        self._sampling_queue = {}
        self._sampling_events = {}

    async def join_sampling(self, service_id: str) -> None:
        if self.running:
            raise RuntimeError("Already running Sampling")
        self.service_id = service_id
        self._membership.set_neighbours_callback(
            self._neighbours_change_callback  # type: ignore
        )
        # initialize RPC
        await self._rpc.register_service(self.service_id, self._handle_rpc)
        # initialize neighbours
        neighbours = self._membership.get_neighbours()
        assert isinstance(neighbours, list)
        self._neighbours = neighbours
        self._degrees_update_task = asyncio.create_task(
            self._degrees_update_process()
        )  # stop degrees updater task
        self.running = True

    async def leave_sampling(self) -> None:
        self._membership.set_neighbours_callback(None)
        self._neighbours = []
        await self._rpc.unregister_service(self.service_id)
        if self._degrees_update_task:  # stop degrees updater task
            self._degrees_update_task.cancel()
            try:
                await self._degrees_update_task
            except asyncio.CancelledError:
                pass
        self.running = False

    async def get_sample(self) -> Node:
        sample_id = self._get_random_sample_id()
        if not self._neighbours:
            raise SamplingError("Unable to sample. Not connected Neighbours")
        node = random.choice(self._neighbours)  # random neighbour
        data = {
            DATA_FIELD_COMMAND: CommandMRWB.SAMPLE,
            DATA_FIELD_SAMPLE_ID: sample_id,
            DATA_FIELD_ORIGIN_NODE: self.my_node,
            DATA_FIELD_TTL: TTL,
        }
        message = Message(self.my_node, self.service_id, data)
        await self._rpc.call_sample(node, message)
        event = asyncio.Event()
        self._sampling_events[sample_id] = event
        try:
            await asyncio.wait_for(event.wait(), timeout=SAMPLING_TIMEOUT)
        except asyncio.TimeoutError:
            del self._sampling_events[sample_id]
            raise SamplingError("Sampling service timeouted")
        sample = self._sampling_queue[sample_id]
        del self._sampling_queue[sample_id]
        return sample

    async def _degrees_update_process(self):
        # maybe is not needed if degrees are updated whenever
        # membership changes neighbours?
        while True:
            for neighbour in self._neighbours:
                await self._update_degree(neighbour)
            await asyncio.sleep(MRWB_DEGREE_REFRESH_FREQUENCY)

    async def _handle_rpc(self, message: Message) -> Optional[int]:
        command = message.data[DATA_FIELD_COMMAND]
        if command == CommandMRWB.GET_DEGREE:
            return len(self._neighbours)
        elif command == CommandMRWB.SAMPLE_RESULT:
            sample_result = parse_node(message.data[DATA_FIELD_SAMPLE_RESULT])
            sample_id = message.data[DATA_FIELD_SAMPLE_ID]
            self._sampling_queue[sample_id] = sample_result
            self._sampling_events[sample_id].set()
            del self._sampling_events[sample_id]
        elif command == CommandMRWB.SAMPLE:
            ttl = message.data[DATA_FIELD_TTL]
            while ttl > 0:
                next_hop = self._choose_neighbour()
                if next_hop != self.my_node:
                    data = {
                        DATA_FIELD_COMMAND: CommandMRWB.SAMPLE,
                        DATA_FIELD_SAMPLE_ID: message.data[DATA_FIELD_SAMPLE_ID],
                        DATA_FIELD_ORIGIN_NODE: message.data[DATA_FIELD_ORIGIN_NODE],
                        DATA_FIELD_TTL: ttl - 1,
                    }
                    message = Message(self.my_node, self.service_id, data)
                    asyncio.create_task(self._rpc.call_sample(next_hop, message))
                    return None
                ttl -= 1
            origin_node = parse_node(message.data[DATA_FIELD_ORIGIN_NODE])
            data = {
                DATA_FIELD_COMMAND: CommandMRWB.SAMPLE_RESULT,
                DATA_FIELD_SAMPLE_ID: message.data[DATA_FIELD_SAMPLE_ID],
                DATA_FIELD_SAMPLE_RESULT: self.my_node,
            }
            message = Message(self.my_node, self.service_id, data)
            asyncio.create_task(self._rpc.call_sample_result(origin_node, message))
        return None

    async def _neighbours_change_callback(self, new_neighbours: List[Node]) -> None:
        old_neighbours = set(self._neighbours)
        self._neighbours = new_neighbours
        new_neighbours_set = set(new_neighbours)
        for neighbour in old_neighbours - new_neighbours_set:
            del self._neighbour_degrees[neighbour]
        for neighbour in new_neighbours_set - old_neighbours:
            await self._update_degree(neighbour)

    def _choose_neighbour(self) -> Node:
        random_neighbour = random.choice(self._neighbours)
        neighbour_degree = self._neighbour_degrees.get(random_neighbour, math.inf)
        my_degree = len(self._neighbours)
        if random.uniform(0, 1) < my_degree / neighbour_degree:
            return random_neighbour
        return self.my_node

    async def _update_degree(self, node: Node):
        data = {DATA_FIELD_COMMAND: CommandMRWB.GET_DEGREE}
        message = Message(self.my_node, self.service_id, data)
        try:
            degree = await self._rpc.call_get_degree(node, message)
            self._neighbour_degrees[node] = degree
        except ConnectionError:
            pass  # let membership to decide whether to remove the node or not

    def _get_random_sample_id(self, size: int = ID_LENGTH):
        id_characters = string.ascii_letters + string.digits + string.punctuation
        return "".join(random.choice(id_characters) for _ in range(size))

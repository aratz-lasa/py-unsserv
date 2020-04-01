import asyncio
import math
import random
from enum import IntEnum, auto
from typing import Dict, List, Optional, Any

from unsserv.common.services_abc import MembershipService, SamplingService
from unsserv.common.data_structures import Message, Node
from unsserv.common.errors import ServiceError
from unsserv.common.rpc.rpc import RPCRegister, RPC
from unsserv.common.utils import parse_node, get_random_id
from unsserv.extreme.sampling.mrwb_config import (
    FIELD_COMMAND,
    FIELD_ORIGIN_NODE,
    FIELD_SAMPLE_ID,
    FIELD_SAMPLE_RESULT,
    FIELD_TTL,
    ID_LENGTH,
    MRWB_DEGREE_REFRESH_FREQUENCY,
    SAMPLING_TIMEOUT,
    TTL,
)


class MRWBCommand(IntEnum):
    GET_DEGREE = auto()
    SAMPLE = auto()
    SAMPLE_RESULT = auto()


class MRWBProtocol:
    def __init__(self, my_node: Node, service_id: Any):
        self.my_node = my_node
        self.service_id = service_id

    def make_sample_message(
        self, sample_id: str, origin_node: Node, ttl: int
    ) -> Message:
        data = {
            FIELD_COMMAND: MRWBCommand.SAMPLE,
            FIELD_SAMPLE_ID: sample_id,
            FIELD_ORIGIN_NODE: origin_node,
            FIELD_TTL: ttl,
        }
        return Message(self.my_node, self.service_id, data)

    def make_sample_result_message(
        self, sample_id: str, sample_result: Node
    ) -> Message:
        data = {
            FIELD_COMMAND: MRWBCommand.SAMPLE_RESULT,
            FIELD_SAMPLE_ID: sample_id,
            FIELD_SAMPLE_RESULT: sample_result,
        }
        return Message(self.my_node, self.service_id, data)

    def make_get_degree_message(self) -> Message:
        data = {FIELD_COMMAND: MRWBCommand.GET_DEGREE}
        return Message(self.my_node, self.service_id, data)


class MRWB(SamplingService):
    _neighbours: List[Node]
    _neighbour_degrees: Dict[Node, int]
    _rpc: RPC
    _sampling_queue: Dict[str, Node]
    _sampling_events: Dict[str, asyncio.Event]
    _protocol: Optional[MRWBProtocol]

    def __init__(self, membership: MembershipService):
        self.my_node = membership.my_node
        self.membership = membership
        self._neighbours = []
        self._neighbour_degrees = {}
        self._rpc = RPCRegister.get_rpc(self.my_node)

        self._sampling_queue = {}
        self._sampling_events = {}

    async def join_sampling(self, service_id: str, **configuration: Any) -> None:
        if self.running:
            raise RuntimeError("Already running Sampling")
        self.service_id = service_id
        self._protocol = MRWBProtocol(self.my_node, service_id)
        # initialize neighbours
        neighbours = self.membership.get_neighbours()
        assert isinstance(neighbours, list)
        self._neighbours = neighbours
        self._degrees_update_task = asyncio.create_task(
            self._neighbours_degrees_maintenance()
        )  # stop degrees updater task
        # initialize RPC
        await self._rpc.register_service(self.service_id, self._handle_rpc)
        self.membership.set_neighbours_callback(
            self._membership_neighbours_callback  # type: ignore
        )
        self.running = True

    async def leave_sampling(self) -> None:
        if not self.running:
            return
        self.membership.set_neighbours_callback(None)
        self._neighbours = []
        self._protocol = None
        await self._rpc.unregister_service(self.service_id)
        if self._degrees_update_task:  # stop degrees updater task
            self._degrees_update_task.cancel()
            try:
                await self._degrees_update_task
            except asyncio.CancelledError:
                pass
        self.running = False

    async def get_sample(self) -> Node:
        if not self.running:
            raise RuntimeError("Sampling service not running")
        sample_id = get_random_id(ID_LENGTH)
        if not self._neighbours:
            raise ServiceError("Unable to peer with neighbours for sampling.")
        node = random.choice(self._neighbours)  # random neighbour
        message = self._protocol.make_sample_message(sample_id, self.my_node, TTL)
        await self._rpc.call_without_response(node, message)
        event = asyncio.Event()
        self._sampling_events[sample_id] = event
        try:
            await asyncio.wait_for(event.wait(), timeout=SAMPLING_TIMEOUT)
        except asyncio.TimeoutError:
            del self._sampling_events[sample_id]
            raise ServiceError("Sampling service timeouted")
        sample = self._sampling_queue[sample_id]
        del self._sampling_queue[sample_id]
        return sample

    async def _neighbours_degrees_maintenance(self):
        # maybe is not needed if degrees are updated whenever
        # membership changes neighbours?
        while True:
            for neighbour in self._neighbours:
                await self._update_degree(neighbour)
            await asyncio.sleep(MRWB_DEGREE_REFRESH_FREQUENCY)

    async def _handle_rpc(self, message: Message) -> Optional[int]:
        command = message.data[FIELD_COMMAND]
        if command == MRWBCommand.GET_DEGREE:
            return len(self._neighbours)
        elif command == MRWBCommand.SAMPLE_RESULT:
            sample_result = parse_node(message.data[FIELD_SAMPLE_RESULT])
            sample_id = message.data[FIELD_SAMPLE_ID]
            self._sampling_queue[sample_id] = sample_result
            self._sampling_events[sample_id].set()
            del self._sampling_events[sample_id]
        elif command == MRWBCommand.SAMPLE:
            ttl = message.data[FIELD_TTL]
            while ttl > 0:
                next_hop = self._choose_neighbour()
                if next_hop != self.my_node:
                    message = self._protocol.make_sample_message(
                        message.data[FIELD_SAMPLE_ID],
                        message.data[FIELD_ORIGIN_NODE],
                        ttl - 1,
                    )
                    asyncio.create_task(
                        self._rpc.call_without_response(next_hop, message)
                    )
                    return None
                ttl -= 1
            origin_node = parse_node(message.data[FIELD_ORIGIN_NODE])
            message = self._protocol.make_sample_result_message(
                message.data[FIELD_SAMPLE_ID], self.my_node
            )
            asyncio.create_task(self._rpc.call_without_response(origin_node, message))
        else:
            raise ValueError("Invalid MON protocol value")
        return None

    def _choose_neighbour(self) -> Node:
        random_neighbour = random.choice(self._neighbours)
        neighbour_degree = self._neighbour_degrees.get(random_neighbour, math.inf)
        my_degree = len(self._neighbours)
        if random.uniform(0, 1) < my_degree / neighbour_degree:
            return random_neighbour
        return self.my_node

    async def _update_degree(self, node: Node):
        message = self._protocol.make_get_degree_message()
        try:
            degree = await self._rpc.call_with_response(node, message)
            self._neighbour_degrees[node] = degree
        except ConnectionError:
            pass  # let membership to decide whether to remove the node or not

    async def _membership_neighbours_callback(self, new_neighbours: List[Node]) -> None:
        old_neighbours = set(self._neighbours)
        new_neighbours_set = set(new_neighbours)
        for neighbour in old_neighbours - new_neighbours_set:
            if (
                neighbour in self._neighbour_degrees
            ):  # due to concurrency there may be errors
                del self._neighbour_degrees[neighbour]
        for neighbour in new_neighbours_set - old_neighbours:
            await self._update_degree(neighbour)
        self._neighbours = new_neighbours

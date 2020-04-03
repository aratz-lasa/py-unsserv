import asyncio
import math
import random
from typing import Dict, List, Any

from unsserv.common.structs import Node
from unsserv.common.errors import ServiceError
from unsserv.common.services_abc import MembershipService, SamplingService
from unsserv.common.utils import get_random_id
from unsserv.extreme.sampling.config import (
    ID_LENGTH,
    MRWB_DEGREE_REFRESH_FREQUENCY,
    SAMPLING_TIMEOUT,
    TTL,
)
from unsserv.extreme.sampling.protocol import MRWBProtocol
from unsserv.extreme.sampling.structs import Sample, SampleResult


class MRWB(SamplingService):
    _neighbours: List[Node]
    _neighbour_degrees: Dict[Node, int]
    _protocol: MRWBProtocol
    _sampling_queue: Dict[str, Node]
    _sampling_events: Dict[str, asyncio.Event]

    def __init__(self, membership: MembershipService):
        self.my_node = membership.my_node
        self.membership = membership
        self._neighbours = []
        self._neighbour_degrees = {}
        self._protocol = MRWBProtocol(self.my_node)

        self._sampling_queue = {}
        self._sampling_events = {}

    async def join_sampling(self, service_id: str, **configuration: Any) -> None:
        if self.running:
            raise RuntimeError("Already running Sampling")
        self.service_id = service_id
        # initialize neighbours
        neighbours = self.membership.get_neighbours()
        assert isinstance(neighbours, list)
        self._neighbours = neighbours
        self._degrees_update_task = asyncio.create_task(
            self._neighbours_degrees_maintenance()
        )  # stop degrees updater task
        # initialize RPC
        await self._initialize_protocol()
        self.membership.set_neighbours_callback(
            self._membership_neighbours_callback  # type: ignore
        )
        self.running = True

    async def leave_sampling(self) -> None:
        if not self.running:
            return
        self.membership.set_neighbours_callback(None)
        self._neighbours = []
        await self._protocol.stop()
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
        sample = Sample(id=sample_id, origin_node=self.my_node, ttl=TTL)
        await self._protocol.sample(node, sample)
        event = asyncio.Event()
        self._sampling_events[sample_id] = event
        try:
            await asyncio.wait_for(event.wait(), timeout=SAMPLING_TIMEOUT)
        except asyncio.TimeoutError:
            del self._sampling_events[sample_id]
            raise ServiceError("Sampling service timeouted")
        sample_result = self._sampling_queue[sample_id]
        del self._sampling_queue[sample_id]
        return sample_result

    async def _neighbours_degrees_maintenance(self):
        # maybe is not needed if degrees are updated whenever
        # membership changes neighbours?
        while True:
            for neighbour in self._neighbours:
                await self._update_degree(neighbour)
            await asyncio.sleep(MRWB_DEGREE_REFRESH_FREQUENCY)

    def _choose_neighbour(self) -> Node:
        random_neighbour = random.choice(self._neighbours)
        neighbour_degree = self._neighbour_degrees.get(random_neighbour, math.inf)
        my_degree = len(self._neighbours)
        if random.uniform(0, 1) < my_degree / neighbour_degree:
            return random_neighbour
        return self.my_node

    async def _update_degree(self, node: Node):
        try:
            degree = await self._protocol.get_degree(node)
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

    async def _handler_sample(self, sender: Node, sample: Sample):
        ttl = sample.ttl
        while ttl > 0:
            next_hop = self._choose_neighbour()
            if next_hop != self.my_node:
                next_sample = Sample(
                    id=sample.id, origin_node=sample.origin_node, ttl=ttl - 1
                )
                asyncio.create_task(self._protocol.sample(next_hop, next_sample))
                return None
            ttl -= 1
        sample_result = SampleResult(sample_id=sample.id, result=self.my_node)
        asyncio.create_task(
            self._protocol.sample_result(sample.origin_node, sample_result)
        )

    async def _handler_sample_result(self, sender: Node, sample_result: SampleResult):
        self._sampling_queue[sample_result.sample_id] = sample_result.result
        self._sampling_events[sample_result.sample_id].set()
        del self._sampling_events[sample_result.sample_id]

    def _handler_get_degree(self, sender: Node):
        return len(self._neighbours)

    async def _initialize_protocol(self):
        self._protocol.set_handler_sample(self._handler_sample)
        self._protocol.set_handler_sample_result(self._handler_sample_result)
        self._protocol.set_handler_get_degree(self._handler_get_degree)
        await self._protocol.start(self.service_id)

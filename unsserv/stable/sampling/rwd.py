import asyncio
import random
from typing import Dict, List, Any

from unsserv.common.errors import ServiceError
from unsserv.common.service_properties import Property
from unsserv.common.services_abc import MembershipService, SamplingService
from unsserv.common.structs import Node
from unsserv.common.utils import get_random_id
from unsserv.common.utils import stop_task
from unsserv.stable.sampling.config import (
    QUANTUM,
    MORE_THAN_MAXIMUM,
    ID_LENGTH,
    SAMPLING_TIMEOUT,
    TTL,
)
from unsserv.stable.sampling.protocol import RWDProtocol
from unsserv.stable.sampling.structs import Sample, SampleResult


class RWD(SamplingService):
    properties = {Property.STABLE}
    _neighbours: List[Node]
    _neighbour_weights: Dict[Node, float]
    _my_weight: float
    _protocol: RWDProtocol
    _sampling_queue: Dict[str, Node]
    _sampling_events: Dict[str, asyncio.Event]
    _new_neighbours_event: asyncio.Event

    def __init__(self, membership: MembershipService):
        self.my_node = membership.my_node

        self.membership = membership
        self._neighbours = []
        self._neighbour_weights = {}
        self._protocol = RWDProtocol(self.my_node)

        self._sampling_queue = {}
        self._sampling_events = {}

        self._new_neighbours_event = asyncio.Event()

    async def join_sampling(self, service_id: str, **configuration: Any) -> None:
        if self.running:
            raise RuntimeError("Already running Sampling")
        self.service_id = service_id
        # initialize neighbours
        neighbours = self.membership.get_neighbours()
        assert isinstance(neighbours, list)
        self._neighbours = neighbours
        self._new_neighbours_event.set()
        self._maintain_weights_task = asyncio.create_task(
            self._weights_maintenance_loop()
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
        if self._maintain_weights_task:  # stop degrees updater task
            await stop_task(self._maintain_weights_task)
        self.running = False

    async def get_sample(self) -> Node:
        if not self.running:
            raise RuntimeError("Sampling service not running")
        sample_id = get_random_id(ID_LENGTH)
        if not self._neighbours:
            raise ServiceError("Unable to peer with neighbours for sampling.")
        event = asyncio.Event()
        self._sampling_events[sample_id] = event
        random_node = self._choose_next_hop()
        sample = Sample(id=sample_id, origin_node=self.my_node, ttl=TTL)
        await self._protocol.sample(random_node, sample)
        try:
            await asyncio.wait_for(event.wait(), timeout=SAMPLING_TIMEOUT)
        except asyncio.TimeoutError:
            del self._sampling_events[sample_id]
            raise ServiceError("Sampling service timeouted")
        sample_result = self._sampling_queue[sample_id]
        del self._sampling_queue[sample_id]
        return sample_result

    def _choose_next_hop(self) -> Node:
        neighbours, weights = zip(*self._neighbour_weights)
        next_hop = random.choices(
            neighbours + [self.my_node], weights + [self._my_weight]
        )[0]
        return next_hop

    async def _weights_maintenance_loop(self):
        await self._new_neighbours_event.wait()
        self._initialize_weights()  # initialize just once
        while True:
            await self._new_neighbours_event.wait()
            self._new_neighbours_event.clear()
            await self._distribute_weights()

    def _initialize_weights(self):
        for neighbour in self._neighbours:
            self._neighbour_weights[neighbour] = 1 / MORE_THAN_MAXIMUM

    async def _distribute_weights(self):
        neighbours = self._neighbours.copy()
        while self._my_weight >= QUANTUM and neighbours:
            neighbour = random.choice(neighbours)
            is_increased = await self._protocol.increase(neighbour)
            if is_increased:
                self._neighbour_weights[neighbour] += QUANTUM
                self._my_weight -= QUANTUM
            else:
                neighbours.remove(neighbour)

    async def _membership_neighbours_callback(self, new_neighbours: List[Node]) -> None:
        old_neighbours = set(self._neighbours)
        new_neighbours_set = set(new_neighbours)
        for neighbour in old_neighbours - new_neighbours_set:
            if (
                neighbour in self._neighbour_weights
            ):  # due to concurrency there may be errors
                self._my_weight += self._neighbour_weights.pop(neighbour)
        self._neighbours = new_neighbours
        self._new_neighbours_event.set()

    async def _handler_sample(self, sender: Node, sample: Sample):
        ttl = sample.ttl
        while ttl > 0:
            next_hop = self._choose_next_hop()
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

    def _handler_get_increase(self, sender: Node) -> bool:
        if self._my_weight >= QUANTUM and sender in self._neighbour_weights:
            self._neighbour_weights[sender] += QUANTUM
            self._my_weight -= QUANTUM
            return True
        return False

    async def _initialize_protocol(self):
        self._protocol.set_handler_sample(self._handler_sample)
        self._protocol.set_handler_sample_result(self._handler_sample_result)
        self._protocol.set_handler_increase(self._handler_get_increase)
        await self._protocol.start(self.service_id)

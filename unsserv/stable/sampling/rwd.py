import asyncio
import random
from typing import Dict, List, Any

from unsserv.common.errors import ServiceError
from unsserv.common.services_abc import IMembershipService, ISamplingService
from unsserv.common.structs import Node, Property
from unsserv.common.utils import get_random_id
from unsserv.common.utils import stop_task
from unsserv.stable.sampling.config import RWDConfig
from unsserv.stable.sampling.protocol import RWDProtocol
from unsserv.stable.sampling.structs import Sample, SampleResult


class RWD(ISamplingService):
    properties = {Property.STABLE}
    _protocol: RWDProtocol
    _config: RWDConfig

    _neighbours: List[Node]
    _neighbour_weights: Dict[Node, float]
    _my_weight: float
    _sampling_queue: Dict[str, Node]
    _sampling_events: Dict[str, asyncio.Event]
    _new_neighbours_event: asyncio.Event

    def __init__(self, membership: IMembershipService):
        self.my_node = membership.my_node

        self.membership = membership
        self._protocol = RWDProtocol(self.my_node)
        self._config = RWDConfig()

        self._neighbours = []
        self._neighbour_weights = {}
        self._sampling_queue = {}
        self._sampling_events = {}

        self._new_neighbours_event = asyncio.Event()

    async def join(self, service_id: str, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Sampling")
        self.service_id = service_id
        await self._initialize_protocol()
        self._config.load_from_dict(configuration)
        # initialize neighbours
        neighbours = self.membership.get_neighbours()
        assert isinstance(neighbours, list)
        self._neighbours = neighbours
        self._new_neighbours_event.set()
        self._maintain_weights_task = asyncio.create_task(
            self._weights_maintenance_loop()
        )  # stop degrees updater task
        # initialize RPC
        self.membership.add_neighbours_handler(
            self._membership_neighbours_handler  # type: ignore
        )
        self.running = True

    async def leave(self):
        if not self.running:
            return
        self.membership.remove_neighbours_handler(self._membership_neighbours_handler)
        self._neighbours = []
        await self._protocol.stop()
        if self._maintain_weights_task:  # stop degrees updater task
            await stop_task(self._maintain_weights_task)
        self.running = False

    async def get_sample(self) -> Node:
        if not self.running:
            raise RuntimeError("Sampling service not running")
        sample_id = get_random_id()
        if not self._neighbours:
            raise ServiceError("Unable to peer with neighbours for sampling.")
        event = asyncio.Event()
        self._sampling_events[sample_id] = event
        random_node = self._choose_next_hop()
        sample = Sample(id=sample_id, origin_node=self.my_node, ttl=self._config.TTL)
        await self._protocol.sample(random_node, sample)
        try:
            await asyncio.wait_for(event.wait(), timeout=self._config.TIMEOUT)
        except asyncio.TimeoutError:
            del self._sampling_events[sample_id]
            raise ServiceError("Sampling service timeouted")
        sample_result = self._sampling_queue[sample_id]
        del self._sampling_queue[sample_id]
        return sample_result

    def _choose_next_hop(self) -> Node:
        neighbours, weights = zip(*self._neighbour_weights.items())
        next_hop = random.choices(
            neighbours + (self.my_node,), weights=weights + (self._my_weight,)
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
            self._neighbour_weights[neighbour] = 1 / self._config.MORE_THAN_MAXIMUM
        self._my_weight = 1 - (len(self._neighbours) / self._config.MORE_THAN_MAXIMUM)

    async def _distribute_weights(self):
        neighbours = self._neighbours.copy()
        while self._my_weight >= self._config.QUANTUM and neighbours:
            neighbour = random.choice(neighbours)
            is_increased = await self._protocol.increase(neighbour)
            if is_increased and neighbour in self._neighbour_weights:
                self._neighbour_weights[neighbour] += self._config.QUANTUM
                self._my_weight -= self._config.QUANTUM
            else:
                neighbours.remove(neighbour)

    async def _membership_neighbours_handler(self, new_neighbours: List[Node]):
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
        if (
            self._my_weight >= self._config.QUANTUM
            and sender in self._neighbour_weights
        ):
            self._neighbour_weights[sender] += self._config.QUANTUM
            self._my_weight -= self._config.QUANTUM
            return True
        return False

    async def _initialize_protocol(self):
        self._protocol.set_handler_sample(self._handler_sample)
        self._protocol.set_handler_sample_result(self._handler_sample_result)
        self._protocol.set_handler_increase(self._handler_get_increase)
        await self._protocol.start(self.service_id)

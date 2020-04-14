import asyncio
import random
from typing import Any, Dict, Optional

from unsserv.common.services_abc import ISearchingService, IMembershipService
from unsserv.common.structs import Node, Property
from unsserv.common.utils import get_random_id
from unsserv.extreme.searching.config import KWalkerConfig
from unsserv.extreme.searching.protocol import KWalkerProtocol
from unsserv.extreme.searching.structs import Walk, WalkResult


class KWalker(ISearchingService):
    properties = {Property.EXTREME}
    _protocol: KWalkerProtocol
    _config: KWalkerConfig

    _search_data: Dict[str, bytes]
    _walk_events: Dict[str, asyncio.Event]
    _walk_results: Dict[str, bytes]

    def __init__(self, membership: IMembershipService):
        self.membership = membership
        self.my_node = membership.my_node
        self._protocol = KWalkerProtocol(self.my_node)
        self._config = KWalkerConfig()

        self._search_data = {}
        self._walk_results = {}
        self._walk_events = {}

    async def join(self, service_id: str, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Searching service")
        self.service_id = service_id
        await self._initialize_protocol()
        self._config.load_from_dict(configuration)
        self._search_data = {}
        self.running = True

    async def leave(self):
        if not self.running:
            return
        await self._protocol.stop()
        self.running = False

    async def publish(self, data_id: str, data: bytes):
        if not self.running:
            raise RuntimeError("Searching service not running")
        if data_id in self._search_data:
            raise KeyError("Data-id already published")
        if not isinstance(data, bytes):
            raise TypeError("Published data must be bytes type")
        if not isinstance(data_id, str):
            raise TypeError("Published data-id must be str type")

        self._search_data[data_id] = data

    async def unpublish(self, data_id: str):
        if not self.running:
            raise RuntimeError("Searching service not running")
        if data_id not in self._search_data:
            raise KeyError("Data-id not published")
        del self._search_data[data_id]

    async def search(self, data_id: str) -> Optional[bytes]:
        candidate_neighbours = self.membership.get_neighbours()
        assert isinstance(candidate_neighbours, list)
        fanout = min(self._config.FANOUT, len(candidate_neighbours))
        walk_id = get_random_id()
        walk = Walk(
            id=walk_id, data_id=data_id, origin_node=self.my_node, ttl=self._config.TTL
        )
        self._walk_events[walk_id] = asyncio.Event()
        for neighbour in random.sample(candidate_neighbours, fanout):
            await self._protocol.walk(neighbour, walk)
        try:
            return await asyncio.wait_for(
                self._get_walk_result(fanout, walk_id), timeout=self._config.TIMEOUT
            )
        except asyncio.TimeoutError:
            return None

    async def _get_walk_result(self, fanout, walk_id):
        results_amount = 0
        result = None
        while results_amount < fanout:
            await self._walk_events[walk_id].wait()
            results_amount += 1
            result = self._walk_results[walk_id]
            if result:
                return result
        return result

    async def _handler_walk(self, sender: Node, walk: Walk):
        result = self._search_data.get(walk.data_id, None)
        if result or walk.ttl < 1:
            walk_result = WalkResult(walk_id=walk.id, result=result)
            asyncio.create_task(
                self._protocol.walk_result(walk.origin_node, walk_result)
            )
        else:
            next_walk = Walk(
                id=walk.id,
                data_id=walk.data_id,
                origin_node=walk.origin_node,
                ttl=walk.ttl - 1,
            )
            candidate_neighbours = self.membership.get_neighbours()
            assert isinstance(candidate_neighbours, list)
            neighbour = random.choice(candidate_neighbours)
            asyncio.create_task(self._protocol.walk(neighbour, next_walk))

    async def _handler_walk_result(self, sender: Node, walk_result: WalkResult):
        self._walk_results[walk_result.walk_id] = walk_result.result
        self._walk_events[walk_result.walk_id].set()

    async def _initialize_protocol(self):
        self._protocol.set_handler_walk(self._handler_walk)
        self._protocol.set_handler_walk_result(self._handler_walk_result)
        await self._protocol.start(self.service_id)

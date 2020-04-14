import asyncio
from contextlib import asynccontextmanager
from functools import reduce
from typing import Optional, Any, List, Dict, Set

from unsserv.common.services_abc import ISearchingService, IMembershipService
from unsserv.common.structs import Node, Property
from unsserv.common.utils import get_random_id, stop_task
from unsserv.stable.searching.config import ABloomConfig
from unsserv.stable.searching.protocol import ABloomProtocol
from unsserv.stable.searching.structs import Search, SearchResult, DataChange
from unsserv.stable.searching.typing import DataID, SearchID


class ABloom(ISearchingService):
    properties = {Property.STABLE}
    _protocol: ABloomProtocol
    _config: ABloomConfig

    _neighbours: List[Node]
    _local_data: Dict[DataID, bytes]
    _abloom_filters: Dict[Node, List[Set[DataID]]]
    _filters_maintenance_task: asyncio.Task
    _search_events: Dict[SearchID, asyncio.Event]
    _search_results: Dict[SearchID, bytes]

    def __init__(self, membership: IMembershipService):
        self.my_node = membership.my_node
        self.membership = membership
        self._protocol = ABloomProtocol(self.my_node)
        self._config = ABloomConfig()

        self._init_structs()

    async def join(self, service_id: str, **configuration: Any):
        if self.running:
            raise RuntimeError("Service already running")
        self.service_id = service_id
        await self._initialize_protocol()
        self._config.load_from_dict(configuration)
        self._init_structs()
        self._filters_maintenance_task = asyncio.create_task(
            self._filter_maintenance_loop()
        )
        self.running = True

    async def leave(self):
        if not self.running:
            return
        await stop_task(self._filters_maintenance_task)
        await self._protocol.stop()
        self.running = False

    async def publish(self, data_id: str, data: bytes):
        if not self.running:
            raise RuntimeError("Service must be running for pubslishing data")
        assert isinstance(data, bytes)
        if data_id in self._local_data:
            raise KeyError("Duplicated Key")
        self._local_data[data_id] = data
        asyncio.create_task(self._publish_to_neighbours(data_id))

    async def unpublish(self, data_id: str):
        if not self.running:
            raise RuntimeError("Service must be running for unpubslishing data")
        del self._local_data[data_id]
        asyncio.create_task(self._unpublish_to_neighbours(data_id))

    async def search(self, data_id: str) -> Optional[bytes]:
        if data_id in self._local_data:
            return self._local_data[data_id]
        next_hop = self._search_data_in_filters(data_id, self._config.DEPTH)
        if not next_hop:
            return None
        async with self._start_search(data_id, next_hop) as search:
            await asyncio.wait_for(
                self._search_events[search.id].wait(), timeout=self._config.TIMEOUT
            )
            return self._search_results[search.id]

    async def _publish_to_neighbours(self, data_id: DataID):
        data_change = DataChange(data_id=data_id, ttl=self._config.DEPTH)
        for neighbour in self._neighbours:
            asyncio.create_task(self._protocol.publish(neighbour, data_change))

    async def _unpublish_to_neighbours(self, data_id: DataID):
        data_change = DataChange(data_id=data_id, ttl=self._config.DEPTH)
        for neighbour in self._neighbours:
            asyncio.create_task(self._protocol.unpublish(neighbour, data_change))

    @asynccontextmanager
    async def _start_search(self, data_id: DataID, next_hop: Node):
        search = Search(
            id=get_random_id(),
            origin_node=self.my_node,
            ttl=self._config.DEPTH,
            data_id=data_id,
        )
        self._search_events[search.id] = asyncio.Event()
        try:
            await self._protocol.search(next_hop, search)
            yield search
        finally:
            del self._search_events[search.id]
            self._remove_id_from_filter(data_id, next_hop)
            if data_id in self._search_results:
                del self._search_results[data_id]

    def _init_structs(self):
        self._neighbours = []
        self._abloom_filters = {}
        self._abloom_filters = {}
        self._local_data = {}

        self._search_events = {}
        self._search_results = {}

    async def _initialize_new_neighbours(self, new_neighbours: List[Node]):
        for neighbour in new_neighbours:
            try:
                self._abloom_filters[neighbour] = await self._protocol.get_filter(
                    neighbour
                )
                self._neighbours.append(neighbour)
            except ConnectionError:
                pass

    async def _filter_maintenance_loop(self):
        while True:
            await asyncio.sleep(self._config.MAINTENANCE_SLEEP)
            new_neighbours = set(self.membership.get_neighbours())
            old_neighbours = set(self._neighbours)
            for neighbour in old_neighbours - new_neighbours:
                del self._abloom_filters[neighbour]
                self._neighbours.remove(neighbour)
            await self._initialize_new_neighbours(list(new_neighbours - old_neighbours))

    def _search_data_in_filters(self, data_id: str, max_depth: int) -> Optional[Node]:
        for neighbour in self._neighbours:
            for depth in range(max_depth):
                if data_id in self._abloom_filters[neighbour][depth]:
                    return neighbour
        return None

    def _remove_id_from_filter(self, data_id: DataID, neighbour: Node):
        for depth in range(self._config.DEPTH):
            if data_id in self._abloom_filters[neighbour][depth]:
                self._abloom_filters[neighbour][depth].remove(data_id)
                return

    async def _handler_publish(self, sender: Node, data_change: DataChange):
        if sender not in self._neighbours:
            return
        depth = self._config.DEPTH - data_change.ttl
        self._abloom_filters[sender][depth].add(data_change.data_id)
        if data_change.ttl < 2:
            return
        next_data_change = DataChange(
            data_id=data_change.data_id, ttl=data_change.ttl - 1
        )
        for neighbour in self._neighbours:
            asyncio.create_task(self._protocol.publish(neighbour, next_data_change))

    async def _handler_unpublish(self, sender: Node, data_change: DataChange):
        if sender not in self._neighbours:
            return
        depth = self._config.DEPTH - data_change.ttl
        self._abloom_filters[sender][depth].remove(data_change.data_id)
        if data_change.ttl < 2:
            return
        next_data_change = DataChange(
            data_id=data_change.data_id, ttl=data_change.ttl - 1
        )
        for neighbour in self._neighbours:
            asyncio.create_task(self._protocol.unpublish(neighbour, next_data_change))

    async def _handler_get_filter(self, sender: Node):
        filter = [set(self._local_data.keys())]
        for depth in range(1, self._config.DEPTH):
            filters = list(
                map(lambda n: self._abloom_filters[n][depth - 1], self._neighbours)
            )
            if filters:
                filter.append(reduce(lambda f1, f2: f1.union(f2), filters))
            else:
                filter.append(set())
        return filter

    async def _handler_search(self, sender: Node, search: Search):
        if search.data_id in self._local_data:
            search_result = SearchResult(
                search_id=search.id, result=self._local_data[search.data_id]
            )
            asyncio.create_task(
                self._protocol.search_result(search.origin_node, search_result)
            )
            return
        next_hop = self._search_data_in_filters(search.data_id, search.ttl - 1)
        if next_hop and search.ttl > 1:
            next_search = Search(
                id=search.id,
                origin_node=search.origin_node,
                ttl=search.ttl - 1,
                data_id=search.data_id,
            )
            asyncio.create_task(self._protocol.search(next_hop, next_search))
        else:
            raise ValueError("Data was not found")

    async def _handler_search_result(self, sender: Node, search_result: SearchResult):
        self._search_results[search_result.search_id] = search_result.result
        self._search_events[search_result.search_id].set()

    async def _initialize_protocol(self):
        self._protocol.set_handler_publish(self._handler_publish)
        self._protocol.set_handler_unpublish(self._handler_unpublish)
        self._protocol.set_handler_get_filter(self._handler_get_filter)
        self._protocol.set_handler_search(self._handler_search)
        self._protocol.set_handler_search_result(self._handler_search_result)
        await self._protocol.start(self.service_id)

from abc import ABC, abstractmethod
import asyncio
import random
from collections import Counter
from contextlib import contextmanager
from typing import List, Set, Counter as CounterType

from unsserv.common.structs import Node
from unsserv.stable.membership.double_layered.config import (
    ACTIVE_VIEW_SIZE,
    ACTIVE_VIEW_MAINTAIN_FREQUENCY,
    TTL,
)
from unsserv.stable.membership.double_layered.protocol import DoubleLayeredProtocol
from unsserv.stable.membership.double_layered.structs import ForwardJoin


class IDoubleLayered(ABC):
    _doble_layered_protocol: DoubleLayeredProtocol
    _active_view: Set[Node]
    _candidate_neighbours: CounterType[Node]
    _local_view_maintenance_task: asyncio.Task

    def __init__(self, my_node: Node):
        self.my_node = my_node
        self._doble_layered_protocol = DoubleLayeredProtocol(my_node)
        self._active_view = set()
        self._candidate_neighbours = Counter()

    async def _start_two_layered(self, service_id: str):
        await self._initialize_double_layered_protocol(service_id)
        self._local_view_maintenance_task = asyncio.create_task(
            self._maintain_active_view_loop()
        )

    async def _stop_two_layered(self):
        self._active_view = set()
        if self._local_view_maintenance_task:
            self._local_view_maintenance_task.cancel()
            try:
                await self._local_view_maintenance_task
            except asyncio.CancelledError:
                pass
        await self._doble_layered_protocol.stop()

    async def _connect_to_node(self, node: Node):
        with self._create_candidate_neighbour(node):
            is_a_priority = len(self._active_view) == 0
            try:
                is_connected = await self._doble_layered_protocol.connect(
                    node, is_a_priority
                )
                if is_connected:
                    self._active_view.add(node)
            except ConnectionError:
                pass

    async def _join_first_time(self):
        bootstrap_nodes = self._get_passive_view_nodes()
        while bootstrap_nodes and len(self._active_view) < ACTIVE_VIEW_SIZE:
            candidate_node = bootstrap_nodes.pop(random.randrange(len(bootstrap_nodes)))
            with self._create_candidate_neighbour(candidate_node):
                try:
                    await self._doble_layered_protocol.join(candidate_node)
                    self._active_view.add(candidate_node)
                    break
                except ConnectionError:
                    pass

    async def _try_disconnect(self, node: Node):
        try:
            await self._doble_layered_protocol.disconnect(node)
        except ConnectionError:
            pass

    async def _maintain_active_view_loop(self):
        await self._join_first_time()
        while True:
            await asyncio.sleep(ACTIVE_VIEW_MAINTAIN_FREQUENCY)
            await self._update_active_view()

    async def _update_active_view(self):
        inactive_nodes = set()
        for node in self._active_view.copy():
            try:
                is_still_connected = await self._doble_layered_protocol.stay_connected(
                    node
                )
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

    async def _handler_join(self, sender: Node):
        while len(self._active_view) >= ACTIVE_VIEW_SIZE:
            random_neighbour = random.choice(list(self._active_view))
            self._active_view.remove(random_neighbour)  # randomly remove
            asyncio.create_task(self._try_disconnect(random_neighbour))
        self._active_view.add(sender)
        forward_join = ForwardJoin(origin_node=sender, ttl=TTL)
        for neighbour in list(filter(lambda n: n != sender, self._active_view)):
            asyncio.create_task(
                self._doble_layered_protocol.forward_join(neighbour, forward_join)
            )

    async def _handler_forward_join(self, sender: Node, forward_join: ForwardJoin):
        if forward_join.ttl == 0:
            asyncio.create_task(self._connect_to_node(forward_join.origin_node))
        else:
            candidate_neighbours = list(
                filter(lambda n: n != forward_join.origin_node, self._active_view)
            ) or [self.my_node]
            neighbour = random.choice(candidate_neighbours)
            next_forward_join = ForwardJoin(
                origin_node=forward_join.origin_node, ttl=forward_join.ttl - 1
            )
            asyncio.create_task(
                self._doble_layered_protocol.forward_join(neighbour, next_forward_join)
            )

    async def _handler_connect(self, sender: Node, is_a_priority: bool):
        if not is_a_priority and len(self._active_view) >= ACTIVE_VIEW_SIZE:
            return False
        while len(self._active_view) >= ACTIVE_VIEW_SIZE:
            random_neighbour = random.choice(list(self._active_view))
            self._active_view.remove(random_neighbour)  # randomly remove
            asyncio.create_task(self._try_disconnect(random_neighbour))
        self._active_view.add(sender)
        return True

    async def _handler_disconnect(self, sender: Node):
        if sender in self._active_view:
            self._active_view.remove(sender)

    async def _handler_stay_connected(self, sender: Node):
        return sender in self._active_view or sender in self._candidate_neighbours

    async def _initialize_double_layered_protocol(self, service_id: str):
        self._doble_layered_protocol.set_handler_join(self._handler_join)
        self._doble_layered_protocol.set_handler_forward_join(
            self._handler_forward_join
        )
        self._doble_layered_protocol.set_handler_connect(self._handler_connect)
        self._doble_layered_protocol.set_handler_disconnect(self._handler_disconnect)
        self._doble_layered_protocol.set_handler_stay_connected(
            self._handler_stay_connected
        )
        await self._doble_layered_protocol.start(service_id)

    @abstractmethod
    def _get_passive_view_nodes(self):
        pass
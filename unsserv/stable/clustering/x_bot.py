import asyncio
import random
from collections import Counter
from typing import Any, Callable, Union, List

from unsserv.common.service_properties import Property
from unsserv.common.services_abc import MembershipService, ClusteringService
from unsserv.common.structs import Node
from unsserv.common.utils import stop_task
from unsserv.common.typing import NeighboursCallback
from unsserv.common.typing import View
from unsserv.stable.clustering.config import (
    ACTIVE_VIEW_SIZE,
    ACTIVE_VIEW_MAINTAIN_FREQUENCY,
    UNBIASED_NODES,
    PASSIVE_SCAN_LENGTH,
)
from unsserv.stable.clustering.protocol import XBotProtocol
from unsserv.stable.clustering.structs import Replace
from unsserv.stable.membership.double_layered.double_layered import IDoubleLayered

RankingFunction = Callable[[Node], Any]


class XBot(ClusteringService, IDoubleLayered):
    properties = {Property.STABLE, Property.SYMMETRIC}
    _protocol: XBotProtocol
    _callback: NeighboursCallback
    _local_view_optimize_task: asyncio.Task

    def __init__(self, membership: MembershipService):
        super().__init__(membership.my_node)
        self.membership = membership
        self._protocol = XBotProtocol(membership.my_node)
        self._callback = None
        self._ranking_function: RankingFunction

    async def join(self, service_id: Any, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Clustering")
        self.service_id = service_id
        self._ranking_function = configuration["ranking_function"]
        await self._start_two_layered(f"double_layered-{service_id}")
        await self._initialize_protocol()
        self._local_view_optimize_task = asyncio.create_task(
            self._optimize_active_view_loop()
        )
        self.running = True

    async def leave(self):
        if not self.running:
            return
        if self._local_view_optimize_task:
            await stop_task(self._local_view_optimize_task)
        await self._protocol.stop()
        await self._stop_two_layered()
        self.running = False

    def get_neighbours(
        self, local_view_format: bool = False
    ) -> Union[List[Node], View]:
        return (
            Counter(self._active_view) if local_view_format else list(self._active_view)
        )

    def add_neighbours_callback(self, callback: NeighboursCallback):
        if not self.running:
            raise RuntimeError("Membership service not running")
        self._callbacks.append(callback)

    def remove_neighbours_callback(self, callback: NeighboursCallback):
        if callback not in self._callbacks:
            raise ValueError("Callback not found")
        self._callbacks.remove(callback)

    def _get_passive_view_nodes(self):
        return self.membership.get_neighbours()

    async def _optimize_active_view_loop(self):
        await asyncio.sleep(ACTIVE_VIEW_MAINTAIN_FREQUENCY)
        while True:
            old_active_view = self._active_view.copy()
            await asyncio.sleep(ACTIVE_VIEW_MAINTAIN_FREQUENCY)
            if len(self._active_view) >= ACTIVE_VIEW_SIZE:
                await self._optimize_active_view()  # todo: create task instead?
            self._call_callback_if_view_changed(old_active_view)

    async def _optimize_active_view(self):
        candidate_neighbours = self.membership.get_neighbours()
        if not candidate_neighbours:
            return
        candidate_neighbours = random.sample(
            candidate_neighbours, min(len(candidate_neighbours), PASSIVE_SCAN_LENGTH)
        )
        biasable_nodes = list(sorted(self._active_view, key=self._ranking_function))[
            UNBIASED_NODES:
        ]
        for old_node in biasable_nodes:
            if not candidate_neighbours:
                return None
            candidate_node = candidate_neighbours.pop()
            if self._get_the_best(candidate_node, old_node) == candidate_node:
                asyncio.create_task(self._optimize(old_node, candidate_node))

    async def _optimize(self, old_node: Node, new_node: Node):
        is_optimized = await self._protocol.optimization(new_node, old_node)
        if not is_optimized:
            return
        if old_node in self._active_view:
            asyncio.create_task(self._doble_layered_protocol.disconnect(old_node))
            self._active_view.remove(old_node)
        self._active_view.add(new_node)

    def _get_the_best(self, node1: Node, node2: Node):
        return (
            node1
            if self._ranking_function(node1) < self._ranking_function(node2)
            else node2
        )

    async def _handler_optimization(self, sender: Node, old_node: Node):
        if len(self._active_view) < ACTIVE_VIEW_SIZE:
            self._active_view.add(sender)
            return True

        replace_node = list(sorted(self._active_view, key=self._ranking_function))[-1]
        replace = Replace(old_node=old_node, origin_node=sender)
        is_replaced = await self._protocol.replace(replace_node, replace)

        if is_replaced:
            self._active_view.remove(replace_node)
            self._active_view.add(sender)
            return True
        return False

    async def _handler_replace(self, sender: Node, replace: Replace):
        if self._get_the_best(replace.old_node, sender) == sender:
            return False
        else:
            is_switched = await self._protocol.switch(
                replace.old_node, replace.origin_node
            )
            if is_switched:
                if sender in self._active_view:
                    self._active_view.remove(sender)
                self._active_view.add(replace.old_node)
                return True
            return False

    async def _handler_switch(self, sender: Node, origin_node: Node):
        if origin_node in self._active_view:
            self._active_view.remove(origin_node)
            self._active_view.add(sender)
            return True
        return False

    async def _initialize_protocol(self):
        self._protocol.set_handler_optimization(self._handler_optimization)
        self._protocol.set_handler_replace(self._handler_replace)
        self._protocol.set_handler_switch(self._handler_switch)
        await self._protocol.start(self.service_id)

import asyncio
import math
import random
from typing import Any, Set

from unsserv.common.errors import ServiceError
from unsserv.common.services_abc import IDisseminationService, IMembershipService
from unsserv.common.structs import Node, Property
from unsserv.common.typing import Handler
from unsserv.common.utils import get_random_id, HandlersManager, stop_task
from unsserv.stable.dissemination.brisa.config import BrisaConfig
from unsserv.stable.dissemination.brisa.protocol import BrisaProtocol
from unsserv.stable.dissemination.brisa.typing import BroadcastLevel


class Brisa(IDisseminationService):
    properties = {Property.STABLE, Property.ONE_TO_MANY}
    _protocol: BrisaProtocol
    _handler_manager: HandlersManager
    _config: BrisaConfig

    _level: BroadcastLevel
    _children: Set[Node]
    _parents: Set[Node]
    _im_root: bool
    _maintenance_task: asyncio.Task

    def __init__(self, membership: IMembershipService):
        self.my_node = membership.my_node
        self.membership = membership
        self._protocol = BrisaProtocol(self.my_node)
        self._handler_manager = HandlersManager()
        self._config = BrisaConfig()

        self._children = set()
        self._parents = set()
        self._level = math.inf
        self._im_root = False

    async def join(self, service_id: str, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Dissemination")
        self.service_id = service_id
        self._im_root = configuration.get("im_root", False)
        await self._initialize_protocol()
        self._handler_manager.add_handler(configuration["broadcast_handler"])
        self._config.load_from_dict(configuration)
        self._maintenance_task = asyncio.create_task(self._maintain_dag_loop())
        self.running = True

    async def leave(self):
        if not self.running:
            return
        await stop_task(self._maintenance_task)
        self._handler_manager.remove_all_handlers()
        await self._protocol.stop()
        self.running = False

    async def broadcast(self, data: bytes):
        if not self.running:
            raise RuntimeError("Dissemination service not running")
        if self._im_root is False:
            raise RuntimeError("Node must be root to broadcast")
        assert isinstance(data, bytes)
        await asyncio.wait_for(self._disseminate(data), timeout=self._config.TIMEOUT)

    def add_broadcast_handler(self, handler: Handler):
        self._handler_manager.add_handler(handler)

    def remove_broadcast_handler(self, handler: Handler):
        self._handler_manager.remove_handler(handler)

    async def _maintain_dag_loop(self):
        if self._im_root:
            self._broadcast_id = get_random_id()
            self._level = 0
        while True:
            await asyncio.sleep(self._config.MAINTENANCE_SLEEP)
            if not self._im_root:
                await self._maintain_parents()
                if not self._parents:
                    continue
            await self._maintain_children()

    async def _maintain_parents(self):
        for parent in self._parents.copy():
            if not await self._protocol.im_your_child(parent):
                self._parents.remove(parent)
        if not self._parents:
            await self._find_parent()

    async def _find_parent(self):
        self._level = math.inf
        candidate_parents = self.membership.get_neighbours()
        while not self._parents and candidate_parents:
            parent = random.choice(candidate_parents)
            candidate_parents.remove(parent)
            try:
                level = await self._protocol.become_my_parent(parent)
                if level == -1:
                    continue
                self._level = min(self._level, level + 1)
                self._parents.add(parent)
            except ConnectionError:
                pass

    async def _maintain_children(self):
        for child in self._children.copy():
            try:
                if not await self._protocol.session(child, self._level):
                    self._children.remove(child)
            except ConnectionError:
                pass
        if len(self._children) < self._config.FANOUT:
            await self._find_children()

    async def _find_children(self):
        candidate_children = self.membership.get_neighbours()
        while len(self._children) < self._config.FANOUT and candidate_children:
            child = random.choice(candidate_children)
            candidate_children.remove(child)
            try:
                if await self._protocol.session(child, self._level):
                    self._children.add(child)
            except ConnectionError:
                pass

    async def _disseminate(self, data: bytes):
        pushed_amount = 0
        for child in self._children:
            try:
                await self._protocol.push(child, data)
                pushed_amount += 1
            except ConnectionError:
                pass
        if pushed_amount == 0:
            raise ServiceError("Unable to peer with neighbours for disseminating")

    async def _handler_session(self, sender: Node, level: BroadcastLevel):
        if self._level <= level:
            return False
        self._parents.add(sender)
        self._level = min(level + 1, self._level)
        return True

    async def _handler_push(self, sender: Node, data: bytes):
        asyncio.create_task(self._disseminate(data))
        self._handler_manager.call_handlers(data)

    async def _handler_im_your_child(self, sender: Node):
        return sender in self._children

    async def _handler_become_my_parent(self, sender: Node):
        if (self._im_root or self._parents) and len(
            self._children
        ) < self._config.FANOUT:
            self._children.add(sender)
            return self._level
        return -1

    async def _initialize_protocol(self):
        self._protocol.set_handler_session(self._handler_session)
        self._protocol.set_handler_push(self._handler_push)
        self._protocol.set_handler_im_your_child(self._handler_im_your_child)
        self._protocol.set_handler_become_my_parent(self._handler_become_my_parent)
        await self._protocol.start(self.service_id)

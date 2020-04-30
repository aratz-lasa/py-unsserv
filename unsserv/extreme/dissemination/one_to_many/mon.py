import asyncio
import random
from typing import Any, List, Dict

from unsserv.common.errors import ServiceError
from unsserv.common.services_abc import IDisseminationService, IMembershipService
from unsserv.common.structs import Node, Property
from unsserv.common.typing import Handler
from unsserv.common.utils import get_random_id, HandlersManager, stop_task
from unsserv.extreme.dissemination.one_to_many.config import MonConfig
from unsserv.extreme.dissemination.one_to_many.protocol import MonProtocol
from unsserv.extreme.dissemination.one_to_many.structs import Session, Broadcast
from unsserv.extreme.dissemination.one_to_many.typing import BroadcastID


class Mon(IDisseminationService):
    properties = {Property.EXTREME, Property.ONE_TO_MANY}
    _protocol: MonProtocol
    _handlers_manager: HandlersManager
    _config: MonConfig

    _levels: Dict[BroadcastID, int]
    _children: Dict[BroadcastID, List[Node]]
    _parents: Dict[BroadcastID, List[Node]]
    _received_data: Dict[
        BroadcastID, Any
    ]  # stores the data received from each broadcast (for avoiding duplicates)
    _children_ready_events: Dict[BroadcastID, asyncio.Event]
    _cleanup_tasks: List[asyncio.Task]

    def __init__(self, membership: IMembershipService):
        self.my_node = membership.my_node
        self.membership = membership
        self._protocol = MonProtocol(self.my_node)
        self._handlers_manager = HandlersManager()
        self._config = MonConfig()

        self._children = {}
        self._parents = {}
        self._levels = {}
        self._received_data = {}

        self._children_ready_events = {}
        self._cleanup_tasks = []

    async def join(self, service_id: str, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Dissemination")
        self.service_id = service_id
        await self._initialize_protocol()
        if "broadcast_handler" in configuration:
            self._handlers_manager.add_handler(configuration["broadcast_handler"])
        self._config.load_from_dict(configuration)
        self.running = True

    async def leave(self):
        if not self.running:
            return
        for task in self._cleanup_tasks:
            await stop_task(task)
        self._handlers_manager.remove_all_handlers()
        await self._protocol.stop()
        self.running = False

    async def broadcast(self, data: bytes):
        if not self.running:
            raise RuntimeError("Dissemination service not running")
        assert isinstance(data, bytes)
        broadcast_id = await asyncio.wait_for(
            self._build_dag(), timeout=self._config.TIMEOUT
        )
        await asyncio.wait_for(
            self._disseminate(broadcast_id, data), timeout=self._config.TIMEOUT
        )

    def add_broadcast_handler(self, handler: Handler):
        self._handlers_manager.add_handler(handler)

    def remove_broadcast_handler(self, handler: Handler):
        self._handlers_manager.remove_handler(handler)

    async def _build_dag(self) -> str:
        broadcast_id = get_random_id()
        self._levels[broadcast_id] = 0
        self._children_ready_events[broadcast_id] = asyncio.Event()
        candidate_children = self.membership.get_neighbours()
        asyncio.create_task(
            self._initialize_children(
                broadcast_id, candidate_children, broadcast_origin=True
            )
        )
        return broadcast_id

    async def _initialize_children(
        self, broadcast_id: str, neighbours: list, broadcast_origin=False
    ):
        children: List[Node] = []
        fanout = min(len(neighbours), self._config.FANOUT)
        session = Session(broadcast_id=broadcast_id, level=self._levels[broadcast_id])
        while neighbours and len(children) <= fanout:
            child = random.choice(neighbours)
            neighbours.remove(child)
            try:
                session_ok = await self._protocol.session(child, session)
            except ConnectionError:
                continue
            if session_ok:
                children.append(child)
        if broadcast_origin and len(children) == 0:
            raise ServiceError("Unable to peer with neighbours for disseminating")
        self._children[broadcast_id] = children
        self._children_ready_events[broadcast_id].set()

        self._cleanup_tasks.append(
            asyncio.create_task(self._cleanup_tree(broadcast_id))
        )

    async def _disseminate(self, broadcast_id: str, data: Any):
        await self._children_ready_events[
            broadcast_id
        ].wait()  # wait children to initialize
        broadcast = Broadcast(id=broadcast_id, data=data)
        pushed_amount = 0
        for child in self._children[broadcast_id]:
            try:
                await self._protocol.push(child, broadcast)
                pushed_amount += 1
            except ConnectionError:
                pass
        if pushed_amount == 0:
            raise ServiceError("Unable to peer with neighbours for disseminating")

    async def _cleanup_tree(self, broadcast_id: str):
        await asyncio.sleep(self._config.TREE_LIFE_TIME)
        if broadcast_id in self._levels:
            del self._levels[broadcast_id]
        if broadcast_id in self._children_ready_events:
            del self._children_ready_events[broadcast_id]
        if broadcast_id in self._children:
            del self._children[broadcast_id]
        if broadcast_id in self._received_data:
            del self._received_data[broadcast_id]

    async def _handler_session(self, sender: Node, session: Session):
        first_time = session.broadcast_id not in self._levels
        if first_time:
            self._parents[session.broadcast_id] = [sender]
            self._levels[session.broadcast_id] = session.level + 1
            candidate_children = list(set(self.membership.get_neighbours()) - {sender})
            self._children_ready_events[session.broadcast_id] = asyncio.Event()
            asyncio.create_task(
                self._initialize_children(session.broadcast_id, candidate_children)
            )
        else:
            if self._levels[session.broadcast_id] <= session.level:
                return False
            if sender not in self._parents[session.broadcast_id]:
                self._parents[session.broadcast_id].append(sender)
        return True

    async def _handler_push(self, sender: Node, broadcast: Broadcast):
        # if already received data, ignores it
        if (
            broadcast.id in self._children_ready_events
            and broadcast.id not in self._received_data
        ):
            self._received_data[broadcast.id] = broadcast.data
            asyncio.create_task(self._disseminate(broadcast.id, broadcast.data))
            self._handlers_manager.call_handlers(broadcast.data)

    async def _initialize_protocol(self):
        self._protocol.set_handler_session(self._handler_session)
        self._protocol.set_handler_push(self._handler_push)
        await self._protocol.start(self.service_id)

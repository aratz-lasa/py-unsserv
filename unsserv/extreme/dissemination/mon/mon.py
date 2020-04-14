import asyncio
import random
from typing import Any, List, Dict

from unsserv.common.structs import Node
from unsserv.common.errors import ServiceError
from unsserv.common.service_properties import Property
from unsserv.common.services_abc import DisseminationService, MembershipService
from unsserv.common.typing import Handler
from unsserv.common.utils import get_random_id, HandlerManager
from unsserv.extreme.dissemination.mon.config import MON_FANOUT
from unsserv.extreme.dissemination.mon.protocol import MonProtocol
from unsserv.extreme.dissemination.mon.structs import Session, Broadcast
from unsserv.extreme.dissemination.mon.typing import BroadcastID


class Mon(DisseminationService):
    properties = {Property.EXTREME, Property.ONE_TO_MANY}
    _handler_manager: HandlerManager
    _protocol: MonProtocol
    _levels: Dict[BroadcastID, int]
    _children: Dict[BroadcastID, List[Node]]
    _parents: Dict[BroadcastID, List[Node]]
    _received_data: Dict[
        BroadcastID, Any
    ]  # stores the data received from each broadcast (for avoiding duplicates)
    _children_ready_events: Dict[BroadcastID, asyncio.Event]

    def __init__(self, membership: MembershipService):
        self.my_node = membership.my_node
        self.membership = membership
        self._handler_manager = HandlerManager()
        self._protocol = MonProtocol(self.my_node)

        self._children = {}
        self._parents = {}
        self._levels = {}
        self._received_data = {}

        self._children_ready_events = {}

    async def join(self, service_id: str, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Dissemination")
        self._handler_manager.add_handler(configuration["broadcast_handler"])
        self.service_id = service_id
        await self._initialize_protocol()
        self.running = True

    async def leave(self):
        if not self.running:
            return
        self._handler_manager.remove_all_handlers()
        await self._protocol.stop()
        self.running = False

    async def broadcast(self, data: bytes):
        if not self.running:
            raise RuntimeError("Dissemination service not running")
        assert isinstance(data, bytes)
        broadcast_id = await self._build_dag()
        await self._disseminate(broadcast_id, data)

    def add_broadcast_handler(self, handler: Handler):
        self._handler_manager.add_handler(handler)

    def remove_broadcast_handler(self, handler: Handler):
        self._handler_manager.remove_handler(handler)

    async def _build_dag(self) -> str:
        broadcast_id = get_random_id()
        self._levels[broadcast_id] = 0
        self._children_ready_events[broadcast_id] = asyncio.Event()
        candidate_children = self.membership.get_neighbours()
        assert isinstance(candidate_children, list)
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
        fanout = min(len(neighbours), MON_FANOUT)
        session = Session(broadcast_id=broadcast_id, level=0)
        while neighbours and len(children) <= fanout:
            child = random.choice(neighbours)
            neighbours.remove(child)
            try:
                session_ok = await self._protocol.session(child, session)
            except Exception:
                continue
            if session_ok:
                children.append(child)
        if broadcast_origin and len(children) == 0:
            raise ServiceError("Unable to peer with neighbours for disseminating")
        self._children[broadcast_id] = children
        self._children_ready_events[broadcast_id].set()

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
            if (
                sender not in self._parents[session.broadcast_id]
            ):  # just in case it is a duplicate
                self._parents[session.broadcast_id].append(sender)
        return True

    async def _handler_push(self, sender: Node, broadcast: Broadcast):
        if (
            broadcast.id not in self._received_data
        ):  # if already received data, ignores it
            self._received_data[broadcast.id] = broadcast.data
            asyncio.create_task(self._disseminate(broadcast.id, broadcast.data))
            self._handler_manager.call_handlers(broadcast.data)

    async def _initialize_protocol(self):
        self._protocol.set_handler_session(self._handler_session)
        self._protocol.set_handler_push(self._handler_push)
        await self._protocol.start(self.service_id)

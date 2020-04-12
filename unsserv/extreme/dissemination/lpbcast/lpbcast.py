import asyncio
import random
from collections import OrderedDict
from typing import Any, List, Union

from unsserv.common.service_properties import Property
from unsserv.common.services_abc import DisseminationService, MembershipService
from unsserv.common.structs import Node
from unsserv.common.typing import BroadcastHandler
from unsserv.common.utils import get_random_id
from unsserv.extreme.dissemination.lpbcast.config import (
    FANOUT,
    LPBCAST_THRESHOLD,
)
from unsserv.extreme.dissemination.lpbcast.protocol import LpbcastProtocol
from unsserv.extreme.dissemination.lpbcast.structs import Event
from unsserv.extreme.dissemination.lpbcast.typing import (
    EventId,
    EventData,
    EventOrigin,
)


class Lpbcast(DisseminationService):
    properties = {Property.EXTREME, Property.MANY_TO_MANY}
    _protocol: LpbcastProtocol
    _broadcast_handler: BroadcastHandler

    _events: "OrderedDict[EventId, List[Union[EventData, EventOrigin]]]"
    _events_digest: "OrderedDict[EventId, EventOrigin]"

    def __init__(self, membership: MembershipService):
        self.my_node = membership.my_node
        self.membership = membership
        self._broadcast_handler = None
        self._protocol = LpbcastProtocol(self.my_node)

        self._events = OrderedDict()
        self._events_digest = OrderedDict()

    async def join_broadcast(self, service_id: str, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Dissemination")
        self._broadcast_handler = configuration["broadcast_handler"]
        self.service_id = service_id
        await self._initialize_protocol()
        self.running = True

    async def leave_broadcast(self):
        if not self.running:
            return
        await self._protocol.stop()
        self._broadcast_handler = None
        self.running = False

    async def broadcast(self, data: bytes):
        if not self.running:
            raise RuntimeError("Dissemination service not running")
        assert isinstance(data, bytes)
        event_id = get_random_id()
        await self._handle_new_event(
            event_id, data, self.my_node, broadcast_origin=True
        )

    async def _handle_new_event(
        self,
        event_id: EventId,
        event_data: EventData,
        event_origin: Node,
        broadcast_origin=False,
    ):
        if event_id in self._events_digest:
            return
        self._events[event_id] = [event_data, event_origin]
        self._events_digest[event_id] = event_origin
        self._purge_events_threshold()
        asyncio.create_task(self._disseminate(event_id, event_data, event_origin))
        if not broadcast_origin:
            asyncio.create_task(self._broadcast_handler(event_data))

    async def _disseminate(
        self, event_id: EventId, event_data: EventData, event_origin: Node
    ):
        candidate_neighbours = self.membership.get_neighbours()
        assert isinstance(candidate_neighbours, list)
        fanout = min(FANOUT, len(candidate_neighbours))
        for neighbour in random.choices(candidate_neighbours, k=fanout):
            try:
                digest = list(map(lambda e: (e[0], e[1]), self._events_digest.items()))
                event = Event(
                    id=event_id, data=event_data, origin=event_origin, digest=digest
                )
                await self._protocol.push_event(neighbour, event)
            except Exception:
                pass  # todo: log the error?

    def _purge_events_threshold(self):
        while LPBCAST_THRESHOLD < len(self._events):
            self._events.popitem(last=False)
        while LPBCAST_THRESHOLD < len(self._events_digest):
            self._events_digest.popitem(last=False)

    async def _retrieve_event(
        self, event_source: Node, event_id: EventId, event_origin: EventOrigin,
    ):

        try:
            event_data, _ = await self._protocol.retrieve_event(event_source, event_id)
            assert isinstance(event_data, bytes)
            return await self._handle_new_event(event_id, event_data, event_origin)
        except Exception:
            pass
        try:
            candidate_neighbours = self.membership.get_neighbours()
            assert isinstance(candidate_neighbours, list)
            random_neighbour = random.choice(candidate_neighbours)
            event_data, _ = await self._protocol.retrieve_event(
                random_neighbour, event_id
            )
            assert isinstance(event_data, bytes)
            return await self._handle_new_event(event_id, event_data, event_origin)
        except Exception:
            pass
        try:
            event_data, _ = await self._protocol.retrieve_event(event_origin, event_id)
            assert isinstance(event_data, bytes)
            return await self._handle_new_event(event_id, event_data, event_origin)
        except Exception:
            pass

    async def _handler_push_event(self, sender: Node, event: Event):
        asyncio.create_task(self._handle_new_event(event.id, event.data, event.origin,))
        for message_id, message_origin in event.digest:
            if message_id not in self._events_digest:  # not the one received
                asyncio.create_task(
                    self._retrieve_event(sender, message_id, message_origin)
                )

    async def _handler_retrieve_event(self, sender: Node, event_id: str):
        return self._events.get(event_id, None)

    async def _initialize_protocol(self):
        self._protocol.set_handler_push_event(self._handler_push_event)
        self._protocol.set_handler_retrieve_event(self._handler_retrieve_event)
        await self._protocol.start(self.service_id)

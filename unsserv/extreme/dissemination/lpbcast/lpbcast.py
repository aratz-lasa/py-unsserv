import asyncio
import random
from collections import OrderedDict
from enum import IntEnum, auto
from typing import Any, List, Optional, Union

from unsserv.common.utils import parse_node, get_random_id
from unsserv.common.data_structures import Message, Node
from unsserv.common.rpc.rpc import RPCRegister, RPC
from unsserv.common.services_abc import DisseminationService, MembershipService
from unsserv.common.typing import BroadcastHandler
from unsserv.extreme.dissemination.lpbcast.lpbcast_config import (
    FIELD_COMMAND,
    FANOUT,
    FIELD_EVENT_ID,
    FIELD_EVENT_DATA,
    FIELD_EVENT_ORIGIN,
    FIELD_DIGEST,
    FIELD_RETRIEVE_EVENT,
    LPBCAST_THRESHOLD,
)
from unsserv.extreme.dissemination.lpbcast.lpbcast_typing import (
    EventId,
    EventData,
    EventOrigin,
)


class LpbcastCommand(IntEnum):
    PUSH_EVENT = auto()
    RETRIEVE_EVENT = auto()


class LpbcastProtocol:
    def __init__(self, my_node: Node, service_id: Any):
        self.my_node = my_node
        self.service_id = service_id

    def make_push_event_message(
        self,
        event_id: EventId,
        event_data: EventData,
        event_origin: Node,
        digest: List[List[Union[EventId, EventOrigin]]],
    ):
        data = {
            FIELD_COMMAND: LpbcastCommand.PUSH_EVENT,
            FIELD_EVENT_ID: event_id,
            FIELD_EVENT_DATA: event_data,
            FIELD_EVENT_ORIGIN: event_origin,
            FIELD_DIGEST: digest,
        }
        return Message(self.my_node, self.service_id, data)

    def make_retrieve_event_message(self, retrieve_event: EventId):
        data = {
            FIELD_COMMAND: LpbcastCommand.RETRIEVE_EVENT,
            FIELD_RETRIEVE_EVENT: retrieve_event,
        }
        return Message(self.my_node, self.service_id, data)


class Lpbcast(DisseminationService):
    _rpc: RPC
    _broadcast_handler: BroadcastHandler
    _protocol: Optional[LpbcastProtocol]

    _events: "OrderedDict[EventId, List[Union[EventData, EventOrigin]]]"
    _events_digest: "OrderedDict[EventId, EventOrigin]"

    def __init__(self, membership: MembershipService):
        self.my_node = membership.my_node
        self.membership = membership
        self._broadcast_handler = None
        self._rpc = RPCRegister.get_rpc(self.my_node)

        self._events = OrderedDict()
        self._events_digest = OrderedDict()

    async def join_broadcast(self, service_id: str, **configuration: Any) -> None:
        if self.running:
            raise RuntimeError("Already running Dissemination")
        self._broadcast_handler = configuration["broadcast_handler"]
        self.service_id = service_id
        self._protocol = LpbcastProtocol(self.my_node, self.service_id)
        await self._rpc.register_service(service_id, self._rpc_handler)
        self.running = True

    async def leave_broadcast(self) -> None:
        if not self.running:
            return
        await self._rpc.unregister_service(self.service_id)
        self._broadcast_handler = None
        self._protocol = None
        self.running = False

    async def broadcast(self, data: bytes) -> None:
        if not self.running:
            raise RuntimeError("Dissemination service not running")
        assert isinstance(data, bytes)
        event_id = get_random_id()
        await self._handle_new_event(
            event_id, data, self.my_node, broadcast_origin=True
        )

    async def _rpc_handler(self, message: Message) -> Any:
        command = message.data[FIELD_COMMAND]

        if command == LpbcastCommand.PUSH_EVENT:
            asyncio.create_task(
                self._handle_new_event(
                    message.data[FIELD_EVENT_ID],
                    message.data[FIELD_EVENT_DATA],
                    parse_node(message.data[FIELD_EVENT_ORIGIN]),
                )
            )
            for message_id, message_origin in message.data[FIELD_DIGEST]:
                if message_id not in self._events_digest:  # not the one received
                    asyncio.create_task(
                        self._retrieve_event(
                            message.node, message_id, parse_node(message_origin)
                        )
                    )

        elif command == LpbcastCommand.RETRIEVE_EVENT:
            retrieve_event_id = message.data[FIELD_RETRIEVE_EVENT]
            event = self._events.get(retrieve_event_id, None)
            return event

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
                message = self._protocol.make_push_event_message(
                    event_id,
                    event_data,
                    event_origin,
                    list(map(lambda e: [e[0], e[1]], self._events_digest.items())),
                )
                await self._rpc.call_send_message(neighbour, message)
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
        message = self._protocol.make_retrieve_event_message(event_id)
        try:
            event_data, _ = await self._rpc.call_send_message(event_source, message)
            assert isinstance(event_data, bytes)
            return await self._handle_new_event(event_id, event_data, event_origin)
        except Exception:
            pass
        try:
            candidate_neighbours = self.membership.get_neighbours()
            assert isinstance(candidate_neighbours, list)
            random_neighbour = random.choice(candidate_neighbours)
            event_data, _ = await self._rpc.call_send_message(random_neighbour, message)
            assert isinstance(event_data, bytes)
            return await self._handle_new_event(event_id, event_data, event_origin)
        except Exception:
            pass
        try:
            event_data, _ = await self._rpc.call_send_message(event_origin, message)
            assert isinstance(event_data, bytes)
            return await self._handle_new_event(event_id, event_data, event_origin)
        except Exception:
            pass

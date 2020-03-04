import asyncio
import random
from collections import OrderedDict
from enum import IntEnum, auto
from typing import Any, List, Optional, Union

from unsserv.common.data_structures import Message, Node
from unsserv.common.rpc.rpc import RPC, RpcBase
from unsserv.common.services_abc import DisseminationService, MembershipService
from unsserv.common.typing import BroadcastHandler
from unsserv.common.utils import get_random_id
from unsserv.extreme.dissemination.lpbcast.lpbcast_config import (
    DATA_FIELD_COMMAND,
    FANOUT,
    DATA_FIELD_EVENT_ID,
    DATA_FIELD_EVENT_DATA,
    DATA_FIELD_EVENT_ORIGIN,
    DATA_FIELD_DIGEST,
    DATA_FIELD_RETRIEVE_EVENT,
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
            DATA_FIELD_COMMAND: LpbcastCommand.PUSH_EVENT,
            DATA_FIELD_EVENT_ID: event_id,
            DATA_FIELD_EVENT_DATA: event_data,
            DATA_FIELD_EVENT_ORIGIN: event_origin,
            DATA_FIELD_DIGEST: digest,
        }
        return Message(self.my_node, self.service_id, data)

    def make_retrieve_event_message(self, retrieve_event: EventId):
        data = {
            DATA_FIELD_COMMAND: LpbcastCommand.RETRIEVE_EVENT,
            DATA_FIELD_RETRIEVE_EVENT: retrieve_event,
        }
        return Message(self.my_node, self.service_id, data)


class LpbcastRPC(RpcBase):
    async def call_push_event(self, destination: Node, message: Message) -> bool:
        rpc_result = await self.push_event(destination.address_info, message)
        return self._handle_call_response(rpc_result)

    async def call_retrieve_event(
        self, destination: Node, message: Message
    ) -> List[Union[EventData, EventOrigin]]:
        rpc_result = await self.retrieve_event(destination.address_info, message)
        return self._handle_call_response(rpc_result)

    async def rpc_push_event(self, node: Node, raw_message: List):
        message = self._decode_message(raw_message)
        await self.registered_services[message.service_id](message)

    async def rpc_retrieve_event(
        self, node: Node, raw_message: List
    ) -> List[Union[EventData, EventOrigin]]:
        message = self._decode_message(raw_message)
        event = await self.registered_services[message.service_id](message)
        return event


class Lpbcast(DisseminationService):
    _rpc: LpbcastRPC
    _broadcast_handler: BroadcastHandler
    _protocol: Optional[LpbcastProtocol]

    _events: "OrderedDict[EventId, List[Union[EventData, EventOrigin]]]"
    _events_digest: "OrderedDict[EventId, EventOrigin]"
    _retrieve_buffer: "OrderedDict[EventId, EventOrigin]"

    def __init__(self, membership: MembershipService, multiplex: bool = True):
        self.my_node = membership.my_node
        self.membership = membership
        self._broadcast_handler = None
        self._rpc = RPC.get_rpc(self.my_node, LpbcastRPC, multiplex=multiplex)

        self._events = OrderedDict()
        self._events_digest = OrderedDict()
        self._retrieve_buffer = OrderedDict()

    async def join_broadcast(
        self, service_id: str, broadcast_handler: BroadcastHandler
    ) -> None:
        if self.running:
            raise RuntimeError("Already running Dissemination")
        self.service_id = service_id
        self._protocol = LpbcastProtocol(self.my_node, self.service_id)
        self._broadcast_handler = broadcast_handler
        await self._rpc.register_service(service_id, self._rpc_handler)
        self.running = True

    async def leave_broadcast(self) -> None:
        await self._rpc.unregister_service(self.service_id)
        self._broadcast_handler = None
        self._protocol = None
        self.running = False

    async def broadcast(self, data: Any) -> None:
        if not self.running:
            raise RuntimeError("Dissemination service not running")
        event_id = get_random_id()
        await self._handle_new_event(
            event_id, data, self.my_node, broadcast_origin=True
        )

    async def _rpc_handler(self, message: Message) -> Any:
        command = message.data[DATA_FIELD_COMMAND]

        if command == LpbcastCommand.PUSH_EVENT:
            asyncio.create_task(
                self._handle_new_event(
                    message.data[DATA_FIELD_EVENT_ID],
                    message.data[DATA_FIELD_EVENT_DATA],
                    Node(message.data[DATA_FIELD_EVENT_ORIGIN]),
                )
            )
            for message_id, message_origin in message.data[DATA_FIELD_DIGEST]:
                asyncio.create_task(
                    self._retrieve_event(message.node, message_id, Node(message_origin))
                )
            print(self.my_node.address_info[1], "Handled PUSH")

        elif command == LpbcastCommand.RETRIEVE_EVENT:
            retrieve_event_id = message.data[DATA_FIELD_RETRIEVE_EVENT]
            return self._events.get(retrieve_event_id, default=None)

    async def _handle_new_event(
        self,
        event_id: EventId,
        event_data: EventData,
        event_origin: Node,
        broadcast_origin=False,
    ):
        if event_id in self._events_digest:
            return
        if event_id in self._retrieve_buffer:
            del self._retrieve_buffer[event_id]
        self._events[event_id] = [event_data, event_origin]
        self._events_digest[event_id] = event_origin
        self._purge_events_threshold()
        asyncio.create_task(self._disseminate(event_id, event_data, event_origin))
        if not broadcast_origin:
            asyncio.create_task(self._broadcast_handler(event_data))
        print(self.my_node.address_info[1], "Handled New Event")

    async def _disseminate(
        self, event_id: EventId, event_data: EventData, event_origin: Node
    ):
        print(self.my_node.address_info[1], "Disseminating...")
        candidate_neighbours = self.membership.get_neighbours()
        assert isinstance(candidate_neighbours, list)
        fanout = min(FANOUT, len(candidate_neighbours))
        for neighbour in random.choices(candidate_neighbours, k=fanout):
            try:
                message = self._protocol.make_push_event_message(
                    event_id,
                    event_data,
                    event_origin,
                    list(self._events_digest.items()),  # type: ignore
                )
                await self._rpc.call_push_event(neighbour, message)
            except Exception:
                pass  # todo: log the error?
        print(self.my_node.address_info[1], "Disseminate")

    def _purge_events_threshold(self):
        while LPBCAST_THRESHOLD < len(self._events):
            self._events.popitem(last=False)
        while LPBCAST_THRESHOLD < len(self._events_digest):
            self._events_digest.popitem(last=False)
        while LPBCAST_THRESHOLD < len(self._retrieve_buffer):
            self._retrieve_buffer.popitem(last=False)

    async def _retrieve_event(
        self, event_source: Node, event_id: EventId, event_origin: EventOrigin,
    ):
        message = self._protocol.make_retrieve_event_message(event_id)
        try:
            event_data, _ = await self._rpc.call_retrieve_event(event_source, message)
            assert isinstance(event_data, bytes)
            return await self._handle_new_event(event_id, event_data, event_origin)
        except Exception:
            pass
        try:
            candidate_neighbours = self.membership.get_neighbours()
            assert isinstance(candidate_neighbours, list)
            random_neighbour = random.choice(candidate_neighbours)
            event_data, _ = await self._rpc.call_retrieve_event(
                random_neighbour, message
            )
            assert isinstance(event_data, bytes)
            return await self._handle_new_event(event_id, event_data, event_origin)
        except Exception:
            pass
        try:
            event_data, _ = await self._rpc.call_retrieve_event(event_origin, message)
            assert isinstance(event_data, bytes)
            return await self._handle_new_event(event_id, event_data, event_origin)
        except Exception:
            pass
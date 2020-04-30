from enum import IntEnum, auto
from typing import Tuple, Sequence, Any, List

from unsserv.common.utils import parse_node
from unsserv.common.structs import Node
from unsserv.common.rpc.structs import Message
from unsserv.common.rpc.protocol import AProtocol, ITranscoder, Command, Data, Handler
from unsserv.extreme.dissemination.many_to_many.structs import Event
from unsserv.extreme.dissemination.many_to_many.typing import Digest

FIELD_COMMAND = "many_to_many-command"
FIELD_EVENT_DATA = "many_to_many-event-data"
FIELD_EVENT_ID = "many_to_many-event-id"
FIELD_EVENT_ORIGIN = "many_to_many-event-origin"
FIELD_DIGEST = "many_to_many-digest"
FIELD_RETRIEVE_EVENT = "many_to_many-retrieve-events"


class LpbcastCommand(IntEnum):
    PUSH_EVENT = auto()
    RETRIEVE_EVENT = auto()


class LpbcastTranscoder(ITranscoder):
    def encode(self, command: Command, *data: Data) -> Message:
        if command == LpbcastCommand.PUSH_EVENT:
            event: Event = data[0]
            message_data = {
                FIELD_COMMAND: LpbcastCommand.PUSH_EVENT,
                FIELD_EVENT_ID: event.id,
                FIELD_EVENT_DATA: event.data,
                FIELD_EVENT_ORIGIN: event.origin,
                FIELD_DIGEST: event.digest,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == LpbcastCommand.RETRIEVE_EVENT:
            event_id: str = data[0]
            message_data = {
                FIELD_COMMAND: LpbcastCommand.RETRIEVE_EVENT,
                FIELD_RETRIEVE_EVENT: event_id,
            }
            return Message(self.my_node, self.service_id, message_data)
        raise ValueError("Invalid Command")

    def decode(self, message: Message) -> Tuple[Command, Sequence[Data]]:
        command = message.data[FIELD_COMMAND]
        if command == LpbcastCommand.PUSH_EVENT:
            event = Event(
                id=message.data[FIELD_EVENT_ID],
                data=message.data[FIELD_EVENT_DATA],
                origin=parse_node(message.data[FIELD_EVENT_ORIGIN]),
                digest=parse_digest(message.data[FIELD_DIGEST]),
            )
            return LpbcastCommand.PUSH_EVENT, [event]
        elif command == LpbcastCommand.RETRIEVE_EVENT:
            event_id = message.data[FIELD_RETRIEVE_EVENT]
            return LpbcastCommand.RETRIEVE_EVENT, [event_id]
        raise ValueError("Invalid Command")


class LpbcastProtocol(AProtocol):
    def _get_new_transcoder(self):
        return LpbcastTranscoder(self.my_node, self.service_id)

    async def push_event(self, destination: Node, event: Event):
        message = self._transcoder.encode(LpbcastCommand.PUSH_EVENT, event)
        return await self._rpc.call_send_message(destination, message)

    async def retrieve_event(self, destination: Node, event_id: str):
        message = self._transcoder.encode(LpbcastCommand.RETRIEVE_EVENT, event_id)
        return await self._rpc.call_send_message(destination, message)

    def set_handler_push_event(self, handler: Handler):
        self._handlers[LpbcastCommand.PUSH_EVENT] = handler

    def set_handler_retrieve_event(self, handler: Handler):
        self._handlers[LpbcastCommand.RETRIEVE_EVENT] = handler


def parse_digest(raw_digest: List[Tuple[str, Any]]) -> Digest:
    digest = []
    for message_id, message_origin in raw_digest:
        digest.append((message_id, parse_node(message_origin)))
    return digest

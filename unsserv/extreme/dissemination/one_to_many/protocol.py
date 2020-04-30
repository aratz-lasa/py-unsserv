from enum import IntEnum, auto
from typing import Tuple, Sequence

from unsserv.common.structs import Node
from unsserv.common.rpc.structs import Message
from unsserv.common.rpc.protocol import AProtocol, ITranscoder, Command, Data, Handler
from unsserv.extreme.dissemination.one_to_many.structs import Session, Broadcast

FIELD_COMMAND = "one_to_many-command"
FIELD_BROADCAST_ID = "one_to_many-broadcast-id"
FIELD_LEVEL = "one_to_many-level"
FIELD_BROADCAST_DATA = "one_to_many-broadcast-data"


class MonCommand(IntEnum):
    SESSION = auto()
    PUSH = auto()


class MonTranscoder(ITranscoder):
    def encode(self, command: Command, *data: Data) -> Message:
        if command == MonCommand.SESSION:
            session: Session = data[0]
            message_data = {
                FIELD_COMMAND: MonCommand.SESSION,
                FIELD_BROADCAST_ID: session.broadcast_id,
                FIELD_LEVEL: session.level,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == MonCommand.PUSH:
            broadcast: Broadcast = data[0]
            message_data = {
                FIELD_COMMAND: MonCommand.PUSH,
                FIELD_BROADCAST_ID: broadcast.id,
                FIELD_BROADCAST_DATA: broadcast.data,
            }
            return Message(self.my_node, self.service_id, message_data)
        raise ValueError("Invalid Command")

    def decode(self, message: Message) -> Tuple[Command, Sequence[Data]]:
        command = message.data[FIELD_COMMAND]
        if command == MonCommand.SESSION:
            session = Session(
                broadcast_id=message.data[FIELD_BROADCAST_ID],
                level=message.data[FIELD_LEVEL],
            )
            return MonCommand.SESSION, [session]
        elif command == MonCommand.PUSH:
            broadcast = Broadcast(
                id=message.data[FIELD_BROADCAST_ID],
                data=message.data[FIELD_BROADCAST_DATA],
            )
            return MonCommand.PUSH, [broadcast]
        raise ValueError("Invalid Command")


class MonProtocol(AProtocol):
    def _get_new_transcoder(self):
        return MonTranscoder(self.my_node, self.service_id)

    async def session(self, destination: Node, session: Session) -> bool:
        message = self._transcoder.encode(MonCommand.SESSION, session)
        return await self._rpc.call_send_message(destination, message)

    async def push(self, destination: Node, broadcast: Broadcast):
        message = self._transcoder.encode(MonCommand.PUSH, broadcast)
        return await self._rpc.call_send_message(destination, message)

    def set_handler_session(self, handler: Handler):
        self._handlers[MonCommand.SESSION] = handler

    def set_handler_push(self, handler: Handler):
        self._handlers[MonCommand.PUSH] = handler

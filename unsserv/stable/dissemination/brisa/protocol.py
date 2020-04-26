from enum import IntEnum, auto
from typing import Tuple, Sequence

from unsserv.common.structs import Node
from unsserv.common.rpc.structs import Message
from unsserv.common.rpc.protocol import AProtocol, ITranscoder, Command, Data, Handler
from unsserv.stable.dissemination.brisa.typing import BroadcastLevel


FIELD_COMMAND = "brisa-command"
FIELD_LEVEL = "brisa-level"
FIELD_BROADCAST_DATA = "brisa-broadcast-data"


class BrisaCommand(IntEnum):
    SESSION = auto()
    PUSH = auto()
    IM_YOUR_CHILD = auto()
    BECOME_MY_PARENT = auto()


class BrisaTranscoder(ITranscoder):
    def encode(self, command: Command, *data: Data) -> Message:
        if command == BrisaCommand.SESSION:
            level: BroadcastLevel = data[0]
            message_data = {
                FIELD_COMMAND: BrisaCommand.SESSION,
                FIELD_LEVEL: level,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == BrisaCommand.PUSH:
            broadcast_data: bytes = data[0]
            message_data = {
                FIELD_COMMAND: BrisaCommand.PUSH,
                FIELD_BROADCAST_DATA: broadcast_data,  # type:ignore
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == BrisaCommand.IM_YOUR_CHILD:
            message_data = {
                FIELD_COMMAND: BrisaCommand.IM_YOUR_CHILD,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == BrisaCommand.BECOME_MY_PARENT:
            message_data = {
                FIELD_COMMAND: BrisaCommand.BECOME_MY_PARENT,
            }
            return Message(self.my_node, self.service_id, message_data)
        raise ValueError("Invalid Command")

    def decode(self, message: Message) -> Tuple[Command, Sequence[Data]]:
        command = message.data[FIELD_COMMAND]
        if command == BrisaCommand.SESSION:
            level: BroadcastLevel = message.data[FIELD_LEVEL]
            return BrisaCommand.SESSION, [level]
        elif command == BrisaCommand.PUSH:
            data: bytes = message.data[FIELD_BROADCAST_DATA]
            return BrisaCommand.PUSH, [data]
        elif command == BrisaCommand.IM_YOUR_CHILD:
            return BrisaCommand.IM_YOUR_CHILD, []
        elif command == BrisaCommand.BECOME_MY_PARENT:
            return BrisaCommand.BECOME_MY_PARENT, []
        raise ValueError("Invalid Command")


class BrisaProtocol(AProtocol):
    def _get_new_transcoder(self):
        return BrisaTranscoder(self.my_node, self.service_id)

    async def session(self, destination: Node, level: BroadcastLevel) -> bool:
        message = self._transcoder.encode(BrisaCommand.SESSION, level)
        return await self._rpc.call_send_message(destination, message)

    async def push(self, destination: Node, data: bytes):
        message = self._transcoder.encode(BrisaCommand.PUSH, data)
        return await self._rpc.call_send_message(destination, message)

    async def im_your_child(self, destination: Node) -> bool:
        message = self._transcoder.encode(BrisaCommand.IM_YOUR_CHILD)
        return await self._rpc.call_send_message(destination, message)

    async def become_my_parent(self, destination: Node) -> BroadcastLevel:
        message = self._transcoder.encode(BrisaCommand.BECOME_MY_PARENT)
        return await self._rpc.call_send_message(destination, message)

    def set_handler_session(self, handler: Handler):
        self._handlers[BrisaCommand.SESSION] = handler

    def set_handler_push(self, handler: Handler):
        self._handlers[BrisaCommand.PUSH] = handler

    def set_handler_im_your_child(self, handler: Handler):
        self._handlers[BrisaCommand.IM_YOUR_CHILD] = handler

    def set_handler_become_my_parent(self, handler: Handler):
        self._handlers[BrisaCommand.BECOME_MY_PARENT] = handler

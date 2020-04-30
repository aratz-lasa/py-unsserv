from enum import IntEnum, auto
from typing import Tuple, Sequence, Optional

from unsserv.common.rpc.protocol import AProtocol, ITranscoder, Command, Data, Handler
from unsserv.common.rpc.structs import Message
from unsserv.common.structs import Node
from unsserv.stable.dissemination.many_to_many.structs import Push
from unsserv.stable.dissemination.many_to_many.typing import (
    Digest,
    PlumData,
    PlumDataId,
)

FIELD_COMMAND = "plum-command"
FIELD_DATA_ID = "plum-data-id"
FIELD_DATA = "plum-data"
FIELD_DIGEST = "plum-digest"


class PlumtreeCommand(IntEnum):
    PUSH = auto()
    IHAVE = auto()
    GET_DATA = auto()
    PRUNE = auto()


class PlumtreeTranscoder(ITranscoder):
    def encode(self, command: Command, *data: Data) -> Message:
        if command == PlumtreeCommand.PUSH:
            push: Push = data[0]
            message_data = {
                FIELD_COMMAND: PlumtreeCommand.PUSH,
                FIELD_DATA: push.data,
                FIELD_DATA_ID: push.data_id,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == PlumtreeCommand.IHAVE:
            digest: Digest = data[0]
            message_data = {
                FIELD_COMMAND: PlumtreeCommand.IHAVE,
                FIELD_DIGEST: digest,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == PlumtreeCommand.GET_DATA:
            data_id: PlumDataId = data[0]
            message_data = {
                FIELD_COMMAND: PlumtreeCommand.GET_DATA,
                FIELD_DATA_ID: data_id,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == PlumtreeCommand.PRUNE:
            message_data = {
                FIELD_COMMAND: PlumtreeCommand.PRUNE,
            }
            return Message(self.my_node, self.service_id, message_data)
        raise ValueError("Invalid Command")

    def decode(self, message: Message) -> Tuple[Command, Sequence[Data]]:
        command = message.data[FIELD_COMMAND]
        if command == PlumtreeCommand.PUSH:
            push = Push(
                data=message.data[FIELD_DATA], data_id=message.data[FIELD_DATA_ID],
            )
            return PlumtreeCommand.PUSH, [push]
        elif command == PlumtreeCommand.IHAVE:
            digest: Digest = message.data[FIELD_DIGEST]
            return PlumtreeCommand.IHAVE, [digest]
        elif command == PlumtreeCommand.GET_DATA:
            digest: PlumDataId = message.data[FIELD_DATA_ID]
            return PlumtreeCommand.GET_DATA, [digest]
        elif command == PlumtreeCommand.PRUNE:
            return PlumtreeCommand.PRUNE, []
        raise ValueError("Invalid Command")


class PlumtreeProtocol(AProtocol):
    def _get_new_transcoder(self):
        return PlumtreeTranscoder(self.my_node, self.service_id)

    async def push(self, destination: Node, push: Push):
        message = self._transcoder.encode(PlumtreeCommand.PUSH, push)
        return await self._rpc.call_send_message(destination, message)

    async def ihave(self, destination: Node, digest: Digest):
        message = self._transcoder.encode(PlumtreeCommand.IHAVE, digest)
        return await self._rpc.call_send_message(destination, message)

    async def get_data(
        self, destination: Node, data_id: PlumDataId
    ) -> Optional[PlumData]:
        message = self._transcoder.encode(PlumtreeCommand.GET_DATA, data_id)
        return await self._rpc.call_send_message(destination, message)

    async def prune(self, destination: Node):
        message = self._transcoder.encode(PlumtreeCommand.PRUNE)
        return await self._rpc.call_send_message(destination, message)

    def set_handler_push(self, handler: Handler):
        self._handlers[PlumtreeCommand.PUSH] = handler

    def set_handler_ihave(self, handler: Handler):
        self._handlers[PlumtreeCommand.IHAVE] = handler

    def set_handler_get_data(self, handler: Handler):
        self._handlers[PlumtreeCommand.GET_DATA] = handler

    def set_handler_prune(self, handler: Handler):
        self._handlers[PlumtreeCommand.PRUNE] = handler

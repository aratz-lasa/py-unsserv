from enum import IntEnum, auto
from typing import Tuple, Sequence, Optional

from unsserv.common.rpc.protocol import AProtocol, ITranscoder, Command, Data, Handler
from unsserv.common.rpc.structs import Message
from unsserv.common.structs import Node
from unsserv.stable.dissemination.brisa.structs import Push
from unsserv.stable.dissemination.brisa.typing import Digest, BrisaData, BrisaDataId

FIELD_COMMAND = "brisa-command"
FIELD_DATA_ID = "brisa-data-id"
FIELD_DATA = "brisa-data"
FIELD_DIGEST = "brisa-digest"


class BrisaCommand(IntEnum):
    PUSH = auto()
    IHAVE = auto()
    GET_DATA = auto()
    PRUNE = auto()


class BrisaTranscoder(ITranscoder):
    def encode(self, command: Command, *data: Data) -> Message:
        if command == BrisaCommand.PUSH:
            push: Push = data[0]
            message_data = {
                FIELD_COMMAND: BrisaCommand.PUSH,
                FIELD_DATA: push.data,
                FIELD_DATA_ID: push.data_id,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == BrisaCommand.IHAVE:
            digest: Digest = data[0]
            message_data = {
                FIELD_COMMAND: BrisaCommand.IHAVE,
                FIELD_DIGEST: digest,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == BrisaCommand.GET_DATA:
            data_id: BrisaDataId = data[0]
            message_data = {
                FIELD_COMMAND: BrisaCommand.GET_DATA,
                FIELD_DATA_ID: data_id,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == BrisaCommand.PRUNE:
            message_data = {
                FIELD_COMMAND: BrisaCommand.PRUNE,
            }
            return Message(self.my_node, self.service_id, message_data)
        raise ValueError("Invalid Command")

    def decode(self, message: Message) -> Tuple[Command, Sequence[Data]]:
        command = message.data[FIELD_COMMAND]
        if command == BrisaCommand.PUSH:
            push = Push(
                data=message.data[FIELD_DATA], data_id=message.data[FIELD_DATA_ID],
            )
            return BrisaCommand.PUSH, [push]
        elif command == BrisaCommand.IHAVE:
            digest: Digest = message.data[FIELD_DIGEST]
            return BrisaCommand.IHAVE, [digest]
        elif command == BrisaCommand.GET_DATA:
            digest: BrisaDataId = message.data[FIELD_DATA_ID]  # type: ignore
            return BrisaCommand.GET_DATA, [digest]
        elif command == BrisaCommand.PRUNE:
            return BrisaCommand.PRUNE, []
        raise ValueError("Invalid Command")


class BrisaProtocol(AProtocol):
    def _get_new_transcoder(self):
        return BrisaTranscoder(self.my_node, self.service_id)

    async def push(self, destination: Node, push: Push):
        message = self._transcoder.encode(BrisaCommand.PUSH, push)
        return await self._rpc.call_send_message(destination, message)

    async def ihave(self, destination: Node, digest: Digest):
        message = self._transcoder.encode(BrisaCommand.IHAVE, digest)
        return await self._rpc.call_send_message(destination, message)

    async def get_data(
        self, destination: Node, data_id: BrisaDataId
    ) -> Optional[BrisaData]:
        message = self._transcoder.encode(BrisaCommand.GET_DATA, data_id)
        return await self._rpc.call_send_message(destination, message)

    async def prune(self, destination: Node):
        message = self._transcoder.encode(BrisaCommand.PRUNE)
        return await self._rpc.call_send_message(destination, message)

    def set_handler_push(self, handler: Handler):
        self._handlers[BrisaCommand.PUSH] = handler

    def set_handler_ihave(self, handler: Handler):
        self._handlers[BrisaCommand.IHAVE] = handler

    def set_handler_get_data(self, handler: Handler):
        self._handlers[BrisaCommand.GET_DATA] = handler

    def set_handler_prune(self, handler: Handler):
        self._handlers[BrisaCommand.PRUNE] = handler

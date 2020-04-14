from enum import IntEnum, auto
from typing import Tuple, Sequence

from unsserv.common.utils import parse_node
from unsserv.common.structs import Node
from unsserv.common.rpc.structs import Message
from unsserv.common.rpc.protocol import AProtocol, ITranscoder, Command, Data, Handler
from unsserv.extreme.searching.structs import WalkResult, Walk

FIELD_COMMAND = "kwalker-command"
FIELD_WALK_ID = "kwalker-walk-id"
FIELD_ORIGIN_NODE = "kwalker-origin-node"
FIELD_TTL = "kwalker-ttl"
FIELD_DATA_ID = "kwalker-data-id"
FIELD_WALK_RESULT = "kwalker-walk-result"


class KWalkerCommand(IntEnum):
    WALK = auto()
    WALK_RESULT = auto()


class KWalkerTranscoder(ITranscoder):
    def encode(self, command: Command, *data: Data) -> Message:
        if command == KWalkerCommand.WALK:
            walk: Walk = data[0]
            message_data = {
                FIELD_COMMAND: KWalkerCommand.WALK,
                FIELD_WALK_ID: walk.id,
                FIELD_DATA_ID: walk.data_id,
                FIELD_ORIGIN_NODE: walk.origin_node,
                FIELD_TTL: walk.ttl,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == KWalkerCommand.WALK_RESULT:
            walk_result: WalkResult = data[0]
            message_data = {
                FIELD_COMMAND: KWalkerCommand.WALK_RESULT,
                FIELD_WALK_ID: walk_result.walk_id,
                FIELD_WALK_RESULT: walk_result.result,
            }
            return Message(self.my_node, self.service_id, message_data)
        raise ValueError("Invalid Command")

    def decode(self, message: Message) -> Tuple[Command, Sequence[Data]]:
        command = message.data[FIELD_COMMAND]
        if command == KWalkerCommand.WALK:
            walk = Walk(
                id=message.data[FIELD_WALK_ID],
                data_id=message.data[FIELD_DATA_ID],
                origin_node=parse_node(message.data[FIELD_ORIGIN_NODE]),
                ttl=message.data[FIELD_TTL],
            )
            return KWalkerCommand.WALK, [walk]
        elif command == KWalkerCommand.WALK_RESULT:
            walk_result = WalkResult(
                walk_id=message.data[FIELD_WALK_ID],
                result=message.data[FIELD_WALK_RESULT],
            )
            return KWalkerCommand.WALK_RESULT, [walk_result]
        raise ValueError("Invalid Command")


class KWalkerProtocol(AProtocol):
    def _get_new_transcoder(self):
        return KWalkerTranscoder(self.my_node, self.service_id)

    async def walk(self, destination: Node, walk: Walk):
        message = self._transcoder.encode(KWalkerCommand.WALK, walk)
        return await self._rpc.call_send_message(destination, message)

    async def walk_result(self, destination: Node, walk_result: WalkResult):
        message = self._transcoder.encode(KWalkerCommand.WALK_RESULT, walk_result)
        return await self._rpc.call_send_message(destination, message)

    def set_handler_walk(self, handler: Handler):
        self._handlers[KWalkerCommand.WALK] = handler

    def set_handler_walk_result(self, handler: Handler):
        self._handlers[KWalkerCommand.WALK_RESULT] = handler

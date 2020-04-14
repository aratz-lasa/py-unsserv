from collections import Counter
from enum import IntEnum, auto
from typing import Tuple, Sequence, Dict, Any

from unsserv.common.gossip.structs import PushData
from unsserv.common.gossip.typing import Payload
from unsserv.common.rpc.protocol import AProtocol, ITranscoder, Command, Data, Handler
from unsserv.common.structs import Node
from unsserv.common.rpc.structs import Message
from unsserv.common.typing import View

FIELD_COMMAND = "gossip-command"
FIELD_VIEW = "gossip-view"
FIELD_PAYLOAD = "gossip-payload"


class GossipCommand(IntEnum):
    PUSH = auto()
    PULL = auto()
    PUSHPULL = auto()


class GossipTranscoder(ITranscoder):
    def encode(self, command: Command, *data: Data) -> Message:
        if command == GossipCommand.PUSH:
            push_data: PushData = data[0]
            message_data = {
                FIELD_COMMAND: GossipCommand.PUSH,
                FIELD_VIEW: push_data.view,
                FIELD_PAYLOAD: push_data.payload,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == GossipCommand.PULL:
            payload: Payload = data[0]
            message_data = {FIELD_COMMAND: GossipCommand.PULL, FIELD_PAYLOAD: payload}
            return Message(self.my_node, self.service_id, message_data)
        elif command == GossipCommand.PUSHPULL:
            push_data: PushData = data[0]  # type: ignore
            message_data = {
                FIELD_COMMAND: GossipCommand.PUSHPULL,
                FIELD_VIEW: push_data.view,
                FIELD_PAYLOAD: push_data.payload,
            }
            return Message(self.my_node, self.service_id, message_data)
        raise ValueError("Invalid Command")

    def decode(self, message: Message) -> Tuple[Command, Sequence[Data]]:
        command = message.data[FIELD_COMMAND]
        if command == GossipCommand.PUSH:
            push_data = PushData(
                view=_parse_view(message.data[FIELD_VIEW]),
                payload=message.data[FIELD_PAYLOAD],
            )
            return GossipCommand.PUSH, [push_data]
        elif command == GossipCommand.PULL:
            payload = message.data[FIELD_PAYLOAD]
            return GossipCommand.PULL, [payload]
        elif command == GossipCommand.PUSHPULL:
            push_data = PushData(
                view=_parse_view(message.data[FIELD_VIEW]),
                payload=message.data[FIELD_PAYLOAD],
            )
            return GossipCommand.PUSHPULL, [push_data]
        raise ValueError("Invalid Command")


class GossipProtocol(AProtocol):
    def _get_new_transcoder(self):
        return GossipTranscoder(self.my_node, self.service_id)

    async def push(self, destination: Node, push_data: PushData):
        message = self._transcoder.encode(GossipCommand.PUSH, push_data)
        return await self._rpc.call_send_message(destination, message)

    async def pull(self, destination: Node, payload: Payload) -> PushData:
        message = self._transcoder.encode(GossipCommand.PULL, payload)
        encoded_push_data = await self._rpc.call_send_message(destination, message)
        return _parse_push_data(encoded_push_data)

    async def pushpull(self, destination: Node, push_data: PushData) -> PushData:
        message = self._transcoder.encode(GossipCommand.PUSHPULL, push_data)
        encoded_push_data = await self._rpc.call_send_message(destination, message)
        return _parse_push_data(encoded_push_data)

    def set_handler_push(self, handler: Handler):
        self._handlers[GossipCommand.PUSH] = handler

    def set_handler_pull(self, handler: Handler):
        self._handlers[GossipCommand.PULL] = handler

    def set_handler_pushpull(self, handler: Handler):
        self._handlers[GossipCommand.PUSHPULL] = handler


def _parse_view(raw_view: Dict[Any, Any]) -> View:
    return Counter(dict(map(lambda n: (Node(*n[0]), n[1]), raw_view.items())))


def _parse_push_data(raw_push_data: Dict[str, Any]) -> PushData:
    return PushData(
        view=_parse_view(raw_push_data["view"]), payload=raw_push_data["payload"]
    )

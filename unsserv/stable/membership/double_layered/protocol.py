from enum import IntEnum, auto
from typing import Tuple, Sequence, Dict, Any

from unsserv.common.rpc.protocol import AProtocol, ITranscoder, Command, Data, Handler
from unsserv.common.structs import Node
from unsserv.common.rpc.structs import Message
from unsserv.common.utils import parse_node
from unsserv.stable.membership.double_layered.structs import ForwardJoin

MessageData = Dict[str, Any]

FIELD_COMMAND = "double_layered-command"
FIELD_TTL = "double_layered-ttl"
FIELD_PRIORITY = "double_layered-priority"
FIELD_ORIGIN_NODE = "double_layered-origin-node"


class DoubleLayeredCommand(IntEnum):
    JOIN = auto()
    FORWARD_JOIN = auto()
    CONNECT = auto()  # it is equivalent to NEIGHBOR in DoubleLayered specification
    DISCONNECT = auto()
    STAY_CONNECTED = auto()


class DoubleLayeredTranscoder(ITranscoder):
    def encode(self, command: Command, *data: Data) -> Message:
        message_data: MessageData  # needed for mypy compliance
        if command == DoubleLayeredCommand.JOIN:
            message_data = {FIELD_COMMAND: DoubleLayeredCommand.JOIN}
            return Message(self.my_node, self.service_id, message_data)
        elif command == DoubleLayeredCommand.FORWARD_JOIN:
            forward_join: ForwardJoin = data[0]
            message_data = {
                FIELD_COMMAND: DoubleLayeredCommand.FORWARD_JOIN,
                FIELD_ORIGIN_NODE: forward_join.origin_node,
                FIELD_TTL: forward_join.ttl,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == DoubleLayeredCommand.CONNECT:
            is_a_priority: bool = data[0]
            message_data = {
                FIELD_COMMAND: DoubleLayeredCommand.CONNECT,
                FIELD_PRIORITY: is_a_priority,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == DoubleLayeredCommand.DISCONNECT:
            message_data = {FIELD_COMMAND: DoubleLayeredCommand.DISCONNECT}
            return Message(self.my_node, self.service_id, message_data)
        elif command == DoubleLayeredCommand.STAY_CONNECTED:
            message_data = {FIELD_COMMAND: DoubleLayeredCommand.STAY_CONNECTED}
            return Message(self.my_node, self.service_id, message_data)
        raise ValueError("Invalid Command")

    def decode(self, message: Message) -> Tuple[Command, Sequence[Data]]:
        command = message.data[FIELD_COMMAND]
        if command == DoubleLayeredCommand.JOIN:
            return DoubleLayeredCommand.JOIN, []
        elif command == DoubleLayeredCommand.FORWARD_JOIN:
            forward_join = ForwardJoin(
                origin_node=parse_node(message.data[FIELD_ORIGIN_NODE]),
                ttl=message.data[FIELD_TTL],
            )
            return DoubleLayeredCommand.FORWARD_JOIN, [forward_join]
        elif command == DoubleLayeredCommand.CONNECT:
            is_a_priority: bool = message.data[FIELD_PRIORITY]
            return DoubleLayeredCommand.CONNECT, [is_a_priority]
        elif command == DoubleLayeredCommand.DISCONNECT:
            return DoubleLayeredCommand.DISCONNECT, []
        elif command == DoubleLayeredCommand.STAY_CONNECTED:
            return DoubleLayeredCommand.STAY_CONNECTED, []
        raise ValueError("Invalid Command")


class DoubleLayeredProtocol(AProtocol):
    def _get_new_transcoder(self):
        return DoubleLayeredTranscoder(self.my_node, self.service_id)

    async def join(self, destination: Node):
        message = self._transcoder.encode(DoubleLayeredCommand.JOIN)
        return await self._rpc.call_send_message(destination, message)

    async def forward_join(self, destination: Node, forward_join: ForwardJoin):
        message = self._transcoder.encode(
            DoubleLayeredCommand.FORWARD_JOIN, forward_join
        )
        return await self._rpc.call_send_message(destination, message)

    async def connect(self, destination: Node, is_a_priority: bool) -> int:
        message = self._transcoder.encode(DoubleLayeredCommand.CONNECT, is_a_priority)
        return await self._rpc.call_send_message(destination, message)

    async def disconnect(self, destination: Node) -> int:
        message = self._transcoder.encode(DoubleLayeredCommand.DISCONNECT)
        return await self._rpc.call_send_message(destination, message)

    async def stay_connected(self, destination: Node) -> int:
        message = self._transcoder.encode(DoubleLayeredCommand.STAY_CONNECTED)
        return await self._rpc.call_send_message(destination, message)

    def set_handler_join(self, handler: Handler):
        self._handlers[DoubleLayeredCommand.JOIN] = handler

    def set_handler_forward_join(self, handler: Handler):
        self._handlers[DoubleLayeredCommand.FORWARD_JOIN] = handler

    def set_handler_connect(self, handler: Handler):
        self._handlers[DoubleLayeredCommand.CONNECT] = handler

    def set_handler_disconnect(self, handler: Handler):
        self._handlers[DoubleLayeredCommand.DISCONNECT] = handler

    def set_handler_stay_connected(self, handler: Handler):
        self._handlers[DoubleLayeredCommand.STAY_CONNECTED] = handler

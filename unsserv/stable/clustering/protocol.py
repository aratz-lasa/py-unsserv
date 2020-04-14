from enum import IntEnum, auto
from typing import Tuple, Sequence

from unsserv.common.rpc.protocol import AProtocol, ITranscoder, Command, Data, Handler
from unsserv.common.structs import Node
from unsserv.common.rpc.structs import Message
from unsserv.common.utils import parse_node
from unsserv.stable.clustering.structs import Replace

FIELD_COMMAND = "xbot-command"
FIELD_OLD_NODE = "xbot-old-node"
FIELD_ORIGIN_NODE = "xbot-origin-node"


class XBotCommand(IntEnum):
    OPTIMIZATION = auto()
    REPLACE = auto()
    SWITCH = auto()


class XBotTranscoder(ITranscoder):
    def encode(self, command: Command, *data: Data) -> Message:
        if command == XBotCommand.OPTIMIZATION:
            old_node: Node = data[0]
            message_data = {
                FIELD_COMMAND: XBotCommand.OPTIMIZATION,
                FIELD_OLD_NODE: old_node,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == XBotCommand.REPLACE:
            replace: Replace = data[0]
            message_data = {
                FIELD_ORIGIN_NODE: replace.origin_node,
                FIELD_OLD_NODE: replace.old_node,
                FIELD_COMMAND: XBotCommand.REPLACE,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == XBotCommand.SWITCH:
            origin_node: Node = data[0]
            message_data = {
                FIELD_COMMAND: XBotCommand.SWITCH,
                FIELD_ORIGIN_NODE: origin_node,
            }
            return Message(self.my_node, self.service_id, message_data)
        raise ValueError("Invalid Command")

    def decode(self, message: Message) -> Tuple[Command, Sequence[Data]]:
        command = message.data[FIELD_COMMAND]
        if command == XBotCommand.OPTIMIZATION:
            old_node: Node = parse_node(message.data[FIELD_OLD_NODE])
            return XBotCommand.OPTIMIZATION, [old_node]
        elif command == XBotCommand.REPLACE:
            replace = Replace(
                old_node=parse_node(message.data[FIELD_OLD_NODE]),
                origin_node=parse_node(message.data[FIELD_ORIGIN_NODE]),
            )
            return XBotCommand.REPLACE, [replace]
        elif command == XBotCommand.SWITCH:
            origin_node: Node = parse_node(message.data[FIELD_ORIGIN_NODE])
            return XBotCommand.SWITCH, [origin_node]
        raise ValueError("Invalid Command")


class XBotProtocol(AProtocol):
    def _get_new_transcoder(self):
        return XBotTranscoder(self.my_node, self.service_id)

    async def optimization(self, destination: Node, old_node: Node):
        message = self._transcoder.encode(XBotCommand.OPTIMIZATION, old_node)
        return await self._rpc.call_send_message(destination, message)

    async def replace(self, destination: Node, replace: Replace):
        message = self._transcoder.encode(XBotCommand.REPLACE, replace)
        return await self._rpc.call_send_message(destination, message)

    async def switch(self, destination: Node, origin_node: Node) -> int:
        message = self._transcoder.encode(XBotCommand.SWITCH, origin_node)
        return await self._rpc.call_send_message(destination, message)

    def set_handler_optimization(self, handler: Handler):
        self._handlers[XBotCommand.OPTIMIZATION] = handler

    def set_handler_replace(self, handler: Handler):
        self._handlers[XBotCommand.REPLACE] = handler

    def set_handler_switch(self, handler: Handler):
        self._handlers[XBotCommand.SWITCH] = handler

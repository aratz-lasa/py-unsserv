import asyncio
from typing import Dict, Set, Tuple, Any, List

from aiorpc import register, serve, RPCClient
from rpcudp.protocol import RPCProtocol

from unsserv.common.gossip.config import RPC_TIMEOUT
from unsserv.common.gossip.rpc.abc import IRPC
from unsserv.data_structures import Message, Node


class RPC:
    rpc_register: Dict = {}
    rpc_types: Set = {"udp", "tcp"}

    @staticmethod
    def get_rpc(node, type: str = "udp", multiplex: bool = False):
        if not multiplex and node in RPC.rpc_register:
            raise ConnectionError("RPC address already in use")
        type = type.lower()
        if type not in RPC.rpc_types:
            raise ValueError(f"Not a valid RPC type: {RPC.rpc_types}")
        return RPC.rpc_register.get(node, RPC._new_rpc(node, type))

    @staticmethod
    def _new_rpc(node: Node, type: str) -> IRPC:
        if type == "udp":
            return RpcUdp(node)
        elif type == "tcp":
            return RpcTcp(node)
        raise ValueError(f"Not a valid RPC type: {RPC.rpc_types}")


class RpcUdp(RPCProtocol, IRPC):
    def __init__(self, node: Node):
        RPCProtocol.__init__(self, RPC_TIMEOUT)
        IRPC.__init__(self, node)

    async def _start(self):
        (
            self._transport,
            protocol,
        ) = await asyncio.get_event_loop().create_datagram_endpoint(
            lambda: self, self.my_node.address_info
        )

    async def _stop(self):
        self._transport.close()
        self._transport = None

    async def call_push(self, destination: Node, message: Message) -> None:
        rpc_result = await self.push(destination.address_info, message)
        self._handle_call_response(rpc_result)

    async def call_pushpull(self, destination: Node, message: Message) -> Message:
        rpc_result = await self.pushpull(destination.address_info, message)
        return decode_message(self._handle_call_response(rpc_result))

    async def rpc_push(self, node: Node, raw_message: List) -> None:
        message = decode_message(raw_message)
        await self.registered_services[message.service_id](message)

    async def rpc_pushpull(self, node: Node, raw_message: List) -> Message:
        message = decode_message(raw_message)
        pull_return_message = await self.registered_services[message.service_id](
            message
        )
        assert pull_return_message
        return pull_return_message

    def _handle_call_response(self, result: Tuple[int, Any]) -> Any:
        """
        If we get a response, returns it.
         Otherwise raise error and remove the node from ILinkStore.
        """
        if not result[0]:
            raise ConnectionError(
                "RPC protocol error. Connection failed or invalid value received"
            )
        return result[1]


class RpcTcp(IRPC):
    _server: asyncio.AbstractServer

    def __init__(self, node: Node):
        IRPC.__init__(self, node)

    async def _start(self):
        register("push", self.rpc_push)
        register("pushpull", self.rpc_pushpull)
        self._server = await asyncio.start_server(serve, *self.my_node.address_info)

    async def _stop(self):
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def call_push(self, destination: Node, message: Message):
        async with RPCClient(*destination.address_info) as client:
            await client.call("push", message)

    async def call_pushpull(self, destination: Node, message: Message):
        async with RPCClient(*destination.address_info) as client:
            pull_message = await client.call("pushpull", message)
        return pull_message

    async def rpc_push(self, raw_message: List):
        message = decode_message(raw_message)
        await self.registered_services[message.service_id](message)

    async def rpc_pushpull(self, raw_message: List) -> Message:
        message = decode_message(raw_message)
        pull_return_message = await self.registered_services[message.service_id](
            message
        )
        assert pull_return_message
        return pull_return_message


def decode_message(raw_message: List) -> Message:
    node = tuple(raw_message[0])
    node = Node(tuple(node[0]), tuple(node[1]))
    return Message(node, raw_message[1], raw_message[2])


# todo: improve encoding decoding
# todo: search/implement better rpcs

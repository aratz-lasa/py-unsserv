import asyncio
from typing import Any, Dict, List, Tuple

from rpcudp.protocol import RPCProtocol

from unsserv.common.rpc.structs import Message
from unsserv.common.structs import Node
from unsserv.common.gossip.config import GossipConfig
from unsserv.common.typing import Handler
from unsserv.common.utils import parse_message


class RPCRegister:
    rpc_register: Dict = {}

    @staticmethod
    def get_rpc(node):
        rpc = RPCRegister.rpc_register.get(node, RPC(node))
        RPCRegister.rpc_register[node] = rpc
        return rpc


class RPC(RPCProtocol):
    my_node: Node
    registered_services: Dict[Node, Handler]

    def __init__(self, node: Node):
        RPCProtocol.__init__(self, GossipConfig.RPC_TIMEOUT)
        self.my_node = node
        self.registered_services = {}

    async def call_send_message(self, destination: Node, message: Message) -> Any:
        rpc_result = await self.send_message(destination.address_info, message)
        return self._handle_call_response(rpc_result)

    async def rpc_send_message(self, node: Node, raw_message: List) -> Any:
        message = parse_message(raw_message)
        return await self.registered_services[message.service_id](message)

    async def register_service(self, service_id: Any, handler: Handler):
        if service_id in self.registered_services:
            raise ValueError("Service ID already registered")
        self.registered_services[service_id] = handler

        if (
            len(self.registered_services) == 1
        ):  # activate when first service is registered
            await self._start()

    async def unregister_service(self, service_id: Any):
        if service_id in self.registered_services:
            del self.registered_services[service_id]
        if (
            len(self.registered_services) == 0
        ):  # deactivate when last service is unregistered
            await self._stop()

    async def _start(self):
        (
            self._transport,
            protocol,
        ) = await asyncio.get_event_loop().create_datagram_endpoint(
            lambda: self, self.my_node.address_info
        )

    async def _stop(self):
        if self._transport:
            self._transport.close()
            self._transport = None

    def _handle_call_response(self, result: Tuple[int, Any]) -> Any:
        """
        If we get a response, returns it.

        Otherwise raise error.
        """
        if not result[0]:
            raise ConnectionError(
                "RPC protocol error. Connection failed or invalid value received"
            )
        return result[1]

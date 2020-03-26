import asyncio
from typing import Any, Dict, List, Tuple

from rpcudp.protocol import RPCProtocol

from unsserv.common.data_structures import Message, Node
from unsserv.common.gossip import gossip_config as config
from unsserv.common.rpc.rpc_typing import RpcCallback
from unsserv.common.utils import parse_message


class RPCRegister:
    rpc_register: Dict = {}

    @staticmethod
    def get_rpc(node, multiplex: bool = False):
        if not multiplex and node in RPCRegister.rpc_register:
            raise ConnectionError("RPC address already in use")
        rpc = RPCRegister.rpc_register.get(node, RPC(node))
        RPCRegister.rpc_register[node] = rpc
        return rpc


class RPC(RPCProtocol):
    my_node: Node
    registered_services: Dict[Node, RpcCallback]

    def __init__(self, node: Node):
        RPCProtocol.__init__(self, config.RPC_TIMEOUT)
        self.my_node = node
        self.registered_services = {}

    async def call_with_response(self, destination: Node, message: Message) -> Any:
        rpc_result = await self.with_response(destination.address_info, message)
        return self._handle_call_response(rpc_result)

    async def rpc_with_response(self, node: Node, raw_message: List) -> Any:
        message = parse_message(raw_message)
        return await self.registered_services[message.service_id](message)

    async def call_without_response(self, destination: Node, message: Message):
        rpc_result = await self.without_response(destination.address_info, message)
        self._handle_call_response(rpc_result)

    async def rpc_without_response(self, node: Node, raw_message: List):
        message = parse_message(raw_message)
        await self.registered_services[message.service_id](message)

    async def register_service(self, service_id: Any, callback: RpcCallback):
        if service_id in self.registered_services:
            raise ValueError("Service ID already registered")
        self.registered_services[service_id] = callback

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

        Otherwise raise error and remove the node from ILinkStore.
        """
        if not result[0]:
            raise ConnectionError(
                "RPC protocol error. Connection failed or invalid value received"
            )
        return result[1]


# todo: improve encoding decoding
# todo: search/implement better rpcs

from abc import ABC
import asyncio
from typing import Any, Dict, List, Tuple

from rpcudp.protocol import RPCProtocol

from unsserv.common.data_structures import Message, Node
from unsserv.common.gossip.gossip_config import RPC_TIMEOUT
from unsserv.common.rpc.rpc_typing import RpcCallback
from unsserv.common.utils import decode_node


class RPC:
    rpc_register: Dict = {}

    @staticmethod
    def get_rpc(node, ProtocolClass: type, multiplex: bool = False):
        if not multiplex and node in RPC.rpc_register:
            raise ConnectionError("RPC address already in use")
        rpc = RPC.rpc_register.get(node, RpcBase(node))

        if not isinstance(rpc, ProtocolClass):

            class NewRPC(ProtocolClass, rpc.__class__):  # type: ignore
                pass

            rpc.__class__ = NewRPC

        RPC.rpc_register[node] = rpc
        return rpc


class RpcBase(RPCProtocol, ABC):
    my_node: Node
    registered_services: Dict[Node, RpcCallback]

    def __init__(self, node: Node):
        RPCProtocol.__init__(self, RPC_TIMEOUT)
        self.my_node = node
        self.registered_services = {}

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

    def _decode_message(self, raw_message: List) -> Message:
        node = decode_node(raw_message[0])
        return Message(node, raw_message[1], raw_message[2])


# todo: improve encoding decoding
# todo: search/implement better rpcs

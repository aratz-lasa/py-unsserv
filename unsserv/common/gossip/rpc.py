import typing

from rpcudp.protocol import RPCProtocol

from unsserv.common.gossip.config import RPC_TIMEOUT
from unsserv.data_structures import Message, Node

PullCallback = typing.Callable[
    [Message], typing.Coroutine[typing.Any, typing.Any, typing.Union[None, Message]]
]


class GossipRPC(RPCProtocol):
    node: Node

    def __init__(self, node: Node, pull_callback: PullCallback):
        RPCProtocol.__init__(self, RPC_TIMEOUT)
        self.node = node
        self.pull_callback: PullCallback
        self.pull_callback = pull_callback

    async def call_push(self, destination: Node, message: Message) -> None:
        rpc_result = await self.push(destination, message)
        self._handle_call_response(rpc_result)

    async def call_pushpull(self, destination: Node, message: Message) -> Message:
        rpc_result = await self.pushpull(destination, message)
        return Message(*self._handle_call_response(rpc_result))

    async def rpc_push(self, node: Node, message: typing.Any) -> None:
        message = Message(*message)
        await self.pull_callback(message)

    async def rpc_pushpull(self, node: Node, message: typing.Any) -> Message:
        message = Message(*message)
        pull_return_message = await self.pull_callback(message)
        assert pull_return_message
        return pull_return_message

    def _handle_call_response(
        self, result: typing.Tuple[int, typing.Any]
    ) -> typing.Any:
        """
        If we get a response, returns it.
         Otherwise raise error and remove the node from ILinkStore.
        """
        if not result[0]:
            raise ConnectionError(
                "RPC protocol error. Connection failed or invalid value received"
            )
        return result[1]

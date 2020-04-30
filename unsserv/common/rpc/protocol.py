import asyncio
from abc import ABC, abstractmethod
from dataclasses import is_dataclass, asdict
from enum import IntEnum
from typing import Any, Tuple, Sequence, Dict, Callable

from unsserv.common.rpc.rpc import RPCRegister, RPC
from unsserv.common.rpc.structs import Message
from unsserv.common.structs import Node

Command = IntEnum
Data = Any
Handler = Callable[..., Any]


class ITranscoder(ABC):
    my_node: Node
    service_id: str

    def __init__(self, my_node: Node, service_id: str):
        self.my_node = my_node
        self.service_id = service_id

    @abstractmethod
    def encode(self, command: Command, *data: Data) -> Message:
        pass

    @abstractmethod
    def decode(self, message: Message) -> Tuple[Command, Sequence[Data]]:
        pass


class AProtocol:
    my_node: Node
    service_id: str
    _rpc: RPC
    _transcoder: ITranscoder
    _handlers: Dict[Command, Handler]

    _running: bool

    def __init__(self, my_node: Node):
        self.my_node = my_node
        self._rpc = RPCRegister.get_rpc(my_node)
        self._handlers = {}

        self._running = False

    async def start(self, service_id: str):
        if self._running:
            raise RuntimeError("Protocol already running")
        self.service_id = service_id
        self._transcoder = self._get_new_transcoder()
        await self._rpc.register_service(service_id, self.handle_rpc)
        self._running = True

    async def stop(self):
        if self._running:
            await self._rpc.unregister_service(self.service_id)
            self._running = False

    async def handle_rpc(self, message: Message):
        command, data = self._transcoder.decode(message)
        handler = self._handlers[command]
        if asyncio.iscoroutinefunction(handler):
            response = await handler(message.node, *data)
            return self._encode_response(response)
        else:
            response = handler(message.node, *data)
            return self._encode_response(response)

    def _encode_response(self, response: Any) -> Any:
        if isinstance(response, list):
            return [self._encode_response(response_item) for response_item in response]
        elif isinstance(response, tuple):
            return tuple(
                self._encode_response(response_item) for response_item in response
            )
        elif hasattr(response, "encode"):
            return response.encode()
        elif is_dataclass(response):
            return asdict(response)
        elif isinstance(response, set):
            return list(response)
        return response

    @abstractmethod
    def _get_new_transcoder(self):
        """
        Method for initializing ITranscoder, because every Protocol implements
        its own.

        :return:
        """
        pass

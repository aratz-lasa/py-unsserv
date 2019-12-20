from abc import ABC, abstractmethod
from typing import Callable, Any, Coroutine, Union, Dict

from unsserv.data_structures import Node, Message

RpcCallback = Callable[[Message], Coroutine[Any, Any, Union[None, Message]]]


class IRPC(ABC):
    my_node: Node
    registered_services: Dict[Node, RpcCallback]

    def __init__(self, node: Node):
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

    @abstractmethod
    async def _start(self):
        pass

    @abstractmethod
    async def _stop(self):
        pass

    @abstractmethod
    async def call_push(self, destination: Node, message: Message):
        pass

    @abstractmethod
    async def call_pushpull(self, destination: Node, message: Message) -> Message:
        pass

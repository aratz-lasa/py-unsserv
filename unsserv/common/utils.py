import asyncio
import random
import string
from abc import ABC, abstractmethod
from typing import List, Dict, Any

from unsserv.common.rpc.structs import Message
from unsserv.common.structs import Node
from unsserv.common.typing import Handler, SyncHandler


def parse_node(raw_node: List) -> Node:
    address_info = tuple(raw_node[0])
    extra = tuple(raw_node[1])
    return Node(address_info, extra)


def parse_message(raw_message: List) -> Message:
    node = parse_node(raw_message[0])
    return Message(node, raw_message[1], raw_message[2])


def get_random_id(size: int = 10) -> str:
    id_characters = string.ascii_letters + string.digits + string.punctuation
    return "".join(random.choice(id_characters) for _ in range(size))


async def stop_task(task: asyncio.Task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


class HandlersManager:
    _handlers: List[Handler]

    def __init__(self):
        self._handlers = []

    def add_handler(self, handler: Handler):
        self._handlers.append(handler)

    def remove_handler(self, handler: Handler):
        self._handlers.remove(handler)

    def remove_all_handlers(self):
        self._handlers.clear()

    def call_handlers(self, *args, **kwargs):
        for handler in self._handlers:
            if asyncio.iscoroutinefunction(handler):
                asyncio.create_task(handler(*args, **kwargs))
            elif hasattr(handler, "func") and asyncio.iscoroutinefunction(
                handler.func
            ):  # in case 'partial' was used
                asyncio.create_task(handler(*args, **kwargs))
            else:
                asyncio.create_task(
                    self._sync_handler_wrapper(handler, *args, **kwargs)
                )

    async def _sync_handler_wrapper(self, sync_handler: SyncHandler, *args, **kwargs):
        sync_handler(*args, **kwargs)


class IConfig(ABC):
    @abstractmethod
    def load_from_dict(self, config_dict: Dict[str, Any]):
        pass

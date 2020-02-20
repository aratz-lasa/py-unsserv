from abc import ABC, abstractmethod
from typing import Tuple, Any

from unsserv.common.data_structures import Message


class IGossipSubscriber(ABC):
    @abstractmethod
    async def new_message(self, message: Message):
        pass

    @abstractmethod
    async def get_data(self) -> Tuple[Any, Any]:
        pass

from abc import ABC, abstractmethod
from typing import Any, Tuple

from unsserv.common.gossip.typing import Payload


class IGossipSubscriber(ABC):
    @abstractmethod
    async def new_message(self, payload: Payload):
        pass

    @abstractmethod
    async def get_data(self) -> Tuple[Any, Any]:
        pass

from abc import ABC, abstractmethod
from typing import Any, Tuple

from unsserv.common.gossip.typing import Payload


class IGossipSubscriber(ABC):
    @abstractmethod
    async def receive_payload(self, payload: Payload):
        pass

    @abstractmethod
    async def get_payload(self) -> Tuple[Any, Any]:
        pass

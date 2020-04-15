from typing import Any

from unsserv.common.services_abc import IDisseminationService
from unsserv.common.typing import Handler


class Plumtree(IDisseminationService):
    async def join(self, service_id: Any, **configuration: Any):
        pass

    async def leave(self):
        pass

    async def broadcast(self, data: bytes):
        pass

    def add_broadcast_handler(self, handler: Handler):
        pass

    def remove_broadcast_handler(self, handler: Handler):
        pass

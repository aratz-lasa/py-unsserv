from typing import Callable, Coroutine, Any

from unsserv.common.api import DisseminationService, MembershipService

BroadcastHandler = Callable[[Any], Coroutine[Any, Any, None]]


class Mon(DisseminationService):
    def __init__(self, membership: MembershipService):
        self.my_node = membership.my_node

    async def join_broadcast(self, service_id: str, broadcast_handler: Any) -> None:
        pass  # todo

    async def leave_broadcast(self) -> None:
        pass  # todo

    async def broadcast(self, data: Any) -> None:
        pass  # todo

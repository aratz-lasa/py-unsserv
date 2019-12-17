import typing

from unsserv.api import MembershipService, neighbours_callback
from unsserv.common.gossip.gossiping import Gossiping
from unsserv.data_structures import Node


class Newscast(MembershipService):
    def __init__(self, node: Node):
        super().__init__(node)
        self._gossiping = Gossiping(node)

    async def join_membership(self, bootstrap_nodes: typing.List[Node] = None):
        pass

    async def leave_membership(self) -> None:
        pass

    def get_neighbours(self) -> typing.List[Node]:
        pass

    def set_neighbours_callback(self, callback: neighbours_callback) -> None:
        pass

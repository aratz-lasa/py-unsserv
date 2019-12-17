import typing

from unsserv.api import MembershipService, NeighboursCallback
from unsserv.common.gossip.gossiping import Gossiping, View
from unsserv.data_structures import Node


class Newscast(MembershipService):
    async def join_membership(self, bootstrap_nodes: typing.List[Node] = None):
        self._gossiping = Gossiping(
            self.my_node, bootstrap_nodes, self._local_view_callback
        )
        await self._gossiping.start()

    async def leave_membership(self) -> None:
        await self._gossiping.stop()

    def get_neighbours(self) -> typing.List[Node]:
        return list(self._gossiping.local_view.keys())

    def set_neighbours_callback(self, callback: NeighboursCallback) -> None:
        self._callback = callback

    async def _local_view_callback(self, local_view: View):
        if self._callback:
            await self._callback(list(local_view.keys()))

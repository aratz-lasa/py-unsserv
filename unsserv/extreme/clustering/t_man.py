from typing import List, Callable, Any, Union
from functools import partial
from unsserv.api import ClusteringService, MembershipService, NeighboursCallback
from unsserv.common.gossip.gossip import (
    View,
    Gossip,
    PeerSelectionPolicy,
    ViewSelectionPolicy,
)
from unsserv.data_structures import Node

RankingFunction = Callable[[Node], Any]


class TMan(ClusteringService):
    _gossip: Gossip
    _callback_raw_format: bool

    def __init__(self, membership: MembershipService, multiplex: bool = True):
        super().__init__(membership, multiplex)
        self._callback_raw_format = False
        self._ranking_function: RankingFunction

    async def join_cluster(
        self, service_id: Any, ranking_function: RankingFunction
    ) -> None:
        if self._gossip.is_running():
            raise RuntimeError("Already joined a cluster")
        self._ranking_function = ranking_function
        random_view_source = partial(self._membership.get_neighbours, True)
        self._gossip = Gossip(
            self._membership.my_node,
            self._membership.get_neighbours(),
            peer_selection=PeerSelectionPolicy.HEAD,
            view_selection=ViewSelectionPolicy.HEAD,
            custom_selection_ranking=self._selection_ranking,
            external_view_source=random_view_source,
            multiplex=True,
        )
        await self._gossip.start()

    async def leave_cluster(self) -> None:
        if self._gossip.is_running():
            await self._gossip.stop()

    def get_neighbours(self, local_view: bool = False) -> Union[List[Node], View]:
        if local_view:
            return self._gossip.local_view
        return list(self._gossip.local_view.keys())

    def set_neighbours_callback(
        self, callback: NeighboursCallback, local_view: bool = False
    ) -> None:
        self._callback = callback
        self._callback_raw_format = local_view

    async def _local_view_callback(self, local_view: View):
        if self._callback:
            if self._callback_raw_format:
                await self._callback(local_view)
            else:
                await self._callback(list(local_view.keys()))

    def _selection_ranking(self, view: View) -> List[Node]:
        return sorted(view.keys(), key=self._ranking_function)
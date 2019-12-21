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
    def __init__(self, membership: MembershipService, multiplex: bool = True):
        super().__init__(membership, multiplex)
        self._callback_raw_format = False
        self._ranking_function: RankingFunction
        self._gossip: Union[Gossip, None] = None

    async def join_cluster(
        self, service_id: Any, ranking_function: RankingFunction
    ) -> None:
        if self._gossip:
            raise RuntimeError("Already joined a cluster")
        self._ranking_function = ranking_function
        random_view_source = partial(self._membership.get_neighbours, True)
        local_view_nodes = self._membership.get_neighbours()
        assert isinstance(local_view_nodes, list)
        self._gossip = Gossip(
            self._membership.my_node,
            service_id=service_id,
            local_view_nodes=local_view_nodes,
            peer_selection=PeerSelectionPolicy.HEAD,
            view_selection=ViewSelectionPolicy.HEAD,
            custom_selection_ranking=self._selection_ranking,
            external_view_source=random_view_source,
            multiplex=True,
        )
        await self._gossip.start()

    async def leave_cluster(self) -> None:
        if self._gossip:
            await self._gossip.stop()
            self._gossip = None

    def get_neighbours(self, local_view: bool = False) -> Union[List[Node], View]:
        if not self._gossip:
            raise RuntimeError("Cluster not joined")
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

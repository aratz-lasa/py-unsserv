from functools import partial
from typing import Any, Callable, List, Union, Optional

from unsserv.common.services_abc import (
    ClusteringService,
    MembershipService,
    NeighboursCallback,
)
from unsserv.common.data_structures import Node
from unsserv.common.gossip.gossip import (
    Gossip,
    View,
)

RankingFunction = Callable[[Node], Any]


class TMan(ClusteringService):
    _multiplex: bool
    _callback: NeighboursCallback
    _callback_raw_format: bool
    _gossip: Optional[Gossip]

    def __init__(self, membership: MembershipService, multiplex: bool = True):
        self.my_node = membership.my_node
        self.membership = membership
        self._multiplex = multiplex
        self._callback = None
        self._ranking_function: RankingFunction
        self._callback_raw_format = False
        self._gossip = None

    async def join(self, service_id: Any, **configuration: Any) -> None:
        if self.running:
            raise RuntimeError("Already running Clustering")
        self.service_id = service_id
        self._ranking_function = configuration["ranking_function"]
        random_view_source = partial(self.membership.get_neighbours, True)
        local_view_nodes = self.membership.get_neighbours()
        assert isinstance(local_view_nodes, list)  # for mypy validation
        self._gossip = Gossip(
            self.membership.my_node,
            service_id=service_id,
            local_view_nodes=local_view_nodes,
            local_view_callback=self._local_view_callback,
            custom_selection_ranking=self._selection_ranking,
            external_view_source=random_view_source,
            multiplex=True,
        )
        await self._gossip.start()
        self.running = True

    async def leave(self) -> None:
        if not self.running:
            return
        await self._gossip.stop()
        self._gossip = None
        self.running = False

    def get_neighbours(
        self, local_view_format: bool = False
    ) -> Union[List[Node], View]:
        if not self.running:
            raise RuntimeError("Clustering service not running")
        if local_view_format:
            return self._gossip.local_view
        return list(self._gossip.local_view.keys())

    def set_neighbours_callback(
        self, callback: NeighboursCallback, local_view_format: bool = False
    ) -> None:
        if not self.running:
            raise RuntimeError("Clustering service not running")
        self._callback = callback
        self._callback_raw_format = local_view_format

    async def _local_view_callback(self, local_view: View):
        if self._callback:
            if self._callback_raw_format:
                await self._callback(local_view)
            else:
                await self._callback(list(local_view.keys()))

    def _selection_ranking(self, view: View) -> List[Node]:
        return sorted(view.keys(), key=self._ranking_function)

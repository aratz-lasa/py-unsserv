from typing import Any, Callable, List, Optional

from unsserv.common.gossip.gossip import (
    Gossip,
    View,
)
from unsserv.common.services_abc import (
    IClusteringService,
    IMembershipService,
)
from unsserv.common.structs import Node, Property
from unsserv.common.typing import Handler
from unsserv.common.utils import HandlersManager

RankingFunction = Callable[[Node], Any]


class TMan(IClusteringService):
    properties = {Property.EXTREME, Property.HAS_GOSSIP, Property.NON_SYMMETRIC}

    gossip: Optional[Gossip]
    _handler_manager: HandlersManager
    _ranking_function: RankingFunction

    def __init__(self, membership: IMembershipService):
        self.my_node = membership.my_node
        self.membership = membership
        self.gossip = None
        self._handler_manager = HandlersManager()

    async def join(self, service_id: Any, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Clustering")
        self.service_id = service_id
        self._ranking_function = configuration["ranking_function"]  # type: ignore
        local_view_nodes = self.membership.get_neighbours()
        assert isinstance(local_view_nodes, list)  # for mypy validation
        self.gossip = Gossip(
            self.membership.my_node,
            service_id=service_id,
            local_view_nodes=local_view_nodes,
            local_view_handler=self._gossip_local_view_handler,
            custom_selection_ranking=self._selection_ranking,
            external_nodes_source=self.membership.get_neighbours,
        )
        await self.gossip.start()
        self.running = True

    async def leave(self):
        if not self.running:
            return
        await self.gossip.stop()
        self._handler_manager.remove_all_handlers()
        self.gossip = None
        self.running = False

    def get_neighbours(self) -> List[Node]:
        if not self.running:
            raise RuntimeError("Clustering service not running")
        return list(self.gossip.local_view.keys())

    def add_neighbours_handler(self, handler: Handler):
        if not self.running:
            raise RuntimeError("Service not running")
        self._handler_manager.add_handler(handler)

    def remove_neighbours_handler(self, handler: Handler):
        self._handler_manager.remove_handler(handler)

    async def _gossip_local_view_handler(self, local_view: View):
        self._handler_manager.call_handlers(list(local_view.keys()))

    def _selection_ranking(self, view: View) -> List[Node]:
        return sorted(view.keys(), key=self._ranking_function)  # type: ignore

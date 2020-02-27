from abc import ABC, abstractmethod
from typing import Any, List, Union

from unsserv.common.data_structures import Node
from unsserv.common.typing import (
    AggregateCallback,
    NeighboursCallback,
    View,
    BroadcastHandler,
)


class IService(ABC):
    my_node: Node
    service_id: Any
    running: bool = False


class ISubService(IService):
    membership: "MembershipService"


class MembershipService(IService):
    @abstractmethod
    async def join(self, service_id: Any, configuration: Any):
        pass

    @abstractmethod
    async def leave(self) -> None:
        pass

    @abstractmethod
    def get_neighbours(self, local_view: bool = False) -> Union[List[Node], View]:
        pass

    @abstractmethod
    def set_neighbours_callback(
        self, callback: NeighboursCallback, local_view: bool = False
    ) -> None:
        pass


class ClusteringService(ISubService, MembershipService):
    pass


class AggregationService(ISubService):
    @abstractmethod
    async def join_aggregation(
        self, service_id: str, aggregation_configuration: Any
    ) -> None:
        pass

    @abstractmethod
    async def leave_aggregation(self) -> None:
        pass

    @abstractmethod
    async def get_aggregate(self) -> Any:
        pass

    @abstractmethod
    def set_aggregate_callback(self, callback: AggregateCallback) -> None:
        pass


class SamplingService(ISubService):
    @abstractmethod
    async def join_sampling(self, service_id: str) -> None:
        pass

    @abstractmethod
    async def leave_sampling(self) -> None:
        pass

    @abstractmethod
    async def get_sample(self) -> Node:
        pass


class DisseminationService(ISubService):
    @abstractmethod
    async def join_broadcast(
        self, service_id: str, broadcast_handler: BroadcastHandler
    ) -> None:
        """Duplicates must handle the user."""
        pass

    @abstractmethod
    async def leave_broadcast(self) -> None:
        pass

    @abstractmethod
    async def broadcast(self, data: Any) -> None:
        pass


class SearchingService(ISubService):
    @abstractmethod
    async def publish(self, data: Any) -> None:
        pass

    @abstractmethod
    async def unpublish(self, data: Any) -> None:
        pass

    @abstractmethod
    async def search(self, id: str) -> Any:
        pass

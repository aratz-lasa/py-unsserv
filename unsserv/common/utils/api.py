from abc import ABC, abstractmethod
from typing import List, Any, Union

from unsserv.common.utils.data_structures import Node
from unsserv.common.utils.typing import NeighboursCallback, View, AggregateCallback


class MembershipService(ABC):
    my_node: Node

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


class ClusteringService(MembershipService):
    pass


class AggregationService(ABC):
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


class SamplingService(ABC):
    @abstractmethod
    async def get_sample(self) -> Node:
        pass


class DisseminationService(ABC):
    @abstractmethod
    async def join_broadcast(self, broadcast_configuration: Any) -> None:
        pass

    @abstractmethod
    async def leave_broadcast(self) -> None:
        pass

    @abstractmethod
    async def broadcast(self, data: Any) -> None:
        pass


class SearchingService(ABC):
    @abstractmethod
    async def publish(self, data: Any) -> None:
        pass

    @abstractmethod
    async def unpublish(self, data: Any) -> None:
        pass

    @abstractmethod
    async def search(self, id: str) -> Any:
        pass
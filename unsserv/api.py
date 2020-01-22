from typing import Callable, List, Coroutine, Any, Union
from abc import ABC, abstractmethod

from unsserv.data_structures import Node
from collections import Counter

View = Counter
AggregateCallback = Callable[[Any], Coroutine[Any, Any, None]]
NeighboursCallback = Union[
    Callable[[Union[List[Node], View]], Coroutine[Any, Any, None]], None
]


class MembershipService(ABC):
    my_node: Node

    @abstractmethod
    async def join_membership(
        self, service_id: Any, bootstrap_nodes: List[Node] = None
    ):
        pass

    @abstractmethod
    async def leave_membership(self) -> None:
        pass

    @abstractmethod
    def get_neighbours(self, local_view: bool = False) -> Union[List[Node], View]:
        pass

    @abstractmethod
    def set_neighbours_callback(
        self, callback: NeighboursCallback, local_view: bool = False
    ) -> None:
        pass


class ClusteringService(ABC):
    @abstractmethod
    async def join_cluster(self, service_id: Any, cluster_configuration: Any) -> None:
        pass

    @abstractmethod
    async def leave_cluster(self) -> None:
        pass

    @abstractmethod
    def get_neighbours(self, local_view: bool = False) -> Union[List[Node], View]:
        pass

    @abstractmethod
    def set_neighbours_callback(
        self, callback: NeighboursCallback, local_view: bool = False
    ) -> None:
        pass


class AggregationService(ABC):
    @abstractmethod
    async def join_aggregation(self, aggregation_configuration: Any) -> None:
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

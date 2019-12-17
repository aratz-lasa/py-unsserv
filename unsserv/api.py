from typing import Callable, List, Coroutine, Any, Union
from abc import ABC, abstractmethod

from unsserv.data_structures import Node

NeighboursCallback = Union[Callable[[List[Node]], Coroutine[Any, Any, None]], None]
AggregateCallback = Callable[[Any], Coroutine[Any, Any, None]]


class MembershipService(ABC):
    my_node: Node
    _callback: NeighboursCallback

    def __init__(self, node: Node):
        self.my_node = node
        self._callback = None

    @abstractmethod
    async def join_membership(self, bootstrap_nodes: List[Node]) -> None:
        pass

    @abstractmethod
    async def leave_membership(self) -> None:
        pass

    @abstractmethod
    def get_neighbours(self) -> List[Node]:
        pass

    @abstractmethod
    def set_neighbours_callback(self, callback: NeighboursCallback) -> None:
        pass


class ClusteringService(ABC):
    @abstractmethod
    async def join_cluster(self, cluster_configuration: Any) -> None:
        pass

    @abstractmethod
    async def leave_cluster(self) -> None:
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

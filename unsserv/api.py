import typing
from abc import ABC, abstractmethod

from unsserv.data_structures import Node

neighbours_callback = typing.Callable[
    [typing.List[Node]], typing.Coroutine[typing.Any, typing.Any, None]
]
aggregate_callback = typing.Callable[[typing.Any], None]


class MembershipService(ABC):
    def __init__(self, node: Node):
        self.node = node

    @abstractmethod
    async def join_membership(self, bootstrap_nodes: typing.List[Node]) -> None:
        pass

    @abstractmethod
    async def leave_membership(self) -> None:
        pass

    @abstractmethod
    def get_neighbours(self) -> typing.List[Node]:
        pass

    @abstractmethod
    def set_neighbours_callback(self, callback: neighbours_callback) -> None:
        pass


class ClusteringService(ABC):
    @abstractmethod
    async def join_cluster(self, cluster_configuration: typing.Any) -> None:
        pass

    @abstractmethod
    async def leave_cluster(self) -> None:
        pass


class AggregationService(ABC):
    @abstractmethod
    async def join_aggregation(self, aggregation_configuration: typing.Any) -> None:
        pass

    @abstractmethod
    async def leave_aggregation(self) -> None:
        pass

    @abstractmethod
    async def get_aggregate(self) -> typing.Any:
        pass

    @abstractmethod
    def set_aggregate_callback(self, callback: aggregate_callback) -> None:
        pass


class SamplingService(ABC):
    @abstractmethod
    async def get_sample(self) -> Node:
        pass


class DisseminationService(ABC):
    @abstractmethod
    async def join_broadcast(self, broadcast_configuration: typing.Any) -> None:
        pass

    @abstractmethod
    async def leave_broadcast(self) -> None:
        pass

    @abstractmethod
    async def broadcast(self, data: typing.Any) -> None:
        pass


class SearchingService(ABC):
    @abstractmethod
    async def publish(self, data: typing.Any) -> None:
        pass

    @abstractmethod
    async def unpublish(self, data: typing.Any) -> None:
        pass

    @abstractmethod
    async def search(self, id: str) -> typing.Any:
        pass

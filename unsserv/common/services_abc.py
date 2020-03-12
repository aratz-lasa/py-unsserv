from abc import ABC, abstractmethod
from typing import Any, List, Union, Optional

from unsserv.common.data_structures import Node
from unsserv.common.typing import (
    AggregateCallback,
    NeighboursCallback,
    View,
)


class IService(ABC):
    """
    UnsServ service interface.

    :ivar my_node: service node
    :ivar service_id: service identifier
    :ivar running: boolean that represents if the service is running
    """

    my_node: Node
    service_id: Any
    running: bool = False


class ISubService(IService):
    """
    Interface of UnsServ service that is not the first layer (check
    documentation's stack).

    :ivar membership: Membership service underlying (from the layer below) the service.
    """

    membership: "MembershipService"


class MembershipService(IService):
    """Membership service interface."""

    @abstractmethod
    async def join(self, service_id: Any, **configuration: Any):
        """
        Join/start memberhsip service.

        :param service_id: memberhsip service's id
        :param configuration: configuration parameters.
        :return:
        """
        pass

    @abstractmethod
    async def leave(self) -> None:
        """
        Leave/stop membership service.

        :return:
        """
        pass

    @abstractmethod
    def get_neighbours(
        self, local_view_format: bool = False
    ) -> Union[List[Node], View]:
        """
        Get the neighbours to which the service is connected.

        :param local_view_format: boolean for getting the neighbours
        as a list or a View.
        :return: neighbours to which the service is connected.
        """
        pass

    @abstractmethod
    def set_neighbours_callback(
        self, callback: NeighboursCallback, local_view_format: bool = False
    ) -> None:
        """
        Set the callback that is executed when neighbours change.

        :param callback: function that will be called.
        :param local_view_format: boolean for getting the neighbours
        as a list or a View.
        :return:
        """
        pass


class ClusteringService(ISubService, MembershipService):
    """Clustering service for having bias when selecting neighbours."""

    pass


class AggregationService(ISubService):
    """Aggregation service."""

    @abstractmethod
    async def join_aggregation(self, service_id: str, **configuration: Any) -> None:
        """
        Join/start aggregation service.

        :param service_id: aggregation service's id
        :param configuration: configuration parameters.
        :return:
        """
        pass

    @abstractmethod
    async def leave_aggregation(self) -> None:
        """
        Leave/stop aggregation service.

        :return:
        """
        pass

    @abstractmethod
    async def get_aggregate(self) -> Any:
        """
        Get current aggregate value.

        :return:
        """
        pass

    @abstractmethod
    def set_aggregate_callback(self, callback: AggregateCallback) -> None:
        """
        Set the callback that is executed when aggregate value change.

        :param callback: function that will be called.
        :return:
        """
        pass


class SamplingService(ISubService):
    """Sampling service."""

    @abstractmethod
    async def join_sampling(self, service_id: str, **configuration: Any) -> None:
        """
        Join/start sampling service.

        :param service_id: sampling service's id
        :param configuration: configuration parameters.
        :return:
        """
        pass

    @abstractmethod
    async def leave_sampling(self) -> None:
        """
        Leave/stop sampling service.

        :return:
        """
        pass

    @abstractmethod
    async def get_sample(self) -> Node:
        """
        Gets a sample.

        :return: sample Node.
        """
        pass


class DisseminationService(ISubService):
    @abstractmethod
    async def join_broadcast(self, service_id: str, **configuration: Any) -> None:
        """
        Join/start dissemination service. Duplicates must handle the user.

        :param service_id: sampling service's id
        :param configuration: configuration parameters.
        :return:
        """
        pass

    @abstractmethod
    async def leave_broadcast(self) -> None:
        """
        Leave/stop dissemination service.

        :return:
        """
        pass

    @abstractmethod
    async def broadcast(self, data: bytes) -> None:
        """
        Disseminates/brodcasts data.

        :param data: bytes data
        :return:
        """
        pass


class SearchingService(ISubService):
    @abstractmethod
    async def join_searching(self, service_id: str, **configuration: Any) -> None:
        """
        Join/start search service.

        :param service_id: search service's id
        :param configuration: configuration parameters.
        :return:
        """
        pass

    @abstractmethod
    async def leave_searching(self) -> None:
        """
        Leave/stop search service.

        :return:
        """
        pass

    @abstractmethod
    async def publish(self, data_id: str, data: bytes) -> None:
        """
        Publish data that can be found when searching.

        :param data_id: data id.
        :param data: bytes data.
        :return:
        """
        pass

    @abstractmethod
    async def unpublish(self, data_id: str) -> None:
        """
        Unpublish data so that cannot be found when searching.

        :param data_id: data id.
        :return:
        """
        pass

    @abstractmethod
    async def search(self, data_id: str) -> Optional[bytes]:
        """
        Search data.

        :param data_id: data id.
        :return: Data bytes or None if not found.
        """
        pass

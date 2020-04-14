from abc import ABC, abstractmethod
from typing import Any, List, Union, Optional, Set

from unsserv.common.service_properties import Property
from unsserv.common.structs import Node
from unsserv.common.typing import Handler, View


class IService(ABC):
    """
    UnsServ service interface.

    :ivar my_node: service node
    :ivar service_id: service identifier
    :ivar running: boolean that represents if the service is running
    """

    @abstractmethod
    async def join(self, service_id: Any, **configuration: Any):
        """
        Join/start service.

        :param service_id: service's id
        :param configuration: configuration parameters.
        :return:
        """
        pass

    @abstractmethod
    async def leave(self):
        """
        Leave/stop service.

        :return:
        """
        pass

    properties: Set[Property]
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
    def add_neighbours_handler(self, handler: Handler):
        """
        Add a callback that is executed when neighbours change.

        :param handler: function that will be called.
        as a list or a View.
        :return:
        """
        pass

    @abstractmethod
    def remove_neighbours_handler(self, handler: Handler):
        """
        Remove the callback that is executed when neighbours change.

        :param handler: callback that will be removed.
        :return:
        """
        pass


class ClusteringService(ISubService, MembershipService):
    """Clustering service for having bias when selecting neighbours."""

    pass


class AggregationService(ISubService):
    """Aggregation service."""

    @abstractmethod
    async def get_aggregate(self) -> Any:
        """
        Get current aggregate value.

        :return: aggregate value.
        """
        pass

    @abstractmethod
    def add_aggregate_handler(self, handler: Handler):
        pass

    @abstractmethod
    def remove_aggregate_handler(self, handler: Handler):
        pass


class SamplingService(ISubService):
    """Sampling service."""

    @abstractmethod
    async def get_sample(self) -> Node:
        """
        Gets a sample.

        :return: sample Node.
        """
        pass


class DisseminationService(ISubService):
    @abstractmethod
    async def broadcast(self, data: bytes):
        """
        Disseminates/brodcasts data.

        :param data: bytes data
        :return:
        """
        pass

    @abstractmethod
    def add_broadcast_handler(self, handler: Handler):
        pass

    @abstractmethod
    def remove_broadcast_handler(self, handler: Handler):
        pass


class SearchingService(ISubService):
    @abstractmethod
    async def publish(self, data_id: str, data: bytes):
        """
        Publish data that can be found when searching.

        :param data_id: data id.
        :param data: bytes data.
        :return:
        """
        pass

    @abstractmethod
    async def unpublish(self, data_id: str):
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

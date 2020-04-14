from abc import ABC, abstractmethod
from typing import Any, List, Optional, Set

from unsserv.common.structs import Node, Property
from unsserv.common.typing import Handler


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

    membership: "IMembershipService"


class IMembershipService(IService):
    """Membership service interface."""

    @abstractmethod
    def get_neighbours(self) -> List[Node]:
        """
        Get the neighbours to which the service is connected.

        as a list or a View.
        :return: neighbours to which the service is connected.
        """
        pass

    @abstractmethod
    def add_neighbours_handler(self, handler: Handler):
        """
        Add a handler that is executed when neighbours change.

        :param handler: function that will be called.
        as a list or a View.
        :return:
        """
        pass

    @abstractmethod
    def remove_neighbours_handler(self, handler: Handler):
        """
        Remove the handler that is executed when neighbours change.

        :param handler: handler that will be removed.
        :return:
        """
        pass


class IClusteringService(ISubService, IMembershipService):
    """Clustering service for having bias when selecting neighbours."""

    pass


class IAggregationService(ISubService):
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


class ISamplingService(ISubService):
    """Sampling service."""

    @abstractmethod
    async def get_sample(self) -> Node:
        """
        Gets a sample.

        :return: sample Node.
        """
        pass


class IDisseminationService(ISubService):
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


class ISearchingService(ISubService):
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

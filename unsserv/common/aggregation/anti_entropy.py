from enum import Enum, auto
from statistics import mean
from typing import Any, Callable, Dict, Tuple, Union

from unsserv.common.services_abc import (
    AggregateCallback,
    AggregationService,
    MembershipService,
)
from unsserv.common.data_structures import Message
from unsserv.common.gossip.gossip import Gossip
from unsserv.common.gossip.gossip_subcriber_abc import IGossipSubscriber


class AggregateType(Enum):
    MEAN = auto()
    MAX = auto()
    MIN = auto()


aggregate_functions: Dict[AggregateType, Callable] = {
    AggregateType.MEAN: mean,
    AggregateType.MAX: max,
    AggregateType.MIN: min,
}


class AntiEntropy(AggregationService, IGossipSubscriber):
    def __init__(self, membership: MembershipService, multiplex: bool = True):
        self.my_node = membership.my_node
        self.multiplex = multiplex
        if not hasattr(membership, "_gossip"):
            raise ValueError(
                "Invalid membership service. "
                "Membership must contain a '_gossip' attribute"
            )
        self.membership = membership
        self._gossip: Gossip = getattr(membership, "_gossip")
        self._aggregate_value: Any = None
        self._aggregate_type: Union[AggregateType, None] = None
        self._aggregate_func: Union[Callable, None] = None
        self._callback: Union[AggregateCallback, None] = None

    async def join_aggregation(
        self, service_id: str, aggregation_configuration: Tuple
    ) -> None:
        if self.running:
            raise RuntimeError("Already running Aggregation")
        self.service_id = service_id
        self._aggregate_type, self._aggregate_value = aggregation_configuration
        self._aggregate_func = aggregate_functions[self._aggregate_type]
        self._gossip.subscribe(self)
        self.running = True

    async def leave_aggregation(self) -> None:
        self._aggregate_type = None
        self._aggregate_value = None
        self._gossip.unsubscribe(self)
        self.running = False

    async def get_aggregate(self) -> Any:
        if not self.running:
            raise RuntimeError("Aggregation service not running")
        return self._aggregate_value

    def set_aggregate_callback(self, callback: AggregateCallback) -> None:
        if not self.running:
            raise RuntimeError("Aggregation service not running")
        self._callback = callback

    async def new_message(self, message: Message):
        assert callable(self._aggregate_func)
        neighbor_aggregate = message.data.get(self.service_id, None)
        self._aggregate_value = self._aggregate_func(
            [self._aggregate_value, neighbor_aggregate]
        )
        await self._callback(self._aggregate_value)

    async def get_data(self) -> Tuple[Any, Any]:
        return self.service_id, self._aggregate_value

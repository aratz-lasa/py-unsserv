from enum import Enum, auto
from statistics import mean
from typing import Any, Callable, Dict, Tuple, Optional

from unsserv.common.data_structures import Message
from unsserv.common.gossip.gossip import Gossip
from unsserv.common.gossip.gossip_subcriber_abc import IGossipSubscriber
from unsserv.common.services_abc import (
    AggregateCallback,
    AggregationService,
    MembershipService,
)


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
    """Aggregation Anti-Entropy service."""

    _gossip: Gossip
    _aggregate_value: Any
    _aggregate_type: Optional[AggregateType]
    _aggregate_func: Optional[Callable]
    _callback: Optional[AggregateCallback]

    def __init__(self, membership: MembershipService):
        self.my_node = membership.my_node
        if not hasattr(membership, "_gossip"):
            raise ValueError(
                "Invalid membership service. "
                "Membership must contain a '_gossip' attribute"
            )
        self.membership = membership
        self._gossip = getattr(membership, "_gossip")
        self._aggregate_value = None
        self._aggregate_type = None
        self._aggregate_func = None
        self._callback = None

    async def join_aggregation(self, service_id: str, **configuration: Any) -> None:
        if self.running:
            raise RuntimeError("Already running Aggregation")
        self._aggregate_type = configuration["aggregate_type"]
        self._aggregate_value = configuration["aggregate_value"]
        self.service_id = service_id
        self._aggregate_func = aggregate_functions[self._aggregate_type]
        self._gossip.subscribe(self)
        self.running = True

    async def leave_aggregation(self) -> None:
        if not self.running:
            return
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
        """IGossipSubscriber implementation."""
        assert callable(self._aggregate_func)
        neighbor_aggregate = message.data.get(self.service_id, None)
        self._aggregate_value = self._aggregate_func(
            [self._aggregate_value, neighbor_aggregate]
        )
        await self._callback(self._aggregate_value)

    async def get_data(self) -> Tuple[Any, Any]:
        """IGossipSubscriber implementation."""
        return self.service_id, self._aggregate_value

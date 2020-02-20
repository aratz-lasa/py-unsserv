from enum import Enum, auto
from statistics import mean
from typing import Any, Tuple, Callable, Union, Dict

from unsserv.common.api import AggregationService, AggregateCallback, MembershipService
from unsserv.common.gossip.gossip import Gossip
from unsserv.common.gossip.gossip_subcriber_interface import IGossipSubscriber
from unsserv.common.data_structures import Message


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
        self._gossip: Gossip = getattr(membership, "_gossip")
        self._aggregate_value: Any = None
        self._aggregate_type: Union[AggregateType, None] = None
        self._aggregate_func: Union[Callable, None] = None
        self._callback: Union[AggregateCallback, None] = None

    async def join_aggregation(
        self, service_id: str, aggregation_configuration: Tuple
    ) -> None:
        if self.running:
            raise RuntimeError("Already running membership")
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
        return self._aggregate_value

    def set_aggregate_callback(self, callback: AggregateCallback) -> None:
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

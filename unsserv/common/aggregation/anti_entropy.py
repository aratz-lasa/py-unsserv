from statistics import mean
from typing import Any, Callable, Dict, Tuple

from unsserv.common.aggregation.config import AggregateType, AntiConfig
from unsserv.common.gossip.gossip import Gossip, IGossipSubscriber
from unsserv.common.gossip.typing import Payload
from unsserv.common.services_abc import IAggregationService, IMembershipService
from unsserv.common.structs import Property
from unsserv.common.typing import Handler
from unsserv.common.utils import HandlersManager

aggregate_functions: Dict[AggregateType, Callable] = {
    AggregateType.MEAN: mean,
    AggregateType.MAX: max,
    AggregateType.MIN: min,
}


class AntiEntropy(IAggregationService, IGossipSubscriber):
    """Aggregation Anti-Entropy service."""

    properties = {Property.EXTREME, Property.STABLE, Property.HAS_GOSSIP}
    gossip: Gossip
    _config: AntiConfig
    _handler_manager: HandlersManager
    _aggregate_value: Any

    def __init__(self, membership: IMembershipService):
        self.my_node = membership.my_node
        if Property.HAS_GOSSIP not in membership.properties:
            raise ValueError(
                "Invalid membership service. "
                "Membership must contain a 'gossip' attribute"
            )
        self.membership = membership
        self.gossip = getattr(membership, "gossip")
        self._aggregate_value = None
        self._handler_manager = HandlersManager()
        self._config = AntiConfig()

    async def join(self, service_id: str, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Aggregation")
        self._config.load_from_dict(configuration)
        self._aggregate_value = configuration["aggregate_value"]
        self.service_id = service_id
        self.gossip.subscribe(self)
        self.running = True

    async def leave(self):
        if not self.running:
            return
        self.gossip.unsubscribe(self)
        self._handler_manager.remove_all_handlers()
        self._aggregate_value = None
        self.running = False

    async def get_aggregate(self) -> Any:
        if not self.running:
            raise RuntimeError("Aggregation service not running")
        return self._aggregate_value

    def add_aggregate_handler(self, handler: Handler):
        self._handler_manager.add_handler(handler)

    def remove_aggregate_handler(self, handler: Handler):
        self._handler_manager.remove_handler(handler)

    async def receive_payload(self, payload: Payload):
        """IGossipSubscriber implementation."""
        aggregate_function = aggregate_functions[self._config.AGGREGATE_TYPE]
        assert callable(aggregate_function)
        neighbor_aggregate = payload.get(self.service_id, None)
        if not neighbor_aggregate:
            return
        self._aggregate_value = aggregate_function(
            [self._aggregate_value, neighbor_aggregate]
        )
        self._handler_manager.call_handlers(self._aggregate_value)

    async def get_payload(self) -> Tuple[Any, Any]:
        """IGossipSubscriber implementation."""
        return self.service_id, self._aggregate_value

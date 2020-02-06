from typing import Any

from unsserv.api import AggregationService, AggregateCallback, MembershipService


class AntiEntropyAggregation(AggregationService):
    def __init__(self, membership: MembershipService, multiplex: bool = True):
        # todo
        self.my_node = membership.my_node
        self.multiplex = multiplex
        self._membership = membership

    async def join_aggregation(self, aggregation_configuration: Any) -> None:
        pass  # todo

    async def leave_aggregation(self) -> None:
        pass  # todo

    async def get_aggregate(self) -> Any:
        pass  # todo

    def set_aggregate_callback(self, callback: AggregateCallback) -> None:
        pass  # todo

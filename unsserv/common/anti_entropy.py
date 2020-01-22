from typing import Any

from unsserv.api import AggregationService, AggregateCallback


class AntiEntropyAggregation(AggregationService):
    async def join_aggregation(self, aggregation_configuration: Any) -> None:
        pass  # todo

    async def leave_aggregation(self) -> None:
        pass  # todo

    async def get_aggregate(self) -> Any:
        pass  # todo

    def set_aggregate_callback(self, callback: AggregateCallback) -> None:
        pass  # todo

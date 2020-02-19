from unsserv.common.utils.api import SamplingService
from unsserv.common.utils.data_structures import Node


class MRWB(SamplingService):
    async def join_sampling(self, service_id: str) -> None:
        pass  # todo

    async def leave_sampling(self) -> None:
        pass  # todo

    async def get_sample(self) -> Node:
        pass  # todo

import asyncio
from enum import IntEnum, auto
from typing import List, Union

from unsserv.common.api import SamplingService, MembershipService
from unsserv.common.rpc.rpc import RpcBase, RPC
from unsserv.common.utils.data_structures import Node, Message

## config
MRWB_DEGREE_REFRESH_FREQUENCY = 5
DATA_FIELD_COMMAND = "mrwb-command"
DATA_FIELD_TTL = "mrwb-ttl"
DATA_FIELD_ORIGIN_NODE = "mrwb-origin-node"


class CommandMRWB(IntEnum):
    GET_DEGREE = auto()
    SAMPLE = auto()


class MRWBRPC(RpcBase):
    async def call_get_degree(self, destination: Node, message: Message) -> int:
        rpc_result = await self.get_degree(destination.address_info, message)
        return self._handle_call_response(rpc_result)

    async def call_sample(self, destination: Node, message: Message):
        rpc_result = await self.sample(destination.address_info, message)
        self.decode_message(self._handle_call_response(rpc_result))

    async def call_sample_result(self, destination: Node, message: Message):
        rpc_result = await self.sample_result(destination.address_info, message)
        self.decode_message(self._handle_call_response(rpc_result))

    async def rpc_get_degree(self, node: Node, raw_message: List) -> int:
        message = self.decode_message(raw_message)
        degree = await self.registered_services[message.service_id](message)
        assert degree
        return degree

    async def rpc_sample(self, node: Node, raw_message: List):
        message = self.decode_message(raw_message)
        await self.registered_services[message.service_id](message)

    async def rpc_sample_result(self, node: Node, raw_message: List):
        message = self.decode_message(raw_message)
        await self.registered_services[message.service_id](message)


class MRWB(SamplingService):
    def __init__(self, membership: MembershipService, multiplex: bool = True):
        self.my_node = membership.my_node
        self._membership = membership
        self._neighbours = []
        self._neighbour_degrees = {}
        self._rpc: MRWBRPC = RPC.get_rpc(
            self.my_node, ProtocolClass=MRWBRPC, multiplex=multiplex
        )

    async def join_sampling(self, service_id: str) -> None:
        if self.running:
            raise RuntimeError("Already running membership")
        self.service_id = service_id
        self._membership.set_neighbours_callback(self._neighbours_change_callback)
        self._neighbours = self._membership.get_neighbours()
        await self._rpc.register_service(self.service_id, self._handle_rpc)
        # todo: start degrees updater task?
        self.running = True

    async def leave_sampling(self) -> None:
        self._membership.set_neighbours_callback(None)
        self._neighbours = []
        await self._rpc.unregister_service(self.service_id)
        # todo: stop degrees updater task?
        self.running = False

    async def get_sample(self) -> Node:
        pass  # todo

    async def _degree_update_task(self):
        # maybe is not neeed if degrees are updated whenever memberhsip changes its neighbours?
        while True:
            for neighbour in self._neighbours:
                await self._update_degree(neighbour)
            await asyncio.sleep(MRWB_DEGREE_REFRESH_FREQUENCY)

    async def _handle_rpc(self, message: Message) -> Union[None, int]:
        command = message.data[DATA_FIELD_COMMAND]
        if command is CommandMRWB.GET_DEGREE:
            pass  # todo
        elif command is CommandMRWB.SAMPLE:
            pass  # todo

    async def _neighbours_change_callback(self, new_neighbours: List[Node]):
        old_neighbours = set(self._neighbours)
        self._neighbours = new_neighbours
        new_neighbours = set(new_neighbours)
        for neighbour in old_neighbours - new_neighbours:
            del self._neighbour_degrees[neighbour]
        for neighbour in new_neighbours - old_neighbours:
            await self._update_degree(neighbour)

    async def _update_degree(self, node: Node):
        data = {DATA_FIELD_COMMAND: CommandMRWB.GET_DEGREE}
        message = Message(self.my_node, self.service_id, data)
        try:
            degree = await self._rpc.call_get_degree(node, message)
            self._neighbour_degrees[node] = degree
        except ConnectionError:
            self._neighbours.remove(node)

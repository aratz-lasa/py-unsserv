import asyncio
import random
from typing import Any, Dict, List, Optional

from enum import IntEnum, auto
from unsserv.common.data_structures import Message, Node
from unsserv.common.rpc.rpc import RpcBase, RPC
from unsserv.common.services_abc import SearchingService, MembershipService
from unsserv.extreme.searching.k_walker_config import (
    DATA_FIELD_COMMAND,
    DATA_FIELD_TTL,
    DATA_FIELD_ORIGIN_NODE,
    DATA_FIELD_WALK_ID,
    DATA_FIELD_WALK_RESULT,
    DATA_FIELD_DATA_ID,
)
from unsserv.extreme.searching import k_walker_config as config
from unsserv.common.utils import get_random_id, parse_node


class KWalkerCommand(IntEnum):
    WALK = auto()
    WALK_RESULT = auto()


class KWalkerProtocol:
    def __init__(self, my_node: Node, service_id: Any):
        self.my_node = my_node
        self.service_id = service_id

    def make_walk_message(
        self, data_id: str, walk_id: str, origin_node: Node, ttl: int
    ) -> Message:
        data = {
            DATA_FIELD_DATA_ID: data_id,
            DATA_FIELD_COMMAND: KWalkerCommand.WALK,
            DATA_FIELD_WALK_ID: walk_id,
            DATA_FIELD_ORIGIN_NODE: origin_node,
            DATA_FIELD_TTL: ttl,
        }
        return Message(self.my_node, self.service_id, data)

    def make_walk_result_message(self, walk_id: str, walk_result: bytes) -> Message:
        data = {
            DATA_FIELD_COMMAND: KWalkerCommand.WALK_RESULT,
            DATA_FIELD_WALK_ID: walk_id,
            DATA_FIELD_WALK_RESULT: walk_result,
        }
        return Message(self.my_node, self.service_id, data)


class KWalkerRPC(RpcBase):
    async def call_walk(self, destination: Node, message: Message):
        rpc_result = await self.walk(destination.address_info, message)
        self._handle_call_response(rpc_result)

    async def call_walk_result(self, destination: Node, message: Message):
        rpc_result = await self.walk_result(destination.address_info, message)
        self._handle_call_response(rpc_result)

    async def rpc_walk(self, node: Node, raw_message: List):
        message = self._decode_message(raw_message)
        await self.registered_services[message.service_id](message)

    async def rpc_walk_result(self, node: Node, raw_message: List):
        message = self._decode_message(raw_message)
        await self.registered_services[message.service_id](message)


class KWalker(SearchingService):
    _search_data: Dict[str, bytes]
    _walk_events: Dict[str, asyncio.Event]
    _walk_results: Dict[str, bytes]
    _rpc: KWalkerRPC
    _protocol: Optional[KWalkerProtocol]
    _ttl: int
    _fanout: int

    def __init__(self, membership: MembershipService, multiplex: bool = True):
        self.membership = membership
        self.my_node = membership.my_node
        self._search_data = {}
        self._rpc = RPC.get_rpc(
            self.my_node, ProtocolClass=KWalkerRPC, multiplex=multiplex
        )
        self._walk_results = {}
        self._walk_events = {}

    async def join_searching(
        self, service_id: str, **kwalker_configuration: Any
    ) -> None:
        if self.running:
            raise RuntimeError("Already running Searching service")
        self.service_id = service_id
        self._search_data = {}
        self._ttl = kwalker_configuration.get("ttl", None) or config.DEFAULT_TTL
        self._fanout = (
            kwalker_configuration.get("fanout", None) or config.DEFAULT_FANOUT
        )
        self._protocol = KWalkerProtocol(self.my_node, self.service_id)
        # initialize RPC
        await self._rpc.register_service(self.service_id, self._handle_rpc)
        self.running = True

    async def leave_searching(self) -> None:
        if not self.running:
            return
        self._protocol = None
        await self._rpc.unregister_service(self.service_id)
        self.running = False

    async def publish(self, data_id: str, data: bytes) -> None:
        if not self.running:
            raise RuntimeError("Searching service not running")
        if data_id in self._search_data:
            raise KeyError("Data-id already published")
        if not isinstance(data, bytes):
            raise TypeError("Published data must be bytes type")
        if not isinstance(data_id, str):
            raise TypeError("Published data-id must be str type")

        self._search_data[data_id] = data

    async def unpublish(self, data_id: str) -> None:
        if not self.running:
            raise RuntimeError("Searching service not running")
        if data_id not in self._search_data:
            raise KeyError("Data-id not published")
        del self._search_data[data_id]

    async def search(self, data_id: str) -> Any:
        candidate_neighbours = self.membership.get_neighbours()
        assert isinstance(candidate_neighbours, list)
        fanout = min(self._fanout, len(candidate_neighbours))
        walk_id = get_random_id()
        message = self._protocol.make_walk_message(
            data_id, walk_id, self.my_node, self._ttl
        )
        self._walk_events[walk_id] = asyncio.Event()
        for neighbour in random.sample(candidate_neighbours, fanout):
            await self._rpc.call_walk(neighbour, message)
        results_amount = 0
        while results_amount < fanout:
            await self._walk_events[walk_id].wait()
            if self._walk_results[walk_id]:
                return self._walk_results[walk_id]
        raise ValueError("Data not found")

    async def _handle_rpc(self, message: Message) -> Any:
        command = message.data[DATA_FIELD_COMMAND]
        if command == KWalkerCommand.WALK:
            ttl = message.data[DATA_FIELD_TTL]
            result = self._search_data.get(message.data[DATA_FIELD_DATA_ID], None)
            if result or ttl < 1:
                origin_node = parse_node(message.data[DATA_FIELD_ORIGIN_NODE])
                message = self._protocol.make_walk_result_message(
                    message.data[DATA_FIELD_WALK_ID], result
                )
                asyncio.create_task(self._rpc.call_walk_result(origin_node, message))
            else:
                message = self._protocol.make_walk_message(
                    message.data[DATA_FIELD_DATA_ID],
                    message.data[DATA_FIELD_WALK_ID],
                    message.data[DATA_FIELD_ORIGIN_NODE],
                    message.data[DATA_FIELD_TTL],
                )
                candidate_neighbours = self.membership.get_neighbours()
                assert isinstance(candidate_neighbours, list)
                neighbour = random.choice(candidate_neighbours)
                asyncio.create_task(self._rpc.call_walk(neighbour, message))
        elif command == KWalkerCommand.WALK_RESULT:
            walk_id = message.data[DATA_FIELD_WALK_ID]
            walk_result = message.data[DATA_FIELD_WALK_RESULT]
            self._walk_results[walk_id] = walk_result
            self._walk_events[walk_id].set()
        else:
            raise ValueError("Invalid KWalker protocol value")

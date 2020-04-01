import asyncio
import random
from enum import IntEnum, auto
from typing import Any, Dict, Optional

from unsserv.common.data_structures import Message, Node
from unsserv.common.rpc.rpc import RPC, RPCRegister
from unsserv.common.services_abc import SearchingService, MembershipService
from unsserv.common.utils import get_random_id, parse_node
from unsserv.extreme.searching import k_walker_config as config
from unsserv.extreme.searching.k_walker_config import (
    FIELD_COMMAND,
    FIELD_TTL,
    FIELD_ORIGIN_NODE,
    FIELD_WALK_ID,
    FIELD_WALK_RESULT,
    FIELD_DATA_ID,
)


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
            FIELD_DATA_ID: data_id,
            FIELD_COMMAND: KWalkerCommand.WALK,
            FIELD_WALK_ID: walk_id,
            FIELD_ORIGIN_NODE: origin_node,
            FIELD_TTL: ttl,
        }
        return Message(self.my_node, self.service_id, data)

    def make_walk_result_message(self, walk_id: str, walk_result: bytes) -> Message:
        data = {
            FIELD_COMMAND: KWalkerCommand.WALK_RESULT,
            FIELD_WALK_ID: walk_id,
            FIELD_WALK_RESULT: walk_result,
        }
        return Message(self.my_node, self.service_id, data)


class KWalker(SearchingService):
    _search_data: Dict[str, bytes]
    _walk_events: Dict[str, asyncio.Event]
    _walk_results: Dict[str, bytes]
    _rpc: RPC
    _protocol: Optional[KWalkerProtocol]
    _ttl: int
    _fanout: int

    def __init__(self, membership: MembershipService):
        self.membership = membership
        self.my_node = membership.my_node
        self._search_data = {}
        self._rpc = RPCRegister.get_rpc(self.my_node)
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

    async def search(self, data_id: str) -> Optional[bytes]:
        candidate_neighbours = self.membership.get_neighbours()
        assert isinstance(candidate_neighbours, list)
        fanout = min(self._fanout, len(candidate_neighbours))
        walk_id = get_random_id()
        message = self._protocol.make_walk_message(
            data_id, walk_id, self.my_node, self._ttl
        )
        self._walk_events[walk_id] = asyncio.Event()
        for neighbour in random.sample(candidate_neighbours, fanout):
            await self._rpc.call_without_response(neighbour, message)
        try:
            return await asyncio.wait_for(
                self._get_walk_result(fanout, walk_id), timeout=config.TIMEOUT
            )
        except asyncio.TimeoutError:
            return None

    async def _handle_rpc(self, message: Message) -> Any:
        command = message.data[FIELD_COMMAND]
        if command == KWalkerCommand.WALK:
            ttl = message.data[FIELD_TTL]
            result = self._search_data.get(message.data[FIELD_DATA_ID], None)
            if result or ttl < 1:
                origin_node = parse_node(message.data[FIELD_ORIGIN_NODE])
                message = self._protocol.make_walk_result_message(
                    message.data[FIELD_WALK_ID], result
                )
                asyncio.create_task(
                    self._rpc.call_without_response(origin_node, message)
                )
            else:
                message = self._protocol.make_walk_message(
                    message.data[FIELD_DATA_ID],
                    message.data[FIELD_WALK_ID],
                    message.data[FIELD_ORIGIN_NODE],
                    message.data[FIELD_TTL] - 1,
                )
                candidate_neighbours = self.membership.get_neighbours()
                assert isinstance(candidate_neighbours, list)
                neighbour = random.choice(candidate_neighbours)
                asyncio.create_task(self._rpc.call_without_response(neighbour, message))
        elif command == KWalkerCommand.WALK_RESULT:
            walk_id = message.data[FIELD_WALK_ID]
            walk_result = message.data[FIELD_WALK_RESULT]
            self._walk_results[walk_id] = walk_result
            self._walk_events[walk_id].set()
        else:
            raise ValueError("Invalid KWalker protocol value")

    async def _get_walk_result(self, fanout, walk_id):
        results_amount = 0
        result = None
        while results_amount < fanout:
            await self._walk_events[walk_id].wait()
            results_amount += 1
            result = self._walk_results[walk_id]
            if result:
                return result
        return result

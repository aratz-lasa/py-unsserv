import asyncio
import random
from collections import Counter
from enum import IntEnum
from typing import Any, Optional, Set, Counter as CounterType, Callable

from unsserv.common.structs import Node, Message
from unsserv.common.rpc.rpc import RPC, RPCRegister
from unsserv.common.services_abc import MembershipService, ClusteringService
from unsserv.common.typing import NeighboursCallback
from unsserv.common.utils import parse_node
from unsserv.stable.clustering.x_bot_config import (
    FIELD_COMMAND,
    ACTIVE_VIEW_SIZE,
    FIELD_OLD_NODE,
    FIELD_REPLACE_RESULT,
    FIELD_OPTIMIZATION_ORIGIN_NODE,
    FIELD_SWITCH_RESULT,
    FIELD_OPTIMIZATION_RESULT,
    FIELD_NEW_NODE,
    ACTIVE_VIEW_MAINTAIN_FREQUENCY,
    UNBIASED_NODES,
    PASSIVE_SCAN_LENGTH,
)
from unsserv.stable.membership.hyparview import (
    HyParViewCommand,
    HyParViewProtocol,
    HyParView,
)

RankingFunction = Callable[[Node], Any]

HyParViewLastCommand = list(HyParViewCommand)[-1]


class XBotCommand(IntEnum):
    OPTIMIZATION = HyParViewLastCommand + 1
    OPTIMIZATION_REPLY = HyParViewLastCommand + 2
    REPLACE = HyParViewLastCommand + 3
    REPLACE_REPLY = HyParViewLastCommand + 4
    SWITCH = HyParViewLastCommand + 5
    SWITCH_REPLY = HyParViewLastCommand + 6


class XBotProtocol(HyParViewProtocol):
    def make_optimization_message(self, old_node: Node):
        data = {
            FIELD_COMMAND: XBotCommand.OPTIMIZATION,
            FIELD_OLD_NODE: old_node,
        }
        return Message(self.my_node, self.service_id, data)

    def make_optimi_reply_message(self, result: bool, old_node: Node):
        data = {
            FIELD_COMMAND: XBotCommand.OPTIMIZATION_REPLY,
            FIELD_OLD_NODE: old_node,
            FIELD_OPTIMIZATION_RESULT: result,
        }
        return Message(self.my_node, self.service_id, data)

    def make_replace_message(self, old_node: Node, origin_node: Node):
        data = {
            FIELD_COMMAND: XBotCommand.REPLACE,
            FIELD_OLD_NODE: old_node,
            FIELD_OPTIMIZATION_ORIGIN_NODE: origin_node,
        }
        return Message(self.my_node, self.service_id, data)

    def make_replace_reply_message(
        self, result: bool, old_node: Node, origin_node: Node
    ):
        data = {
            FIELD_COMMAND: XBotCommand.REPLACE_REPLY,
            FIELD_OLD_NODE: old_node,
            FIELD_OPTIMIZATION_ORIGIN_NODE: origin_node,
            FIELD_REPLACE_RESULT: result,
        }
        return Message(self.my_node, self.service_id, data)

    def make_switch_message(self, origin_node: Node, new_node: Node):
        data = {
            FIELD_COMMAND: XBotCommand.SWITCH,
            FIELD_OPTIMIZATION_ORIGIN_NODE: origin_node,
            FIELD_NEW_NODE: new_node,
        }
        return Message(self.my_node, self.service_id, data)

    def make_switch_reply_message(
        self, result: bool, origin_node: Node, new_node: Node
    ):
        data = {
            FIELD_COMMAND: XBotCommand.SWITCH_REPLY,
            FIELD_OPTIMIZATION_ORIGIN_NODE: origin_node,
            FIELD_NEW_NODE: new_node,
            FIELD_SWITCH_RESULT: result,
        }
        return Message(self.my_node, self.service_id, data)


class XBot(ClusteringService, HyParView):
    _rpc: RPC
    _protocol: Optional[XBotProtocol]
    _active_view: Set[Node]
    _callback: NeighboursCallback
    _callback_raw_format: bool
    _local_view_maintenance_task: asyncio.Task
    _candidate_neighbours: CounterType[Node]

    def __init__(self, membership: MembershipService):
        self.membership = membership
        self.my_node = membership.my_node
        self._rpc = RPCRegister.get_rpc(self.my_node)
        self._callback = None
        self._callback_raw_format = False
        self._active_view = set()
        self._candidate_neighbours = Counter()
        self._ranking_function: RankingFunction

    async def join(self, service_id: Any, **configuration: Any):
        if self.running:
            raise RuntimeError("Already running Clustering")
        self.service_id = service_id
        self._ranking_function = configuration["ranking_function"]
        self._protocol = XBotProtocol(self.my_node, self.service_id)
        await self._rpc.register_service(service_id, self._handle_rpc)
        self._local_view_maintenance_task = asyncio.create_task(
            self._maintain_active_view()
        )
        self.running = True

    async def leave(self) -> None:
        if not self.running:
            return
        self._active_view = set()
        if self._local_view_maintenance_task:
            self._local_view_maintenance_task.cancel()
            try:
                await self._local_view_maintenance_task
            except asyncio.CancelledError:
                pass
        await self._rpc.unregister_service(self.service_id)
        self._protocol = None
        self.running = False

    async def _handle_rpc(self, message: Message) -> Any:
        try:
            return await super()._handle_rpc(message)
        except (ValueError, KeyError):
            pass
        command = message.data[FIELD_COMMAND]

        if command == XBotCommand.OPTIMIZATION:
            old_node = message.data[FIELD_OLD_NODE]
            if len(self._active_view) < ACTIVE_VIEW_SIZE:
                self._active_view.add(message.node)
                self._try_call_callback()
                reply_message = self._protocol.make_optimi_reply_message(True, old_node)
                asyncio.create_task(
                    self._rpc.call_send_message(message.node, reply_message)
                )
            else:
                replace_node = list(
                    sorted(self._active_view, key=self._ranking_function)
                )[-1]
                replace_message = self._protocol.make_replace_message(
                    old_node, message.node
                )
                asyncio.create_task(
                    self._rpc.call_send_message(replace_node, replace_message)
                )
        elif command == XBotCommand.OPTIMIZATION_REPLY:
            result = message.data[FIELD_OPTIMIZATION_RESULT]
            if not result:
                return None
            old_node = parse_node(message.data[FIELD_OLD_NODE])
            if old_node in self._active_view:
                disconnect_message = self._protocol.make_disconnect_message()
                asyncio.create_task(
                    self._rpc.call_send_message(old_node, disconnect_message)
                )
                self._active_view.remove(old_node)
            self._active_view.add(message.node)
            self._try_call_callback()
        elif command == XBotCommand.REPLACE:
            old_node = parse_node(message.data[FIELD_OLD_NODE])
            if self._get_the_best(old_node, message.node) == message.node:
                replace_message = self._protocol.make_replace_reply_message(
                    False, old_node, message.node
                )
                asyncio.create_task(
                    self._rpc.call_send_message(message.node, replace_message)
                )
            else:
                switch_message = self._protocol.make_switch_message(
                    message.data[FIELD_OPTIMIZATION_ORIGIN_NODE], message.node
                )
                asyncio.create_task(
                    self._rpc.call_send_message(old_node, switch_message)
                )
        elif command == XBotCommand.REPLACE_REPLY:
            is_replaced = message.data[FIELD_REPLACE_RESULT]
            optimization_node = parse_node(message.data[FIELD_OPTIMIZATION_ORIGIN_NODE])
            old_node = parse_node(message.data[FIELD_OLD_NODE])
            if is_replaced:
                self._active_view.remove(message.node)
                self._active_view.add(optimization_node)
                self._try_call_callback()
            reply_message = self._protocol.make_optimi_reply_message(
                is_replaced, old_node
            )
            asyncio.create_task(
                self._rpc.call_send_message(optimization_node, reply_message)
            )
        elif command == XBotCommand.SWITCH:
            origin_node = parse_node(message.data[FIELD_OPTIMIZATION_ORIGIN_NODE])
            new_node = parse_node(message.data[FIELD_NEW_NODE])
            pivot_node = message.node
            is_switched = False
            if origin_node in self._active_view:
                self._active_view.remove(origin_node)
                self._active_view.add(message.node)
                is_switched = True
                self._try_call_callback()
            reply_message = self._protocol.make_switch_reply_message(
                is_switched, origin_node, new_node
            )
            asyncio.create_task(self._rpc.call_send_message(pivot_node, reply_message))
        elif command == XBotCommand.SWITCH_REPLY:
            is_switched = message.data[FIELD_SWITCH_RESULT]
            new_node = parse_node(message.data[FIELD_NEW_NODE])
            origin_node = message.data[FIELD_OPTIMIZATION_ORIGIN_NODE]
            if is_switched:
                if new_node in self._active_view:
                    self._active_view.remove(new_node)
                self._active_view.add(message.node)
                self._try_call_callback()
            reply_message = self._protocol.make_replace_reply_message(
                is_switched, message.node, origin_node
            )
            asyncio.create_task(self._rpc.call_send_message(new_node, reply_message))
        else:
            raise ValueError("Invalid XBot protocol value")
        return None

    async def _maintain_active_view(self):
        await self._join_first_time()
        old_active_view = self._active_view.copy()
        while True:
            await asyncio.sleep(ACTIVE_VIEW_MAINTAIN_FREQUENCY)
            await self._update_active_view()
            if len(self._active_view) >= ACTIVE_VIEW_SIZE:
                await self._optimize_active_view()  # todo: create task instead?
            if old_active_view != self._active_view:
                self._try_call_callback()
                old_active_view = self._active_view.copy()

    def _get_passive_view_nodes(self):
        return self.membership.get_neighbours()

    async def _optimize_active_view(self):
        candidate_neighbours = self.membership.get_neighbours()
        if not candidate_neighbours:
            return
        candidate_neighbours = random.sample(
            candidate_neighbours, min(len(candidate_neighbours), PASSIVE_SCAN_LENGTH)
        )
        biasable_nodes = list(sorted(self._active_view, key=self._ranking_function))[
            UNBIASED_NODES:
        ]
        for old_node in biasable_nodes:
            if not candidate_neighbours:
                return None
            candidate_node = candidate_neighbours.pop()
            if self._get_the_best(candidate_node, old_node) == candidate_node:
                message = self._protocol.make_optimization_message(old_node)
                asyncio.create_task(
                    self._rpc.call_send_message(candidate_node, message)
                )

    def _get_the_best(self, node1: Node, node2: Node):
        return (
            node1
            if self._ranking_function(node1) < self._ranking_function(node2)
            else node2
        )

    def _try_call_callback(self):
        asyncio.create_task(self._local_view_callback(Counter(list(self._active_view))))

from enum import IntEnum, auto
from typing import Tuple, Sequence, List

from unsserv.common.rpc.protocol import AProtocol, ITranscoder, Command, Data, Handler
from unsserv.common.structs import Node
from unsserv.common.rpc.structs import Message
from unsserv.common.utils import parse_node
from unsserv.stable.searching.structs import Search, SearchResult, DataChange
from unsserv.stable.searching.typing import DataID

FIELD_COMMAND = "abloom-command"
FIELD_DATA_ID = "abloom-data-id"
FIELD_TTL = "abloom-ttl"
FIELD_ORIGIN_NODE = "abloom-origin-node"
FIELD_SEARCH_RESULT = "abloom-search-result"
FIELD_SEARCH_ID = "abloom-search-id"


class ABloomCommand(IntEnum):
    PUBLISH = auto()
    UNPUBLISH = auto()
    GET_FILTER = auto()
    SEARCH = auto()
    SEARCH_RESULT = auto()


class ABloomTranscoder(ITranscoder):
    def encode(self, command: Command, *data: Data) -> Message:
        if command == ABloomCommand.PUBLISH:
            data_change: DataChange = data[0]
            message_data = {
                FIELD_COMMAND: ABloomCommand.PUBLISH,
                FIELD_DATA_ID: data_change.data_id,
                FIELD_TTL: data_change.ttl,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == ABloomCommand.UNPUBLISH:
            data_change: DataChange = data[0]  # type: ignore
            message_data = {
                FIELD_COMMAND: ABloomCommand.UNPUBLISH,
                FIELD_DATA_ID: data_change.data_id,
                FIELD_TTL: data_change.ttl,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == ABloomCommand.GET_FILTER:
            message_data = {FIELD_COMMAND: ABloomCommand.GET_FILTER}
            return Message(self.my_node, self.service_id, message_data)
        elif command == ABloomCommand.SEARCH:
            search: Search = data[0]
            message_data = {
                FIELD_COMMAND: ABloomCommand.SEARCH,
                FIELD_SEARCH_ID: search.id,
                FIELD_ORIGIN_NODE: search.origin_node,
                FIELD_TTL: search.ttl,
                FIELD_DATA_ID: search.data_id,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == ABloomCommand.SEARCH_RESULT:
            search_result: SearchResult = data[0]
            message_data = {
                FIELD_COMMAND: ABloomCommand.SEARCH_RESULT,
                FIELD_SEARCH_ID: search_result.search_id,
                FIELD_SEARCH_RESULT: search_result.result,
            }
            return Message(self.my_node, self.service_id, message_data)
        raise ValueError("Invalid Command")

    def decode(self, message: Message) -> Tuple[Command, Sequence[Data]]:
        command = message.data[FIELD_COMMAND]
        if command == ABloomCommand.PUBLISH:
            data_change = DataChange(
                data_id=message.data[FIELD_DATA_ID], ttl=message.data[FIELD_TTL]
            )
            return ABloomCommand.PUBLISH, [data_change]
        elif command == ABloomCommand.UNPUBLISH:
            data_change = DataChange(
                data_id=message.data[FIELD_DATA_ID], ttl=message.data[FIELD_TTL]
            )
            return ABloomCommand.UNPUBLISH, [data_change]
        elif command == ABloomCommand.GET_FILTER:
            return ABloomCommand.GET_FILTER, []
        elif command == ABloomCommand.SEARCH:
            search = Search(
                id=message.data[FIELD_SEARCH_ID],
                origin_node=parse_node(message.data[FIELD_ORIGIN_NODE]),
                ttl=message.data[FIELD_TTL],
                data_id=message.data[FIELD_DATA_ID],
            )
            return ABloomCommand.SEARCH, [search]
        elif command == ABloomCommand.SEARCH_RESULT:
            search_result = SearchResult(
                search_id=message.data[FIELD_SEARCH_ID],
                result=message.data[FIELD_SEARCH_RESULT],
            )
            return ABloomCommand.SEARCH_RESULT, [search_result]
        raise ValueError("Invalid Command")


class ABloomProtocol(AProtocol):
    def _get_new_transcoder(self):
        return ABloomTranscoder(self.my_node, self.service_id)

    async def publish(self, destination: Node, data_change: DataChange):
        message = self._transcoder.encode(ABloomCommand.PUBLISH, data_change)
        return await self._rpc.call_send_message(destination, message)

    async def unpublish(self, destination: Node, data_change: DataChange):
        message = self._transcoder.encode(ABloomCommand.UNPUBLISH, data_change)
        return await self._rpc.call_send_message(destination, message)

    async def get_filter(self, destination: Node):
        message = self._transcoder.encode(ABloomCommand.GET_FILTER)
        raw_filter = await self._rpc.call_send_message(destination, message)
        return parse_filter(raw_filter)

    async def search(self, destination: Node, search: Search):
        message = self._transcoder.encode(ABloomCommand.SEARCH, search)
        return await self._rpc.call_send_message(destination, message)

    async def search_result(self, destination: Node, search_result: SearchResult):
        message = self._transcoder.encode(ABloomCommand.SEARCH_RESULT, search_result)
        return await self._rpc.call_send_message(destination, message)

    def set_handler_publish(self, handler: Handler):
        self._handlers[ABloomCommand.PUBLISH] = handler

    def set_handler_unpublish(self, handler: Handler):
        self._handlers[ABloomCommand.UNPUBLISH] = handler

    def set_handler_get_filter(self, handler: Handler):
        self._handlers[ABloomCommand.GET_FILTER] = handler

    def set_handler_search(self, handler: Handler):
        self._handlers[ABloomCommand.SEARCH] = handler

    def set_handler_search_result(self, handler: Handler):
        self._handlers[ABloomCommand.SEARCH_RESULT] = handler


def parse_filter(raw_filter: List[List[DataID]]):
    filter = []
    for depth in raw_filter:
        filter.append(set(depth))
    return filter

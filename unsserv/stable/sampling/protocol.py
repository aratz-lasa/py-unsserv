from enum import IntEnum, auto
from typing import Tuple, Sequence

from unsserv.common.utils import parse_node
from unsserv.common.structs import Node
from unsserv.common.rpc.structs import Message
from unsserv.common.rpc.protocol import AProtocol, ITranscoder, Command, Data, Handler
from unsserv.stable.sampling.structs import Sample, SampleResult

FIELD_COMMAND = "rwd-command"
FIELD_TTL = "rwd-ttl"
FIELD_ORIGIN_NODE = "rwd-origin-node"
FIELD_SAMPLE_RESULT = "rwd-sample-result"
FIELD_SAMPLE_ID = "rwd-sample-id"


class RWDCommand(IntEnum):
    INCREASE = auto()
    SAMPLE = auto()
    SAMPLE_RESULT = auto()


class RWDTranscoder(ITranscoder):
    def encode(self, command: Command, *data: Data) -> Message:
        if command == RWDCommand.SAMPLE:
            sample: Sample = data[0]
            message_data = {
                FIELD_COMMAND: RWDCommand.SAMPLE,
                FIELD_SAMPLE_ID: sample.id,
                FIELD_ORIGIN_NODE: sample.origin_node,
                FIELD_TTL: sample.ttl,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == RWDCommand.SAMPLE_RESULT:
            sample_result: SampleResult = data[0]
            message_data = {
                FIELD_COMMAND: RWDCommand.SAMPLE_RESULT,
                FIELD_SAMPLE_ID: sample_result.sample_id,
                FIELD_SAMPLE_RESULT: sample_result.result,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == RWDCommand.INCREASE:
            message_data = {FIELD_COMMAND: RWDCommand.INCREASE}
            return Message(self.my_node, self.service_id, message_data)
        raise ValueError("Invalid Command")

    def decode(self, message: Message) -> Tuple[Command, Sequence[Data]]:
        command = message.data[FIELD_COMMAND]
        if command == RWDCommand.SAMPLE:
            sample = Sample(
                id=message.data[FIELD_SAMPLE_ID],
                origin_node=parse_node(message.data[FIELD_ORIGIN_NODE]),
                ttl=message.data[FIELD_TTL],
            )
            return RWDCommand.SAMPLE, [sample]
        elif command == RWDCommand.SAMPLE_RESULT:
            sample_result = SampleResult(
                sample_id=message.data[FIELD_SAMPLE_ID],
                result=parse_node(message.data[FIELD_SAMPLE_RESULT]),
            )
            return RWDCommand.SAMPLE_RESULT, [sample_result]
        elif command == RWDCommand.INCREASE:
            return RWDCommand.INCREASE, []
        raise ValueError("Invalid Command")


class RWDProtocol(AProtocol):
    def _get_new_transcoder(self):
        return RWDTranscoder(self.my_node, self.service_id)

    async def sample(self, destination: Node, sample: Sample):
        message = self._transcoder.encode(RWDCommand.SAMPLE, sample)
        return await self._rpc.call_send_message(destination, message)

    async def sample_result(self, destination: Node, sample_result: SampleResult):
        message = self._transcoder.encode(RWDCommand.SAMPLE_RESULT, sample_result)
        return await self._rpc.call_send_message(destination, message)

    async def increase(self, destination: Node) -> bool:
        message = self._transcoder.encode(RWDCommand.INCREASE)
        return await self._rpc.call_send_message(destination, message)

    def set_handler_sample(self, handler: Handler):
        self._handlers[RWDCommand.SAMPLE] = handler

    def set_handler_sample_result(self, handler: Handler):
        self._handlers[RWDCommand.SAMPLE_RESULT] = handler

    def set_handler_increase(self, handler: Handler):
        self._handlers[RWDCommand.INCREASE] = handler

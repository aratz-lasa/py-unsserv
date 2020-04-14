from enum import IntEnum, auto
from typing import Tuple, Sequence

from unsserv.common.utils import parse_node
from unsserv.common.structs import Node
from unsserv.common.rpc.structs import Message
from unsserv.common.rpc.protocol import AProtocol, ITranscoder, Command, Data, Handler
from unsserv.extreme.sampling.structs import Sample, SampleResult

FIELD_COMMAND = "mrwb-command"
FIELD_TTL = "mrwb-ttl"
FIELD_ORIGIN_NODE = "mrwb-origin-node"
FIELD_SAMPLE_RESULT = "mrwb-sample-result"
FIELD_SAMPLE_ID = "mrwb-sample-id"


class MRWBCommand(IntEnum):
    GET_DEGREE = auto()
    SAMPLE = auto()
    SAMPLE_RESULT = auto()


class MRWBTranscoder(ITranscoder):
    def encode(self, command: Command, *data: Data) -> Message:
        if command == MRWBCommand.SAMPLE:
            sample: Sample = data[0]
            message_data = {
                FIELD_COMMAND: MRWBCommand.SAMPLE,
                FIELD_SAMPLE_ID: sample.id,
                FIELD_ORIGIN_NODE: sample.origin_node,
                FIELD_TTL: sample.ttl,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == MRWBCommand.SAMPLE_RESULT:
            sample_result: SampleResult = data[0]
            message_data = {
                FIELD_COMMAND: MRWBCommand.SAMPLE_RESULT,
                FIELD_SAMPLE_ID: sample_result.sample_id,
                FIELD_SAMPLE_RESULT: sample_result.result,
            }
            return Message(self.my_node, self.service_id, message_data)
        elif command == MRWBCommand.GET_DEGREE:
            message_data = {FIELD_COMMAND: MRWBCommand.GET_DEGREE}
            return Message(self.my_node, self.service_id, message_data)
        raise ValueError("Invalid Command")

    def decode(self, message: Message) -> Tuple[Command, Sequence[Data]]:
        command = message.data[FIELD_COMMAND]
        if command == MRWBCommand.SAMPLE:
            sample = Sample(
                id=message.data[FIELD_SAMPLE_ID],
                origin_node=parse_node(message.data[FIELD_ORIGIN_NODE]),
                ttl=message.data[FIELD_TTL],
            )
            return MRWBCommand.SAMPLE, [sample]
        elif command == MRWBCommand.SAMPLE_RESULT:
            sample_result = SampleResult(
                sample_id=message.data[FIELD_SAMPLE_ID],
                result=parse_node(message.data[FIELD_SAMPLE_RESULT]),
            )
            return MRWBCommand.SAMPLE_RESULT, [sample_result]
        elif command == MRWBCommand.GET_DEGREE:
            return MRWBCommand.GET_DEGREE, []
        raise ValueError("Invalid Command")


class MRWBProtocol(AProtocol):
    def _get_new_transcoder(self):
        return MRWBTranscoder(self.my_node, self.service_id)

    async def sample(self, destination: Node, sample: Sample):
        message = self._transcoder.encode(MRWBCommand.SAMPLE, sample)
        return await self._rpc.call_send_message(destination, message)

    async def sample_result(self, destination: Node, sample_result: SampleResult):
        message = self._transcoder.encode(MRWBCommand.SAMPLE_RESULT, sample_result)
        return await self._rpc.call_send_message(destination, message)

    async def get_degree(self, destination: Node) -> int:
        message = self._transcoder.encode(MRWBCommand.GET_DEGREE)
        return await self._rpc.call_send_message(destination, message)

    def set_handler_sample(self, handler: Handler):
        self._handlers[MRWBCommand.SAMPLE] = handler

    def set_handler_sample_result(self, handler: Handler):
        self._handlers[MRWBCommand.SAMPLE_RESULT] = handler

    def set_handler_get_degree(self, handler: Handler):
        self._handlers[MRWBCommand.GET_DEGREE] = handler

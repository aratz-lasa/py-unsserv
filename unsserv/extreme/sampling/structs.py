from dataclasses import dataclass
from unsserv.common.data_structures import Node


@dataclass
class Sample:
    id: str
    origin_node: Node
    ttl: int


@dataclass
class SampleResult:
    sample_id: str
    result: Node

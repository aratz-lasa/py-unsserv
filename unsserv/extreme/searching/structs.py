from dataclasses import dataclass
from unsserv.common.data_structures import Node


@dataclass
class Walk:
    id: str
    data_id: str
    origin_node: Node
    ttl: int


@dataclass
class WalkResult:
    walk_id: str
    result: bytes

from dataclasses import dataclass
from unsserv.common.structs import Node


@dataclass
class ForwardJoin:
    origin_node: Node
    ttl: int

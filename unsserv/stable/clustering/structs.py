from dataclasses import dataclass

from unsserv.common.structs import Node


@dataclass
class Replace:
    origin_node: Node
    old_node: Node

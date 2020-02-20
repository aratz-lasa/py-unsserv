from typing import List

from unsserv.common.data_structures import Node


def parse_node(raw_node: List) -> Node:
    address_info = tuple(raw_node[0])
    extra = tuple(raw_node[1])
    return Node(address_info, extra)

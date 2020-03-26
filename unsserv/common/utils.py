import random
import string
from typing import List

from unsserv.common.data_structures import Message
from unsserv.common.data_structures import Node


def parse_node(raw_node: List) -> Node:
    address_info = tuple(raw_node[0])
    extra = tuple(raw_node[1])
    return Node(address_info, extra)


def parse_message(raw_message: List) -> Message:
    node = parse_node(raw_message[0])
    return Message(node, raw_message[1], raw_message[2])


def get_random_id(size: int = 10) -> str:
    id_characters = string.ascii_letters + string.digits + string.punctuation
    return "".join(random.choice(id_characters) for _ in range(size))

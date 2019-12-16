from dataclasses import dataclass


@dataclass(unsafe_hash=True, frozen=True)
class Node:
    host: str
    port: int


@dataclass(unsafe_hash=True, frozen=True)
class Message:
    """
    Message class representing messages sent from one node to other.
    It contains:
        (host, port),
        {
            "data1": any
            "data2": any
        }
    Data is represented as dict, because multiple services
    may piggyback their data into the message.
    """

    node: Node
    data: dict

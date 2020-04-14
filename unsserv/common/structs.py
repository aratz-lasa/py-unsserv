from collections import namedtuple
from enum import Enum, auto

Node = namedtuple("Node", ["address_info", "extra"], defaults=[None, tuple()])


class Property(Enum):
    EXTREME = auto()
    STABLE = auto()
    SYMMETRIC = auto()
    NON_SYMMETRIC = auto()
    HAS_GOSSIP = auto()
    ONE_TO_MANY = auto()
    MANY_TO_MANY = auto()

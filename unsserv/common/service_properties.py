from enum import Enum, auto


class Property(Enum):
    EXTREME = auto()
    STABLE = auto()
    SYMMETRIC = auto()
    NON_SYMMETRIC = auto()
    HAS_GOSSIP = auto()
    ONE_TO_MANY = auto()
    MANY_TO_MANY = auto()

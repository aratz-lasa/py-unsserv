from enum import Enum, auto
from typing import Dict, Any

from unsserv.common.utils import IConfig


class AggregateType(Enum):
    MEAN = auto()
    MAX = auto()
    MIN = auto()


class AntiConfig(IConfig):
    AGGREGATE_TYPE = None

    def load_from_dict(self, config_dict: Dict[str, Any]):
        self.AGGREGATE_TYPE = config_dict["aggregate_type"]

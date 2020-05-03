from typing import Dict, Any

from unsserv.common.utils import IConfig


class MonConfig(IConfig):
    TIMEOUT = 5  # seconds
    FANOUT = 10
    TREE_LIFE = 10

    def load_from_dict(self, config_dict: Dict[str, Any]):
        self.TIMEOUT = config_dict.get("timeout", MonConfig.TIMEOUT)
        self.FANOUT = config_dict.get("fanout", MonConfig.FANOUT)
        self.TREE_LIFE = config_dict.get("tree_life", MonConfig.TREE_LIFE)

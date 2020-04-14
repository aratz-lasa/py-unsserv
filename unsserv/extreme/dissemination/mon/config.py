from typing import Dict, Any

from unsserv.common.utils import IConfig


class MonConfig(IConfig):
    TIMEOUT = 30  # seconds
    FANOUT = 10

    def load_from_dict(self, config_dict: Dict[str, Any]):
        self.TIMEOUT = config_dict.get("timeout", MonConfig.TIMEOUT)
        self.FANOUT = config_dict.get("fanout", MonConfig.FANOUT)

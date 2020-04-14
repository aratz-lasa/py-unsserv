from typing import Dict, Any

from unsserv.common.utils import IConfig


class KWalkerConfig(IConfig):
    TTL = 4
    FANOUT = 4
    TIMEOUT = 4

    def load_from_dict(self, config_dict: Dict[str, Any]):
        self.TTL = config_dict.get("ttl", KWalkerConfig.TTL)
        self.FANOUT = config_dict.get("fanout", KWalkerConfig.FANOUT)
        self.TIMEOUT = config_dict.get("timeout", KWalkerConfig.TIMEOUT)

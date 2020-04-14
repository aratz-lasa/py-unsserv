from typing import Dict, Any

from unsserv.common.utils import IConfig


class MRWBConfig(IConfig):
    MAINTENANCE_SLEEP = 0.5
    TIMEOUT = 2
    TTL = 10

    def load_from_dict(self, config_dict: Dict[str, Any]):
        self.MAINTENANCE_SLEEP = config_dict.get(
            "MAINTENANCE_SLEEP", MRWBConfig.MAINTENANCE_SLEEP
        )
        self.TIMEOUT = config_dict.get("timeout", MRWBConfig.TIMEOUT)
        self.TTL = config_dict.get("ttl", MRWBConfig.TTL)

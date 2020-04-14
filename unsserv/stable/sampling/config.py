from typing import Dict, Any

from unsserv.common.utils import IConfig


class RWDConfig(IConfig):
    TIMEOUT = 2
    TTL = 10
    MAINTENANCE_SLEEP = 0.5
    QUANTUM = 0.1
    MORE_THAN_MAXIMUM = 30

    def load_from_dict(self, config_dict: Dict[str, Any]):
        self.TIMEOUT = config_dict.get("timeout", RWDConfig.TIMEOUT)
        self.TTL = config_dict.get("ttl", RWDConfig.TTL)
        self.MAINTENANCE_SLEEP = config_dict.get(
            "MAINTENANCE_SLEEP", RWDConfig.MAINTENANCE_SLEEP
        )
        self.QUANTUM = config_dict.get("quantum", RWDConfig.QUANTUM)
        self.MORE_THAN_MAXIMUM = config_dict.get(
            "more_than_maximum", RWDConfig.MORE_THAN_MAXIMUM
        )

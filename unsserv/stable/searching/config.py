from typing import Dict, Any

from unsserv.common.utils import IConfig


class ABloomConfig(IConfig):
    DEPTH = 2
    TIMEOUT = 10
    MAINTENANCE_SLEEP = 1

    def load_from_dict(self, config_dict: Dict[str, Any]):
        self.DEPTH = config_dict.get("depth", ABloomConfig.DEPTH)
        self.TIMEOUT = config_dict.get("timeout", ABloomConfig.TIMEOUT)
        self.MAINTENANCE_SLEEP = config_dict.get(
            "maintenance_sleep", ABloomConfig.MAINTENANCE_SLEEP
        )

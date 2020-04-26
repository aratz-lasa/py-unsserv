from typing import Dict, Any

from unsserv.common.utils import IConfig


class BrisaConfig(IConfig):
    TIMEOUT = 5  # seconds
    FANOUT = 3
    MAINTENANCE_SLEEP = 0.5

    def load_from_dict(self, config_dict: Dict[str, Any]):
        self.TIMEOUT = config_dict.get("timeout", BrisaConfig.TIMEOUT)
        self.FANOUT = config_dict.get("fanout", BrisaConfig.FANOUT)
        self.MAINTENANCE_SLEEP = config_dict.get(
            "maintenance_sleep", BrisaConfig.MAINTENANCE_SLEEP
        )

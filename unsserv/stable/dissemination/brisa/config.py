from typing import Dict, Any

from unsserv.common.utils import IConfig


class BrisaConfig(IConfig):
    TIMEOUT = 3  # seconds
    MAINTENANCE_SLEEP = 1
    BUFFER_LIMIT = 100

    def load_from_dict(self, config_dict: Dict[str, Any]):
        self.TIMEOUT = config_dict.get("timeout", BrisaConfig.TIMEOUT)
        self.MAINTENANCE_SLEEP = config_dict.get(
            "maintenance_sleep", BrisaConfig.MAINTENANCE_SLEEP
        )
        self.BUFFER_LIMIT = config_dict.get("buffer_limit", BrisaConfig.BUFFER_LIMIT)

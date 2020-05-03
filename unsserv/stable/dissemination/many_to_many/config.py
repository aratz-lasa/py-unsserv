from typing import Dict, Any

from unsserv.common.utils import IConfig


class PlumtreeConfig(IConfig):
    RETRIEVE_TIMEOUT = 3  # seconds
    MAINTENANCE_SLEEP = 1
    BUFFER_LIMIT = 100

    def load_from_dict(self, config_dict: Dict[str, Any]):
        self.RETRIEVE_TIMEOUT = config_dict.get(
            "retrieve_timeout", PlumtreeConfig.RETRIEVE_TIMEOUT
        )
        self.MAINTENANCE_SLEEP = config_dict.get(
            "maintenance_sleep", PlumtreeConfig.MAINTENANCE_SLEEP
        )
        self.BUFFER_LIMIT = config_dict.get("buffer_limit", PlumtreeConfig.BUFFER_LIMIT)

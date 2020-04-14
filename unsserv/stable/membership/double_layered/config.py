from typing import Dict, Any

from unsserv.common.utils import IConfig


class DoubleLayeredConfig(IConfig):
    TTL = 10
    ACTIVE_VIEW_SIZE = 6
    MAINTENANCE_SLEEP = 1

    def load_from_dict(self, config_dict: Dict[str, Any]):
        self.TTL = config_dict.get("ttl", DoubleLayeredConfig.TTL)
        self.ACTIVE_VIEW_SIZE = config_dict.get(
            "active_view_size", DoubleLayeredConfig.ACTIVE_VIEW_SIZE
        )
        self.MAINTENANCE_SLEEP = config_dict.get(
            "MAINTENANCE_SLEEP", DoubleLayeredConfig.MAINTENANCE_SLEEP
        )

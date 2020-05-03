from typing import Dict, Any

from unsserv.common.utils import IConfig


class BrisaConfig(IConfig):
    FANOUT = 3
    MAINTENANCE_SLEEP = 0.5

    def load_from_dict(self, config_dict: Dict[str, Any]):
        self.FANOUT = config_dict.get("fanout", BrisaConfig.FANOUT)
        self.MAINTENANCE_SLEEP = config_dict.get(
            "maintenance_sleep", BrisaConfig.MAINTENANCE_SLEEP
        )

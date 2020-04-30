from typing import Dict, Any

from unsserv.common.utils import IConfig


class LpbcastConfig(IConfig):
    FANOUT = 10
    THRESHOLD = 10

    def load_from_dict(self, config_dict: Dict[str, Any]):
        self.FANOUT = config_dict.get("fanout", LpbcastConfig.FANOUT)
        self.THRESHOLD = config_dict.get("threhold", LpbcastConfig.THRESHOLD)

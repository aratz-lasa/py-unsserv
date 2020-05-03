from typing import Dict, Any

from unsserv.common.utils import IConfig


class LpbcastConfig(IConfig):
    FANOUT = 10
    BUFFER_LIMIT = 100

    def load_from_dict(self, config_dict: Dict[str, Any]):
        self.FANOUT = config_dict.get("fanout", LpbcastConfig.FANOUT)
        self.BUFFER_LIMIT = config_dict.get("buffer_limit", LpbcastConfig.BUFFER_LIMIT)

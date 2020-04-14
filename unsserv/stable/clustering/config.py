from math import ceil
from typing import Dict, Any

from unsserv.stable.membership.double_layered.config import DoubleLayeredConfig


class XBotConfig(DoubleLayeredConfig):
    UNBIASED_NODES = ceil(DoubleLayeredConfig.ACTIVE_VIEW_SIZE * 0.2)

    def load_from_dict(self, config_dict: Dict[str, Any]):
        super().load_from_dict(config_dict)
        self.UNBIASED_NODES = config_dict.get(
            "unbiased_nodes", XBotConfig.UNBIASED_NODES
        )

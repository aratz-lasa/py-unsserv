from math import ceil


ACTIVE_VIEW_SIZE = 10
UNBIASED_PERCENT = 0.2
UNBIASED_NODES = ceil(ACTIVE_VIEW_SIZE * UNBIASED_PERCENT)
PASSIVE_SCAN_LENGTH = 4
TTL = 10
ID_LENGTH = 10
ACTIVE_VIEW_MAINTAIN_FREQUENCY = 1

FIELD_COMMAND = "xbot-command"
FIELD_TTL = "xbot-ttl"
FIELD_PRIORITY = "xbot-priority"
FIELD_ORIGIN_NODE = "xbot-origin-node"
# Optimization config
FIELD_OLD_NODE = "xbot-old-node"
FIELD_NEW_NODE = "xbot-new-node"
FIELD_OPTIMIZATION_ORIGIN_NODE = "xbot-optimization-node"
FIELD_PIVOT_NODE = "xbot-pivot-node"
FIELD_OPTIMIZATION_RESULT = "xbot-optimizatio-result"
FIELD_REPLACE_RESULT = "xbot-replace-result"
FIELD_SWITCH_RESULT = "xbot-switch-result"

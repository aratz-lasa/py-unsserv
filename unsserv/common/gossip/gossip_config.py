LOCAL_VIEW_SIZE = 10  # todo: select a proper size
GOSSIPING_FREQUENCY = 0.2  # todo: select a proper size
DATA_FIELD_VIEW = "view"

RPC_TIMEOUT = 0.15

assert RPC_TIMEOUT < GOSSIPING_FREQUENCY

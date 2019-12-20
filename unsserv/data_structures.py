from collections import namedtuple

Node = namedtuple("Node", ["address_info", "extra"], defaults=[None, tuple()])


# 'address_info' refers to host and port,
# and 'extra' is any other piggybacked information
Message = namedtuple("Message", ["node", "service_id", "data"])

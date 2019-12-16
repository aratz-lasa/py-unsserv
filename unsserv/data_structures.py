from collections import namedtuple

Node = namedtuple("Node", ["host", "port"])


Message = namedtuple("Message", ["node", "data"])

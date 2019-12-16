from unsserv.data_structures import Node


def get_random_nodes(amount, first_port=7772, host="127.0.0.1"):
    return list(map(lambda p: Node(host, p), range(first_port, first_port + amount)))

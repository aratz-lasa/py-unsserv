Advanced usage
===============

Choose UnsServ version: Extreme or Stable
------------------------------------------
Every UnsServ service including Membership offer two different versions: Extreme and Stable.

In order to choose what version is wanted, the :code:`is_extreme` parameter is used. By default
it is set to :code:`True` So that, if :code:`True` the Extreme version is instantiated.

In case the Stable version is wanted:

.. code-block:: python

    from unsserv import join_network
    membership = await join_network(("127.0.0.1", 77771), "network.id", is_extreme=False)


"Node" data structure
----------------------
The basic data structure used in UnsServ is :code:`Node`, which is a `namedtuple`_. If the
services are instantiated manually, :code:`Node` data structures must be used. This is the
data structure representing the P2P node. It contains two fields:

* :code:`address_info`: a tuple containing the IP and the port.
* :code:`extra`: a tuple containing any other property or identifier. This
  field is intended for defining Use-Case specific attributes.

In order to make use of :code:`Node`, first it is imported:

.. code-block:: python

    from unsserv.common.structs import Node
And then a node is instantiated:

.. code-block:: python

    node = Node(address_info("127.0.0.1", 7771))
UnsServ also publishes a high-level API for instantiating nodes:

.. code-block:: python

    from unsserv import get_node

    node = get_node("127.0.0.1", 7771)


.. _namedtuple: https://docs.python.org/3/library/collections.html#collections.namedtuple

Manual instantiation
---------------------
The importable function such as :code:`join_network`, :code:`get_clustering_service`,
:code:`get_X_service` (where X is any service) are high-level API calls. They take
care of parsing nodes, and selecting the specific Protocol depending on the service and
the version. However it is possible to import the protocol an instantiate it manually.

The protocols folder structure is the following:

| unsserv
| ├── extreme
| │   ├── membership  ├── **Newscast**
| │   ├── clustering  ├── **TMan**
| │   ├── aggregation ├── **AntiEntropy**
| │   ├── sampling    ├── **MRWB**
| │   ├── dissemination
| │   │   ├── one_to_many  ├── **LPBCast**
| │   │   └── many_to_many ├── **Mon**
| │   └── searching ├── **KWalker**
| └── stable
|     ├── membership  ├── **HyParView**
|     ├── clustering  ├── **XBot**
|     ├── aggregation ├── **AntiEntropy**
|     ├── sampling    ├── **RWD**
|     ├── dissemination
|     │   ├── one_to_many   ├── **Brisa**
|     │   └── many_to_many  ├── **Plumtree**
|     └── searching ├── **ABloom**
|

For example this is how the Extreme Memership is instantiated:

.. code-block:: python

    from unsserv.common.structs import Node
    from unsserv.extreme.membership import Newscast

    node = Node(address_info=("127.0.0.1", 7771))
    membership = Newscast(node)
    await membership.join("network.id")


Handlers
---------
Some of the services make use of handler in order to call them whenever a value is updated. In case
of Membership or Clustering the handlers are called whenever the neighbours change.

Aggregation and Dissemination services also use the handlers. In case of Aggregation for calling it
when the aggregate value is updated. And the Dissemination service calls the handler for notifying the
user about a broadcast.

All of this four services expose two methods for adding and removing handlers, in the form of
:code:`add_X_handler(handler)` and :code:`remove_X_handler(handler)`, where :code:`X` is dependant
on each service. The handler must be a function that receives a single parameter, and it can be either
asynchronous or synchronous.

For example in case of Membership or Clustering service:

.. code-block:: python

    async def handler(neighbours):
        ...
    membership.add_neighbours_handler(handler)
Or in case of Aggregation service:

.. code-block:: python

    def handler(aggregate_value):
        ...
    aggregation.add_aggregate_handler(handler)

Configuration parameters
-------------------------
Each service can modify its working behavior by means of the configuration parameters.

The configuration parameters are passed as arguments when initializing/starting the service.

When using high-level API calls:

.. code-block:: python

    from unsserv import join_network

    membership = await join_network(("127.0.0.1", 77771), "network.id", local_view_size=5)
:code:`local_view_size` is a configuration parameter.
Or directly initializing it:

.. code-block:: python

    from unsserv.extreme.membership import Newscast
    ...
    membership = Newscast(node)
    await membership.join("network.id", local_view_size=5)

Membership Extreme (Newscast)
++++++++++++++++++++++++++++++

* :code:`local_view_size`: the maximum amount of neighbours to connect with at any given time.
  By default :code:`10`.
* :code:`gossiping_frequency`: the frequency in seconds at which neighbours are updated. By default
  :code:`0.2`
* :code:`peer_selection`: the policy for selecting what neighbour to connect with for exchanging the
  the neighbours. The available policies are:

  * :code:`rand`: uniform randomly select an available node from the view.
  * :code:`head`: select the first node from the view, that is, the node that has spent the least
    time in the local view (as neighbor).
  * :code:`tail`: select the last node from the view, that is, the node that has been in the local
    view the longest (as a neighbor).
  By default is :code:`rand`.
* :code:`view_selection`: the policy for selecting what peer to keep when updating the neighbours
  and they exceed the :code:`local_view_size`. The available policies are the same as in
  :code:`peer_selection`. By default is :code:`head`.
* :code:`view_propagation`: the policy for selecting how the peers exchange are done. The available policies
  are:

  * :code:`push`: the node sends its view to the selected peer.
  * :code:`pull`: the node requests the view from the selected peer.
  * :code:`pushpull`: the node and selected peer exchange their respective views.
  By default is :code:`pushpull`.
* :code:`rpc_timeout`: the timeout in seconds waited for a response from the neighbour.
  By default is :code:`1`.

Membership Stable (HyParView)
++++++++++++++++++++++++++++++
It exposes the same configuration as `Membership Extreme (Newscast)`_.

And additionally:

* :code:`ttl`: when joining the neighbours randomness degree expected.
* :code:`maintenance_sleep`: how frequent neighbours are checked whether they are still alive.
* :code:`active_view_size`: the maximum amount of neighbours active nieghbours. This service
  uses two gorup of neighbours: passive and active. Passive are used whenever an active neighbour fails.

Clustering Extreme (TMan)
++++++++++++++++++++++++++
It exposes the same configuration as `Membership Extreme (Newscast)`_.

Clustering Stable (XBot)
+++++++++++++++++++++++++
It exposes the same configuration as `Membership Stable (HyParView)`_.

And additionally:

* :code:`unbiased_nodes`: the amount of neighbours from the active view that are not biased
  towards a cluster. By default is: the 20% of the active neighbours.

Aggregation Extreme & Stable (AntiEntropy)
+++++++++++++++++++++++++++++++++++++++++++
It exposes the same configuration as `Membership Extreme (Newscast)`_.

Sampling Extreme (MRWB)
++++++++++++++++++++++++
* :code:`ttl`: when sampling a node the randomness degree expected. By default is :code:`10`.
* :code:`timeout`: the timeout in seconds waited for carrying out the sampling.
  By default is :code:`2`.
* :code:`maintenance_sleep`: how frequent neighbours are maintained (keepalives
  or any other maintenace echanism). By deault is: :code:`0.5`.

Sampling Stable (RWD)
++++++++++++++++++++++
It exposes the same configuration as `Sampling Extreme (MRWB)`_.

And additionally:

* :code:`quantum`: a global parameter needed by the protocol. It must be equal or lower than
  1/:code:`local_view_size`. By default is: :code:`0.1`.
* :code:`more_than_maximum`: a value that must be higher than the local view size.
  This value is needed by the sampling protocol. By default is: :code:`30`.

Searching Extreme (KWalker)
++++++++++++++++++++++++++++
* :code:`ttl`: when searching the precision degree expected (how likely it is to find).
  By default is :code:`4`.
* :code:`timeout`: the timeout in seconds waited for carrying out the search.
  By default is :code:`4`.
* :code:`fanout`: for each :code:`ttl`, how many neighbours are queried. In total in every search:
  :code:`ttl*fanout` nodes are queried. By default is :code:`4`.


Searching Stable (ABloom)
++++++++++++++++++++++++++
* :code:`depth`: when searching the precision degree expected (how likely it is to find).
  By default is :code:`2`.
* :code:`timeout`: the timeout in seconds waited for carrying out the search.
  By default is :code:`10`.
* :code:`maintenance_sleep`: how frequent neighbours are maintained (keepalives
  or any other maintenace echanism). By deault is: :code:`1`.

Dissemination Extreme Many-Many (LPBCast)
++++++++++++++++++++++++++++++++++++++++++
* :code:`fanout`: the amount of neighbours to whom the broadcast is forwarded in each hop.
  By default is :code:`10`.
* :code:`buffer_limit`: the amount of broadcast that are kept once received. The broadcasts are saved for
  discarding duplicates. By default is :code:`100`.

Dissemination Extreme One-Many (Mon)
+++++++++++++++++++++++++++++++++++++++++
* :code:`timeout`: the timeout in seconds waited for building a tree for broadcasting.
  The tree is built on-demand when data is broadcasted. By default is :code:`5`.
* :code:`tree_life`: the time in seconds that on-demand trees are maintained. by default is:
  :code:`10`.
* :code:`fanout`: the amount of neighbours to whom the broadcast is forwarded in each hop.
  By default is :code:`10`.

Dissemination Stable Many-Many (Plumtree)
++++++++++++++++++++++++++++++++++++++++++
* :code:`retrieve_timeout`: the timeout in seconds waited for retrieving an unreceived broadcast.
  When timeout expires manually is queried the broadcast origin node. By default is :code:`3`.
* :code:`maintenance_sleep`: how frequent neighbours are maintained (keepalives
  or any other maintenace echanism). By deault is: :code:`1`.
* :code:`buffer_limit`: the amount of broadcast that are kept once received. The broadcasts are saved for
  discarding duplicates. By default is :code:`100`.

Dissemination Stable One-Many (Brisa)
+++++++++++++++++++++++++++++++++++++++++
* :code:`fanout`: the amount of neighbours to whom the broadcast is forwarded in each hop.
  By default is :code:`3`.
* :code:`maintenance_sleep`: how frequent neighbours are maintained (keepalives
  or any other maintenace echanism). By deault is: :code:`0.5`.
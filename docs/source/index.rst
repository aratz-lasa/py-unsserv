.. py_unsserv documentation master file

py-unsserv: P2P out-of-the-box
===============================

.. image:: https://badge.fury.io/py/unsserv.svg
   :target: https://badge.fury.io/py/unsserv
.. image:: https://img.shields.io/badge/python-3.7-blue.svg
   :target: https://www.python.org/downloads/release/python-370/
.. image:: https://travis-ci.com/aratz-lasa/py-unsserv.svg?branch=master
   :target: https://travis-ci.com/aratz-lasa/py-unsserv
.. image:: https://codecov.io/gh/aratz-lasa/py-unsserv/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/aratz-lasa/py-unsserv
.. image:: https://img.shields.io/badge/code%20style-pep8-orange.svg
   :target: https://www.python.org/dev/peps/pep-0008/
.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black
.. image:: http://www.mypy-lang.org/static/mypy_badge.svg
   :target: http://mypy-lang.org/

**py-unsserv** is high-level Python library, designed to offer out-of-the-box peer-to-peer (p2p)
network, as well as a bunch of functionalities on top of it. These functionalities include:

* Membership (or p2p network creation)
* Clustering
* Metrics aggregation
* Nodes sampling
* Dissemination (or broadcasting)
* Data searching (key-value caching oriented)

**Look how easy it is to setup a P2P network and broadcast some data:**

.. literalinclude:: _code_examples/index_example.py


Installation
=============
To install py-unsserv, you just need to run the following command on your terminal:

.. code-block:: bash

   $ pip install unsserv


User's guide
=============

.. toctree::
   :maxdepth: 2

   concepts
   quickstart
   advanced_usage



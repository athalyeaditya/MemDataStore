MemDataStore
============

A abstraction representing data stored in memory(JVM) or can be extended for data stored in shared memory also. Includes a small framework to query the data.

We had developed this small framework for benchmarking various persistence types for our application.(RDBMS, Distributed cache).
This framework stores data in form of tuples in in-memory data structures and allows querying them using a simple API. Currently simple predicates are
supported but framework can be extended to support joins as well.
The query framework creates an execution plan from the query API and executes it. Optionally the API can be provided with a hint to allow it do cost
based optimization which creates a different execution plan. Presently only tuple count is used as optimization hint.

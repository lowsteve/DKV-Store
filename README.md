## DKV-Store

DKV-Store is a distributed key-value store implemented in Java as an exercise
in distributed systems.

## Motivation

This project is an educational exercise used to develop my understanding of
some of the key concepts of distributed systems programming. Some of the topics
explored in the development of DKV-Store include:
* Distributing system load over multiple storage servers by exploiting the
  capabilites of consistent hashing.
* Coordinating the actions of multiple storage servers using message queues.
* Replicating data records across multiple storage servers using a stragtegy
  that guarantees eventual consistency.
* Detecting failures and reconciling data records in the event of a topology
  change.

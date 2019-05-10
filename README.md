## DKV-Store

DKV-Store is a distributed key-value store implemented in Java as an exercise
in distributed systems.

## Motivation

This project is an educational exercise used to develop my understanding of
some of the key concepts of distributed systems programming. Some of the topics
explored in the development of DKV-Store include:

* Distributing system load over multiple storage servers by exploiting the
  capabilities of consistent hashing.
* Coordinating the actions of multiple storage servers using message queues.
* Replicating data records across multiple storage servers using a strategy
  that guarantees eventual consistency.
* Detecting failures and reconciling data records in the event of a topology
  change.

## Dependencies

DKV-Store takes advantage of the following tools/libraries/frameworks:

Tool/Library/Framework | Description | Usage
---------------------- | ----------- | -----
Apache Ant | Build Tool | Used to build DKV-Store binaries.
Apache Log4j | Logging Utility | Used to log various events in DKV-Store as well for the printing of debugging messages.
Apache ZooKeeper | Configuration System | Used for maintaining the consistent hash ring of the storage service, and for implementing message passing queues.
Google Gson | JSON Serialization Library | Used to serialize and deserialize data records and messages to be passed between storage servers and the client CLI.


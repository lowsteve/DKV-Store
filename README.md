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

## Building & Setting Up DKV-Store


#### Building

The following instructions demonstrate how to build the code on a Linux/Unix
based system. As mentioned above, DKV-Store is built with Apache Ant and
requires that you have a JDK installed on your system such as Oracle's Java SE
Development Kit, or OpenJDK. These build instructions have been tested with
OpenJDK 11.0.3. To verify that you have the required software installed, run
the following commands in your favorite terminal emulator:

```
$ java --version
openjdk 11.0.3 2019-04-16
OpenJDK Runtime Environment (build 11.0.3+4)
OpenJDK 64-Bit Server VM (build 11.0.3+4, mixed mode)
```

```
$ ant -version
Apache Ant(TM) version 1.10.5 compiled on September 9 2018
```

If you do not see output similar to the above, you can install Java by
referring to the official documentation available
[here](https://www.oracle.com/technetwork/java/javase/downloads/index.html), or
through the package manager included with your Linux distribution. Similarly,
if you need to install Apache Ant, you can refer to their official
documentation which is available
[here](https://ant.apache.org/).

With those requirements out of the way, you can now clone and build DKV-Store
as follows:

```
$ git clone https://github.com/lowsteve/DKV-Store.git
$ cd DKV-Store/
$ ant build-jar
```

This should compile the following files:

File | Description
---- | -----------
m2-client.jar | The CLI client used put, get, and delete keys and values from the storage service.
m2-ecs.jar | The CLI ECS client used by administrators to configure the storage service.
m2-server.jar | The storage servers that are launched either locally or remotely through the ECS client.
perfclient.jar | A client used for running performance tests on the server.

#### SSH Setup

It should be noted that the ECS client launches servers by making an SSH call
with public key encryption, so if you intend to try out DKV-Store, you will
need to generate an SSH key and copy it over to any server on which you intend
to run an m2-server.jar. Assuming you would just like to try DKV-Store on a
local machine (the default for this project), and assuming you have an SSH
implementation installed, you can accomplish this as follows:

```
$ ssh-keygen -t rsa            # Press enter at each prompt.
$ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
$ chmod og-wx ~/.ssh/authorized-keys
```

Please use caution when running ssh-keygen to avoid overwriting any keys you
may have already generated on your system.

#### ZooKeeper Setup

Before we get to the running instructions for DKV-Store, the final thing we need
is to have a ZooKeeper instance running. ZooKeeper releases can be downloaded
from the official website [here](https://zookeeper.apache.org/releases.html).
These instructions have been tested and are known to work with ZooKeeper 3.4.14.

Upon downloading ZooKeeper, you should end up with a file such as
`zookeeper-3.4.14.tar.gz`. Move this file to a directory of your choosing and
extract it as follows:

```
$ tar -xzf zookeeper-3.4.14.tar.gz
```

Navigate into this newly created directory (`zookeeper-3.4.14/`). The ZooKeeper
server requires a configuration file in order to run. By default, it looks for
this file in `conf/zoo.cfg` under the root of the ZooKeeper directory. A sample
configuration file is provided with ZooKeeper releases under
`conf/zoo_sample.cfg`, which you will need to rename or copy:

```
$ cp conf/zoo_sample.cfg conf/zoo.cfg
```

A full explanation of this configuration file is outside the scope of this
guide, however you can check the official ZooKeeper documentation if you'd like
to know more. For simplicity, these instructions will run DKV-Store with
ZooKeeper in its default configuration.

Next, we'll need to start the ZooKeeper server. A launcher for the server can
be found in the `bin/` directory, and is called `zkServer.sh`. The server can
be run in either the foreground or background as follows:

background:
```
$ ./bin/zkServer.sh start
```

foreground:
```
$ ./bin/zkServer.sh start-foreground
```

By default this will launch the ZooKeeper service on the localhost at port
2181.

You should now be ready to run DKV-Store locally.

## Running DKV-Store


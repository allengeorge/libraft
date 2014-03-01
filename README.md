# libraft [![Build Status](https://travis-ci.org/allengeorge/libraft.png?branch=master)](https://travis-ci.org/allengeorge/libraft) [![Coverage Status](https://coveralls.io/repos/allengeorge/libraft/badge.png?branch=master)](https://coveralls.io/r/allengeorge/libraft?branch=master)

libraft is a Java library that implements the [Raft distributed consensus protocol]
(https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf "In Search of an Understandable Consensus Algorithm").

## Release History

* **0.1.0**: Initial release. (11 Dec, 2013)
* **0.1.1**: Bug-fix release. (25 Jan, 2014)

For more information check the detailed [release history] (https://github.com/allengeorge/libraft/wiki/Release-History "libraft release history").

## Features

libraft **completely** implements all checked features below. Unchecked features are
planned but do not have an implementation timeline.

- [x] Leader election
- [x] Log replication
- [o] Log compaction (snapshots)
- [o] Online cluster reconfiguration

## Overview

libraft consists of 2 components:

* libraft-core
* libraft-agent

Correctness, safety, and understandability were major factors during design
and implementation. Performance was an explicit non-goal.

### libraft-core

libraft-core contains an implementation of the Raft algorithm. It also defines interfaces
for the components (such as timers, packet senders, log and metadata storage, *etc*.) it needs
to operate. These interfaces make very few demands on the underlying implementation and should
be easy to integrate into an existing stack.

### libraft-agent

libraft-agent:

1. Provides reference implementations for the interfaces defined in libraft-core.
2. Provides a facade over libraft-core that can be instantiated to provide consensus services.

It uses JDBC for persistence, TCP connections for networking, and json for the
wire format. It wires these implementations up to the algorithm classes from libraft-core.
libraft-agent also defines a simple [json configuration format] (#configuring-libraft "Configuring libraft")
with which users can configure their cluster. Finally, it provides a single class - `RaftAgent` - with
which applications can control the lifecycle of a Raft server and interact with the Raft cluster.

### Samples

libraft also comes with a fully-implemented sample application in `libraft-samples/kayvee`. *KayVee*
is a distributed, consistent, key-value store that uses `RaftAgent` to durably replicate key-value
pairs to a cluster. It demonstrates how libraft can be integrated into an application to provide consensus services.

## Getting libraft

The simplest way to use libraft is through Gradle or Maven - simply
add `libraft-agent` as a dependency. Replace `${libraft.version.latest}`
in the code below with the latest version on Maven Central.

### Gradle

```groovy
dependencies {
    compile 'io.libraft:libraft-agent:${libraft.version.latest}'
}
```

### Maven

```xml
<dependencies>
    <dependency>
        <groupId>io.libraft</groupId>
        <artifactId>libraft-agent</artifactId>
        <version>${libraft.version.latest}</version>
    </dependency>
</dependencies>
```

Alternatively, if you only want the algorithm components and
intend to build your own implementations for its interfaces, only `libraft-core` is necessary.
Simply add `libraft-core` as a dependency, and replace `${libraft.version.latest}`
in the code below with the latest version on Maven Central.

### Gradle

```groovy
dependencies {
    compile 'io.libraft:libraft-core:${libraft.version.latest}'
}
```

### Maven

```xml
<dependencies>
    <dependency>
        <groupId>io.libraft</groupId>
        <artifactId>libraft-core</artifactId>
        <version>${libraft.version.latest}</version>
    </dependency>
</dependencies>
```

## Configuring libraft

`RaftAgent` can be configured in one of two ways:

1. A json configuration file.
2. A `RaftConfiguration` instance.

Both methods are equivalent and have the same properties.

### Properties

`libraft-agent` exposes the following configuration properties. **bolded** properties are **required**.

* `minElectionTimeout`: minimum election timeout for a Raft server
* `additionalElectionTimeoutRange`: maximum additional time added to `minElectionTimeout` to get the applied election timeout.
                                    Election timeout is defined using the following formula: `electionTimeout = minElectionTimeout + randomInRange(0, additionalElectionTimeoutRange)`.
* `rpcTimeout`: maximum time a Raft server will wait for a response to an RPC request
* `heartbeatInterval`: maximum time between messages from a Raft leader
* `connectTimeout`: maximum time a Raft server will wait to establish a connection to another Raft server
* `minReconnectInterval`: minimum interval a Raft server will wait before reconnecting to another Raft server
* `additionalReconnectIntervalRange`: maximum additional time added to `minReconnectInterval` to get the applied reconnect interval.
                                      Reconnect interval is defined using the following formula: `reconnectInterval = minReconnectInterval + randomInRange(0, additionalReconnectIntervalRange)`.
* **`database`**: Raft database configuration block
    * **`driverClass`**: fully-qualified class name of the JDBC driver
    * **`url`**: JDBC connection URL
    * **`user`**: database user id
    * **`password`**: database password (may be empty or omitted)
* **`cluster`**: Raft cluster configuration block
    * **`self`**: unique id of the *local* Raft server
    * **`members`**: Raft cluster configuration. Defines *all* the members in the cluster, *including* the local server.
        * **`id`**: unique id of the Raft server
        * **`endpoint`**: address - in "host:port" format - at which this server can be reached

### Sample

A sample configuration file for a 5-server Raft cluster is given below. Note that
`self` is `S_00`, indicating that this is the configuration file for server `S_00` in the cluster.
This file includes *all* fields, both required and optional.

```json
{
    "minElectionTimeout": 180,
    "additionalElectionTimeoutRange": 120,
    "rpcTimeout": 30,
    "heartbeatInterval": 15,
    "connectTimeout": 5000,
    "minReconnectInterval": 10000,
    "additionalReconnectIntervalRange": 1000,

    "database": {
        "driverClass": "org.h2.Driver",
        "url": "jdbc:h2:test_db",
        "user": "test",
        "password": "test"
    },

    "cluster": {
        "self": "S_00",
        "members": [
            {
                "id": "S_00",
                "endpoint": "192.168.1.100:9990"
            },
            {
                "id": "S_01",
                "endpoint": "192.168.1.100:9991"
            },
            {
                "id": "S_02",
                "endpoint": "192.168.1.100:9992"
            },
            {
                "id": "S_03",
                "endpoint": "192.168.1.100:9993"
            },
            {
                "id": "S_04",
                "endpoint": "192.168.1.100:9994"
            }
        ]
    }
}
```

## Using libraft

The simplest way to use libraft is to instantiate a `RaftAgent` within an application
node (such as a server). This `RaftAgent` will allow the node to participate in a Raft cluster:
it can become a leader or follower and will be notified of leadership changes as well as
applied (i.e. committed) commands.

Creating a `RaftAgent` requires:

1. A valid [configuration file or object] (#configuring-libraft "Configuring libraft").
2. A specialization of `Command` for the application commands that will be committed to the Raft cluster.
3. A specialization of `RaftListener` that will be notified of leadership changes and applied commands.

Sample json configuration file ("agent.config"):

```json
{
    "database": {
        "driverClass": "org.h2.Driver",
        "url": "jdbc:h2:test_db",
        "user": "test",
        "password": "test"
    },

    "cluster": {
        "self": "agent0",
        "members": [
            {
                "id": "agent2",
                "endpoint": "192.168.1.100:9990"
            },
            {
                "id": "agent1",
                "endpoint": "192.168.1.100:9991"
            },
            {
                "id": "agent0",
                "endpoint": "192.168.1.100:9992"
            }
        ]
    }
}
```

Sample java code:

```java

//
// Command to be replicated to the Raft cluster
//

public class MyCommand implements Command {

    enum CommandType {
        GET,
        SET
    }

    @JsonProperty
    CommandType commandType;

    @JsonProperty
    String key;

    @JsonProperty
    String value;

    // constructor, getters, and setters follow...
}

//
// Listener that will be notified of events in the Raft cluster
//

public class MyRaftListener implements RaftListener {

    @Override
    public void onLeadershipChange(@Nullable String leader) {
        //...
    }

    @Override
    public void applyCommitted(long index, Command command) {
        //...
    }
}

//
// Application code
//

// create the listener that will be tied to the RaftAgent
RaftListener raftListener = new MyRaftListener();

// create the RaftAgent using the configuration in "agent.config.json"
// the listener created above of leadership changes and applied commands
RaftAgent raftAgent = RaftAgent.fromConfigurationFile("agent.config", raftListener);

// indicate that the application command uses Jackson annotations
// RaftAgent has special support for this
raftAgent.setupJacksonAnnotatedCommandSerializationAndDeserialization(MyCommand.class);

// initialize the RaftAgent
// (sets up state, but does not connect to any servers, start any timers, etc.)
// this allows the application to assume that it has sole access to resources
// allowing it to perform verification and bootstrap tasks in single-threaded mode
raftAgent.initialize();

// start the RaftAgent (this starts the internal server/client, timers, etc.)
raftAgent.start();

// submit commands (if leader)
if (raftListener.isLeader()) {
    raftAgent.submitCommand(new MyCommand(MyCommand.CommandType.SET, "key0", "val0"));
    raftAgent.submitCommand(new MyCommand(MyCommand.CommandType.SET, "key1", "val1"));
    // more work...
} else {
    // more work...
}

// stop the agent (stops the internal server/clients, timers, etc.)
raftAgent.stop();
```

`RaftAgent` supports more than what is outlined here (non-Jackson-annotated command serialization, *etc.*)
For more examples, see `KayVee.java` in `libraft-samples/kayvee` and `RaftAgentTest.java` in `libraft-agent`.

## Building libraft

libraft requires java 1.6 and gradle 1.10+ to build.

To build and install into the local maven cache, run `gradle` from the repository root.

```shell
gradle build
gradle install
```

libraft also ships with a gradle wrapper, which allows the code to be built without
prior installation of gradle. To use gradle wrapper run `gradlew` from the repository root.

```shell
./gradlew build
./gradlew install
```

To clean build artifacts run `gradle clean` or `gradlew clean` from the repository root.

## Issues

The full list of issues can be seen at
[Github Issues] (https://github.com/allengeorge/libraft/issues "libraft issues").

There are no known safety issues.

## Reporting Issues

Please submit all code or documentation issues, comments and concerns or feature requests to
[Github Issues] (https://github.com/allengeorge/libraft/issues "libraft issues").

If you have other libraft or Raft questions, please post to the
[raft-dev] (https://groups.google.com/forum/#!forum/raft-dev "raft-dev")
mailing list.

## Thanks!

libraft stands on the shoulders of others. This implementation would not be possible
without the very detailed (and clear!) paper published by Diego Ongaro and John Ousterhout.
Moreover, this library was built on a huge host of open-source software - many thanks to the teams behind them!

Just *some* of the open-source software libraft uses include:

* [guava-libraries] (https://code.google.com/p/guava-libraries/ "Google Core Libraries for Java 1.6+")
* [junit] (http://www.junit.org "A programmer-oriented testing framework for Java")
* [mockito] (https://code.google.com/p/mockito/ "Simpler & better mocking")
* [hamcrest] (https://code.google.com/p/hamcrest/ "Hamcrest - Library of Matchers for Building Test Expressions")
* [logback] (http://logback.qos.ch/ "The Generic, Reliable Fast & Flexible Logging Framework")
* [jacoco] (http://www.eclemma.org/jacoco/ "JaCoCo Java Code Coverage Library")
* [netty] (http://www.netty.io "netty")
* [jackson] (https://github.com/FasterXML/jackson "FasterXML/Jackson")
* [hibernate validator] (http://www.hibernate.org/subprojects/validator.html "Bean Validation Reference Implementation")
* [h2] (http://www.h2database.com "H2 Database Engine")
* [dropwizard] (http://www.dropwizard.io "Dropwizard - Production Ready out of the Box")
* [jersey] (https://jersey.java.net/ "Jersey - RESTful Web Services In Java")
* [jetty] (http://www.eclipse.org/jetty/ "Jetty - Servlet Engine and HTTP Server")
* [jdbi] (http://www.jdbi.org "JDBI")
* [sqlite-jdbc (xerial)] (https://bitbucket.org/xerial/sqlite-jdbc "SQLite JDBC Driver")
* [gradle] (http://www.gradle.org "Gradle - Build Automation Evolved")
* [coveralls-gradle-plugin] (https://github.com/kt3k/coveralls-gradle-plugin "Coveralls Gradle Plugin")

It's this - and much, much more! - that makes libraft possible.

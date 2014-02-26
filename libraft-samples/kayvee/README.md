# KayVee

KayVee is a [Dropwizard] (http://www.dropwizard.io "Dropwizard - Production-ready out of the box")-based
server that uses libraft-core and libraft-agent to create a distributed, consistent, key-value store.

## Getting KayVee

KayVee is available in a distribution zip. Simply download
`kayvee-${libraft.version.latest}-dist.zip` from Maven Central, where
`${libraft.version.latest}` is the latest available version.

Alternatively, you can [build a KayVee distribution zip] (#building-kayvee "Building KayVee") locally.

## Building KayVee

### Gradle

KayVee requires Java 1.6+ and gradle 1.8+. From the repository root, run:

```
gradle :libraft-samples:kayvee:build
```

To create a distribution zip containing configuration files and a simple start script, run:

```
gradle :libraft-samples:kayvee:dist
```

The resulting distribution is built to `${repository-root}/libraft-samples/kayvee/build/distributions`
and can be used on any *nix with Java.

To remove build artifacts, run:

```
gradle :libraft-samples:kayvee:clean
```

### Gradle Wrapper

KayVee also ships with a gradle wrapper, allowing it to be built in an
environment without an installed gradle. To use the gradle wrapper, substitute `gradlew`
for `gradle` in the repository root.

To build, run:

```
./gradlew :libraft-samples:kayvee:build
```

To create a distribution zip, run:

```
./gradlew :libraft-samples:kayvee:dist
```

To clean build artifacts, run:

```
./gradlew :libraft-samples:kayvee:clean
```

## Configuring KayVee

KayVee uses a YAML configuration file. It extends the
[Dropwizard configuration] (https://github.com/dropwizard/dropwizard/blob/v0.6.2/dropwizard-example/example.yml "Example Dropwizard 0.6.2 configuration file"),
allowing all standard Dropwizard blocks and parameters to be used
to configure KayVee. In addition, KayVee defines two more
configuration blocks - `raftDatabase` and `cluster` - both of which are
identical to the ones described in the libraft README.md.

### Sample

The following is a sample configuration file for `SERVER_00`
(notice that `self` is set to `SERVER_00`) in a 3-server cluster.

```yaml
http:
  # port on which KayVee listens for client requests
  port: 6080
  # port on which KayVee listens for admin commands
  adminPort: 6081

# raft-agent Log and Store database
raftDatabase:
  driverClass: org.sqlite.JDBC
  url: jdbc:sqlite:kayvee_raft.db
  user: test
  # password may be empty or omitted
  password: test

# cluster configuration
cluster:
  # unique id of _this_ server
  self: SERVER_00

  # lists _all_ the members in the cluster
  # members can be defined in any order
  members:
    # configuration block describing _this_ server
    # uses settings defined in the 'http' block above for kayVeeUrl
    - id: SERVER_00
      # http (i.e. client API) url
      kayVeeUrl: http://localhost:6080
      # raft consensus system endpoint
      raftEndpoint: localhost:9080
    # configuration blocks for the _other_ servers
    - id: SERVER_01
      kayVeeUrl: http://localhost:6085
      raftEndpoint: localhost:9085
    - id: SERVER_02
      kayVeeUrl: http://localhost:6090
      raftEndpoint: localhost:9090

logging:
  level: INFO

  console:
    enabled: true

  file:
    enabled: true
    currentLogFilename: kayvee.log
    archivedLogFilenamePattern: kayvee-%d.log.gz
    archivedFileCount: 3
```

## Running KayVee

To run KayVee, first extract `kayvee-${libraft.version.latest}-dist.zip`.
(The default is `kayvee-${libraft.version.latest}`.) From within that directory, run:

```
./kayvee server kayvee.yml
```

This default `kayvee.yml` configuration starts `SERVER_00` in a 3-server cluster.
It creates both a database and logs in the current directory and starts logging to the console.
If `SERVER_00` starts successfully, you should see messages of the form:

```
WARN  [2013-11-27 05:04:59,006] io.libraft.agent.rpc.FinalUpstreamHandler: SERVER_00: closing channel to SERVER_02: Connection refused: localhost/127.0.0.1:9090
WARN  [2013-11-27 05:04:59,011] io.libraft.agent.rpc.FinalUpstreamHandler: SERVER_00: closing channel to SERVER_01: Connection refused: localhost/127.0.0.1:9085
...
...

```

as this server tries to connect to its peers in the cluster.

## Running a KayVee cluster

Setting up a KayVee cluster simply involves creating a
configuration file for each server in the cluster. The configuration
file should specify the unique ids and endpoints of *all* the
cluster servers, and `self` should be set to the unique id of the
*local* server.

Example configurations for a 3-server cluster are included
in `${repository-root}/libraft-samples/kayvee/testenv`. To set up
this cluster simply extract `kayvee-${libraft.version.latest}-dist.zip`
to a directory of your choice (The default is `kayvee-${libraft.version.latest}`.)
Copy the configurations from `${repository-root}/libraft-samples/kayvee/testenv`
to that directory, and from within there, run:

```
./kayvee server s01-kayvee.yml
./kayvee server s02-kayvee.yml
./kayvee server s03-kayvee.yml
```

## Using KayVee

KayVee can be controlled via a REST API. It provides the following operations:

* GET
* ALL (get all)
* SET
* CAS (compare and set)
* DEL (delete)

Note that in the descriptions below, null refers to the json `null` type.

### GET

Get the value of a key.

#### Example

```shell
curl 'http://localhost:6080/keys/leslie'
```

If the key exists:

```json
{
    "key": "leslie",
    "value": "lamport"
}
```

#### Errors

* `301` if the server receiving the request is not the cluster leader. The response will have `Location` set to: `leaderUrl/requestPath`.
* `404` if the key does not exist.
* `500` if the there was an unexpected server error.
* `503` if the server receiving the request is not the leader, and the cluster has no leader.

### ALL

Get all the key-value pairs stored in KayVee.

#### Example

```shell
curl 'http://localhost:6080/keys'
```

If there are any entries:

```json
[
    {
        "key": "leslie",
        "value": "lamport"
    },
    {
        "key": "nancy",
        "value": "lynch"
    },
    {
        "key": "mike",
        "value": "burrows"
    }
]
```

A `204` is returned if no entries are present.

#### Errors

* `301` if the server receiving the request is not the cluster leader. The response will have `Location` set to: `leaderUrl/requestPath`.
* `500` if the there was an unexpected server error.
* `503` if the server receiving the request is not the leader, and the cluster has no leader.

### SET

Create a new key if it does not exist. Update the key with the new value if it does.
Value *cannot* be null or empty.

#### Example

```shell
curl -XPUT -H 'Content-Type: application/json' 'http://localhost:6080/keys/nancy' -d '
{
    "newValue": "lynch"
}'
```

If the set completed successfully:

```json
{
    "key": "nancy",
    "value": "lynch"
}
```

#### Errors

* `301` if the server receiving the request is not the cluster leader. The response will have `Location` set to: `leaderUrl/requestPath`.
* `400` if new value is null.
* `500` if the there was an unexpected server error.
* `503` if the server receiving the request is not the leader, and the cluster has no leader.

### CAS

Create, update or delete a key if the expected value equals the current value.

A key is created if:

1. The key does not exist.
2. Expected value is null.
3. New value is not null.

A key is updated if:

1. The key exists.
2. Expected value is not null.
3. Expected value equals current value.
4. New value is not null or empty.

A key is deleted if:

1. The key exists.
2. New value is null.
3. Expected value is not null.
4. Expected value equals current value.

#### Example

```shell
curl -XPUT -H 'Content-Type: application/json' 'http://localhost:6080/keys/leslie' -d '
{
    "expectedValue": "lamport",
    "newValue": "lamport@microsoft"
}'
```

If the key was updated successfully, or a new key created:

```json
{
    "key": "leslie",
    "value": "lamport@microsoft"
}
```

A `204` is returned if the cas deletes the key.

#### Errors

* `301` if the server receiving the request is not the cluster leader. The response will have `Location` set to: `leaderUrl/requestPath`.
* `400` if both expected value and new value are null.
* `404` if the expected value is not null but the key does not exist.
* `409` if the expected value is null but the key already exists.
* `409` if the expected value and current value do not match.
* `500` if the there was an unexpected server error.
* `503` if the server receiving the request is not the leader, and the cluster has no leader.

### DEL

Deletes a key. noop if the key does not exist.

#### Example

```shell
curl -XDELETE 'http://localhost:6080/keys/nancy'
```

A `204` is returned if the key is deleted or did not exist.

#### Errors:

* `301` if the server receiving the request is not the cluster leader. The response will have `Location` set to: `leaderUrl/requestPath`.
* `500` if the there was an unexpected server error.
* `503` if the server receiving the request is not the leader, and the cluster has no leader.


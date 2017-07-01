## bolt.x

A [Vert.x](http://vertx.io/) [EventBus bridge](http://vertx.io/docs/vertx-tcp-eventbus-bridge/java/) verticle to access [Neo4j](https://neo4j.com/) database via [bolt](https://boltprotocol.org/) protocol.
It supports async handling of [cypher](http://www.opencypher.org/) requests.

### Prerequisites

- Apache Maven
- JDK 8+
- vertx-tcp-eventbus-bridge
- neo4j-java-driver

### Running the project

Once you have retrieved the project, you can check that everything works with:

```
mvn test exec:java
```

Please note that the test assumes Neo4j is running on localhost with default settings.

### Building the project

To build the project, just use:

```
mvn clean package
```

It generates a _fat-jar_ in the `target` directory.

### Deploying the project

You can just use `redeploy.sh` or `redeploy.bat`.

### Settings

You can set these system-property values.

  - boltx.port (EventBus port - default is 7000)
  - boltx.bolt.uri (Bolt driver uri - default is "bolt://localhost:7687")
  - boltx.neo4j.username (Neo4j username - default is "neo4j")
  - boltx.neo4j.password (Neo4j password - default is "neo4j")

Example:
```
redeploy.bat -Dboltx.port=7000 -Dboltx.bolt.uri="bolt://localhost:7687" -Dboltx.neo4j.username=neo4j -Dboltx.neo4j.password=abc
```

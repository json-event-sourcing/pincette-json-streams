# JSON Streams

- [An Application](#an-application)
- [MongoDB Aggregation Pipelines](#mongodb-aggregation-pipelines)
- [JSON Event Sourcing Aggregates](#json-event-sourcing-aggregates)
- [JSON Event Sourcing Reactors](#json-event-sourcing-reactors)
- [Stream Mergers](#stream-mergers)
- [Stream Joiners](#stream-joiners)
- [Data Serialisation](#data-serialisation)
- [Troubleshooting](#troubleshooting)
- [Building It](#building-it)
- [The Command Line](#the-command-line)
- [Configuration](#configuration)
- [Docker Image](#docker-image)

With this tool you can run event streaming applications using JSON messages in a declarative way. The unit of execution is a [Kafka Streams](https://docs.confluent.io/current/streams/architecture.html) topology, which consists of parts that are tied together in a Kafka Streams network. There are five kinds of parts:

- [MongoDB aggregation pipelines](https://www.javadoc.io/static/net.pincette/pincette-mongo-streams/1.0.1/net/pincette/mongo/streams/Pipeline.html);
- [JSON Event Sourcing](https://github.com/json-event-sourcing/pincette-jes) aggregates;
- JSON Event Sourcing reactors;
- Stream mergers;
- Stream joiners.

Everything can be described with a set of JSON and [JSLT](https://github.com/schibsted/jslt) files. The tool has a build command with which you create one big JSON file, where everything is inlined. The result can also be stored in a MongoDB collection. The run command can either use a file or a MongoDB collection to run from. In the latter case all documents are run as separate topologies. Optionally you can add a MongoDB query to run only a subset.

## An Application

The structure of an application is very simple. It has an ```application``` field, which is a unique name, an optional ```version``` field and a ```parts``` field. The latter is an array of objects, which describe structures that contribute to the Kafka Streams topology. Instead of an object you can also use a relative filename. The part will be loaded from there then.

```
{
  "application": "myapp",
  "version": "1.0",
  "parts": [
    {
      ...    
    },
    "mypart.json",
    ...    
  ]  
}
```

The name of the application will also be the name of the Kafka consumer group that Kafka Streams will create. The application will also correspond to a Java logger.

Sometimes you may want to work with logical environments, but in the same physical environment. For example, you may have a "prd" and a "tst" environment and only one MongoDB database and only one Kafka cluster. In such a case you can add "${ENV}" to the names of Kafka topics and MongoDB collections. It will be replaced with the configured environment name. The configuration is explained below.

The Kafka Streams topologies that are created run with at least once semantics. This means you may see duplicate messages sometimes.

## MongoDB Aggregation Pipelines

Doing event streaming usually comes with pipelines that transform and/or aggregate the data. Since the data this tool understands is always in JSON format, an interesting language to describe the transformations and aggregations is the [MongoDB Aggregation Pipeline](https://docs.mongodb.com/manual/reference/operator/aggregation/) expression language. A pipeline part has the following fields:

|Field|Mandatory|Description|
|---|---|---|
|fromStream|Exclusive with ```fromTopic```|The name of the stream to which this stream will be connected.|
|fromTopic|Exclusive with ```fromStream```|The name of the Kafka topic to which this stream will be connected as a consumer.|
|name|Yes|The name of the stream. Other parts can connect to this stream with that name.|
|pipeline|Yes|An array of [pipeline stages](https://www.javadoc.io/static/net.pincette/pincette-mongo-streams/1.0.1/net/pincette/mongo/streams/Pipeline.html). These are either JSON objects or relative filenames, in which case the stage is loaded from there.|
|toString|No|A boolean field that, when the ```toTopic``` field is present, will cause the JSON messages to be written as strings.| 
|toTopic|No|The name of the Kafka topic to which this stream will be connected as a producer.|
|type|Yes|The value is always ```stream```.|

The following example groups the messages produced by the ```$probe``` stage. It sums up the ```count``` field. You will likely need this when you use that pipeline stage, because the probe emits values per topic partition.

```
[
  {
    "application": "json-streams-probe",
    "version": "1.0",
    "parts": [
      {
        "type": "stream",
        "name": "group-probe",
        "fromTopic": "pipeline-monitor-part-${ENV}",
        "toTopic": "pipeline-monitor-${ENV}",
        "pipeline": [
          {
            "$group": {
              "_id": {
                "name": "$name",
                "minute": "$minute"
              },
              "_collection": "group-probe-${ENV}",
              "count": {
                "$sum": "$count"
              }
            }
          },
          {
            "$set": {
              "name": "$_id.name",
              "minute": "$_id.minute"
            }
          }
        ]
      }
    ]
  }
]
```

## JSON Event Sourcing Aggregates

Even in an event driven architecture you need to manage the state of ordinary applications. An approach to do this is [JSON Event Sourcing](https://github.com/json-event-sourcing/pincette-jes). State is managed in an aggregate. An aggregate can define commands, which may have effect. When they do, the change is recorded and published as an event.

You manage the state by writing reducers for commands. A reducer receives a command and the current state of an aggregate instance. Its task is to calculate the new state. Reducers are supposed to not have side effects. As such reducers are merely JSON transformations.

With this tool you write reducers in [JSLT](https://github.com/schibsted/jslt). A script receives an object with the fields ```command``` and ```state```. It should return the new state. Scripts are allowed to import other scripts. You should always use relative filenames.

An aggregate part has the following fields:

|Field|Mandatory|Description|
|---|---|---|
|aggregateType|Yes|The name of the aggregate. Usually is it composed as ```<app>-<type>```.|
|commands|Yes|An array of JSON objects.|
|type|Yes|The value is always ```aggregate```.|

Commands have the following fields:

|Field|Mandatory|Description|
|---|---|---|
|name|Yes|The name of the command. It will be available in the reducer as the field ```/command/_command```.|
|reducer|Yes|The relative filename of a JSLT script.|
|validator|No|A command validator described as a [Mongo Validator](https://www.javadoc.io/static/net.pincette/pincette-mongo/2.0/net/pincette/mongo/Validator.html).|

An aggregate creates the streams with the names ```<app>-<type>-aggregate```, ```<app>-<type>-command```, ```<app>-<type>-event```, ```<app>-<type>-event-full``` and ```<app>-<type>-reply```. Their meaning is described in [JSON Event Sourcing](https://github.com/json-event-sourcing/pincette-jes#the-kafka-topics). You can connect to those streams in other parts of the application.

The following example is the aggregate ```plusminus-counter```. It has the commands ```plus```, ```minus``` and ```put```. All have an effect on the field ```value```.

```
[
  {
    "application": "plusminus",
    "version": "1.0",
    "parts": [
      {
        "type": "aggregate",
        "aggregateType": "plusminus-counter",
        "commands": [
          {
            "name": "plus",
            "reducer": "reducers/plus.jslt",
            "validator": {
              "include": [
                "validators/operator.json"
              ],
              "conditions": [
                {
                  "_command": "plus"
                }
              ]
            }
          },
          {
            "name": "minus",
            "reducer": "reducers/minus.jslt",
            "validator": {
              "include": [
                "validators/operator.json"
              ],
              "conditions": [
                {
                  "_command": "minus"
                }
              ]
            }
          },
          {
            "name": "put",
            "reducer": "reducers/put.jslt",
            "validator": {
              "include": [
                "validators/type.json"
              ],
              "conditions": [
                {
                  "_command": "put"
                },
                {
                  "value": 0,
                  "$code": "INIT"
                }
              ]
            }
          }
        ]
      }
    ]
  }
]

```

The first two reducers below move the context into the ```state``` field. They change the ```value``` field and just copy all the others. The last reducer is for the ```put``` command. It moves the context into the ```command``` field, because the whole command will become the new state. Only the ```_command``` field is removed.

You don't have to provide the ```put``` command, because it is built in. The reason to add it is validation.

```
.state | {
  "value": .value + 1,
  *: .
}
```

```
.state | {
  "value": .value - 1,
  *: .
}
```

```
.command | {
  "_command": null,
  *: .
}
```

The validators check the command names and if the commands are for the right aggregate type. The latter is not strictly necessary, because commands of the wrong type are ignored by the aggregate. For the commands ```plus``` and ```minus``` an extra check is done. The ```value``` field should not be present. Below are the included validators.

```
{
  "conditions": [
    {
      "_type": "plusminus-counter"
    }
  ]
}
```

```
{
  "include": [
    "type.json"
  ],
  "conditions": [
    {
      "value": {
        "$exists": false
      },
      "$code": "OPERATOR"
    }
  ]
}
```

## JSON Event Sourcing Reactors

Sometimes events from one aggregate type should be propagated to another aggregate type. A reactor is a mediating component with which you can translate events to commands. You provide it with a transformation function, a MongoDB query to find the IDs of the destination aggregate instances to which the command should be sent and optionally a filter function to filter the events. In this tools the transformation function is a JSLT script. The filter function is a [MongoDB query expression](https://www.javadoc.io/static/net.pincette/pincette-mongo/2.0/net/pincette/mongo/Match.html).

A reactor part has the following fields:

|Field|Mandatory|Description|
|---|---|---|
|destinations|Yes|A MongoDB aggregation pipeline, which is an array of pipeline stages. It should project to only the ```_id``` field, because the rest is not used.|
|destinationType|Yes|The name of the destination aggregate. Usually is it composed as ```<app>-<type>```.|
|eventToCommand|Yes|The relative filename of a JSLT script.|
|filter|No|A MongoDB query expression. Its input will be a full event.|
|sourceType|Yes|The name of the source aggregate. Usually is it composed as ```<app>-<type>```.|
|type|Yes|The value is always ```reactor```.|

The following example is a reactor that sends the ```plus``` command to all other counters whenever a counter changes from 2 to 3. You can see that a reactor always receives a "full event". Those have the extra fields ```_before``` and ```_after```, which represent the previous and the new state respectively.

```
[
  {
    "application": "plusminus",
    "version": "1.0",
    "parts": [
      {
        "type": "reactor",
        "sourceType": "plusminus-counter",
        "destinationType": "plusminus-counter",
        "destinations": [
          {
            "$match": {
              "_id": {
                "$ne": "$$event._after._id"
              }
            }
          }
        ],
        "eventToCommand": "plus_command.jslt",
        "filter": {
          "$expr": {
            "$jes-changed": {
              "pointer": "/value",
              "from": 2,
              "to": 3
            }
          }
        }
      }
    ]
  }
]
```

This is the command generator:

```
{
  "_type": "plusminus-counter",
  "_command": "plus"
}
```

## Stream Mergers

You can merge several Kafka Streams or topics with a merge part. All the messages from all the input sources will be sent to the output. A merge part has the following fields:

|Field|Mandatory|Description|
|---|---|---|
|fromStreams|Eclusive with ```fromTopics```|An array of stream names.|
|fromTopics|Exclusive with ```fromStreams```|An array of Kafka topic names.|
|name|Yes|The name of the output stream. Other streams can connect to it with that name.|
|toString|No|A boolean field that, when the ```toTopic``` field is present, will cause the JSON messages to be written as strings.| 
|toTopic|No|The name of the Kafka topic to which this stream will be connected as a producer.|
|type|Yes|The value is always ```merge```.|

This is an example:

```
{
  "type": "merge",
  "name": "merged-init",
  "fromStreams": [
    "absences-init",
    "activities-init",
    "owners-init",
    "subscriptions-init"
  ]
}
```

## Stream Joiners

It is possible to join two Kafka streams or topics using any criterion that can be expressed in terms of the messages themselves. The join is always done within a configured time window. A join part has the following fields:

|Field|Mandatory|Description|
|---|---|---|
|left|Yes|An object with two fields. The first one is either ```fromStream``` or ```fromTopic```. The second is the ```on``` field, which is a MongoDB query expression.|
|right|Yes|An object with two fields. The first one is either ```fromStream``` or ```fromTopic```. The second is the ```on``` field, which is a MongoDB query expression.|
|name|Yes|The name of the output stream. Other streams can connect to it with that name.|
|toString|No|A boolean field that, when the ```toTopic``` field is present, will cause the JSON messages to be written as strings.| 
|toTopic|No|The name of the Kafka topic to which this stream will be connected as a producer.|
|type|Yes|The value is always ```join```.|
|window|Yes|The time window in milliseconds.|

The following example joins the command and event streams from the above-mentioned aggregate over a time window of 5 seconds, using the correlation ID. It writes the results to the topic ```string-test``` serialised as strings.

```
{
  "name": "joined",
  "type": "join",
  "toTopic": "string-test",
  "toString": true,
  "window": 5000,
  "left": {
    "fromStream": "plusminus-counter-command",
    "on": "$_corr"
  },
  "right": {
    "fromStream": "plusminus-counter-event",
    "on": "$_corr"
  }
}
```

## Data Serialisation

All messages are serialised with the [JSON Event Sourcing serialiser](https://www.javadoc.io/static/net.pincette/pincette-jes-util/1.3.2/net/pincette/jes/util/JsonSerde.html). It first encodes a ```JsonObject``` in [CBOR](https://tools.ietf.org/html/rfc7049). Then it is compressed in GZIP format (see also [RFC 1951](https://tools.ietf.org/html/rfc1951) and [RFC 1952](https://tools.ietf.org/html/rfc1952)). The deserialiser falls back to JSON in string format.

## Troubleshooting

Creating declarative data pipelines like this can quickly become quite complex. So you need to be able to debug them. There are a few tools to help with this.

Wherever you write MongoDB expressions that are not used to actually query the database, you can use the ```$trace``` operator. It can be wrapped around another expression. The result is the same, but as a side effect some tracing is written to the log.

The same technique is available for JSLT scripts. There you have the custom function ```trace```, which can also be wrapped around another expression.

With the custom MongoDB aggregation pipeline stage ```$trace``` you get the contents of whatever goes through it. If you set its value to ```null``` the entire message is traced. Alternatively you can provide a MongoDB expression to show only a piece.

The custom MongoDB aggregation pipeline stage ```$probe``` emits the number of messages it has seen per minute to a Kafka topic. If the topics in your data pipeline are partitioned you should combine this with a grouping pipeline as shown above. You can give your probes a name in order to distinguish the various places where you've put them.

A common error is to create more than one stream from a Kafka topic. This is not allowed within the same topology. So you first create a stream from the topic and then connect that stream to other streams.

## Building It

You can build the tool with ```mvn clean package```. This will produce a self-contained JAR-file in the ```target``` directory with the form ```pincette-json-streams-<version>-jar-with-dependencies.jar```. You can launch this JAR with ```java -jar``` and then the rest of the command line.

The total number of threads across all the instances should not exceed the number of partitions for the Kafka topics. Additional threads will be idle.

You can run the JVM with the option ```-mx128m```.

## The Command Line

The command line has the commands ```build``` and ```run```. You can also use ```help``` to get a short overview, as well as ```build help``` and ```run help```.

|Command|Option|Mandatory|Description|
|---|---|---|---|
|build|-f \| --file|Yes|A file with an array of topologies (applications). It will inline everything resulting in one big JSON file. When no MongoDB collection has been specified it will dump the result on the terminal.|
||-c \| --collection|No|A MongoDB collection to which the generated file will be written using the application name as the ID. This means that each topology in the given array will go to its own document. Existing documents are overwritten.|
|run|-f \| --file|No|A file with an array of topologies (applications). It builds and then runs all the topologies.|
||-c \| --collection|No|A MongoDB collection with topologies, which are all run. If neither a collection nor a file is given the tool will try to take the MongoDB collection from the configuration.|
||-q \| --query|No|A MongoDB query to select the topologies to run.|

## Configuration

The configuration is managed by the 
[Lightbend Config package](https://github.com/lightbend/config). By default it will try to load ```conf/application.conf```. An alternative configuration may be loaded by adding ```-Dconfig.resource=myconfig.conf```, where the file is also supposed to be in the ```conf``` directory. The following entries are available.

|Entry|Description|
|---|---|
|environment|The name of the environment, e.g. "tst", "prd". When it is present it will be used for the aggregates, reactors and as a replacement for occurrences of ```${ENV}```.|
|kafka|All Kafka settings come below this entry. So for example, the setting ```bootstrap.servers``` would go to the entry ```kafka.bootstrap.servers```.|
|kafka.num.stream.threads|The number of worker threads per instance.|
|kafka.replication.factor|Check your Kafka cluster settings for this.|
|logLevel|The log level as defined in [java.util.logging.Level](https://docs.oracle.com/javase/8/docs/api/java/util/logging/Level.html).|
|logTopic|The Kafka topic where the errors will be logged in the [Elastic Common Schema](https://www.elastic.co/guide/en/ecs/current/index.html).|
|mongodb.uri|The MongoDB connection URL.|
|mongodb.database|The MongoDB database.|
|mongodb.collection|The default MongoDB collection where builds are written and run from.|

## Docker Image

Docker images can be found at [https://hub.docker.com/repository/docker/jsoneventsourcing/pincette-json-streams](https://hub.docker.com/repository/docker/jsoneventsourcing/pincette-json-streams). You should add a configuration layer with a Docker file that looks like this:

```
FROM registry.hub.docker.com/jsoneventsourcing/pincette-json-streams:<version>
COPY conf/tst.conf /conf/application.conf
```

So wherever your configuration file comes from, it should always end up at ```/conf/application.conf```.

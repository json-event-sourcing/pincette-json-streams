# Configuration

The configuration is managed by the 
[Lightbend Config package](https://github.com/lightbend/config). By default it will try to load `conf/application.conf`. An alternative configuration may be loaded by adding `-Dconfig.resource=myconfig.conf`, where the file is also supposed to be in the `conf` directory. Yet another alternative is referring to a file with `-Dconfig.file=myconfig.conf`, where the file is found in the working directory. The following entries are available.

|Entry|Mandatory|Description|
|---|---|---|
|contextPath|No|The context path used in generated URLs.|
|environment|No|The name of the environment, e.g. "tst", "prd". When it is present it will be used for the applications as a replacement for occurrences of `${ENV}`.|
|groupIdSuffix|No|When set `-<suffix>` will be added to the consumer group ID of the applications. Without it the IDs are just the application names. With this configuration field you can run multiple environments on the same Kafka cluster.|
|kafka|Yes|All Kafka settings come below this entry. So for example, the setting `bootstrap.servers` would go to the entry `kafka.bootstrap.servers`.|
|kafka.num.stream.threads|No|The number of worker threads per instance.|
|kafka.replication.factor|No|Check your Kafka cluster settings for this.|
|keepAliveInterval|No|The interval between keep-alive signals from a running instance. These are saved in the same collection as in `mongodb.collection`. The default value is `10s`.|
|leaderInterval|No|The interval between attempts to become or stay the leader of a group of instances. The leader record is saved in the same collection as in `mongodb.collection`. The leader distributes the work among all instances. The default value is `10s`.|
|log|No|The log level for the applications and JSON Streams runtime can be changed here. See also [Logging](logging.md).|
|logLevel|No|The log level as defined in [java.util.logging.Level](https://docs.oracle.com/javase/8/docs/api/java/util/logging/Level.html). The default level is `SEVERE`.|
|logTopic|No|The Kafka topic where the errors will be logged in the [Elastic Common Schema](https://www.elastic.co/guide/en/ecs/current/index.html).|
|mongodb.uri|Yes|The MongoDB connection URL.|
|mongodb.database|Yes|The MongoDB database.|
|mongodb.collection|No|The default MongoDB collection where builds are written and run from. If it is not provided then it should be present in the command-line.|
|plugins|No|The directory from where the plugins are loaded.|
|restartBackoff|No|The waiting time before a application that was in an error state will be restarted. The default value is `10s`.|
|topologyTopic|No|When this entry is present topology life cycle events will be published to it.|
|work.averageMessageTimeEstimate|No|The estimated average time it takes to process one message. This is used in message lag calculations. The default value is 50 milliseconds.|
|work.excessMessageLagTopic|No|If this Kafka topic is provided, then the calculated overall message lag will be published on it. This is a JSON message with the fields `_id`, `excessMessageLag` and `time`.|
|work.instancesTopic|No|If this Kafka topic is provided, then a message with all the running instances and their work is published on it by the leader.|
|work.maximumAppsPerInstance|No|The maximum number of applications a running instance is willing to pick up. The default value is 10.|

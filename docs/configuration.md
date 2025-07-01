# Configuration

The configuration is managed by the 
[Lightbend Config package](https://github.com/lightbend/config). By default it will try to load `conf/application.conf`. An alternative configuration may be loaded by adding `-Dconfig.resource=myconfig.conf`, where the file is also supposed to be in the `conf` directory. Yet another alternative is referring to a file with `-Dconfig.file=myconfig.conf`, where the file is found in the working directory. The following entries are available, but you can add your own as well and use them in the application parameters.

If you are on AWS you can load secrets by specifying AWS Secrets Manager ARNs as values of configuration entries. The secret string will become the real value. AWS secrets are actually JSON strings. You can extract just one field from a secret with an expression like `<ARN>[<field>]`.

|Entry|Mandatory|Description|
|---|---|---|
|backgroundInterval|No|The interval for background work such as becoming the cluster leader and keep alive signals. The default is 5s.|
|backpressureTimeout|No|When set to a value larger than zero, it will set a timeout after which an error signal is sent if no backpressure signal was received from downstream. This causes the application where it happens to be restarted. The logs will detail where that was.|
|batchSize|No|The size of the batches that are written to Kafka. This determines the overall batch size throughout the system. The default value is 100. Reduce it if your messages are rather large, which saves memory.|
|batchTimeout|No|The time after which a message batch is flushed if it is not yet full and when additional messages are requested. The default is 50ms.|
|contextPath|No|The context path used in generated URLs.|
|environment|No|The name of the environment, e.g. "tst", "prd". When it is present it will be used for the applications as a replacement for occurrences of `${ENV}`.|
|groupIdSuffix|No|When set `-<suffix>` will be added to the consumer group ID of the applications. Without it the IDs are just the application names. With this configuration field you can run multiple environments on the same Kafka cluster.|
|kafka|Yes|All Kafka settings come below this entry. So for example, the setting `bootstrap.servers` would go to the entry `kafka.bootstrap.servers`.|
|kafka.num.stream.threads|No|The number of worker threads per instance.|
|kafka.replication.factor|No|Check your Kafka cluster settings for this.|
|log|No|The log level for the applications and JSON Streams runtime can be changed here. See also [Telemetry](telemetry.md).|
|logLevel|No|The log level as defined in [java.util.logging.Level](https://docs.oracle.com/javase/8/docs/api/java/util/logging/Level.html). The default level is `SEVERE`.|
|mongodb.uri|Yes|The MongoDB connection URL.|
|mongodb.database|Yes|The MongoDB database.|
|mongodb.collection|No|The default MongoDB collection where builds are written and run from. If it is not provided then it should be present in the command-line. Make sure to create a [TTL](https://www.mongodb.com/docs/manual/core/index-ttl/) index on the field `aliveAt`. Set the expiration period higher than the `keepAliveInterval` and `leaderInterval` configuration settings. This index is needed for cases where instances suddenly stop.|
|namespace|No|A name to make a distinction between several JSON Streams clusters in the same environment. The default value is `json-streams`.|
|otlp.grpc|No|The OpenTelemetry endpoint for logs and metrics. It should be a URL like `http://localhost:4317`.|
|otlp.http|No|The OpenTelemetry endpoint for traces. It should be a URL like `http://localhost:4318`. You only need it if you run the [`traces` application](https://github.com/json-event-sourcing/pincette-json-streams/tree/master/apps/traces).|
|plugins|No|The directory from where the plugins are loaded.|
|throttleTime|No|Since the Kafka poller has to run in its own thread, backpressure co-ordination may be too slow, which can cause topic publisher batch queues to grow too large. If the messages are large as well, then you may have memory issues. With this configuration entry you can throttle the poller. This has to stay under the value of the Kafka parameter `max.poll.interval.ms`. Note that this causes latency spikes.|
|tracesTopic|No|The Kafka topic to which the event traces are sent. If it is not set, then no tracing will happen.|
|work|No|Configuration for the leader to divide the work amongst the instances.|
|work.averageMessageTimeEstimate|No|The estimated average time it takes to process one message. This is used in message lag calculations. The default value is 20 milliseconds. With this parameter you can tune the capacity of one instance.|
|work.excessMessageLagTopic|No|If this Kafka topic is provided, then the calculated overall message lag will be published on it. This is a JSON message with the fields `_id`, `desired`, `running` and `time`.|
|work.instancesTopic|No|If this Kafka topic is provided, then a message with all the running instances and their work is published on it by the leader.|
|work.maximumAppsPerInstance|No|The maximum number of applications a running instance is willing to pick up. The default value is 50.|
|work.interval|No|The time between two work sessions. The default is one minute. The precision is less than 10 seconds.|


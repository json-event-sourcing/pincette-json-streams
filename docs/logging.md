# Logging

The JSON Streams runtime uses the Java logger `net.pincette.json.streams`. You can set its level in the configuration entry `log`. There are the lower level loggers. Set their level to `FINEST` to see the tracing.

|Logger|Description|
|---|---|
|net.pincette.json.streams.build|Shows the JSON of the built apps, before they are started.|
|net.pincette.json.streams.config|Writes the configuration that is used to run the instance.|
|net.pincette.json.streams.keepalive|Traces the heart beat of the instances.|
|net.pincette.json.streams.leader|Traces the operations of the leader of the instances.|

Here is a configuration example:

```
log.net.pincette.json.streams = "INFO"
log.net.pincette {
  json.streams {
    keepalive = "FINEST"
    leader = "FINEST"
  }
  mongo.streams = "INFO"
}
```

There is also a Java logger for each application. Its name is simply the name of the application. You can change the log level directly under the `log` configuration entry.

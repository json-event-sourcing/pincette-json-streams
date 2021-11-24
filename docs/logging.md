# Logging

The JSON Streams runtime uses the Java logger `net.pincette.json.streams`. You can set its level in the configuration entry `log`. There are currently three lower level loggers. These are `net.pincette.json.streams.change`, for tracing the MongoDB watch on the JSON Streams management collection, `net.pincette.json.streams.keepalive`, for tracing the heart beat of the runtime instances, and `net.pincette.json.streams.leader`, for tracing the leader management. Set their level to `FINEST` to see the tracing. Here is a configuration example:

```
log.net.pincette {
  json {
    streams = "INFO"
    streams.change = "FINEST"
    streams.keepalive = "FINEST"
    streams.leader = "FINEST"
  }
  mongo.streams = "INFO"
}
```

There is also a Java logger for each application. Its name is simply the name of the application. You can change the log level directly under the `log` configuration entry.
# Joining Streams

It is possible to join two streams or topics using any criterion that can be expressed in terms of the messages themselves. The join is always done within a configured time window. A join part has the following fields:

|Field|Mandatory|Description|
|---|---|---|
|left|Yes|An object with two fields. The first is one of `fromCollection`, `fromStream` or `fromTopic`, which are defined in [streams](streams.md). The second is the `on` field, which is a MongoDB query expression.|
|right|Yes|An object with two fields. The first is one of `fromCollection`, `fromStream` or `fromTopic`, which are defined in [streams](streams.md). The second is the `on` field, which is a MongoDB query expression.|
|name|Yes|The name of the output stream. Other streams can connect to it with that name.|
|toCollection|No|The name of the MongoDB collection to which the messages are sent.|
|toString|No|A boolean field that, when the `toTopic` field is present, will cause the JSON messages to be written as strings.| 
|toTopic|No|The name of the Kafka topic to which this stream will be connected as a producer.|
|type|Yes|The value is always `join`.|
|window|No|The time window in milliseconds. The default is an infinite window.|

The produced messages will have the fields `left` and `right`, with the respective messages from the incoming streams.

Two MongoDB collections will be generated. The name is structured as `<app>-<part>-join-<side>[-<env>]`. The original messages are saved in the collections. The additional field `_join_timestamp` is added for the join window. Create indexes on `_join_timestamp` and based on the fields `left.on` and `right.on` respectively. If you use MongoDB Atlas, then you use the field `_join_timestamp` to do automatic archiving. The value of the field is epoch millis.

The following example joins the command and event streams from the above-mentioned aggregate over a time window of 5 seconds, using the correlation ID. It writes the results to the topic `string-test` serialised as strings.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "join"
    name: "joined"
    toTopic: "string-test"
    toString: true
    window: 5000
    left:
      fromStream: "plusminus-counter-command"
      on: "$_corr"
    right:
      fromStream: "plusminus-counter-event"
      on: "$_corr"
```

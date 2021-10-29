# Joining Streams

It is possible to join two streams or topics using any criterion that can be expressed in terms of the messages themselves. The join is always done within a configured time window. A join part has the following fields:

|Field|Mandatory|Description|
|---|---|---|
|left|Yes|An object with two fields. The first one is either `fromStream` or `fromTopic`. The second is the `on` field, which is a MongoDB query expression.|
|right|Yes|An object with two fields. The first one is either `fromStream` or `fromTopic`. The second is the `on` field, which is a MongoDB query expression.|
|name|Yes|The name of the output stream. Other streams can connect to it with that name.|
|toString|No|A boolean field that, when the `toTopic` field is present, will cause the JSON messages to be written as strings.| 
|toTopic|No|The name of the Kafka topic to which this stream will be connected as a producer.|
|type|Yes|The value is always `join`.|
|window|Yes|The time window in milliseconds.|

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

# Merging Streams

You can merge several streams or topics with a merge part. All the messages from all the input sources will be sent to the output. A merge part has the following fields:

|Field|Mandatory|Description|
|---|---|---|
|fromCollections|Exclusive with `fromTopics` and `fromStreams`|An array of MongoDB collection names. See also [streams](streams.md).|
|fromStreams|Eclusive with `fromTopics` and `fromCollections`|An array of stream names.|
|fromTopics|Exclusive with `fromStreams` and `fromCollections`|An array of Kafka topic names.|
|name|Yes|The name of the output stream. Other streams can connect to it with that name.|
|toCollection|No|The name of the MongoDB collection to which the messages are sent.|
|toString|No|A boolean field that, when the `toTopic` field is present, will cause the JSON messages to be written as strings.| 
|toTopic|No|The name of the Kafka topic to which this stream will be connected as a producer.|
|type|Yes|The value is always `merge`.|

This is an example:

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "merge"
    name: "merged-init"
    fromStreams:
      - "absences-init"
      - "activities-init"
      - "owners-init"
      - "subscriptions-init"
```

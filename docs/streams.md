# Streams

A stream is an aggregation pipeline with stages that transform JSON messages. They can consume messages from other streams of Kafka topics. They can produce messages to other streams that are connected to it or to Kafka topics. The transformations are [MongoDB aggregation pipeline stages](https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/). The following is a simple example that filters out messages based on a field in them.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-filtered"
    pipeline:
      - $match:
          shouldFilter: true
```

When pipeline stages are large it is more convenient to put them in a separate file. A file can contain just one stage or an array of stages. This example moves the stage to another file.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-filtered"
    pipeline:
      - "filter.yml"
```

Where the file `filter.yml` is then like this:

```yaml
$match:
  shouldFilter: true
```

## Supported Fields

|Field|Mandatory|Description|
|---|---|---|
|fromStream|Exclusive with `fromTopic`|The name of the stream to which this stream will be connected.|
|fromTopic|Exclusive with `fromStream`|The name of the Kafka topic to which this stream will be connected as a consumer.|
|name|Yes|The part name of the stream. Other parts can connect to this stream with that name. It should be unique within the application.|
|pipeline|No|An array of pipeline stages. These are either objects or relative filenames, in which case the stage is loaded from there.|
|toString|No|A boolean field that, when the `toTopic` field is present, will cause the JSON messages to be written as strings.| 
|toTopic|No|The name of the Kafka topic to which this stream will be connected as a producer.|
|type|Yes|The value is always `stream`.|

## Supported Aggregation Stages

These are described on the [aggregation pipeline stages](aggregation_stages.md#aggregation-pipeline-stages) page.
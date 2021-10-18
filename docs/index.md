# Introduction

JSON Streams is a low-code event streaming platform, where messages are exchanged in JSON format. With the platform you can:

* Manage state in an event-driven way.
* Transform event streams.
* Validate event streams.
* Join event streams.
* Merge multiple event streams into one.

The languages you need to work with it are the [MongoDB Operator](https://docs.mongodb.com/manual/reference/operator/query/) language and [JSLT](https://github.com/schibsted/jslt). The backbone of an event stream is a [MongoDB aggregation pipeline](https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/). This is a streaming implementation of the aggregation language. State is managed with aggregates that have reducers written in JSLT, which is a JSON transformation language. A reducer receives a command and the current state of an aggregate instance. It returns the new state. The difference is saved and published as an event.

The platform comes with a Docker image. Multiple instances will form a cluster and divide the work amongst them. It needs [Kafka](https://kafka.apache.org) and MongoDB to run. It can auto-scale based on message lag constraints.
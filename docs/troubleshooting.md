# Troubleshooting

Creating declarative data pipelines like this can quickly become quite complex. So you need to be able to debug them. There are a few tools to help with this.

Wherever you write MongoDB expressions that are not used to actually query the database, you can use the `$trace` operator. It can be wrapped around another expression. The result is the same, but as a side effect some tracing is written to the log. The Java logger for this is `net.pincette.mongo.expressions`. Its level is `INFO` by default.

The same technique is available for JSLT scripts. There you have the custom function `trace`, which can also be wrapped around another expression. The Java logger for this is `net.pincette.json.streams`. Its level is `INFO` by default.

With the custom MongoDB aggregation pipeline stage `$trace` you get the contents of whatever goes through it. If you set its value to `null` the entire message is traced. Alternatively you can provide a MongoDB expression to show only a piece. The Java logger for this is `net.pincette.mongo.streams`. Its level is `INFO` by default.

The custom MongoDB aggregation pipeline stage `$probe` emits the number of messages it has seen per minute to a Kafka topic. If the topics in your data pipeline are partitioned you should combine this with a grouping pipeline as shown above. You can give your probes a name in order to distinguish the various places where you've put them.

If you have doubts about how the applications are spread across the running instances, you can look in the logs if the log level is at least `INFO`. You will also see which instance is the leader. The MongoDB collection where the built applications are saved, which is set in the `mongodb.collection` configuration entry, also contains a document for each running instance and for the leader. Make sure there is a [TTL](https://www.mongodb.com/docs/manual/core/index-ttl/) index on the `aliveAt` field with an expiration period that is higher than the `keepAliveInterval` and `leaderInterval` settings.
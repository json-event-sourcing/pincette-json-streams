# Aggregation Pipeline Stages

This page lists the supported [MongoDB aggregation pipeline stages](https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/), with possible deviations. Inside the stages the [aggregation pipeline operators](aggregation_operators.md) can be used.

<a id="addFields"></a>
### [$addFields](https://docs.mongodb.com/manual/reference/operator/aggregation/addFields/)

The following example pulls three fields from the incoming message and adds them up to form the new field `totalScore`.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-added"
    pipeline:
      - $addFields:
          totalScore:
            $add:
              - "$totalHomework"
              - "$totalQuiz"
              - "$extraCredit"
```

<a id="bucket"></a>
### [$bucket](https://docs.mongodb.com/manual/reference/operator/aggregation/bucket/)

A bucket is a grouping aggregation over a message stream. The state of the aggregation is kept in a MongoDB collection. The extension field `_collection` can be used to specify that collection. If the field is not present a collection name will be generated from a digest of the bucket expression. This means that if you change the expression the collection will change too. This effectively resets the aggregation.

The document in the MongoDB collection will have the technical field `_timestamp`, which can be used to create a TTL index.

The accumulator expressions [`$addToSet`](https://docs.mongodb.com/manual/reference/operator/aggregation/addToSet/#mongodb-group-grp.-addToSet), [`$avg`](https://docs.mongodb.com/manual/reference/operator/aggregation/avg/#mongodb-group-grp.-avg), [`$count`](https://www.mongodb.com/docs/manual/reference/operator/aggregation/count-accumulator/#mongodb-group-grp.-count), [`$last`](https://www.mongodb.com/docs/manual/reference/operator/aggregation/last/#mongodb-group-grp.-last), [`$max`](https://docs.mongodb.com/manual/reference/operator/aggregation/max/#mongodb-group-grp.-max), [`$mergeObjects`](https://docs.mongodb.com/manual/reference/operator/aggregation/mergeObjects/#mongodb-expression-exp.-mergeObjects), [`$min`](https://docs.mongodb.com/manual/reference/operator/aggregation/min/#mongodb-group-grp.-min), [`$push`](https://docs.mongodb.com/manual/reference/operator/aggregation/push/#mongodb-group-grp.-push), [`$stdDevPop`](https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevPop/#mongodb-group-grp.-stdDevPop) and [`$sum`](https://docs.mongodb.com/manual/reference/operator/aggregation/sum/#mongodb-group-grp.-sum) are supported.

Note that this is a streaming implementation. So, instead of receiving one message with the final result as you would with a MongoDB collection, you get all incremental changes to the aggregation.

The following example groups all message per the `year_born` field and pours them in five buckets. Those that don't fit in one of the buckets fall into the "Other" bucket. The output messages will have an array of artists and the count.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-grouped"
    pipeline:
      - $bucket:
          groupBy: "$year_born"
          boundaries:
            - 1840
            - 1850
            - 1860
            - 1870
            - 1880
          default: "Other"
          output:
            count:
              $sum: 1
            artists:
              $push:
                name:
                  $concat:
                    - "$first_name"
                    - " "
                    - "$last_name"
                year_born: "$year_born"
```

### [$count](https://docs.mongodb.com/manual/reference/operator/aggregation/count/)

The same remarks apply as for the `$bucket` stage. The following example counts the scores that are higher than 80.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-counted-scores"
    pipeline:
      - $match:
          score:
            $gt: 80
      - $count: "passing_scores"
```

### $deduplicate

With this extension stage you can filter away duplicate messages according to a given expression, which goes in the `expression` field. The `collection` field denotes the MongoDB collection that is used for the state. If you use Atlas Archiving you can keep the collection small. Use the `_timestamp` field for this. Its value is epoch millis. The optional field `cacheWindow` is the time in milliseconds that messages are kept in a cache for duplicate checking. The default value is 1000. An example:

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-unique"
    pipeline:
      - $deduplicate:
          expression:
            $concat:
              - "$_id"
              _ "$_corr"              
          collection: "deduplicate-state"
          cacheWindow: 60000          
```

The commit to the collection happens when the next stage asks for more messages, which means it has successfully the previous ones. When the next stage is buffered and the previous stage hasn't yet sent as many messages as the buffer size when it completes, then those last messages won't be committed. They will be offered again when the application restarts.

### $delay

With this extension stage you can send a messages to a Kafka topic with a delay. The order of the messages is not guaranteed. The stage is an object with two fields. The
`duration` field is the number of milliseconds the operation is delayed. The `topic` field is the Kafka topic to which the message is sent after the delay.
Note that message loss is possible if there is a failure in the middle of a delay.

The main use-case for this stage is retry logic. The following example submits messages to some HTTP-endpoint. The second part forwards successful submissions to the `my-submitted-messages` Kafka topic. The third part filters on messages where the service was unavailable. After five seconds the messages are sent to the original topic. Note that in this case the five seconds are applied to every individual message.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "submit"
    fromTopic: "my-messages"
    pipeline:
      - $http:
          url: "https://my-domain/"
          method: "POST"
          body: "$$ROOT"
          as: "result"
  - type: "stream"
    name: "success"
    fromStream: "submit"
    toTopic: "my-submitted-messages"
    pipeline:
      - $match:
          httpError:
            $exists: false
  - type: "stream"
    name: "failed"
    fromStream: "submit"
    pipeline:
      - $match:
          httpError.statusCode: 503
      - $unset: "httpError"
      - $delay:
          duration: 5000
          topic: "my-messages"

```

### $delete

This is an extension stage to delete documents from a MongoDB collection. It has the mandatory fields `from` and `on`. The former is a MongoDB collection. The latter is either a string or a non-empty array of strings. It represents fields in the incoming JSON object. The stage deletes records from the collection for which the given fields have the same values as for the incoming JSON object. The output of the stage is the incoming JSON object. In case of failure it will retry forever. Here is an example:

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-deleted"
    pipeline:
      - $delete:
          from: "my-collection"
          on: "id"     
```

### [$group](https://docs.mongodb.com/manual/reference/operator/aggregation/group/)

The same remarks apply as for the [`$bucket`](#bucket) stage. The following example groups the messages by the `item` field, which becomes the `_id` of the output messages. The `totalSaleAmount` output field is the sum of the multiplications of the `price` and `quantity` fields. The second stage then selects those for which the field is greater than or equal to 100.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-grouped"
    pipeline:
      - $group:
          _id: "$item"
          totalSaleAmount:
            $sum:
              $multiply:
                - "$price"
                - "$quantity"
      - $match:
          totalSaleAmount:
            $gte: 100
  
```

### $http

With this extension stage you can "join" a JSON HTTP API with a data stream or cause side effects to it. The object supports the following fields:

|Field|Mandatory|Description|
|---|---|---|
|as|No|This output field will carry the response body. Without it the response body is ignored.|
|body|No|The result of this expression will be used as the request body.|
|headers|No|The expression should yield an object. Its contents will be added as HTTP headers. Array values will result in multi-valued headers.|
|method|Yes|The HTTP method. The expression should yield a string.|
|sslContext|No|This object can be used for client-side authentication. Its `keyStore` field should refer to a PKCS#12 key store file. Its `password` field should provide the password for the keys in the key store file.|
|unwind|No|When this Boolean field is set to `true` and when the response body is a JSON array, then for each entry in the array a message will be produced with the array entry in the `as` field. If the array is empty no messages are produced at all.|
|url|Yes|The URL that will be called. The expression should yield a string.|

HTTP errors are put in the `httpError` field, which contains the field integer `statusCode` and `body`, which can be any JSON value.

In the following example the URL is constructed with the `id` field in the incoming message. The header `X-My-Header` is populated with the `meta` field. The output message will have the extra `result` field.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-http"
    pipeline:
      - $http:
          url:
            $concat:
              - "https://my-domain/"
              - "$id"
          method: "GET"
          headers:
            X-My-Header: "$meta"
          as: "result"
```
### $jq

This extension stage transforms the incoming message with a [JQ](https://github.com/eiiches/jackson-jq) script. Its specification should be a string. If it starts with `resource:/` the script will be loaded as a class path resource, otherwise it is interpreted as a filename. If the transformation changes or adds the `_id` field then that will become the key of the outgoing message.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-jslt"
    pipeline:
      - $jq: "my_script.jq"
```

### $jslt

This extension stage transforms the incoming message with a [JSLT](https://github.com/schibsted/jslt) script. Its specification should be a string. If it starts with `resource:/` the script will be loaded as a class path resource, otherwise it is interpreted as a filename. If the transformation changes or adds the `_id` field then that will become the key of the outgoing message.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-jslt"
    pipeline:
      - $jslt: "my_script.jslt"
```

### $lag

This extension stage fetches the message lag for the Kafka cluster. It adds a JSON object to the message in the field that is defined in the `as` field of the specification. The keys of the generated object are the consumer groups. The values are objects where the keys are the topics and the values are again objects. In there each partition has a key with a number as the value. An example:

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "message-lag"
    fromTopic: "message-lag-trigger"
    toTopic: "message-lag"
    pipeline:
      - $lag:
          as: messageLag      
```

The generated messages look like this:

```json
{
  "messageLag": {
    "my-consumer": {
      "topic1": {
        "0": 0,
        "1": 6,
        "2": 10,
        "3": 0
      },
      "topic2": {
        "0": 0,
        "1": 11,
        "2": 0,
        "3": 3
      }
    }
  }
}  
```
### $log

With this extension stage you can write something to a Java logger. The incoming message will come out unchanged. The expression of the stage is an object with at least the field `message`. The expression in this field will be converted to a string after evaluation. The logger to which the entries are sent has the same name as the application. The optional field `level` can be used to set the Java log level. If its not there, the default log level will be used.

You can extra fields in the optional `attributes` field. They will be added to the emitted OpenTelemtry attributes.

If the incoming message has the `_corr` field, then the log entry will have a trace ID that is derived from it by removing the dashes from the UUID. A span ID will also be added. Its value will be the first half of the trace ID, because that is how JSON Streams defines the root span ID.

The following example logs the field `subobject.field` with the log level `INFO`.

```yaml
---
application: "my-app"
version: "1.0.0"
parts:
  - type: "stream"
    name: " my-stream"
    fromTopic: "my-messages"
    pipeline:
      - $log:
          message: $subobject.field"
          level: "INFO"
          attributes:          
            dataset: "test"
            original: "$value"
```

### [$lookup](https://docs.mongodb.com/manual/reference/operator/aggregation/lookup/)

The extra optional Boolean field `inner` is available to make this pipeline stage behave like an inner join instead of an outer left join, which is the default. When the other optional Boolean field `unwind` is set, multiple objects may be returned where the `as` field will have a single value instead of an array. In this case the join will always be an inner join. With the unwind feature you can avoid the accumulation of large arrays in memory. When both the extra fields `connectionString` and `database` are present the query will go to that database instead of the default one.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-lookup"
    pipeline:
      - $lookup:
          from: "my-collection"
          as: "result"
          let:
            field1: "$field1"
            field2: "$field2"
          pipeline:
            - $match:
                record.field1: "$$field1"
                record.field2: "$$field2"
                record.error:
                  $exists: false  
```

### [$match](https://docs.mongodb.com/manual/reference/operator/aggregation/match/)

See several other examples. The expressions support the [query operators](query.md).

### [$merge](https://docs.mongodb.com/manual/reference/operator/aggregation/merge/)

Pipeline values for the `whenMatched` field are currently not supported. The `into` field can only be the name of a collection. The database is always the one from the configuration. The optional `key` field accepts an expression, which is applied to the incoming message. When it is present it will be used as the value for the `_id` field in the MongoDB collection. The output of the stream is whatever has been updated to or taken from the MongoDB collection. The value of the `_id` field of the incoming message will be kept for the output message.

### [$out](https://docs.mongodb.com/manual/reference/operator/aggregation/out/)

Because streams are infinite this operator behaves like `$merge` with the following settings:

```yaml
into: "<output-collection>"
on: "_id"
whenMatched: "replace"
whenNotMatched: "insert"
```

### $per

This extension stage is an object with the mandatory fields `amount` and `as`. It accumulates the amount of messages and produces a message with only the field denoted by the `as` field. The field is an array of messages. With the optional `timeout` field, which is a number of milliseconds, a batch of messages can be emitted before it is full. In that case the length of the generated array will vary. An example

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-batch"
    pipeline:
      - $per:
          amount: 100
          as: "batch"
          timeout: 500      
```

### $probe

With this extension stage you can monitor the throughput anywhere in a pipeline. The specification is an object with the fields `name` and `topic`, which is the name of a Kafka topic. It will write messages to that topic with the fields `name`, `minute` and `count`, which represents the number of messages it has seen in that minute. Note that if your pipeline is running on multiple topic partitions you should group the messages on the specified topic by the name and minute and sum the count. That is because every instance of the pipeline only sees the messages that pass on the partitions that are assigned to it.

Say you have an application with a probe like the following:

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
      - $probe:
          name: "filtering"
          topic: "pipeline-probe-part      
```

The application that then aggregates all topic partitions could then look like this:

```yaml
---
- application: "json-streams-probe"
  version: "1.0"
  parts:
    - type: "stream"
      name: "group-probe"
      fromTopic: "pipeline-probe-part"
      toTopic: "pipeline-probe"
      pipeline:
        - $group:
            _id:
              name: "$name"
              minute: "$minute"
            _collection: "group-probe"
            count:
              $sum: "$count"
        - $set:
            name: "$_id.name"
            minute: "$_id.minute"
            perSecond:
              $round:
                - $divide:
                    - "$count"
                    - 60
                - 1
```

The counts are grouped over the probe name and then the `perSecond` is added. If all other applications write their probes to the `pipeline-probe-part` topic, then you only need to run the above application once. Otherwise you would pull out the pipeline and embed it in your applications.

### [$project](https://docs.mongodb.com/manual/reference/operator/aggregation/project/)
In the following example the resulting messages will only contain the calculated field `totalScore`, together with the field `_id`, which is output by default.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-added"
    pipeline:
      - $addFields:
          totalScore:
            $add:
              - "$totalHomework"
              - "$totalQuiz"
              - "$extraCredit"
      - $project:
          totalScore: 1  
```

### [$redact](https://docs.mongodb.com/manual/reference/operator/aggregation/redact/)

In the following example objects with the field `age` with a value greater than 17 will be processed further. The others will be removed.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-redacted"
    pipeline:
      - $redact:
          $cond:
            if:
              $gt:
                - "$age"
                - 17
            then: "$$DESCEND"
            else: "$$PRUNE"
```

### [$replaceRoot](https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot/)

In the following example the contents of the field `subobject` will become the output message. 

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-replaced"
    pipeline:
      - $replaceRoot:
          newRoot: "$subobject"
```

### [$replaceWith](https://docs.mongodb.com/manual/reference/operator/aggregation/replaceWith/)

This stage works like the `$replaceRoot` stage, but without the `newRoot` field. The previous example would become the following:

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-replaced"
    pipeline:
      - $replaceWith: "$subobject"
```

### $send

With this extension stage you can send a message to a Kafka topic. The stage is an object with a `topic` field, which is the Kafka topic to which the message is sent. The main use-case for this stage is dynamic routing of messages to topics. The messages that are processed by the following example have the field `topic`. They are forwarded to the corresponding topic.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    pipeline:
      - $send:
          topic: "$topic"
```

### [$set](https://docs.mongodb.com/manual/reference/operator/aggregation/set/)

This is a synonym for the [`$addFields`](#addFields) stage.

### $setKey

With this extension stage you can change the Kafka key of the message without changing the message itself. The stage expects an expression, the result of which will be converted to a string. Changing the key will repartition the stream. A use-case for this is serialising a message stream based on some business key.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-key"
    pipeline:
      - $setKey:
          $concat:
            - "$field1"
            - "$field2"      
```

### $signJwt

A `$http` stage sometime needs a signed JSON Web Token for its request. With this extension stage you can generate and sign one and put it in a temporary field, which you remove after the `$http` stage. The following fields are ssupported:

|Field|Mandatory|Description|
|---|---|---|
|as|Yes|The name of the field that will receive the JWT as a string.|
|aud|No|The string expression that becomes the standard `aud` JWT claim.|
|claims|No|The object expression that contains any non-standard JWT claims.|
|iss|No|The string expression that becomes the standard `iss` JWT claim.|
|kid|No|The string expression that becomes the standard `kid` JWT claim.|
|privateKey|Yes|The string that represents the private key. You would pull that from the configuration. Don't put it the application directly.|
|sub|No|The string expression that becomes the standard `sub` JWT claim.|
|ttl|No|The time to live in seconds for the token. The default is five seconds. Making this longer reduces CPU usage.|

An example:

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-enriched"
    pipeline:
      - $signJwt:
          privateKey: "${SECRET.larsPrivateKey}"
          as: "_token"
          ttl: 2592000
          sub: "$_jwt.sub"
          claims:
            roles: "$_jwt.roles"
      - $http:
          url: "${END_POINT}"
          method: "POST"
          headers:
            Authorization:
              $concat:
                - "Bearer "
                - "$_token"
          body:
            traceId:
              $jes-uuid: null
            data: "my data"
          as: "response"
      - $unset: "_token"
```

### $s3Attachments

This extension stage lets you post a number of S3-objects as attachments to an HTTP endpoint. The default content type is `multipart/mixed`. You can change this in the headers. The object supports the following fields:

|Field|Mandatory|Description|
|---|---|---|
|attachments|Yes|An array of objects with the mandatory fields `bucket` and `key`. The `multipart/mixed ` request body will be constructed in the order of the array.  Extra fields will be used as MIME part headers. The headers `Content-Length` and `Content-Transfer-Encoding` can't be overridden.|
|headers|No|The expression should yield an object. Its contents will be added as HTTP headers. Array values will result in multi-valued headers.|
|sslContext|No|This object can be used for client-side authentication. Its `keyStore` field should refer to a PKCS#12 key store file. Its `password` field should provide the password for the keys in the key store file.|
|url|Yes|The URL that will be called. The expression should yield a string.|

HTTP errors are put in the `httpError` field, which contains the field integer `statusCode` and `body`, which can be any JSON value.

The following is an example with an input message:

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "stream"
    fromTopic: "in"
    toTopic: "out"
    pipeline:
      - $s3Attachments:
          url: "http://localhost:9000"
          attachments: "$attachments"
```

```json
{
  "_id": "87AF18F1-40FB-4C1E-9EE1-2A51927B5A4A",
  "attachments": [
    {
      "bucket": "my-bucket",
      "key": "com2012_0429nl01.pdf",
      "x-my-header": "value"      
    },
    {
      "bucket": "my-bucket",
      "key": "com2012_0444nl01.pdf"
    },
    {
      "bucket": "my-bucket",
      "key": "com2012_0445nl01.pdf"
    },
    {
      "bucket": "my-bucket",
      "key": "com2012_0448nl01.pdf"
    }
  ]
}
```

### $s3Csv

With this extension stage you can consume a CSV-file that resides in an S3-bucket. The lines are emitted as individual JSON messages. The first line will be used for the field names of the messages. The mandatory fields are `bucket` and `key`, which are the name of the bucket and the object key respectively. The optional field `separator` has the default value "\t". You must ensure the proper credentials are available, either as an IAM-role or environment variables.

The following example consumes a topic that receives all kinds of S3-events. It selects a bucket and the CSV-objects in it.


```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-app"
    fromTopic: "s3-events"
    toTopic: "csv-messages"
    pipeline:
      - $match:
          eventName:
            $regex: "/^ObjectCreated:.*$/"
          s3.bucket.name: "my-csv-bucket"
          s3.object.key:
            $regex: "/^.*.csv$/"
      - $s3Csv:
          bucket: "$s3.bucket.name"
          key: "$s3.object.key"
          separator: "\t"
```

### $s3Out

This extension stage lets you write individual JSON messages to an S3-bucket. It has only the fields `bucket` and `key`, which are the name of the bucket and the object key respectively. You must ensure the proper credentials are available, either as an IAM-role or environment variables.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    pipeline:
      - $s3Out:
          $bucket: "my-bucket"
          $key:        
            $concat:
              - "$field1"
              - "$field2"      
```

### $s3Transfer

This extension stage lets you post an S3-object fetched from an HTTP endpoint. The object supports the following fields:

| Field          | Mandatory | Description                                                                                                                                                                                                 |
|----------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url            | Yes       | The URL that will be called. The expression should yield a string.                                                                                                                                          |
| headers        | No        | The expression should yield an object. Its contents will be added as HTTP headers. Array values will result in multi-valued headers.                                                                        |
| sslContext     | No        | This object can be used for client-side authentication. Its `keyStore` field should refer to a PKCS#12 key store file. Its `password` field should provide the password for the keys in the key store file. |
| bucket         | Yes       | The name of the S3 bucket.                                                                                                                                                                                  |
| key            | Yes       | The S3 object key.                                                                                                                                                                                          |
| as             | Yes       | The name of the field that will receive the S3 Object URL.                                                                                                                                                  |
| contentType    | No        | The content type of the S3-object.                                                                                                                                                                          |
| contentLength  | No        | The content length, in bytes, of the s3-object.                                                                                                                                                             |
**Note**: if the contentLength is not provided, the S3 object will be stored in a temporal file in order to calculate its length.

HTTP errors are put in the `httpError` field, which contains the field integer `statusCode` and `body`, which can be any JSON value.

The following is an example with an input message:

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "stream"
    fromTopic: "in"
    toTopic: "out"
    pipeline:
      - $s3Transfer:
          url: "http://localhost:9000"
          bucket: "my-bucket"
          key: "image.jpg"
          as: "s3Response"
          contentType: "image/jpeg"
          contentLength: 9494
```

### $throttle

With this extension stage you can limit the throughput in a pipeline by setting the `maxPerSecond` field. An example:

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    pipeline:
      - $throttle:
          maxPerSecond: 1000
```

### $trace

This extension stage writes all JSON objects that pass through it to the Java logger `net.pinctte.mongo.streams` with level `INFO`. That is, when the stage doesn't have an expression (set to `null`). If you give it an expression, its result will be written to the logger. This can be used for pipeline debugging. This is an example:

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
      - $trace: null     
```

### [$unset](https://docs.mongodb.com/manual/reference/operator/aggregation/unset/)

In the following example the field `myfield` is removed in the output messages.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-unset"
    pipeline:
      - $unset: "myfield"
```

### [$unwind](https://docs.mongodb.com/manual/reference/operator/aggregation/unwind/)

The Boolean extension option `newIds` will cause UUIDs to be generated for the output documents if the given array was not absent or empty. If the incoming messages have the `values` array, then there will be as many output messages as there are values in that array. For each, the `values` field will have the corresponding single value.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-messages"
    toTopic: "my-messages-unwind"
    pipeline:
      - $unwind: "$values"
```

### $validate

This extension stage performs validation on the incoming messages with a [validator](validator.md). When it finds errors, the field `_error` will be set to `true`. The field `errors` will then have an array of objects with the field `location`, which will be set to the JSON pointer of the validation error. The optional field `code` will be set to whatever code the validation condition has defined.

In the following example the presence of the field `subobject.myfield` is checked. If it is absent an error entry with the location `/subobject` and the code `REQUIRED` will be added.

```yaml
---
application: "my-app"
version: "1.0"
parts:
  - type: "stream"
    name: "my-validated-messages"
    fromTopic: "my-messages"
    pipeline:
      - $validate:
          conditions:
            - subobject.myfield:
                $exists: true
              $code: "REQUIRED"          
```

The value of this stage can also be a filename.
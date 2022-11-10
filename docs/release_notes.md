# Release Notes

## 2.2.8

* Reduce memory usage.
* Switch from the Jetty HTTP client to the JDK HTTP client in the `$http` stage.
* Add `batchSize` configuration parameter.
* Fix topic publisher.
* Fix backpressure error in retry publisher.
* Allow other content types than `multipart/mixed` in the `s3Attachments` stage.

## 2.2.7

* Report and discard messages that can't be written to Kafka because they are too large.
* Add extra fields to attachment objects in the `$s3Attachments` stage, which are used as extra MIME part headers.

## 2.2.6

* Fix the stalling merge when a branch rarely emits something.

## 2.2.5

* Fix the exception in the `$log` stage when there are no parameters.
* Call the Kafka consumer API less when doing pause and resume.

## 2.2.4

* Fix exception during leader election.
* Add `restart` command to the CLI.
* Load AWS secrets only when they are really needed.

## 2.2.3

* Fix retry in `$http` when the response stream fails.
* Fix too deep exception nesting.
* Fix blocking of merge when one of the branches sends nothing.
* Add retry to `$s3Attachments` stage.
* Fix retry in `$s3Csv` when the response stream fails.

## 2.2.2

* Fix exception handling in response stream of the `$http` stage.
* Fix concurrency issue with underlying `flatMap` reactive streams processor.
* Fix branch completion issue with the underlying `Merge` reactive streams subscriber.
* Fix logging to Kafka for applications.
* Allow all Elastic Common Schema fields in the `$log` stage.

## 2.2.1

* Fix missing MongoDB codecs.

## 2.2

* Replace Kafka Streams with a Reactive Streams implementation.
* Add `fromCollection` and `toCollection` to streams and joins, which makes it possible to use MongoDB collections as sources and sinks.
* Add `fromCollections` to the merge.
* Add the `$throttle` aggregation stage, which limits the throughput of a pipeline.
* Add the `$deduplicate` aggregation stage, which lets you filter away duplicate messages based on an expression.
* Add the `$s3Csv` aggregation stage, which consumes CSV-files in an S3-bucket and emits the lines as JSON messages.
* Add the `$s3Out` aggregation stage with which you can write JSON messages into an S3-bucket.
* Add the `s3Attachments` aggregation stage, which lets you post S3-objects as a `multipart/mixed` body to an HTTP endpoint.
* Add the `$per` aggregation stage, which creates batches of messages that can be processed together down stream.
* Add the `$signJwt` aggregation stage, with which you can generate signed JSON Web Tokens.
* The auto-scaler was changed to emit the desired number of running instances instead of a synthetic metric.
* Add the `test` command to the CLI. This introduces a method to write unit tests for your applications.
* Add the `include` object with which application or pipeline parts can be included with parameter overrides.
* Support AWS Secrets Manager ARNs in configuration entries.
* Add the `config-json:` prefix to the parameters syntax.

## 2.1.1

* Fix the issue where multiple parameter references in the same string are not replaced correctly.
* Add config and build tracing.
* Fix config injection for parameters that are objects or arrays.

## 2.1

* Add the operators `$base64Encode`, `$base64Decode`, `$uriEncode`, `$uriDecode`, `$jsonToString` and `$stringToJson`.
* Add the command-line option `-d, --directory` to the commands `doc` and `dot`. This writes their outputs to the given directory using the application name to construct the filename in it.
* Make the command-line option `-a, --application` optional for the commands `doc` and `dot`. When no application is given all the deployed applications are run.
* Add the command-line option `-g, --global` to the `dot` command. It generates a graph that connects topics and applications for all the deployed applications.
* Make it possible to add prefixes and suffixes in parameter references.
* Add the `work.maximumInstances` configuration entry, which is used to normalise the excess message lag between 0 and 100.
* Fix leader and keep-alive exception.
* Fix aggregates using `dev` as the default environment.

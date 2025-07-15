# Release Notes

## 2.5.0

* Improve Reactive Streams throughput.
* Fix backpressure loss in aggregates and consume the `backpressureTimeout` configuration entry in them.
* Fix retries not stopping after an application has been restarted.
* Allow application overrides for certain configuration entries.
* Add execution time tracing for individual stages through the `_trace` field.
* Document `$jslt` operator.
* Add `$jq` operator and `$jq` aggregation pipeline stage.
* Improve test harness.

## 2.4.5

* Add configuration entry `backpressureTimeout`.

## 2.4.4

* Fix bug in application scaling.
* Add tracing for application scaling.
* Add the `INSTANCE` environment variable to set the JSON Streams instance ID instead of the default generated UUID.
* Add the metrics counters `json_streams.starts` and `json_streams.stops`.
* Fix the bug where a join could get stuck in some cases.
* Fix the bug where error signals where not propagated, preventing the restart of an application.

## 2.4.3

* Fix metrics.
* Fix the bug where an application stops consuming when an aggregate reducer throws an exception.
* Don't shut down an instance when an application fails to start. Retry instead.
* Rebuild an application when it is restarted.
* Improve the generation of event traces.
* Fix background processing that could get stuck.

## 2.4.2

* Log exceptions in `$lag` stage.
* Fix message lag output from `$lag` stage being empty.

## 2.4.1

* Add more plugin load logging.

## 2.4.0

* Switch to OpenTelemetry.
* Add event tracing.
* Add the JSLT custom function `substr`.
* Allow multiple files for the `--file` option of the `run` command.
* Add the aggregation expressions `$count` and `$last`.
* Adapt the `$log` stage to OpenTelemetry.

## 2.3.18

* Fix backpressure issue when command results in access denied error.

## 2.3.17

* Log warning when parameter substitution from a configuration value fails.
* Optimise producer batch timeout handling.
 
## 2.3.16

* Fix substitution of `ENV` when there are no other parameters in an application.

## 2.3.15

* Fix duplicate command issue.

## 2.3.14

* Reload the client certificate key store file when it has changed.

## 2.3.13

* Fix the generation of links in markdown for array fields.

## 2.3.12

* Fix the `$s3Csv` stage, which could be overwhelmed with large files, resulting in timeouts.
 
## 2.3.11

* Set the default message batch size to 100 instead of 500.
* Don't beautify the output of the `$trace` stage.

## 2.3.10

* Restart an application when a Kafka offset commit fails.
* Fix aggregate href resolution.

## 2.3.9

* Fix the overwriting of the `Content-Type` header in the `$http` stage.

## 2.3.8

* Reduce memory consumption.
* Fix setting empty headers in `$http`.
* Fix aggregate command preprocessor being ignored when the commands are written as an array instead of an object.

## 2.3.7

* Fix open stream termination bug.

## 2.3.6

* Log `onError` signals at the level of the applications.
* Fix the propagation of exceptions as `onError` signals in some cases, which would inhibit the automatic restart of the application.
* Fix exception in `$http` stage.

## 2.3.5

* Fix `$http` pipeline stage.
* Fix `$lag` timeout.
* Fix backpressure violations.
* Improve backpressure towards Kafka topics.

## 2.3.4

* Fix `$http` pipeline stage.
* Fix the shutdown procedure to avoid unnecessary duplicate messages.

## 2.3.3

* Improve aggregate performance.
* Fix topic offset commit, which could come before batches were sent to Kafka.

## 2.3.2

* Set the default batch flush timeout to 50ms.
* Add the configuration parameter `batchTimeout`.

## 2.3.1

* Fix the expansion of "href objects" in aggregates when they are in an array.

## 2.3

* Add `$replaceOne` and `$replaceAll` string operators.
* Add `preprocessor` pipeline field for aggregate commands.
* Allow an aggregate reducer to be a pipeline.
* Fix `$s3Attachments`.
* Fix the validation of stream joins, where the `window` field was still mandatory.

## 2.2.13

* Fix missing `_jwt` in aggregate and reply messages.
* Handle exceptions in reducers by generating an invalid command.

## 2.2.12

* Avoid duplication of `_jwt` field.

## 2.2.11

* Refix backpressure issue in retry publisher.
* Add the `throttleTime` configuration parameter, with which you can throttle the Kafka poll loop.

## 2.2.10

* Fix the recovery from a Kafka producer error.

## 2.2.9

* Fix aggregate performance issue caused by the batch timeout when traffic is very low.
* Improve validation error messages.

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
* Add the `preprocessor` field to aggregates. It contains a pipeline to pre-process commands before they are reduced.

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

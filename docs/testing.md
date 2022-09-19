# Testing

To write unit tests for your application you have to add the `test` folder to the top folder of the application. The `test` command in the CLI will use it to run the app in test-mode. It uses a real Kafka cluster and a real MongoDB system, so you should give it a configuration in the same way as you would for the `run` command. If you don't provide the `mongodb.database` field, a database will be created and destroyed for the test. The configuration can be as minimal as the following:

```
logLevel = "FINEST"
kafka.bootstrap.servers = "localhost:9092"
mongodb.uri = "mongodb://localhost:27017"
plugins = "plugins"
```

Below the `test` folder there are three folder levels. The first level consists of only the subfolders `topics` and `collections`. Within each you add the `from` and `to` subfolders. At the lowest level you create a subfolder for each topic or collection the application uses. The topic and collection folders contain the test-messages. The messages in a `from` subfolder are loaded in alphabetical order. The messages in a `to` subfolder are used to compare the actual output with.

If there are differences the command will report them and exit with code 1. If the test produces less messages than there are in some `to` subfolder, then the test will not complete. Set the log-level to `FINEST` follow what happens. Amongst other things, it will tell you how many messages are still expected for some topic or collection.

You can look at the unit tests in the source of JSON Streams to find some examples.
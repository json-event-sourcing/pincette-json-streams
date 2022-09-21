# Building And Running It

You can build the tool with `mvn clean package`. You can launch it like this for example:

```
java --module-path target/modules -m application run -f <filename>
```

The total number of threads across all the instances should not exceed the number of partitions for the Kafka topics. Additional threads will be idle.

You can run the JVM with the option `-mx512m`.

If you want to use another configuration in the `conf` directory, say `tst.conf`, then you can add `-Dconfig.resource=tst.conf` after `java`.

You can change the [log level](https://docs.oracle.com/javase/8/docs/api/java/util/logging/Level.html) by adding `-Djava.util.logging.config.file=logging.properties`. The `logging.properties` file would then look like this:

```
handlers=java.util.logging.ConsoleHandler
.level=INFO
java.util.logging.ConsoleHandler.level=INFO
java.util.logging.ConsoleHandler.formatter=java.util.logging.SimpleFormatter
```

Alternatively you can use the configuration parameter `logLevel`.

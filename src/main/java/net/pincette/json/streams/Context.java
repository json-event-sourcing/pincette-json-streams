package net.pincette.json.streams;

import com.mongodb.reactivestreams.client.MongoDatabase;
import com.typesafe.config.Config;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.JsonObject;
import net.pincette.mongo.Features;
import net.pincette.mongo.Validator;
import net.pincette.mongo.streams.Stage;
import org.apache.kafka.clients.producer.KafkaProducer;

class Context {
  final Config config;
  final MongoDatabase database;
  final MongoDatabase databaseArchive;
  final String environment;
  final Features features;
  final Level logLevel;
  final String logTopic;
  final Logger logger;
  final KafkaProducer<String, JsonObject> producer;
  final Map<String, Stage> stageExtensions;
  final Validator validator;

  Context() {
    this(null, null, null, null, null, null, null, null, null, null, null);
  }

  @SuppressWarnings("java:S107") // Internal constructor for immutable object.
  private Context(
      final Config config,
      final MongoDatabase database,
      final MongoDatabase databaseArchive,
      final String environment,
      final Features features,
      final Level logLevel,
      final String logTopic,
      final Logger logger,
      final KafkaProducer<String, JsonObject> producer,
      final Map<String, Stage> stageExtensions,
      final Validator validator) {
    this.config = config;
    this.database = database;
    this.databaseArchive = databaseArchive;
    this.environment = environment;
    this.features = features != null ? features : new Features();
    this.logLevel = logLevel;
    this.logTopic = logTopic;
    this.logger = logger;
    this.producer = producer;
    this.stageExtensions = stageExtensions;
    this.validator = validator;
  }

  Context doTask(final Consumer<Context> task) {
    task.accept(this);

    return this;
  }

  Context doTaskIf(final Predicate<Context> predicate, final Consumer<Context> task) {
    if (predicate.test(this)) {
      task.accept(this);
    }

    return this;
  }

  Context withConfig(final Config config) {
    return new Context(
        config,
        database,
        databaseArchive,
        environment,
        features,
        logLevel,
        logTopic,
        logger,
        producer,
        stageExtensions,
        validator);
  }

  Context withDatabase(final MongoDatabase database) {
    return new Context(
        config,
        database,
        databaseArchive,
        environment,
        features,
        logLevel,
        logTopic,
        logger,
        producer,
        stageExtensions,
        validator);
  }

  Context withDatabaseArchive(final MongoDatabase databaseArchive) {
    return new Context(
        config,
        database,
        databaseArchive,
        environment,
        features,
        logLevel,
        logTopic,
        logger,
        producer,
        stageExtensions,
        validator);
  }

  Context withEnvironment(final String environment) {
    return new Context(
        config,
        database,
        databaseArchive,
        environment,
        features,
        logLevel,
        logTopic,
        logger,
        producer,
        stageExtensions,
        validator);
  }

  Context withFeatures(final Features features) {
    return new Context(
        config,
        database,
        databaseArchive,
        environment,
        features,
        logLevel,
        logTopic,
        logger,
        producer,
        stageExtensions,
        validator);
  }

  Context with(final UnaryOperator<Context> with) {
    return with.apply(this);
  }

  Context withIf(final Predicate<Context> predicate, final UnaryOperator<Context> with) {
    return predicate.test(this) ? with.apply(this) : this;
  }

  Context withLogLevel(final Level logLevel) {
    return new Context(
        config,
        database,
        databaseArchive,
        environment,
        features,
        logLevel,
        logTopic,
        logger,
        producer,
        stageExtensions,
        validator);
  }

  Context withLogTopic(final String logTopic) {
    return new Context(
        config,
        database,
        databaseArchive,
        environment,
        features,
        logLevel,
        logTopic,
        logger,
        producer,
        stageExtensions,
        validator);
  }

  Context withLogger(final Logger logger) {
    return new Context(
        config,
        database,
        databaseArchive,
        environment,
        features,
        logLevel,
        logTopic,
        logger,
        producer,
        stageExtensions,
        validator);
  }

  Context withProducer(final KafkaProducer<String, JsonObject> producer) {
    return new Context(
        config,
        database,
        databaseArchive,
        environment,
        features,
        logLevel,
        logTopic,
        logger,
        producer,
        stageExtensions,
        validator);
  }

  Context withStageExtensions(final Map<String, Stage> stageExtensions) {
    return new Context(
        config,
        database,
        databaseArchive,
        environment,
        features,
        logLevel,
        logTopic,
        logger,
        producer,
        stageExtensions,
        validator);
  }

  Context withValidator(final Validator validator) {
    return new Context(
        config,
        database,
        databaseArchive,
        environment,
        features,
        logLevel,
        logTopic,
        logger,
        producer,
        stageExtensions,
        validator);
  }
}

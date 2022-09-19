package net.pincette.json.streams;

import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.logging.Logger.getLogger;
import static net.pincette.jes.util.MongoExpressions.operators;
import static net.pincette.json.JsltCustom.customFunctions;
import static net.pincette.json.JsltCustom.trace;
import static net.pincette.json.streams.Logging.LOGGER;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Collections.union;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.typesafe.config.Config;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import net.pincette.mongo.Features;
import net.pincette.mongo.Validator;
import net.pincette.mongo.streams.Stage;

class Context {
  final MongoClient client;
  final Config config;
  final MongoDatabase database;
  final String environment;
  final Features features;
  final String instance = randomUUID().toString();
  final String logTopic;
  final Producer producer;
  final Map<String, Stage> stageExtensions;
  final Consumer<String> testLoadCollection;
  final Validator validator;

  Context() {
    this(null, null, null, null, null, null, null, null, null, null);
  }

  @SuppressWarnings("java:S107") // Internal constructor for immutable object.
  private Context(
      final MongoClient client,
      final Config config,
      final MongoDatabase database,
      final String environment,
      final Features features,
      final String logTopic,
      final Producer producer,
      final Map<String, Stage> stageExtensions,
      final Validator validator,
      final Consumer<String> testLoadCollection) {
    this.client = client;
    this.config = config;
    this.database = database;
    this.environment = environment;
    this.features =
        features != null
            ? features
            : new Features()
                .withCustomJsltFunctions(union(customFunctions(), set(trace(getLogger(LOGGER)))));
    this.logTopic = logTopic;
    this.producer = producer;
    this.stageExtensions = stageExtensions;
    this.validator = validator;
    this.testLoadCollection = testLoadCollection;
  }

  private static <K, V> Map<K, V> mergeable(final Map<K, V> map) {
    return ofNullable(map).orElseGet(Collections::emptyMap);
  }

  private static <T> Set<T> mergeable(final Collection<T> collection) {
    return ofNullable(collection)
        .map(c -> (Set<T>) new HashSet<>(c))
        .orElseGet(Collections::emptySet);
  }

  private static Features mergeFeatures(final Features f1, final Features f2) {
    return f1.withExpressionExtensions(
            merge(mergeable(f1.expressionExtensions), mergeable(f2.expressionExtensions)))
        .withMatchExtensions(merge(mergeable(f1.matchExtensions), mergeable(f2.matchExtensions)))
        .withCustomJsltFunctions(
            union(mergeable(f1.customJsltFunctions), mergeable(f2.customJsltFunctions)))
        .withJsltResolver(f2.jsltResolver != null ? f2.jsltResolver : f1.jsltResolver);
  }

  Context addFeatures(final Features features) {
    return this.withFeatures(mergeFeatures(this.features, features));
  }

  Context addStageExtensions(final Map<String, Stage> stageExtensions) {
    return this.withStageExtensions(merge(mergeable(this.stageExtensions), stageExtensions));
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

  Context withClient(final MongoClient client) {
    return new Context(
        client,
        config,
        database,
        environment,
        features,
        logTopic,
        producer,
        stageExtensions,
        validator,
        testLoadCollection);
  }

  Context withConfig(final Config config) {
    return new Context(
        client,
        config,
        database,
        environment,
        features,
        logTopic,
        producer,
        stageExtensions,
        validator,
        testLoadCollection);
  }

  Context withDatabase(final MongoDatabase database) {
    return new Context(
        client,
        config,
        database,
        environment,
        mergeFeatures(
            features, new Features().withExpressionExtensions(operators(database, environment))),
        logTopic,
        producer,
        stageExtensions,
        validator,
        testLoadCollection);
  }

  Context withEnvironment(final String environment) {
    return new Context(
        client,
        config,
        database,
        environment,
        database != null
            ? mergeFeatures(
                features, new Features().withExpressionExtensions(operators(database, environment)))
            : features,
        logTopic,
        producer,
        stageExtensions,
        validator,
        testLoadCollection);
  }

  Context withFeatures(final Features features) {
    return new Context(
        client,
        config,
        database,
        environment,
        features,
        logTopic,
        producer,
        stageExtensions,
        validator,
        testLoadCollection);
  }

  Context with(final UnaryOperator<Context> with) {
    return with.apply(this);
  }

  Context withIf(final Predicate<Context> predicate, final UnaryOperator<Context> with) {
    return predicate.test(this) ? with.apply(this) : this;
  }

  Context withLogTopic(final String logTopic) {
    return new Context(
        client,
        config,
        database,
        environment,
        features,
        logTopic,
        producer,
        stageExtensions,
        validator,
        testLoadCollection);
  }

  Context withProducer(final Producer producer) {
    return new Context(
        client,
        config,
        database,
        environment,
        features,
        logTopic,
        producer,
        stageExtensions,
        validator,
        testLoadCollection);
  }

  Context withStageExtensions(final Map<String, Stage> stageExtensions) {
    return new Context(
        client,
        config,
        database,
        environment,
        features,
        logTopic,
        producer,
        stageExtensions,
        validator,
        testLoadCollection);
  }

  Context withTestLoadCollection(final Consumer<String> testLoadCollection) {
    return new Context(
        client,
        config,
        database,
        environment,
        features,
        logTopic,
        producer,
        stageExtensions,
        validator,
        testLoadCollection);
  }

  Context withValidator(final Validator validator) {
    return new Context(
        client,
        config,
        database,
        environment,
        features,
        logTopic,
        producer,
        stageExtensions,
        validator,
        testLoadCollection);
  }
}

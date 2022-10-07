package net.pincette.json.streams;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.in;
import static java.lang.Boolean.TRUE;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.now;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.JsonFields.TIMESTAMP;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.getStrings;
import static net.pincette.json.streams.Common.ALIVE_AT;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.aliveAtUpdate;
import static net.pincette.json.streams.Common.config;
import static net.pincette.json.streams.Common.instanceMessage;
import static net.pincette.json.streams.Logging.LOGGER_NAME;
import static net.pincette.json.streams.Logging.exception;
import static net.pincette.json.streams.Logging.getLogger;
import static net.pincette.json.streams.Logging.trace;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Collection.deleteOne;
import static net.pincette.mongo.JsonClient.aggregate;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.util.Collections.difference;
import static net.pincette.util.Collections.intersection;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.union;
import static net.pincette.util.ScheduledCompletionStage.composeAsyncAfter;
import static net.pincette.util.ScheduledCompletionStage.runAsyncAfter;
import static net.pincette.util.Util.must;

import com.mongodb.client.model.Field;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import java.util.logging.Logger;
import javax.json.JsonObject;
import org.bson.Document;
import org.bson.conversions.Bson;

class KeepAlive {
  private static final String ACTUAL = "actual";
  private static final Duration DEFAULT_KEEP_ALIVE_INTERVAL = ofSeconds(10);
  private static final String DESIRED = "desired";
  private static final String KEEP_ALIVE_INTERVAL = "keepAliveInterval";
  private static final Logger KEEP_LOGGER = getLogger(LOGGER_NAME + ".keepalive");

  private final MongoCollection<Document> collection;
  private final Context context;
  private final Duration interval;
  private final Set<String> running = new ConcurrentSkipListSet<>();
  private final Consumer<String> startApplication;
  private final Consumer<String> stopApplication;
  private boolean stop;

  KeepAlive(
      final MongoCollection<Document> collection,
      final Consumer<String> startApplication,
      final Consumer<String> stopApplication,
      final Context context) {
    this.collection = collection;
    this.startApplication = startApplication;
    this.stopApplication = stopApplication;
    this.context = context;
    this.interval = getKeepAliveInterval(context);
  }

  private static Set<String> desired(final JsonObject alive) {
    return getStrings(alive, DESIRED).collect(toSet());
  }

  private static Duration getKeepAliveInterval(final Context context) {
    return config(
        context, config -> config.getDuration(KEEP_ALIVE_INTERVAL), DEFAULT_KEEP_ALIVE_INTERVAL);
  }

  private Set<String> applicationsToRestart(final JsonObject alive) {
    return intersection(desired(alive), running);
  }

  private Set<String> applicationsToStart(final JsonObject alive) {
    return difference(desired(alive), running);
  }

  private Set<String> applicationsToStop(final JsonObject alive) {
    return difference(running, desired(alive));
  }

  private Bson criterion() {
    return eq(ID, context.instance);
  }

  private CompletionStage<Boolean> deleteAlive() {
    return deleteOne(collection, criterion())
        .thenApply(result -> trace(instanceMessage("deleteAlive", context), result, KEEP_LOGGER))
        .thenApply(result -> result != null && result.wasAcknowledged())
        .thenApply(result -> must(result, r -> r));
  }

  private CompletionStage<Boolean> next() {
    return TRUE.equals(trace(instanceMessage("next stop", context), stop, KEEP_LOGGER))
        ? deleteAlive()
        : setAlive()
            .thenComposeAsync(
                result ->
                    composeAsyncAfter(
                        this::next,
                        trace(instanceMessage("next interval", context), interval, KEEP_LOGGER)))
            .exceptionally(
                e -> {
                  exception(e);
                  runAsyncAfter(
                      this::next,
                      trace(instanceMessage("next interval", context), interval, KEEP_LOGGER));
                  return false;
                });
  }

  private CompletionStage<Boolean> reconcile(final JsonObject alive) {
    return updatedApplications(alive)
        .thenApply(
            updated -> {
              union(applicationsToStop(alive), updated).forEach(stopApplication);
              union(applicationsToStart(alive), updated).forEach(startApplication);
              return true;
            });
  }

  private CompletionStage<Boolean> setAlive() {
    return findOne(collection, criterion())
        .thenComposeAsync(
            result -> result.map(this::reconcile).orElseGet(() -> completedFuture(true)))
        .thenComposeAsync(
            result ->
                aliveAtUpdate(
                    this::criterion,
                    () -> new Field<>(ACTUAL, fromJson(from(running.stream().sorted()))),
                    collection,
                    KEEP_LOGGER));
  }

  KeepAlive start() {
    next();

    return this;
  }

  void start(final String application) {
    running.add(application);
  }

  KeepAlive stop() {
    stop = true;
    deleteAlive().toCompletableFuture().join();

    return this;
  }

  void stop(final String application) {
    running.remove(application);
  }

  private CompletionStage<Set<String>> updatedApplications(final JsonObject alive) {
    return aggregate(collection, updatedApplicationsCriterion(alive))
        .thenApply(
            results -> results.stream().map(r -> r.getString(APPLICATION_FIELD)).collect(toSet()));
  }

  private List<Bson> updatedApplicationsCriterion(final JsonObject alive) {
    return list(
        match(
            and(
                in(APPLICATION_FIELD, applicationsToRestart(alive)),
                gt(TIMESTAMP, alive.getString(ALIVE_AT, now().toString())))),
        project(fromJson(o(f(ID, v(0)), f(APPLICATION_FIELD, v(1))))));
  }
}

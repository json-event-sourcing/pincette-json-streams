package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static java.lang.Boolean.TRUE;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.now;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.json.streams.Common.ALIVE_AT;
import static net.pincette.json.streams.Common.INSTANCE;
import static net.pincette.json.streams.Common.LEADER;
import static net.pincette.json.streams.Common.aliveAtUpdate;
import static net.pincette.json.streams.Common.config;
import static net.pincette.json.streams.Common.instanceMessage;
import static net.pincette.json.streams.Logging.LOGGER_NAME;
import static net.pincette.json.streams.Logging.exception;
import static net.pincette.json.streams.Logging.getLogger;
import static net.pincette.json.streams.Logging.trace;
import static net.pincette.mongo.BsonUtil.toDocument;
import static net.pincette.mongo.Collection.deleteOne;
import static net.pincette.mongo.Collection.insertOne;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.util.Collections.list;
import static net.pincette.util.ScheduledCompletionStage.composeAsyncAfter;
import static net.pincette.util.ScheduledCompletionStage.runAsyncAfter;
import static net.pincette.util.Util.must;

import com.mongodb.client.model.Field;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

class Leader {
  private static final Duration DEFAULT_LEADER_INTERVAL = ofSeconds(10);
  private static final String LEADER_INTERVAL = "leaderInterval";
  private static final Logger LEADER_LOGGER = getLogger(LOGGER_NAME + ".leader");

  private final MongoCollection<Document> collection;
  private final Context context;
  private final Duration interval;
  private final Consumer<Boolean> onResult;
  private boolean stop;

  Leader(
      final Consumer<Boolean> onResult,
      final MongoCollection<Document> collection,
      final Context context) {
    this.onResult = onResult;
    this.collection = collection;
    this.context = context;
    this.interval = getLeaderInterval(context);
  }

  private static Duration getLeaderInterval(final Context context) {
    return config(context, config -> config.getDuration(LEADER_INTERVAL), DEFAULT_LEADER_INTERVAL);
  }

  private CompletionStage<Boolean> becomeLeader() {
    return tryToBeFirst()
        .thenComposeAsync(result -> TRUE.equals(result) ? completedFuture(true) : tryMyself());
  }

  private Bson criterion() {
    return and(
        eq(ID, LEADER),
        eq(
            INSTANCE,
            trace(() -> instanceMessage("criterion", context), context.instance, LEADER_LOGGER)));
  }

  private CompletionStage<Boolean> deleteLeader() {
    return deleteOne(collection, criterion())
        .thenApply(
            result -> trace(() -> instanceMessage("deleteLeader", context), result, LEADER_LOGGER))
        .thenApply(DeleteResult::wasAcknowledged)
        .thenApply(result -> must(result, r -> r));
  }

  private CompletionStage<Boolean> next() {
    return TRUE.equals(trace(() -> instanceMessage("next stop", context), stop, LEADER_LOGGER))
        ? deleteLeader()
        : becomeLeader()
            .thenAccept(this::notification)
            .thenComposeAsync(
                result ->
                    composeAsyncAfter(
                        this::next,
                        trace(
                            () -> instanceMessage("next interval", context),
                            interval,
                            LEADER_LOGGER)))
            .exceptionally(
                e -> {
                  exception(e);
                  runAsyncAfter(
                      this::next,
                      trace(
                          () -> instanceMessage("next interval", context),
                          interval,
                          LEADER_LOGGER));
                  return false;
                });
  }

  private void notification(final boolean isLeader) {
    if (isLeader) {
      LEADER_LOGGER.info(() -> "Instance " + context.instance + " is the leader");
    }

    onResult.accept(isLeader);
  }

  Leader stop() {
    stop = true;
    deleteLeader().toCompletableFuture().join();

    return this;
  }

  Leader start() {
    runAsyncAfter(this::next, interval);

    return this;
  }

  private CompletionStage<Boolean> tryMyself() {
    final Supplier<String> me = () -> instanceMessage("tryMyself", context);

    return findOne(collection, criterion())
        .thenComposeAsync(
            json ->
                json.map(
                        j ->
                            aliveAtUpdate(
                                this::criterion,
                                () -> new Field<>(INSTANCE, context.instance),
                                collection,
                                LEADER_LOGGER))
                    .orElseGet(() -> completedFuture(trace(me, false, LEADER_LOGGER))))
        .exceptionally(e -> trace(me, false, LEADER_LOGGER));
  }

  private CompletionStage<Boolean> tryToBeFirst() {
    final Supplier<String> me = () -> instanceMessage("tryToBeFirst", context);

    return insertOne(
            collection,
            trace(
                me,
                toDocument(
                    new BsonDocument(
                        list(
                            new BsonElement(ID, new BsonString(LEADER)),
                            new BsonElement(INSTANCE, new BsonString(context.instance)),
                            new BsonElement(ALIVE_AT, new BsonDateTime(now().toEpochMilli()))))),
                LEADER_LOGGER))
        .thenApply(result -> trace(me, result, LEADER_LOGGER))
        .thenApply(result -> result != null && result.wasAcknowledged())
        .exceptionally(e -> trace(me, false, LEADER_LOGGER));
  }
}

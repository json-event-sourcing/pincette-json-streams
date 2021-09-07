package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static java.lang.Boolean.TRUE;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.now;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.json.streams.Common.ALIVE_AT;
import static net.pincette.json.streams.Common.INSTANCE;
import static net.pincette.json.streams.Common.LEADER;
import static net.pincette.json.streams.Common.aliveAtUpdate;
import static net.pincette.json.streams.Common.config;
import static net.pincette.mongo.BsonUtil.toDocument;
import static net.pincette.mongo.Collection.deleteOne;
import static net.pincette.mongo.Collection.insertOne;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.util.Collections.list;
import static net.pincette.util.ScheduledCompletionStage.composeAsyncAfter;
import static net.pincette.util.Util.must;

import com.mongodb.client.model.Field;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

class Leader {
  private static final Duration DEFAULT_LEADER_INTERVAL = ofSeconds(10);
  private static final String LEADER_INTERVAL = "leaderInterval";

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
    return and(eq(ID, LEADER), eq(INSTANCE, context.instance));
  }

  private CompletionStage<Boolean> deleteLeader() {
    return deleteOne(collection, criterion())
        .thenApply(DeleteResult::wasAcknowledged)
        .thenApply(result -> must(result, r -> r));
  }

  private CompletionStage<Boolean> next() {
    return stop
        ? deleteLeader()
        : becomeLeader()
            .thenAccept(this::notification)
            .thenComposeAsync(result -> composeAsyncAfter(this::next, interval));
  }

  private void notification(final boolean isLeader) {
    if (isLeader) {
      context.logger.info(() -> "Instance " + context.instance + " is the leader");
    }

    onResult.accept(isLeader);
  }

  Leader stop() {
    stop = true;
    deleteLeader().toCompletableFuture().join();

    return this;
  }

  Leader start() {
    next();

    return this;
  }

  private CompletionStage<Boolean> tryMyself() {
    return findOne(collection, criterion())
        .thenComposeAsync(
            json ->
                json.map(
                        j ->
                            aliveAtUpdate(
                                this::criterion,
                                () -> new Field<>(INSTANCE, context.instance),
                                collection))
                    .orElseGet(() -> completedFuture(false)));
  }

  private CompletionStage<Boolean> tryToBeFirst() {
    return insertOne(
            collection,
            toDocument(
                new BsonDocument(
                    list(
                        new BsonElement(ID, new BsonString(LEADER)),
                        new BsonElement(INSTANCE, new BsonString(context.instance)),
                        new BsonElement(ALIVE_AT, new BsonDateTime(now().toEpochMilli()))))))
        .thenApply(InsertOneResult::wasAcknowledged)
        .exceptionally(e -> false);
  }
}

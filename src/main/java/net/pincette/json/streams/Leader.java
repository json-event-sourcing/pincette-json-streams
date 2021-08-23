package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static java.time.Duration.ofSeconds;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.json.streams.Common.INSTANCE;
import static net.pincette.json.streams.Common.aliveAtUpdate;
import static net.pincette.json.streams.Common.config;
import static net.pincette.mongo.Collection.deleteOne;
import static net.pincette.util.ScheduledCompletionStage.composeAsyncAfter;
import static net.pincette.util.Util.must;

import com.mongodb.client.model.Field;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.bson.Document;
import org.bson.conversions.Bson;

class Leader {
  private static final Duration DEFAULT_LEADER_INTERVAL = ofSeconds(10);
  private static final String LEADER_FIELD = "leader";
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
    return aliveAtUpdate(
        this::criterion, () -> new Field<>(INSTANCE, context.instance), collection);
  }

  private Bson criterion() {
    return and(eq(ID, LEADER_FIELD), eq(INSTANCE, context.instance));
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

    return this;
  }

  Leader start() {
    next();

    return this;
  }
}

package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.eq;
import static java.time.Duration.ofSeconds;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.streams.Common.ACTUAL;
import static net.pincette.json.streams.Common.aliveAtUpdate;
import static net.pincette.json.streams.Common.config;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Collection.deleteOne;
import static net.pincette.util.ScheduledCompletionStage.composeAsyncAfter;
import static net.pincette.util.Util.must;

import com.mongodb.client.model.Field;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentSkipListSet;
import org.bson.Document;
import org.bson.conversions.Bson;

class KeepAlive {
  private static final Duration DEFAULT_KEEP_ALIVE_INTERVAL = ofSeconds(10);
  private static final String KEEP_ALIVE_INTERVAL = "keepAliveInterval";

  private final MongoCollection<Document> collection;
  private final Context context;
  private final Duration interval;
  private final Set<String> running = new ConcurrentSkipListSet<>();
  private boolean stop;

  KeepAlive(final MongoCollection<Document> collection, final Context context) {
    this.collection = collection;
    this.context = context;
    this.interval = getKeepAliveInterval(context);
  }

  private static Duration getKeepAliveInterval(final Context context) {
    return config(
        context, config -> config.getDuration(KEEP_ALIVE_INTERVAL), DEFAULT_KEEP_ALIVE_INTERVAL);
  }

  private Bson criterion() {
    return eq(ID, context.instance);
  }

  private CompletionStage<Boolean> deleteAlive() {
    return deleteOne(collection, criterion())
        .thenApply(DeleteResult::wasAcknowledged)
        .thenApply(result -> must(result, r -> r));
  }

  private CompletionStage<Boolean> next() {
    return stop
        ? deleteAlive()
        : setAlive().thenComposeAsync(result -> composeAsyncAfter(this::next, interval));
  }

  private CompletionStage<Boolean> setAlive() {
    return aliveAtUpdate(
        this::criterion, () -> new Field<>(ACTUAL, fromJson(from(running.stream()))), collection);
  }

  KeepAlive start() {
    next();

    return this;
  }

  void start(final String application) {
    running.add(application);
    next();
  }

  KeepAlive stop() {
    stop = true;

    return this;
  }

  void stop(final String application) {
    running.remove(application);
  }
}

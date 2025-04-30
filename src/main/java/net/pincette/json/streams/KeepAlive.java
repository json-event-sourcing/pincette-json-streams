package net.pincette.json.streams;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.in;
import static java.time.Instant.now;
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
import static net.pincette.json.streams.Common.instanceMessage;
import static net.pincette.json.streams.Logging.LOGGER_NAME;
import static net.pincette.json.streams.Logging.getLogger;
import static net.pincette.json.streams.Logging.trace;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.util.Collections.difference;
import static net.pincette.util.Collections.intersection;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.union;
import static net.pincette.util.StreamUtil.stream;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.UpdateOptions;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.json.JsonObject;
import net.pincette.mongo.BsonUtil;
import org.bson.BsonDateTime;
import org.bson.Document;
import org.bson.conversions.Bson;

class KeepAlive {
  private static final String ACTUAL = "actual";
  private static final String DESIRED = "desired";
  private static final Logger KEEPALIVE_LOGGER = getLogger(LOGGER_NAME + ".keepalive");

  private final MongoCollection<Document> collection;
  private final Context context;
  private final Set<String> running = new ConcurrentSkipListSet<>();
  private final Consumer<String> startApplication;
  private final Consumer<String> stopApplication;

  KeepAlive(
      final MongoCollection<Document> collection,
      final Consumer<String> startApplication,
      final Consumer<String> stopApplication,
      final Context context) {
    this.collection = collection;
    this.startApplication = startApplication;
    this.stopApplication = stopApplication;
    this.context = context;
  }

  private static Set<String> desired(final JsonObject alive) {
    return getStrings(alive, DESIRED).collect(toSet());
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

  private void reconcile(final JsonObject alive) {
    final Set<String> updated = updatedApplications(alive);

    union(applicationsToStop(alive), updated).forEach(stopApplication);
    union(applicationsToStart(alive), updated).forEach(startApplication);
  }

  Set<String> running() {
    return running;
  }

  void setAlive() {
    final Supplier<String> me = () -> instanceMessage("setAlive", context);

    stream(collection.find(trace(me, criterion(), KEEPALIVE_LOGGER)).iterator())
        .findFirst()
        .map(Bson::toBsonDocument)
        .map(BsonUtil::fromBson)
        .ifPresent(alive -> reconcile(trace(me, alive, KEEPALIVE_LOGGER)));

    updateAlive();
  }

  void start(final String application) {
    running.add(application);
  }

  void stop() {
    collection.deleteOne(criterion());
  }

  void stop(final String application) {
    running.remove(application);
  }

  private boolean updateAlive() {
    final Supplier<String> me = () -> instanceMessage("updateAlive", context);

    return trace(
            me,
            collection.updateOne(
                trace(me, criterion(), KEEPALIVE_LOGGER),
                list(
                    Aggregates.set(
                        new Field<>(ALIVE_AT, new BsonDateTime(now().toEpochMilli())),
                        new Field<>(ACTUAL, fromJson(from(running.stream().sorted()))))),
                new UpdateOptions().upsert(true)),
            KEEPALIVE_LOGGER)
        .wasAcknowledged();
  }

  private Set<String> updatedApplications(final JsonObject alive) {
    return stream(collection.aggregate(updatedApplicationsCriterion(alive)).iterator())
        .map(r -> r.getString(APPLICATION_FIELD))
        .collect(toSet());
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

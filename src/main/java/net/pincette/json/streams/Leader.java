package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.setOnInsert;
import static java.time.Instant.now;
import static java.util.Optional.ofNullable;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.json.streams.Common.ALIVE_AT;
import static net.pincette.json.streams.Common.INSTANCE;
import static net.pincette.json.streams.Common.LEADER;
import static net.pincette.json.streams.Common.instanceMessage;
import static net.pincette.json.streams.Logging.LOGGER_NAME;
import static net.pincette.json.streams.Logging.getLogger;
import static net.pincette.json.streams.Logging.trace;
import static net.pincette.util.Util.tryToGet;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Updates;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.bson.BsonDateTime;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

class Leader {
  private static final Logger LEADER_LOGGER = getLogger(LOGGER_NAME + ".leader");

  private final MongoCollection<Document> collection;
  private final Context context;

  Leader(final MongoCollection<Document> collection, final Context context) {
    this.collection = collection;
    this.context = context;
  }

  private Bson criterion() {
    return and(
        eq(ID, LEADER),
        eq(
            INSTANCE,
            trace(() -> instanceMessage("criterion", context), context.instance, LEADER_LOGGER)));
  }

  boolean isLeader() {
    final Supplier<String> me = () -> instanceMessage("isLeader", context);

    return trace(
        me,
        tryToGet(
                () ->
                    ofNullable(
                            collection.findOneAndUpdate(
                                trace(me, criterion(), LEADER_LOGGER),
                                Updates.combine(
                                    setOnInsert(ID, new BsonString(LEADER)),
                                    setOnInsert(INSTANCE, new BsonString(context.instance)),
                                    set(ALIVE_AT, new BsonDateTime(now().toEpochMilli()))),
                                new FindOneAndUpdateOptions().upsert(true)))
                        .map(result -> trace(me, result, LEADER_LOGGER))
                        .isPresent(),
                e -> false)
            .orElse(false),
        LEADER_LOGGER);
  }

  @SuppressWarnings("java:S106") // The log system is already down.
  void stop() {
    if (collection.deleteOne(criterion()).getDeletedCount() == 0) {
      System.err.println(
          "ERROR: "
              + instanceMessage(
                  "The leader entry could not be deleted. The instance may not be the leader.",
                  context));
    } else {
      System.err.println("INFO: " + instanceMessage("The leader has stopped.", context));
    }
  }
}

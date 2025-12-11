package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.eq;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.time.Instant.now;
import static java.util.Arrays.stream;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static javax.json.JsonValue.NULL;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.JsonFields.TIMESTAMP;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.streams.Common.S3CSV;
import static net.pincette.json.streams.Logging.logStageObject;
import static net.pincette.json.streams.S3Util.getObject;
import static net.pincette.mongo.Collection.updateOne;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Flatten.flatMap;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.rs.Probe.probeValue;
import static net.pincette.rs.Util.completablePublisher;
import static net.pincette.rs.Util.lines;
import static net.pincette.rs.Util.onCompleteProcessor;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.zip;
import static net.pincette.util.Util.isDouble;
import static net.pincette.util.Util.isLong;
import static net.pincette.util.Util.must;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.streams.Stage;
import net.pincette.rs.Source;
import net.pincette.rs.streams.Message;
import net.pincette.util.Pair;
import net.pincette.util.State;
import org.bson.BsonDateTime;
import org.bson.Document;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

class S3CsvStage {
  private static final String BUCKET = "bucket";
  private static final String ETAG = "eTag";
  private static final String KEY = "key";
  private static final String LAST_LINE = "lastLine";
  private static final String RESUME_COLLECTION = "resumeCollection";
  private static final String SEPARATOR = "separator";

  private S3CsvStage() {}

  private static JsonObject createMessage(final String[] header, final String[] row) {
    return zip(stream(header), stream(row))
        .filter(pair -> !pair.first.isEmpty() && !pair.second.isEmpty())
        .reduce(createObjectBuilder(), (b, p) -> b.add(p.first, value(p.second)), (b1, b2) -> b1)
        .build();
  }

  private static Function<Message<String, JsonObject>, Publisher<Message<String, JsonObject>>>
      getCsv(
          final Function<
                  JsonObject,
                  CompletionStage<Optional<Pair<GetObjectResponse, Publisher<ByteBuffer>>>>>
              get,
          final Function<JsonObject, JsonValue> separator,
          final MongoCollection<Document> resumeCollection,
          final Function<JsonObject, String> key,
          final Supplier<Logger> logger) {
    return message ->
        completablePublisher(
            () ->
                stringValue(separator.apply(message.value))
                    .map(
                        s ->
                            get.apply(message.value)
                                .thenComposeAsync(
                                    publisher ->
                                        lastLine(resumeCollection, key.apply(message.value))
                                            .thenApply(line -> pair(publisher, line.orElse(-1))))
                                .thenApply(
                                    pair ->
                                        pair.first
                                            .map(
                                                buffers ->
                                                    records(
                                                        message.key,
                                                        buffers.second,
                                                        s,
                                                        resumeCollection,
                                                        key.apply(message.value),
                                                        pair.second,
                                                        logger))
                                            .orElseGet(() -> Source.of(message))))
                    .orElseGet(() -> completedFuture(Source.of(message))));
  }

  private static boolean isCsv(final GetObjectRequest request, final GetObjectResponse response) {
    return "text/csv".equals(response.contentType())
        || request.key().toLowerCase().endsWith(".csv");
  }

  private static Function<JsonObject, String> key(
      final Function<JsonObject, JsonValue> bucket,
      final Function<JsonObject, JsonValue> key,
      final Function<JsonObject, JsonValue> eTag) {
    return json ->
        stringValue(bucket.apply(json)).orElse("")
            + "/"
            + stringValue(key.apply(json)).orElse("")
            + "/"
            + stringValue(eTag.apply(json)).orElse("");
  }

  private static CompletionStage<Optional<Integer>> lastLine(
      final MongoCollection<Document> resumeCollection, final String key) {
    return ofNullable(resumeCollection)
        .map(
            c ->
                findOne(c, eq(ID, key))
                    .thenApply(json -> json.map(j -> j.getInt(LAST_LINE, -1)).filter(v -> v != -1)))
        .orElseGet(() -> completedFuture(empty()));
  }

  private static Publisher<Message<String, JsonObject>> records(
      final String id,
      final Publisher<ByteBuffer> publisher,
      final String separator,
      final MongoCollection<Document> resumeCollection,
      final String key,
      final int lastLine,
      final Supplier<Logger> logger) {
    final State<Integer> currentLine = new State<>(0);
    final State<String[]> header = new State<>();

    logger.get().info(() -> "Will start emitting from " + key + " at line " + (lastLine + 1));

    return with(publisher)
        .map(lines())
        .headTail(
            head -> header.set(head.split(separator)),
            line ->
                currentLine.get() > lastLine
                    ? message(id, createMessage(header.get(), line.split(separator)))
                    : message((String) null, (JsonObject) null))
        .map(
            probeValue(
                v -> {
                  currentLine.set(currentLine.get() + 1);

                  if (currentLine.get() % 10000 == 0) {
                    logger
                        .get()
                        .info(() -> "Consumed " + currentLine.get() + " messages from " + key);
                  }
                }))
        .filter(m -> m.key != null)
        .mapAsync(
            v ->
                updateLastLine(resumeCollection, key, currentLine.get() - 1).thenApply(result -> v))
        .map(onCompleteProcessor(() -> logger.get().info(() -> "Complete consuming " + key)))
        .get();
  }

  private static MongoCollection<Document> resumeCollection(
      final JsonObject expr, final Context context) {
    return getValue(expr, "/" + RESUME_COLLECTION)
        .flatMap(JsonUtil::stringValue)
        .map(context.database::getCollection)
        .orElse(null);
  }

  private static Function<JsonObject, JsonValue> separator(
      final JsonObject expr, final Context context) {
    return getValue(expr, "/" + SEPARATOR)
        .map(s -> function(s, context.features))
        .orElseGet(() -> (json -> createValue("\t")));
  }

  static Stage s3CsvStage(final Context context) {
    return (expression, c) -> {
      if (!isObject(expression)) {
        logStageObject(S3CSV, expression);

        return passThrough();
      }

      final JsonObject expr = expression.asJsonObject();

      must(
          expr.containsKey(BUCKET)
              && expr.containsKey(KEY)
              && (!expr.containsKey(RESUME_COLLECTION) || expr.containsKey(ETAG)));

      final Function<JsonObject, JsonValue> bucket =
          function(expr.getValue("/" + BUCKET), context.features);
      final Function<JsonObject, JsonValue> eTag =
          getValue(expr, "/" + ETAG).map(e -> function(e, context.features)).orElse(json -> NULL);
      final Function<JsonObject, JsonValue> key =
          function(expr.getValue("/" + KEY), context.features);
      final Function<
              JsonObject, CompletionStage<Optional<Pair<GetObjectResponse, Publisher<ByteBuffer>>>>>
          get = getObject(bucket, key, context.logger, S3CsvStage::isCsv);
      final MongoCollection<Document> resumeCollection = resumeCollection(expr, context);
      final Function<JsonObject, JsonValue> separator = separator(expr, context);

      return flatMap(
          getCsv(get, separator, resumeCollection, key(bucket, key, eTag), context.logger));
    };
  }

  private static CompletionStage<Boolean> updateLastLine(
      final MongoCollection<Document> resumeCollection, final String key, final int lastLine) {
    return resumeCollection != null && lastLine % 1000 == 0
        ? updateOne(
                resumeCollection,
                eq(ID, key),
                Aggregates.set(
                    new Field<>(TIMESTAMP, new BsonDateTime(now().toEpochMilli())),
                    new Field<>(LAST_LINE, lastLine)),
                new UpdateOptions().upsert(true))
            .thenApply(res -> must(res.wasAcknowledged(), r -> true))
        : completedFuture(true);
  }

  private static JsonValue value(final String s) {
    final Supplier<JsonValue> tryDouble =
        () -> isDouble(s) ? createValue(parseDouble(s)) : createValue(s);

    return isLong(s) ? createValue(parseLong(s)) : tryDouble.get();
  }
}

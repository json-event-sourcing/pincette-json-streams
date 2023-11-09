package net.pincette.json.streams;

import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.util.Arrays.stream;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.streams.Common.BACKOFF;
import static net.pincette.json.streams.Common.S3CSV;
import static net.pincette.json.streams.Logging.exception;
import static net.pincette.json.streams.Logging.logStageObject;
import static net.pincette.json.streams.S3Util.getObject;
import static net.pincette.mongo.Expression.function;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Flatten.flatMap;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.rs.Util.completablePublisher;
import static net.pincette.rs.Util.lines;
import static net.pincette.rs.Util.retryPublisher;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.StreamUtil.zip;
import static net.pincette.util.Util.isDouble;
import static net.pincette.util.Util.isLong;
import static net.pincette.util.Util.must;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.mongo.streams.Stage;
import net.pincette.rs.Source;
import net.pincette.rs.streams.Message;
import net.pincette.util.Pair;
import net.pincette.util.State;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

class S3CsvStage {
  private static final String BUCKET = "bucket";
  private static final String KEY = "key";
  private static final String SEPARATOR = "separator";

  private S3CsvStage() {}

  private static JsonObject createMessage(final String[] header, final String[] row) {
    return zip(stream(header), stream(row))
        .filter(pair -> !pair.first.isEmpty() && !pair.second.isEmpty())
        .reduce(createObjectBuilder(), (b, p) -> b.add(p.first, value(p.second)), (b1, b2) -> b1)
        .build();
  }

  private static boolean isCsv(final GetObjectRequest request, final GetObjectResponse response) {
    return "text/csv".equals(response.contentType())
        || request.key().toLowerCase().endsWith(".csv");
  }

  private static Publisher<Message<String, JsonObject>> records(
      final String id, final Publisher<ByteBuffer> publisher, final String separator) {
    final State<String[]> header = new State<>();

    return with(publisher)
        .map(lines())
        .headTail(
            head -> header.set(head.split(separator)),
            line -> message(id, createMessage(header.get(), line.split(separator))))
        .get();
  }

  private static Function<Message<String, JsonObject>, Publisher<Message<String, JsonObject>>>
      retryGet(
          final Function<
                  JsonObject,
                  CompletionStage<Optional<Pair<GetObjectResponse, Publisher<ByteBuffer>>>>>
              get,
          final Function<JsonObject, JsonValue> separator,
          final Supplier<Logger> logger) {
    return message ->
        retryPublisher(
            () ->
                completablePublisher(
                    () ->
                        stringValue(separator.apply(message.value))
                            .map(
                                s ->
                                    get.apply(message.value)
                                        .thenApply(
                                            publisher ->
                                                publisher
                                                    .map(
                                                        buffers ->
                                                            records(message.key, buffers.second, s))
                                                    .orElseGet(() -> Source.of(message))))
                            .orElseGet(() -> completedFuture(Source.of(message)))),
            BACKOFF,
            e -> exception(e, () -> S3CSV, logger));
  }

  static Stage s3CsvStage(final Context context) {
    return (expression, c) -> {
      if (!isObject(expression)) {
        logStageObject(S3CSV, expression);

        return passThrough();
      }

      final JsonObject expr = expression.asJsonObject();

      must(expr.containsKey(BUCKET) && expr.containsKey(KEY));

      final Function<
              JsonObject, CompletionStage<Optional<Pair<GetObjectResponse, Publisher<ByteBuffer>>>>>
          get =
              getObject(
                  function(expr.getValue("/" + BUCKET), context.features),
                  function(expr.getValue("/" + KEY), context.features),
                  context.logger,
                  S3CsvStage::isCsv);
      final Function<JsonObject, JsonValue> separator =
          getValue(expr, "/" + SEPARATOR)
              .map(s -> function(s, context.features))
              .orElseGet(() -> (json -> createValue("\t")));

      return flatMap(retryGet(get, separator, context.logger));
    };
  }

  private static JsonValue value(final String s) {
    final Supplier<JsonValue> tryDouble =
        () -> isDouble(s) ? createValue(parseDouble(s)) : createValue(s);

    return isLong(s) ? createValue(parseLong(s)) : tryDouble.get();
  }
}

package net.pincette.json.streams;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.empty;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.streams.Common.S3OUT;
import static net.pincette.json.streams.Common.tryToGetForever;
import static net.pincette.json.streams.Logging.logStageObject;
import static net.pincette.mongo.Expression.function;
import static net.pincette.rs.Async.mapAsync;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.rs.json.Util.writeObject;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.must;
import static org.reactivestreams.FlowAdapters.toPublisher;
import static software.amazon.awssdk.core.async.AsyncRequestBody.fromPublisher;
import static software.amazon.awssdk.services.s3.S3AsyncClient.create;
import static software.amazon.awssdk.services.s3.model.PutObjectRequest.builder;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.mongo.streams.Stage;
import net.pincette.rs.Source;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class S3OutStage {
  private static final String BUCKET = "bucket";
  private static final String KEY = "key";

  private static final S3AsyncClient client = create();

  private S3OutStage() {}

  private static Function<JsonObject, CompletionStage<Optional<JsonObject>>> putObject(
      final Function<JsonObject, JsonValue> bucket,
      final Function<JsonObject, JsonValue> key,
      final Supplier<Logger> logger) {
    return json ->
        Optional.of(pair(bucket.apply(json), key.apply(json)))
            .map(
                pair ->
                    pair(
                        stringValue(pair.first).orElse(null),
                        stringValue(pair.second).orElse(null)))
            .filter(pair -> pair.first != null && pair.second != null)
            .map(
                pair ->
                    toS3(json, s3Request(pair.first, pair.second), logger).thenApply(Optional::of))
            .orElseGet(() -> completedFuture(empty()));
  }

  private static PutObjectRequest s3Request(final String bucket, final String key) {
    return builder().bucket(bucket).key(key).contentType("application/json").build();
  }

  static Stage s3OutStage(final Context context) {
    return (expression, c) -> {
      if (!isObject(expression)) {
        logStageObject(S3OUT, expression);

        return passThrough();
      }

      final JsonObject expr = expression.asJsonObject();

      must(expr.containsKey(BUCKET) && expr.containsKey(KEY));

      final Function<JsonObject, CompletionStage<Optional<JsonObject>>> put =
          putObject(
              function(expr.getValue("/" + BUCKET), context.features),
              function(expr.getValue("/" + KEY), context.features),
              context.logger);

      return mapAsync(m -> put.apply(m.value).thenApply(json -> json.map(m::withValue).orElse(m)));
    };
  }

  private static CompletionStage<JsonObject> toS3(
      final JsonObject json, final PutObjectRequest request, final Supplier<Logger> logger) {
    return tryToGetForever(
            () ->
                client.putObject(
                    request.toBuilder()
                        .contentLength((long) string(json).getBytes(UTF_8).length)
                        .build(),
                    fromPublisher(
                        toPublisher(with(Source.of((JsonValue) json)).map(writeObject()).get()))),
            () -> "Bucket: " + request.bucket() + ", key: " + request.key(),
            logger)
        .thenApply(result -> json);
  }
}

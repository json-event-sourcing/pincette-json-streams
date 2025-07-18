package net.pincette.json.streams;

import static java.util.Optional.empty;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.streams.Common.tryToGetForever;
import static net.pincette.util.Pair.pair;
import static org.reactivestreams.FlowAdapters.toFlowPublisher;
import static software.amazon.awssdk.services.s3.S3AsyncClient.create;
import static software.amazon.awssdk.services.s3.model.GetObjectRequest.builder;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.util.Pair;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;

class S3Util {
  private static final S3AsyncClient client = create();

  private S3Util() {}

  static Function<
          JsonObject, CompletionStage<Optional<Pair<GetObjectResponse, Publisher<ByteBuffer>>>>>
      getObject(
          final Function<JsonObject, JsonValue> bucket,
          final Function<JsonObject, JsonValue> key,
          final Supplier<Logger> logger) {
    return getObject(bucket, key, logger, (req, res) -> true);
  }

  static Function<
          JsonObject, CompletionStage<Optional<Pair<GetObjectResponse, Publisher<ByteBuffer>>>>>
      getObject(
          final Function<JsonObject, JsonValue> bucket,
          final Function<JsonObject, JsonValue> key,
          final Supplier<Logger> logger,
          final BiPredicate<GetObjectRequest, GetObjectResponse> condition) {
    return json ->
        Optional.of(pair(bucket.apply(json), key.apply(json)))
            .map(
                pair ->
                    pair(
                        stringValue(pair.first).orElse(null),
                        stringValue(pair.second).orElse(null)))
            .filter(pair -> pair.first != null && pair.second != null)
            .map(pair -> getObject(pair.first, pair.second, logger, condition))
            .orElseGet(() -> completedFuture(empty()));
  }

  static Function<JsonObject, Optional<URL>> getObjectUrl(
          final Function<JsonObject, JsonValue> bucket, final Function<JsonObject, JsonValue> key) {
    return json ->
            Optional.of(pair(bucket.apply(json), key.apply(json)))
                    .map(
                            pair ->
                                    pair(
                                            stringValue(pair.first).orElse(null),
                                            stringValue(pair.second).orElse(null)))
                    .filter(pair -> pair.first != null && pair.second != null)
                    .map(pair -> getObjectUrl(pair.first, pair.second));
  }

  static CompletionStage<Optional<Pair<GetObjectResponse, Publisher<ByteBuffer>>>> getObject(
      final String bucket, final String key, final Supplier<Logger> logger) {
    return getObject(bucket, key, logger, (req, res) -> true);
  }

  static CompletionStage<Optional<Pair<GetObjectResponse, Publisher<ByteBuffer>>>> getObject(
      final String bucket,
      final String key,
      final Supplier<Logger> logger,
      final BiPredicate<GetObjectRequest, GetObjectResponse> condition) {
    return getStream(s3Request(bucket, key), logger, condition);
  }

  static URL getObjectUrl(final String bucket, final String key) {
    return client.utilities().getUrl(GetUrlRequest.builder().bucket(bucket).key(key).build());
  }

  private static CompletionStage<Optional<Pair<GetObjectResponse, Publisher<ByteBuffer>>>>
      getStream(
          final GetObjectRequest request,
          final Supplier<Logger> logger,
          final BiPredicate<GetObjectRequest, GetObjectResponse> condition) {
    return tryToGetForever(
            () -> client.getObject(request, new Response()),
            () -> "Bucket: " + request.bucket() + ", key: " + request.key(),
            logger)
        .thenApply(
            pair ->
                Optional.of(pair.first)
                    .filter(response -> condition.test(request, response))
                    .map(response -> pair(response, toFlowPublisher(pair.second))));
  }

  private static GetObjectRequest s3Request(final String bucket, final String key) {
    return builder().bucket(bucket).key(key).build();
  }

  private static class Response
      implements AsyncResponseTransformer<
          GetObjectResponse, Pair<GetObjectResponse, SdkPublisher<ByteBuffer>>> {
    private final CompletableFuture<Pair<GetObjectResponse, SdkPublisher<ByteBuffer>>> future =
        new CompletableFuture<>();
    private GetObjectResponse objectResponse;

    @Override
    public void exceptionOccurred(final Throwable error) {
      future.completeExceptionally(error);
    }

    @Override
    public void onResponse(final GetObjectResponse response) {
      this.objectResponse = response;
    }

    @Override
    public void onStream(final SdkPublisher<ByteBuffer> publisher) {
      future.complete(pair(objectResponse, publisher));
    }

    @Override
    public CompletableFuture<Pair<GetObjectResponse, SdkPublisher<ByteBuffer>>> prepare() {
      return future;
    }
  }
}

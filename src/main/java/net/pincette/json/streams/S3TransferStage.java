package net.pincette.json.streams;

import static java.net.http.HttpClient.newBuilder;
import static java.net.http.HttpResponse.BodyHandlers.ofPublisher;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Stream.concat;
import static javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm;
import static net.pincette.json.JsonUtil.*;
import static net.pincette.json.streams.Common.S3TRANSFER;
import static net.pincette.json.streams.Common.tryToGetForever;
import static net.pincette.json.streams.Logging.logStageObject;
import static net.pincette.json.streams.S3Util.getObjectUrl;
import static net.pincette.mongo.Expression.function;
import static net.pincette.rs.Async.mapAsync;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.s3.util.Util.putObject;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.*;
import net.pincette.function.SideEffect;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.streams.Stage;
import net.pincette.rs.Chain;
import net.pincette.rs.FlattenList;
import net.pincette.util.Builder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.security.KeyStore;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

class S3TransferStage {

  private static final String AS = "as";
  private static final String URL = "url";
  private static final String BODY = "body";
  private static final String HEADERS = "headers";
  private static final String HTTP_ERROR = "httpError";
  private static final String BUCKET = "bucket";
  private static final String KEY = "key";
  private static final String KEY_STORE = "keyStore";
  private static final String PASSWORD = "password";
  private static final String SSL_CONTEXT = "sslContext";
  private static final String STATUS_CODE = "statusCode";

  private S3TransferStage() {}

  static Stage s3TransferStage(Context outerContext) {
    return (expression, context) -> {
      if (!isObject(expression)) {
        logStageObject(S3TRANSFER, expression);

        return passThrough();
      }

      final JsonObject jsonExpression = expression.asJsonObject();
      must(
          jsonExpression.containsKey(URL)
              && jsonExpression.containsKey(BUCKET)
              && jsonExpression.containsKey(KEY)
              && jsonExpression.containsKey(AS));

      final Function<JsonObject, CompletionStage<JsonObject>> execute =
          execute(
              getClient(jsonExpression),
              function(jsonExpression.getValue("/" + URL), context.features),
              getValue(jsonExpression, "/" + HEADERS)
                  .map(headers -> function(headers, context.features))
                  .orElse(json -> emptyObject()),
              function(jsonExpression.getValue("/" + BUCKET), context.features),
              function(jsonExpression.getValue("/" + KEY), context.features),
              function(jsonExpression.getValue("/" + AS), context.features),
              outerContext.logger);

      return mapAsync(message -> execute.apply(message.value).thenApply(message::withValue));
    };
  }

  private static Function<JsonObject, CompletionStage<JsonObject>> execute(
      final HttpClient client,
      final Function<JsonObject, JsonValue> urlMapper,
      final Function<JsonObject, JsonValue> headersMapper,
      final Function<JsonObject, JsonValue> bucketMapper,
      final Function<JsonObject, JsonValue> keyMapper,
      final Function<JsonObject, JsonValue> asMapper,
      final Supplier<Logger> logger) {
    return message ->
        tryToGetForever(
            () ->
                stringValue(urlMapper.apply(message))
                    .flatMap(url -> createRequest(url, headersMapper.apply(message)))
                    .map(
                        request ->
                            client
                                .sendAsync(request, ofPublisher())
                                .thenApply(
                                    response ->
                                        Chain.with(response.body())
                                            .map(FlattenList.flattenList())
                                            .get())
                                .thenComposeAsync(
                                    publisher ->
                                        putObject(
                                                putRequest(bucketMapper, keyMapper).apply(message),
                                                publisher)
                                            .thenApply(
                                                putResponse ->
                                                    getObjectUrl(bucketMapper, keyMapper)
                                                        .apply(message))
                                            .thenApply(
                                                url ->
                                                    url.map(
                                                            value ->
                                                                annotateMessage(
                                                                    message, value, asMapper))
                                                        .orElseGet(() -> serverError(message)))))
                    .orElseGet(() -> completedFuture(badRequest(message))),
            () -> S3TRANSFER,
            logger);
  }

  private static HttpClient getClient(final JsonObject expression) {
    final HttpClient.Builder builder =
        newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NORMAL);

    return ofNullable(expression.getJsonObject(SSL_CONTEXT))
        .flatMap(S3TransferStage::createSslContext)
        .map(builder::sslContext)
        .orElse(builder)
        .build();
  }

  private static Optional<SSLContext> createSslContext(final JsonObject sslContext) {
    final String password = sslContext.getString(PASSWORD);

    return tryToGetRethrow(() -> SSLContext.getInstance("TLSv1.3"))
        .flatMap(
            context ->
                getKeyStore(sslContext.getString(KEY_STORE), password)
                    .flatMap(store -> getKeyManagerFactory(store, password))
                    .flatMap(S3TransferStage::getKeyManagers)
                    .map(managers -> pair(context, managers)))
        .map(
            pair ->
                SideEffect.<SSLContext>run(
                        () -> tryToDoRethrow(() -> pair.first.init(pair.second, null, null)))
                    .andThenGet(() -> pair.first));
  }

  private static Optional<KeyManager[]> getKeyManagers(final KeyManagerFactory factory) {
    return Optional.of(factory.getKeyManagers()).filter(managers -> managers.length > 0);
  }

  private static Optional<KeyManagerFactory> getKeyManagerFactory(
      final KeyStore keyStore, final String password) {
    return tryToGetRethrow(() -> KeyManagerFactory.getInstance(getDefaultAlgorithm()))
        .map(
            factory ->
                SideEffect.<KeyManagerFactory>run(
                        () -> tryToDoRethrow(() -> factory.init(keyStore, password.toCharArray())))
                    .andThenGet(() -> factory));
  }

  private static Optional<KeyStore> getKeyStore(final String keyStore, final String password) {
    return tryToGetRethrow(() -> KeyStore.getInstance("pkcs12"))
        .map(
            store ->
                SideEffect.<KeyStore>run(
                        () ->
                            tryToDoRethrow(
                                () ->
                                    store.load(
                                        new FileInputStream(keyStore), password.toCharArray())))
                    .andThenGet(() -> store));
  }

  private static Optional<HttpRequest> createRequest(
      final String url, final JsonValue jsonHeaders) {
    return objectValue(jsonHeaders)
        .map(headers -> setHeaders(HttpRequest.newBuilder().uri(URI.create(url)), headers))
        .map(request -> request.GET().build());
  }

  private static HttpRequest.Builder setHeaders(
      final HttpRequest.Builder builder, final JsonObject headers) {
    return concat(
            headers.entrySet().stream()
                .filter(entry -> isString(entry.getValue()))
                .map(entry -> pair(entry.getKey(), asString(entry.getValue()).getString())),
            headers.entrySet().stream()
                .filter(entry -> isArray(entry.getValue()))
                .flatMap(
                    entry ->
                        asArray(entry.getValue()).stream()
                            .flatMap(value -> stringValue(value).stream())
                            .map(raw -> pair(entry.getKey(), raw))))
        .reduce(builder, (b, p) -> b.header(p.first, p.second), (b1, b2) -> b1);
  }

  private static JsonObject badRequest(final JsonObject message) {
    return addError(message, 400, null);
  }

  private static JsonObject serverError(final JsonObject message) {
    return addError(message, 500, "ERROR");
  }

  private static JsonObject addError(
      final JsonObject message, final int statusCode, final String body) {
    return createObjectBuilder(message)
        .add(
            HTTP_ERROR,
            Builder.create(JsonUtil::createObjectBuilder)
                .update(builder -> builder.add(STATUS_CODE, statusCode))
                .updateIf(() -> ofNullable(body), (builder, value) -> builder.add(BODY, value))
                .build())
        .build();
  }

  private static JsonObject annotateMessage(
      final JsonObject message, final URL url, final Function<JsonObject, JsonValue> asMapper) {
    return stringValue(asMapper.apply(message))
        .map(key -> createObjectBuilder(message).add(key, url.toString()).build())
        .orElse(message);
  }

  private static Function<JsonObject, PutObjectRequest> putRequest(
      final Function<JsonObject, JsonValue> bucketMapper,
      final Function<JsonObject, JsonValue> keyMapper) {
    return json ->
        PutObjectRequest.builder()
            .bucket(stringValue(bucketMapper.apply(json)).orElse(""))
            .key(stringValue(keyMapper.apply(json)).orElse(""))
            .build();
  }
}

package net.pincette.json.streams;

import static java.net.http.HttpClient.newBuilder;
import static java.net.http.HttpRequest.BodyPublishers.fromPublisher;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.concat;
import static javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm;
import static net.pincette.json.JsonUtil.arrayValue;
import static net.pincette.json.JsonUtil.asArray;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.emptyArray;
import static net.pincette.json.JsonUtil.emptyObject;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.objectValue;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.streams.Common.S3ATTACHMENTS;
import static net.pincette.json.streams.Common.tryToGetForever;
import static net.pincette.json.streams.Logging.logStageObject;
import static net.pincette.json.streams.S3Util.getObject;
import static net.pincette.mongo.Expression.function;
import static net.pincette.rs.Async.mapAsync;
import static net.pincette.rs.Base64.base64Encoder;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.composeAsyncStream;
import static net.pincette.util.Util.getLastSegment;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.segments;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGetRethrow;

import java.io.FileInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import net.pincette.function.SideEffect;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.streams.Stage;
import net.pincette.rs.Concat;
import net.pincette.rs.Source;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

class S3AttachmentsStage {
  private static final String ATTACHMENTS = "attachments";
  private static final String BODY = "body";
  private static final String BUCKET = "bucket";
  private static final String CONTENT_DISPOSITION = "Content-Disposition";
  private static final String CONTENT_LENGTH = "Content-Length";
  private static final String CONTENT_TRANSFER_ENCODING = "Content-Transfer-Encoding";
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String HEADERS = "headers";
  private static final String HTTP_ERROR = "httpError";
  private static final String KEY = "key";
  private static final String KEY_STORE = "keyStore";
  private static final String MIME_VERSION = "MIME-Version";
  private static final String PASSWORD = "password";
  private static final String SSL_CONTEXT = "sslContext";
  private static final String STATUS_CODE = "statusCode";
  private static final String URL = "url";

  private S3AttachmentsStage() {}

  private static JsonObject addMimeHeaders(final JsonObject headers, final String boundary) {
    return createObjectBuilder(headers)
        .add(
            CONTENT_TYPE,
            ofNullable(headers.getString(CONTENT_TYPE, null)).orElse("multipart/mixed")
                + "; boundary=\""
                + boundary
                + "\"")
        .add(MIME_VERSION, "1.0")
        .build();
  }

  private static JsonObject addError(
      final JsonObject message, final int statusCode, final String body) {
    return createObjectBuilder(message)
        .add(
            HTTP_ERROR,
            create(JsonUtil::createObjectBuilder)
                .update(b -> b.add(STATUS_CODE, statusCode))
                .updateIf(() -> ofNullable(body), (b, v) -> b.add(BODY, v))
                .build())
        .build();
  }

  private static JsonObject annotateMessage(
      final JsonObject message, final HttpResponse<String> response) {
    return ok(response) ? message : addError(message, response.statusCode(), response.body());
  }

  private static Attachment attachment(final JsonObject attachment) {
    final Attachment att =
        new Attachment(attachment.getString(BUCKET, null), attachment.getString(KEY, null));

    attachment.entrySet().stream()
        .filter(
            e -> !e.getKey().equals(BUCKET) && !e.getKey().equals(KEY) && isString(e.getValue()))
        .forEach(e -> att.setHeader(e.getKey(), asString(e.getValue()).getString()));

    return att;
  }

  private static List<Attachment> attachments(final JsonValue attachments) {
    return arrayValue(attachments).stream()
        .flatMap(JsonArray::stream)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .map(S3AttachmentsStage::attachment)
        .filter(att -> att.bucket != null && att.key != null)
        .toList();
  }

  private static CompletionStage<Publisher<ByteBuffer>> attachmentsPublisher(
      final List<Attachment> attachments, final Supplier<Logger> logger, final String boundary) {
    return composeAsyncStream(
            attachments.stream().map(att -> attachmentPublisher(att, logger, boundary)))
        .thenApply(
            stream -> Concat.of(concat(stream, Stream.of(trailerPublisher(boundary))).toList()));
  }

  private static CompletionStage<Publisher<ByteBuffer>> attachmentPublisher(
      final Attachment attachment, final Supplier<Logger> logger, final String boundary) {
    return getObject(attachment.bucket, attachment.key, logger)
        .thenApply(
            o ->
                o.map(
                        pair ->
                            Concat.of(
                                headerPublisher(pair.first, attachment, boundary),
                                with(pair.second).map(base64Encoder()).get()))
                    .orElseGet(net.pincette.rs.Util::empty));
  }

  private static Optional<CompletionStage<HttpRequest>> createRequest(
      final String url,
      final JsonValue headers,
      final JsonValue attachments,
      final Supplier<Logger> logger) {
    final List<Attachment> atts = attachments(attachments);
    final String boundary = randomUUID().toString();

    return objectValue(headers)
        .map(h -> !atts.isEmpty() ? addMimeHeaders(h, boundary) : h)
        .map(h -> setHeaders(HttpRequest.newBuilder().uri(URI.create(url)).expectContinue(true), h))
        .map(
            request ->
                attachmentsPublisher(atts, logger, boundary)
                    .thenApply(publisher -> request.POST(fromPublisher(publisher)).build()));
  }

  private static Optional<SSLContext> createSslContext(final JsonObject sslContext) {
    final String password = sslContext.getString(PASSWORD);

    return tryToGetRethrow(() -> SSLContext.getInstance("TLSv1.3"))
        .flatMap(
            context ->
                getKeyStore(sslContext.getString(KEY_STORE), password)
                    .flatMap(store -> getKeyManagerFactory(store, password))
                    .flatMap(S3AttachmentsStage::getKeyManagers)
                    .map(managers -> pair(context, managers)))
        .map(
            pair ->
                SideEffect.<SSLContext>run(
                        () -> tryToDoRethrow(() -> pair.first.init(pair.second, null, null)))
                    .andThenGet(() -> pair.first));
  }

  private static Function<JsonObject, CompletionStage<JsonObject>> execute(
      final HttpClient client,
      final Function<JsonObject, JsonValue> url,
      final Function<JsonObject, JsonValue> headers,
      final Function<JsonObject, JsonValue> attachments,
      final Supplier<Logger> logger) {
    return message ->
        tryToGetForever(
            () ->
                stringValue(url.apply(message))
                    .flatMap(
                        u ->
                            createRequest(
                                u, headers.apply(message), attachments.apply(message), logger))
                    .map(
                        request ->
                            request.thenComposeAsync(
                                r ->
                                    client
                                        .sendAsync(r, ofString())
                                        .thenApply(response -> annotateMessage(message, response))))
                    .orElseGet(() -> completedFuture(addError(message, 400, null))),
            () -> S3ATTACHMENTS,
            logger);
  }

  private static String extraAttachmentHeaders(final Attachment attachment) {
    return attachment.headers.entrySet().stream()
        .filter(e -> !e.getKey().equals(CONTENT_TYPE) && !e.getKey().equals(CONTENT_DISPOSITION))
        .map(e -> e.getKey() + ":" + e.getValue() + "\r\n")
        .collect(joining());
  }

  private static HttpClient getClient(final JsonObject expression) {
    final HttpClient.Builder builder =
        newBuilder().version(Version.HTTP_1_1).followRedirects(Redirect.NORMAL);

    return ofNullable(expression.getJsonObject(SSL_CONTEXT))
        .flatMap(S3AttachmentsStage::createSslContext)
        .map(builder::sslContext)
        .orElse(builder)
        .build();
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

  private static Publisher<ByteBuffer> headerPublisher(
      final GetObjectResponse response, final Attachment attachment, final String boundary) {
    return Source.of(
        wrap(
            ("\r\n--"
                    + boundary
                    + "\r\n"
                    + CONTENT_TYPE
                    + ":"
                    + ofNullable(attachment.headers.get(CONTENT_TYPE))
                        .orElseGet(response::contentType)
                    + "\r\n"
                    + CONTENT_LENGTH
                    + ":"
                    + response.contentLength()
                    + "\r\n"
                    + CONTENT_TRANSFER_ENCODING
                    + ":base64\r\n"
                    + CONTENT_DISPOSITION
                    + ":"
                    + ofNullable(attachment.headers.get(CONTENT_DISPOSITION))
                        .orElseGet(
                            () ->
                                "attachment;filename=\""
                                    + getLastSegment(attachment.key, "/").orElse(attachment.key)
                                    + "\"")
                    + "\r\n"
                    + extraAttachmentHeaders(attachment)
                    + "\r\n")
                .getBytes(UTF_8)));
  }

  private static <T> boolean ok(final HttpResponse<T> response) {
    return response.statusCode() == 200 || response.statusCode() == 201;
  }

  private static HttpRequest.Builder setHeaders(
      final HttpRequest.Builder builder, final JsonObject headers) {
    return concat(
            headers.entrySet().stream()
                .filter(e -> isString(e.getValue()))
                .map(e -> pair(e.getKey(), asString(e.getValue()).getString())),
            headers.entrySet().stream()
                .filter(e -> isArray(e.getValue()))
                .flatMap(
                    e ->
                        asArray(e.getValue()).stream()
                            .flatMap(v -> stringValue(v).stream())
                            .map(s -> pair(e.getKey(), s))))
        .reduce(builder, (b, p) -> b.header(p.first, p.second), (b1, b2) -> b1);
  }

  static Stage s3AttachmentsStage(final Context context) {
    return (expression, c) -> {
      if (!isObject(expression)) {
        logStageObject(S3ATTACHMENTS, expression);

        return passThrough();
      }

      final JsonObject expr = expression.asJsonObject();

      must(expr.containsKey(URL) && expr.containsKey(ATTACHMENTS));

      final Function<JsonObject, CompletionStage<JsonObject>> execute =
          execute(
              getClient(expr),
              function(expr.getValue("/" + URL), context.features),
              getValue(expr, "/" + HEADERS)
                  .map(h -> function(h, context.features))
                  .orElse(json -> emptyObject()),
              getValue(expr, "/" + ATTACHMENTS)
                  .map(b -> function(b, context.features))
                  .orElse(json -> emptyArray()),
              context.logger);

      return mapAsync(m -> execute.apply(m.value).thenApply(m::withValue));
    };
  }

  private static Publisher<ByteBuffer> trailerPublisher(final String boundary) {
    return Source.of(wrap(("\r\n--" + boundary + "--\r\n").getBytes(UTF_8)));
  }

  private static class Attachment {
    private final String bucket;
    private final Map<String, String> headers = new HashMap<>();
    private final String key;

    private Attachment(final String bucket, final String key) {
      this.bucket = bucket;
      this.key = key;
    }

    private static String normalize(final String s) {
      return segments(s, "-")
          .map(
              segment ->
                  segment.subSequence(0, 1).toString().toUpperCase()
                      + segment.subSequence(1, segment.length()).toString().toLowerCase())
          .collect(joining("-"));
    }

    private void setHeader(final String key, final String value) {
      Optional.of(normalize(key))
          .filter(k -> !k.equals(CONTENT_LENGTH) && !k.equals(CONTENT_TRANSFER_ENCODING))
          .ifPresent(k -> headers.put(k, value));
    }
  }
}

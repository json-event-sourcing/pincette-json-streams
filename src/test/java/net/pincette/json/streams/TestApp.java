package net.pincette.json.streams;

import static com.typesafe.config.ConfigValueFactory.fromAnyRef;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.nio.ByteBuffer.wrap;
import static java.nio.channels.FileChannel.open;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Optional.ofNullable;
import static net.pincette.io.PathUtil.copy;
import static net.pincette.io.PathUtil.delete;
import static net.pincette.json.JsonUtil.createDiff;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.json.streams.Common.LOG_TOPIC;
import static net.pincette.json.streams.Common.createContext;
import static net.pincette.json.streams.KafkaProvider.tester;
import static net.pincette.json.streams.Logging.init;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Util.onComplete;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.Util.autoClose;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetWithRethrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static software.amazon.awssdk.core.sync.RequestBody.fromBytes;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpRequest;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.activation.FileDataSource;
import javax.json.JsonObject;
import javax.mail.BodyPart;
import javax.mail.internet.MimeMultipart;
import net.pincette.function.SideEffect;
import net.pincette.io.DevNullInputStream;
import net.pincette.io.PathUtil;
import net.pincette.io.StreamConnector;
import net.pincette.netty.http.HttpServer;
import net.pincette.netty.http.RequestHandler;
import net.pincette.rs.Concat;
import net.pincette.rs.Fanout;
import net.pincette.rs.Source;
import net.pincette.rs.WritableByteChannelSubscriber;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class TestApp {
  private static final String KAFKA = "localhost:9092";

  private static net.pincette.json.streams.Test<
          ConsumerRecord<String, JsonObject>,
          ProducerRecord<String, JsonObject>,
          ConsumerRecord<String, String>,
          ProducerRecord<String, String>>
      test;

  private TestApp() {}

  @AfterAll
  public static void afterAll() {
    test.close();
  }

  @BeforeAll
  public static void beforeAll() {
    final Config config = createConfig();

    init(config);
    test =
        new net.pincette.json.streams.Test<>(
            () -> tester(config), () -> tester(config), createContext(config, null));
  }

  private static Stream<BodyPart> bodyParts(final MimeMultipart mmp) {
    return tryToGetRethrow(mmp::getCount)
        .map(
            count ->
                rangeExclusive(0, count)
                    .flatMap(i -> tryToGetRethrow(() -> mmp.getBodyPart(i)).stream()))
        .orElseGet(Stream::empty);
  }

  private static Optional<Path> copyApp(final String name) {
    return tryToGetRethrow(() -> createTempDirectory("json-streams"))
        .map(
            target ->
                SideEffect.<Path>run(() -> target.toFile().deleteOnExit()).andThenGet(() -> target))
        .flatMap(
            target ->
                ofNullable(TestApp.class.getResource("/" + name))
                    .flatMap(source -> tryToGetRethrow(source::toURI))
                    .map(source -> pair(Path.of(source), target)))
        .map(
            pair ->
                SideEffect.<Path>run(() -> copy(pair.first, pair.second))
                    .andThenGet(() -> pair.second.resolve(name)));
  }

  private static Config createConfig() {
    return ConfigFactory.empty()
        .withValue(LOG_TOPIC, fromAnyRef("log"))
        .withValue("kafka.bootstrap.servers", fromAnyRef(KAFKA))
        .withValue("plugins", fromAnyRef("plugins"));
  }

  private static void deleteS3(final String bucket, final String key) {
    tryToDoWithRethrow(
        S3Client::create,
        client ->
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build()));
  }

  private static boolean expandMultipart(final File multipart, final File directory) {
    return tryToGetRethrow(() -> new MimeMultipart(new FileDataSource(multipart)))
        .map(
            mmp ->
                bodyParts(mmp)
                    .allMatch(
                        part -> {
                          final String name = tryToGetRethrow(part::getFileName).orElse(null);
                          final File file = new File(directory, Objects.requireNonNull(name));

                          tryToDoRethrow(
                              () ->
                                  StreamConnector.copy(
                                      part.getInputStream(),
                                      new FileOutputStream(file),
                                      false,
                                      true));

                          return Arrays.equals(read(file), read("/files/" + name));
                        }))
        .orElse(false);
  }

  private static Subscriber<ByteBuffer> fileSubscriber(final Path file) {
    return tryToGetRethrow(() -> open(file, WRITE, SYNC))
        .map(WritableByteChannelSubscriber::writableByteChannel)
        .orElseGet(net.pincette.rs.Util::devNull);
  }

  private static byte[] getS3(final String bucket, final String key) {
    return tryToGetWithRethrow(
            S3Client::create,
            client ->
                read(
                    () ->
                        client.getObject(
                            GetObjectRequest.builder().bucket(bucket).key(key).build())))
        .orElseGet(() -> new byte[0]);
  }

  private static Publisher<ByteBuffer> multipartHeader(final HttpRequest request) {
    return Source.of(
        wrap(
            (CONTENT_TYPE + ":" + request.headers().get(CONTENT_TYPE) + "\r\n\r\n")
                .getBytes(UTF_8)));
  }

  private static RequestHandler multipartSaver(final Predicate<Path> onComplete) {
    return (request, requestBody, response) -> {
      final CompletableFuture<Publisher<ByteBuf>> future = new CompletableFuture<>();

      if (request.method().equals(POST)) {
        tryToGetRethrow(() -> createTempFile("test", ".mmp"))
            .ifPresent(
                tempFile ->
                    Concat.of(
                            multipartHeader(request),
                            with(requestBody).map(ByteBuf::nioBuffer).get())
                        .subscribe(
                            Fanout.of(
                                fileSubscriber(tempFile),
                                onComplete(
                                    () -> {
                                      response.setStatus(
                                          onComplete.test(tempFile) ? OK : INTERNAL_SERVER_ERROR);
                                      response.headers().setInt("Content-Length", 0);
                                      future.complete(null);
                                      delete(tempFile);
                                    }))));

      } else {
        response.setStatus(METHOD_NOT_ALLOWED);
        future.complete(null);
      }

      return future;
    };
  }

  private static void putResourceS3(final String resource, final String bucket, final String key) {
    tryToDoWithRethrow(
        S3Client::create,
        client ->
            client.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).build(),
                fromBytes(read(resource))));
  }

  static byte[] read(final File file) {
    return read(
        () ->
            tryToGetRethrow(() -> (InputStream) new FileInputStream(file))
                .orElseGet(DevNullInputStream::new));
  }

  static byte[] read(final String resource) {
    return read(
        () ->
            tryToGetRethrow(() -> TestApp.class.getResourceAsStream(resource))
                .orElseGet(DevNullInputStream::new));
  }

  static byte[] read(final Supplier<InputStream> in) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    tryToDoRethrow(() -> StreamConnector.copy(in.get(), out));

    return out.toByteArray();
  }

  private static void runTest(final String name) {
    copyApp(name)
        .ifPresent(
            directory -> {
              try {
                assertTrue(test.test(directory.resolve("application.yaml")));
              } finally {
                delete(directory.getParent());
              }
            });
  }

  @Test
  @DisplayName("app1")
  void app1() {
    runTest("app1");
  }

  @Test
  @DisplayName("app2")
  void app2() {
    runTest("app2");
  }

  @Test
  @DisplayName("app3")
  void app3() {
    runTest("app3");
  }

  @Test
  @DisplayName("app4")
  void app4() {
    runTest("app4");
  }

  @Test
  @DisplayName("app5")
  void app5() {
    runTest("app5");
  }

  @Test
  @DisplayName("app6")
  void app6() {
    runTest("app6");
  }

  @Test
  @DisplayName("app7")
  void app7() {
    runTest("app7");
  }

  @Test
  @DisplayName("app8")
  void app8() {
    runTest("app8");
  }

  @Test
  @DisplayName("app9")
  void app9() {
    runTest("app9");
  }

  @Test
  @DisplayName("app10")
  void app10() {
    runTest("app10");
  }

  @Test
  @DisplayName("app11")
  void app11() {
    runTest("app11");
  }

  @Test
  @DisplayName("app12")
  void app12() {
    runTest("app12");
  }

  @Test
  @DisplayName("app13")
  void app13() {
    runTest("app13");
  }

  @Test
  @DisplayName("app14")
  void app14() {
    runTest("app14");
  }

  @Test
  @DisplayName("app15")
  void app15() {
    runTest("app15");
  }

  @Test
  @DisplayName("app16")
  void app16() {
    runTest("app16");
  }

  @Test
  @DisplayName("app17")
  void app17() {
    runTest("app17");
  }

  @Test
  @DisplayName("app18")
  void app18() {
    runTest("app18");
  }

  @Test
  @DisplayName("app19")
  void app19() {
    runTest("app19");
  }

  @Test
  @DisplayName("app20")
  void app20() {
    runTest("app20");
  }

  @Test
  @DisplayName("app21")
  void app21() {
    runTest("app21");
  }

  @Test
  @DisplayName("app22")
  void app22() {
    runTest("app22");
  }

  @Test
  @DisplayName("app23")
  void app23() {
    runTest("app23");
  }

  @Test
  @DisplayName("app24")
  void app24() {
    runTest("app24");
  }

  @Test
  @DisplayName("app25")
  void app25() {
    runTest("app25");
  }

  @Test
  @DisplayName("app26")
  void app26() {
    runTest("app26");
  }

  @Test
  @DisplayName("app27")
  void app27() {
    runTest("app27");
  }

  @Test
  @DisplayName("app28")
  void app28() {
    runTest("app28");
  }

  @Test
  @DisplayName("app29")
  void app29() {
    runTest("app29");
  }

  @Test
  @DisplayName("app30")
  void app30() {
    runTest("app30");
  }

  @Test
  @DisplayName("app31")
  void app31() {
    runTest("app31");
  }

  @Test
  @DisplayName("app32")
  void app32() {
    runTest("app32");
  }

  @Test
  @DisplayName("app33")
  void app33() {
    runTest("app33");
  }

  @Test
  @DisplayName("s3attachments")
  void s3Attachments() {
    putResourceS3("/files/com2012_0429nl01.pdf", "lars-tst-docs", "com2012_0429nl01.pdf");
    putResourceS3("/files/com2012_0444nl01.pdf", "lars-tst-docs", "com2012_0444nl01.pdf");
    putResourceS3("/files/com2012_0445nl01.pdf", "lars-tst-docs", "com2012_0445nl01.pdf");
    putResourceS3("/files/com2012_0448nl01.pdf", "lars-tst-docs", "com2012_0448nl01.pdf");

    tryToDoWithRethrow(
        autoClose(() -> createTempDirectory("jsonstreams"), PathUtil::delete),
        directory -> {
          final HttpServer server =
              new HttpServer(
                  9000, multipartSaver(body -> expandMultipart(body.toFile(), directory.toFile())));

          server.run();
          runTest("s3attachments");
          server.close();
        });

    deleteS3("lars-tst-docs", "com2012_0429nl01.pdf");
    deleteS3("lars-tst-docs", "com2012_0444nl01.pdf");
    deleteS3("lars-tst-docs", "com2012_0445nl01.pdf");
    deleteS3("lars-tst-docs", "com2012_0448nl01.pdf");
  }

  @Test
  @DisplayName("s3csv")
  void s3Csv() {
    putResourceS3(
        "/files/jsonstreams-s3csv-test.csv", "lars-tst-docs", "jsonstreams-s3csv-test.csv");
    runTest("s3csv");
    deleteS3("lars-tst-docs", "jsonstreams-s3csv-test.csv");
  }

  @Test
  @DisplayName("s3out")
  void s3Out() {
    runTest("s3out");

    assertEquals(
        0,
        createDiff(
                createReader(
                        TestApp.class.getResourceAsStream(
                            "/s3out/test/topics/from/in/message.json"))
                    .readObject(),
                createReader(
                        new ByteArrayInputStream(
                            getS3("lars-tst-docs", "jsonstreams-s3out-test" + ".json")))
                    .readObject())
            .toJsonArray()
            .size());

    deleteS3("lars-tst-docs", "jsonstreams-s3out-test.json");
  }
}

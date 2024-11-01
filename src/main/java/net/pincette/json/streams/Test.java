package net.pincette.json.streams;

import static com.mongodb.client.model.changestream.FullDocument.UPDATE_LOOKUP;
import static java.lang.String.valueOf;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.list;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.emptySet;
import static java.util.Comparator.comparing;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;
import static net.pincette.jes.JsonFields.AFTER;
import static net.pincette.jes.JsonFields.BEFORE;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.JsonFields.SEQ;
import static net.pincette.jes.JsonFields.TIMESTAMP;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.streams.Application.APP_VERSION;
import static net.pincette.json.streams.Common.COLLECTION_CHANGES;
import static net.pincette.json.streams.Common.waitFor;
import static net.pincette.json.streams.Logging.info;
import static net.pincette.json.streams.Logging.severe;
import static net.pincette.json.streams.Logging.trace;
import static net.pincette.mongo.Collection.drop;
import static net.pincette.mongo.Collection.insertMany;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.LambdaSubscriber.lambdaSubscriber;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.ScheduledCompletionStage.runAsyncAfter;
import static net.pincette.util.StreamUtil.composeAsyncStream;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.padWith;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGetWithRethrow;
import static org.reactivestreams.FlowAdapters.toFlowPublisher;
import static org.reactivestreams.FlowAdapters.toSubscriber;

import com.mongodb.client.result.InsertManyResult;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonReader;
import net.pincette.mongo.BsonUtil;
import net.pincette.mongo.Collection;
import net.pincette.rs.Source;
import net.pincette.rs.streams.Message;
import net.pincette.rs.streams.Streams;
import net.pincette.util.Pair;
import org.bson.Document;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "test",
    version = APP_VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Run an application in test-mode. It should have a test folder.")
class Test<T, U, V, W> implements Callable<Integer> {
  private static final Path COLLECTIONS_FROM = Paths.get("test", "collections", "from");
  private static final Path COLLECTIONS_TO = Paths.get("test", "collections", "to");
  private static final String MONGODB = "mongodb://localhost:27017";
  private static final Path TOPICS_FROM = Paths.get("test", "topics", "from");
  private static final Path TOPICS_TO = Paths.get("test", "topics", "to");

  private Context context;
  private boolean keep;
  private final Supplier<Context> contextSupplier;
  private final Supplier<Provider<T, U, V, W>> providerSupplier;
  private final Supplier<TestProvider<T, U, V, W>> testProviderSupplier;

  @Option(
      names = {"-f", "--file"},
      required = true,
      description = "A JSON or YAML file containing an application.")
  private File file;

  Test(
      final Supplier<Provider<T, U, V, W>> providerSupplier,
      final Supplier<TestProvider<T, U, V, W>> testProviderSupplier,
      final Supplier<Context> contextSupplier) {
    this.providerSupplier = providerSupplier;
    this.testProviderSupplier = testProviderSupplier;
    this.contextSupplier = contextSupplier;
  }

  private static Context addLoadCollection(final Context context, final TestContext testContext) {
    return context.withTestLoadCollection(
        collection ->
            runAsyncAfter(
                () ->
                    loadCollection(
                        context.database.getCollection(collection),
                        testContext.collectionsFromMessages.get(collection)),
                ofSeconds(1)));
  }

  private static <V, W> Streams<String, JsonObject, V, W> addMessagesFrom(
      final Streams<String, JsonObject, V, W> streams,
      final Map<String, List<JsonObject>> messages) {
    return messages.entrySet().stream()
        .reduce(
            streams,
            (s, e) ->
                s.to(
                    trace(() -> "load messages for topic", e.getKey()),
                    with(Source.of(inputMessages(e.getValue()))).map(Logging::trace).get()),
            (s1, s2) -> s1);
  }

  private static <V, W> Streams<String, JsonObject, V, W> addExpectedTopics(
      final Streams<String, JsonObject, V, W> streams,
      final Map<String, Pair<List<JsonObject>, List<JsonObject>>> expected,
      final AtomicInteger running) {
    return expected.entrySet().stream()
        .reduce(
            streams,
            (s, e) ->
                s.consume(
                    e.getKey(),
                    values(e.getKey(), e.getValue().first, e.getValue().second, streams, running)),
            (s1, s2) -> s1);
  }

  private static boolean assertResults(
      final Map<String, Pair<List<JsonObject>, List<JsonObject>>> results) {
    return results.entrySet().stream()
        .map(e -> compareAndLog(e.getKey(), e.getValue().first, e.getValue().second))
        .reduce((r1, r2) -> r1 && r2)
        .orElse(true);
  }

  private static boolean compareAndLog(
      final String topicOrCollection,
      final List<JsonObject> expected,
      final List<JsonObject> actual) {
    final List<JsonObject> ex = sorted(expected);
    final List<JsonObject> ac = sorted(actual);
    final boolean result = ex.equals(ac);

    info(() -> "Results for " + topicOrCollection);

    if (result) {
      info("OK");
    } else {
      info(() -> "Expected: " + stringOf(ex));
      info(() -> "Actual: " + stringOf(ac));
    }

    return result;
  }

  private static String compareKey(final JsonObject json) {
    return ofNullable(json.getString(ID, null)).orElse("_no_id")
        + padWith(valueOf(json.getInt(SEQ, 0)), '0', 10)
        + ofNullable(json.getString(CORR, null)).orElse("_no_corr")
        + json.hashCode();
  }

  private static List<Message<String, JsonObject>> inputMessages(final List<JsonObject> messages) {
    return messages.stream()
        .map(
            m ->
                message(
                    ofNullable(m.getString(ID, null)).orElseGet(() -> randomUUID().toString()), m))
        .toList();
  }

  private static CompletionStage<Boolean> loadCollection(
      final MongoCollection<Document> collection, final List<JsonObject> messages) {
    return insertMany(
            collection,
            messages.stream().map(BsonUtil::fromJson).map(BsonUtil::toDocument).toList())
        .thenApply(InsertManyResult::wasAcknowledged)
        .thenApply(result -> must(result, r -> r));
  }

  private static Message<String, JsonObject> removeTimestamps(
      final Message<String, JsonObject> message) {
    return message.withValue(
        create(() -> createObjectBuilder(message.value).remove(TIMESTAMP))
            .updateIf(
                () -> ofNullable(message.value.getJsonObject(BEFORE)),
                (b, o) -> b.add(BEFORE, createObjectBuilder(o).remove(TIMESTAMP)))
            .updateIf(
                () -> ofNullable(message.value.getJsonObject(AFTER)),
                (b, o) -> b.add(AFTER, createObjectBuilder(o).remove(TIMESTAMP)))
            .build()
            .build());
  }

  private static List<JsonObject> sorted(final List<JsonObject> list) {
    return list.stream().sorted(comparing(Test::compareKey)).toList();
  }

  private static String stringOf(final List<JsonObject> messages) {
    return messages.stream().map(json -> string(json, true)).collect(joining(","));
  }

  private static <V, W> Consumer<Publisher<Message<String, JsonObject>>> values(
      final String topicOrCollection,
      final List<JsonObject> expected,
      final List<JsonObject> results,
      final Streams<String, JsonObject, V, W> streams,
      final AtomicInteger running) {
    return pub ->
        pub.subscribe(
            lambdaSubscriber(
                message -> {
                  results.add(
                      trace(
                          () -> "receive message for " + topicOrCollection,
                          removeTimestamps(message).value));
                  trace("still expecting " + (expected.size() - results.size()) + " messages");

                  if (results.size() == expected.size() && running.decrementAndGet() == 0) {
                    streams.stop();
                  }
                }));
  }

  private static Map<String, Pair<List<JsonObject>, List<JsonObject>>> withResults(
      final Map<String, List<JsonObject>> expected) {
    return expected.entrySet().stream()
        .collect(toMap(Entry::getKey, e -> pair(e.getValue(), new ArrayList<>())));
  }

  private static Context withTestDatabase(final Context context) {
    final Context ctx =
        context.client == null ? context.withClient(MongoClients.create(MONGODB)) : context;

    return ctx.database == null
        ? ctx.withDatabase(ctx.client.getDatabase(randomUUID().toString()))
        : context;
  }

  public Integer call() {
    try {
      init();

      if (!test(file.toPath())) {
        severe("Test failed.");

        return 1;
      }

      return 0;
    } finally {
      close();
    }
  }

  void close() {
    if (!keep) {
      context
          .database
          .drop()
          .subscribe(toSubscriber(lambdaSubscriber(v -> {}, context.client::close)));
    }
  }

  private CompletionStage<Void> collectionTask(
      final Set<String> collections, final Function<String, CompletionStage<Void>> task) {
    return composeAsyncStream(collections.stream().map(task))
        .thenApply(results -> results.reduce(null, (v1, v2) -> v1));
  }

  private Context completeContext(
      final Context context, final TestContext testContext, final Producer producer) {
    return addLoadCollection(context, testContext).withProducer(producer);
  }

  private CompletionStage<Void> createCollection(final String collection) {
    return Collection.create(context.database, collection).thenAccept(c -> {});
  }

  private CompletionStage<Void> createCollections(final Set<String> collections) {
    return collectionTask(collections, this::createCollection);
  }

  private CompletionStage<Void> dropCollection(final String collection) {
    return drop(context.database.getCollection(collection));
  }

  private CompletionStage<Void> dropCollections(final Set<String> collections) {
    return collectionTask(collections, this::dropCollection);
  }

  private void init() {
    if (context == null) {
      context = withTestDatabase(contextSupplier.get());
      keep = context.database != null;
    }
  }

  private void initCollections(final Set<String> collections) {
    dropCollections(collections)
        .thenComposeAsync(r -> createCollections(collections))
        .toCompletableFuture()
        .join();
  }

  private Map<String, Pair<List<JsonObject>, List<JsonObject>>> start(
      final Streams<String, JsonObject, T, U> streams,
      final Map<String, List<JsonObject>> messagesFrom,
      final Map<String, List<JsonObject>> messagesTo,
      final AtomicInteger running) {
    final Map<String, Pair<List<JsonObject>, List<JsonObject>>> expectedWithResults =
        withResults(messagesTo);

    addExpectedTopics(addMessagesFrom(streams, messagesFrom), expectedWithResults, running).start();

    return expectedWithResults;
  }

  boolean test(final Path application) {
    Set<String> allTopics = emptySet();
    final Provider<T, U, V, W> provider = providerSupplier.get();
    final TestProvider<T, U, V, W> testProvider = testProviderSupplier.get();

    try {
      final TestContext testContext = new TestContext(application);

      must(testContext.allSet());
      init();

      final Optional<App<T, U, V, W>> app =
          new Run<>(
                  () -> provider,
                  () -> completeContext(context, testContext, testProvider.getProducer()))
              .createApp(application.toFile());

      allTopics = testContext.allTopics();
      testProvider.deleteTopics(allTopics);
      testProvider.createTopics(allTopics);
      info("Topics created");
      initCollections(testContext.allCollections());
      info("Collections created");

      return app.map(
              a -> {
                a.start();
                waitFor(() -> testProvider.allAssigned(a.name(), testContext.topicsFrom));
                info("Topics assigned");

                final AtomicInteger running =
                    new AtomicInteger(
                        testContext.topicsTo.size() + testContext.collectionsTo.size());
                final Streams<String, JsonObject, T, U> streams =
                    testProvider.builder(a.name() + "-test");
                final Map<String, Pair<List<JsonObject>, List<JsonObject>>>
                    expectedWithResultsCollections =
                        watchCollections(streams, testContext.collectionsToMessages, running);
                final Map<String, Pair<List<JsonObject>, List<JsonObject>>>
                    expectedWithResultsTopics =
                        start(
                            streams,
                            testContext.topicsFromMessages,
                            testContext.topicsToMessages,
                            running);

                waitFor(() -> completedFuture(running.intValue() == 0));
                a.stop();

                return assertResults(expectedWithResultsCollections)
                    && assertResults(expectedWithResultsTopics);
              })
          .orElse(false);
    } finally {
      testProvider.deleteTopics(allTopics);
      info("Topics deleted");
      tryToDoRethrow(provider::close);
      tryToDoRethrow(testProvider::close);
    }
  }

  private Publisher<JsonObject> watchCollection(final String collection) {
    return with(toFlowPublisher(
            context
                .database
                .getCollection(collection)
                .watch(COLLECTION_CHANGES)
                .fullDocument(UPDATE_LOOKUP)))
        .map(Common::changedDocument)
        .get();
  }

  private Map<String, Pair<List<JsonObject>, List<JsonObject>>> watchCollections(
      final Streams<String, JsonObject, T, U> streams,
      final Map<String, List<JsonObject>> expected,
      final AtomicInteger running) {
    final Map<String, Pair<List<JsonObject>, List<JsonObject>>> expectedWithResults =
        withResults(expected);

    expectedWithResults.forEach(
        (collection, expectedAndActual) ->
            values(collection, expectedAndActual.first, expectedAndActual.second, streams, running)
                .accept(with(watchCollection(collection)).map(Common::toMessage).get()));

    return expectedWithResults;
  }

  private static class TestContext {
    private final Set<String> collectionsFrom;
    private final Map<String, List<JsonObject>> collectionsFromMessages;
    private final Set<String> collectionsTo;
    private final Map<String, List<JsonObject>> collectionsToMessages;
    private final Set<String> topicsFrom;
    private final Map<String, List<JsonObject>> topicsFromMessages;
    private final Set<String> topicsTo;
    private final Map<String, List<JsonObject>> topicsToMessages;

    private TestContext(final Path application) {
      collectionsFrom = names(application, COLLECTIONS_FROM).orElse(null);
      collectionsFromMessages =
          loadMessagesPerDirectory(application, COLLECTIONS_FROM).orElse(null);
      collectionsTo = names(application, COLLECTIONS_TO).orElse(null);
      collectionsToMessages = loadMessagesPerDirectory(application, COLLECTIONS_TO).orElse(null);
      topicsFrom = names(application, TOPICS_FROM).orElse(null);
      topicsFromMessages = loadMessagesPerDirectory(application, TOPICS_FROM).orElse(null);
      topicsTo = names(application, TOPICS_TO).orElse(null);
      topicsToMessages = loadMessagesPerDirectory(application, TOPICS_TO).orElse(null);
    }

    private static Stream<Path> listDirectory(final Path directory) throws IOException {
      return exists(directory) ? list(directory) : empty();
    }

    private static Optional<List<JsonObject>> loadMessages(final Path directory) {
      return tryToGetWithRethrow(
          () -> listDirectory(directory),
          list -> list.sorted().flatMap(path -> parse(path).stream()).toList());
    }

    private static Optional<Map<String, List<JsonObject>>> loadMessagesPerDirectory(
        final Path application, final Path directory) {
      return loadMessagesPerDirectory(application.getParent().resolve(directory));
    }

    private static Optional<Map<String, List<JsonObject>>> loadMessagesPerDirectory(
        final Path directory) {
      return tryToGetWithRethrow(
          () -> exists(directory) ? list(directory) : Stream.<Path>empty(),
          list ->
              map(
                  list.filter(Files::isDirectory)
                      .flatMap(
                          d ->
                              loadMessages(d).stream()
                                  .map(m -> pair(d.getFileName().toString(), m)))));
    }

    private static Optional<Set<String>> names(final Path application, final Path testDirectory) {
      return names(application.getParent().resolve(testDirectory));
    }

    private static Optional<Set<String>> names(final Path directory) {
      return tryToGetWithRethrow(
          () -> exists(directory) ? list(directory) : Stream.<Path>empty(),
          list ->
              list.filter(Files::isDirectory)
                  .map(Path::getFileName)
                  .map(Path::toString)
                  .collect(toSet()));
    }

    private static Optional<JsonObject> parse(final Path path) {
      return tryToGetWithRethrow(
          () -> createReader(new FileReader(path.toFile())), JsonReader::readObject);
    }

    private boolean allSet() {
      return collectionsFrom != null
          && collectionsFromMessages != null
          && collectionsTo != null
          && collectionsToMessages != null
          && topicsFrom != null
          && topicsFromMessages != null
          && topicsTo != null
          && topicsToMessages != null;
    }

    private Set<String> allCollections() {
      return concat(collectionsFrom.stream(), collectionsTo.stream()).collect(toSet());
    }

    private Set<String> allTopics() {
      return concat(topicsFrom.stream(), topicsTo.stream()).collect(toSet());
    }
  }
}

package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.eq;
import static java.lang.Boolean.TRUE;
import static java.lang.Runtime.getRuntime;
import static java.util.Optional.ofNullable;
import static net.pincette.jes.util.Href.setContextPath;
import static net.pincette.json.streams.Application.APP_VERSION;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.BACKOFF;
import static net.pincette.json.streams.Common.addKafkaLogger;
import static net.pincette.json.streams.Common.application;
import static net.pincette.json.streams.Common.build;
import static net.pincette.json.streams.Common.createApplicationContext;
import static net.pincette.json.streams.Common.removeSuffix;
import static net.pincette.json.streams.Logging.LOGGER;
import static net.pincette.json.streams.Logging.LOGGER_NAME;
import static net.pincette.json.streams.Logging.info;
import static net.pincette.json.streams.Read.readTopologies;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.ScheduledCompletionStage.runAsyncAfter;
import static net.pincette.util.Util.doForever;
import static net.pincette.util.Util.isUUID;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGetSilent;

import com.mongodb.reactivestreams.client.MongoCollection;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.BsonUtil;
import net.pincette.mongo.Features;
import net.pincette.mongo.Match;
import org.bson.Document;
import org.bson.conversions.Bson;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "run",
    version = APP_VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Runs applications from a file containing a JSON array or a MongoDB collection.")
class Run<T, U, V, W> implements Runnable {
  private static final String CONTEXT_PATH = "contextPath";
  private static final String PLUGINS = "plugins";
  private final Supplier<Context> contextSupplier;
  private final Supplier<Provider<T, U, V, W>> providerSupplier;
  private final BlockingQueue<Runnable> runQueue = new LinkedBlockingQueue<>();
  private final Map<String, App<T, U, V, W>> running = new HashMap<>();
  private Context context;
  @ArgGroup() private FileOrCollection fileOrCollection;
  private KeepAlive keepAlive;
  private Leader leader;
  private Provider<T, U, V, W> provider;

  Run(
      final Supplier<Provider<T, U, V, W>> providerSupplier,
      final Supplier<Context> contextSupplier) {
    this.providerSupplier = providerSupplier;
    this.contextSupplier = contextSupplier;
  }

  private static boolean excludeCLIGenerated(final String group) {
    return !isUUID(group);
  }

  private static Context loadPlugins(final Context context) {
    return tryToGetSilent(() -> context.config.getString(PLUGINS))
        .map(Paths::get)
        .map(Plugins::load)
        .map(plugins -> withPlugins(plugins, context))
        .orElse(context);
  }

  private static Context prepareContext(final Context context) {
    return context
        .doTask(c -> LOGGER.info("Connecting to Kafka ..."))
        .withLogger(() -> LOGGER)
        .withProducer(new Producer(context.config))
        .doTask(c -> addKafkaLogger(LOGGER_NAME, APP_VERSION, c))
        .doTask(c -> LOGGER.info("Loading plugins ..."))
        .with(Run::loadPlugins)
        .doTask(c -> LOGGER.info("Settings up metrics ..."))
        .doTask(
            c ->
                setContextPath(tryToGetSilent(() -> c.config.getString(CONTEXT_PATH)).orElse(null)))
        .doTask(c -> LOGGER.info("Loading applications ..."));
  }

  private static Context withPlugins(final Plugins plugins, final Context context) {
    return context
        .addStageExtensions(plugins.stageExtensions)
        .addFeatures(
            new Features()
                .withExpressionExtensions(plugins.expressionExtensions)
                .withMatchExtensions(plugins.matchExtensions)
                .withCustomJsltFunctions(plugins.jsltFunctions));
  }

  @SuppressWarnings("java:S106") // The logger is no longer available when shutting down.
  private void close() {
    System.out.println("SHUTDOWN");

    if (keepAlive != null) {
      System.out.println("Stopping keep alive");
      keepAlive.stop();
    }

    if (leader != null) {
      System.out.println("Stopping leader election");
      leader.stop();
    }

    new ArrayList<>(running.values())
        .forEach(
            app -> {
              System.out.println("Stopping " + app.name());
              app.stop();
              System.out.println("Stopped " + app.name());
            });
  }

  Optional<App<T, U, V, W>> createApp(final File file) {
    provider = providerSupplier.get();
    context = prepareContext(contextSupplier.get());

    return createApplications(readTopologies(file), file, context).findFirst();
  }

  private App<T, U, V, W> createApplication(final JsonObject specification, final Context context) {
    return new App<T, U, V, W>()
        .withSpecification(specification)
        .withContext(context)
        .withBuilder(provider::builder)
        .withStringBuilder(provider::stringBuilder)
        .withMessageLag(() -> provider.messageLag(Run::excludeCLIGenerated))
        .withOnError(t -> restart(application(specification)));
  }

  private Stream<App<T, U, V, W>> createApplications(
      final Stream<Loaded> loaded, final File topFile, final Context context) {
    return loaded
        .map(l -> pair(l.specification, createApplicationContext(l, topFile, context)))
        .map(pair -> build(pair.first, true, pair.second))
        .filter(Validate::validateApplication)
        .map(specification -> createApplication(specification, context));
  }

  private boolean fromCollection() {
    return getFile().isEmpty();
  }

  private MongoCollection<Document> getApplicationCollection() {
    return context.database.getCollection(
        Common.getApplicationCollection(
            getCollectionOptions().map(o -> o.collection).orElse(null), context));
  }

  private Stream<Loaded> getApplications() {
    return getFile()
        .map(Read::readTopologies)
        .orElseGet(
            () -> Common.getApplications(getApplicationCollection(), getFilter()).map(Loaded::new));
  }

  private Optional<FileOrCollection.CollectionOptions> getCollectionOptions() {
    return ofNullable(fileOrCollection).map(f -> f.collection);
  }

  private Optional<File> getFile() {
    return ofNullable(fileOrCollection).map(f -> f.file).map(o -> o.file);
  }

  private Bson getFilter() {
    return tryWith(this::getFilterQuery).or(this::getFilterApplication).get().orElse(null);
  }

  private Bson getFilterApplication() {
    return getCollectionOptions()
        .map(o -> o.selection.application)
        .map(a -> eq(APPLICATION_FIELD, a))
        .orElse(null);
  }

  private Bson getFilterQuery() {
    return getCollectionOptions()
        .map(o -> o.selection.query)
        .flatMap(JsonUtil::from)
        .map(JsonValue::asJsonObject)
        .map(BsonUtil::fromJson)
        .orElse(null);
  }

  private void restart(final String application) {
    info(() -> "Restart " + application);
    ofNullable(running.get(application))
        .ifPresent(
            app ->
                runQueue.add(
                    () -> {
                      app.stop();
                      runAsyncAfter(() -> runQueue.add(app::start), BACKOFF);
                    }));
  }

  public void run() {
    final Predicate<JsonObject> filter = ofNullable(getFilter()).map(Match::predicate).orElse(null);

    info(() -> "Version " + APP_VERSION);
    provider = providerSupplier.get();
    context = prepareContext(contextSupplier.get());

    if (fromCollection() && filter == null) {
      startWork();
    } else {
      final Stream<App<T, U, V, W>> applications =
          createApplications(getApplications(), getFile().orElse(null), context);

      runQueue.add(
          () ->
              applications.forEach(
                  app -> {
                    running.put(app.name(), app);
                    app.start();
                  }));
    }

    start();
    tryToDoRethrow(provider::close);
    info("Stopped");
  }

  private void start() {
    getRuntime().addShutdownHook(new Thread(this::close));
    info("Ready");
    doForever(() -> runQueue.take().run());
  }

  private void start(final String application) {
    runQueue.add(
        () ->
            createApplications(
                    Common.getApplications(
                            getApplicationCollection(), eq(APPLICATION_FIELD, application))
                        .map(Loaded::new),
                    null,
                    context)
                .forEach(this::start));
  }

  private void start(final App<T, U, V, W> application) {
    running.put(application.name(), application);
    application.start();
    keepAlive.start(removeSuffix(application.name(), context));
  }

  private void startWork() {
    final var collection = getApplicationCollection();
    final var work = new Work<>(collection, provider, context);

    keepAlive = new KeepAlive(getApplicationCollection(), this::start, this::stop, context).start();
    leader =
        new Leader(
                isLeader -> {
                  if (TRUE.equals(isLeader)) {
                    work.giveWork();
                  }
                },
                collection,
                context)
            .start();
  }

  private void stop(final String application) {
    runQueue.add(
        () ->
            ofNullable(running.remove(application))
                .ifPresent(
                    app -> {
                      app.stop();
                      keepAlive.stop(removeSuffix(application, context));
                    }));
  }

  private static class FileOrCollection {
    @ArgGroup(exclusive = false)
    private FileOptions file;

    @ArgGroup(exclusive = false)
    private CollectionOptions collection;

    private static class FileOptions {
      @Option(
          names = {"-f", "--file"},
          required = true,
          description = "A JSON or YAML file containing an array of applications.")
      private File file;
    }

    private static class CollectionOptions {
      @Option(
          names = {"-c", "--collection"},
          description = "A MongoDB collection containing the applications.")
      private String collection;

      @ArgGroup private TopologySelection selection;

      private static class TopologySelection {
        @Option(
            names = {"-a", "--application"},
            description =
                "An application from the MongoDB collection containing the applications. This is "
                    + "a shorthand for the query {\"application\": \"name\"}.")
        private String application;

        @Option(
            names = {"-q", "--query"},
            description = "The MongoDB query into the collection containing the applications.")
        private String query;
      }
    }
  }
}

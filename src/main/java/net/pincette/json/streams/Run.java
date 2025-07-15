package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.eq;
import static java.lang.Boolean.FALSE;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Runtime.getRuntime;
import static java.time.Duration.ofSeconds;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.logging.Level.SEVERE;
import static net.pincette.config.Util.configValue;
import static net.pincette.jes.tel.OtelUtil.counter;
import static net.pincette.jes.tel.OtelUtil.metrics;
import static net.pincette.jes.util.Href.setContextPath;
import static net.pincette.json.streams.Application.APP_VERSION;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.BACKOFF;
import static net.pincette.json.streams.Common.DATABASE;
import static net.pincette.json.streams.Common.JSON_STREAMS;
import static net.pincette.json.streams.Common.MONGODB_URI;
import static net.pincette.json.streams.Common.addOtelLogger;
import static net.pincette.json.streams.Common.application;
import static net.pincette.json.streams.Common.applicationAttributes;
import static net.pincette.json.streams.Common.build;
import static net.pincette.json.streams.Common.createApplicationContext;
import static net.pincette.json.streams.Common.meterProvider;
import static net.pincette.json.streams.Common.removeSuffix;
import static net.pincette.json.streams.Common.tryToGetForever;
import static net.pincette.json.streams.Logging.LOGGER;
import static net.pincette.json.streams.Logging.LOGGER_NAME;
import static net.pincette.json.streams.Logging.exception;
import static net.pincette.json.streams.Logging.getLogger;
import static net.pincette.json.streams.Logging.info;
import static net.pincette.json.streams.Read.readTopologies;
import static net.pincette.json.streams.Work.WorkContext.maximumAppsPerInstance;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.ScheduledCompletionStage.runAsyncAfter;
import static net.pincette.util.Util.doForever;
import static net.pincette.util.Util.doUntil;
import static net.pincette.util.Util.isUUID;
import static net.pincette.util.Util.tryToDo;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToDoSilent;
import static net.pincette.util.Util.tryToGetSilent;

import com.mongodb.reactivestreams.client.MongoCollection;
import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.BsonUtil;
import net.pincette.mongo.Features;
import net.pincette.util.State;
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
    description = "Runs applications from files containing applications or a MongoDB collection.")
class Run<T, U, V, W> implements Runnable {
  private static final String BACKGROUND_INTERVAL = "backgroundInterval";
  private static final String CONTEXT_PATH = "contextPath";
  private static final Duration DEFAULT_BACKGROUND_INTERVAL = ofSeconds(5);
  private static final String METRIC_STARTS = "json_streams.starts";
  private static final String METRIC_STOPS = "json_streams.stops";
  private static final int MAX_POOL_SIZE = 20;
  private static final int MIN_POOL_SIZE = 5;
  private static final String PLUGINS = "plugins";

  private final Supplier<Context> contextSupplier;
  private final Map<String, Consumer<String>> appCounterConsumers = new HashMap<>();
  private final Set<AutoCloseable> appCounters = new HashSet<>();
  private final Supplier<Provider<T, U, V, W>> providerSupplier;
  private final BlockingQueue<Runnable> runQueue = new LinkedBlockingQueue<>();
  private final Map<String, App<T, U, V, W>> running = new HashMap<>();
  private Context context;
  @ArgGroup() private FileOrCollection fileOrCollection;
  private KeepAlive keepAlive;
  private Leader leader;
  private Provider<T, U, V, W> provider;
  private boolean stop;

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
    final var namespace = Common.namespace(context.config);

    return context
        .doTask(c -> LOGGER.info("Connecting to Kafka ..."))
        .withLogger(() -> LOGGER)
        .withProducer(new Producer(context.config))
        .doTask(c -> addOtelLogger(LOGGER_NAME, APP_VERSION, c))
        .doTask(Run::reactivePoolSize)
        .withMetrics(metrics(namespace, JSON_STREAMS, APP_VERSION, context.config).orElse(null))
        .doTask(c -> LOGGER.info("Loading plugins ..."))
        .with(Run::loadPlugins)
        .doTask(c -> LOGGER.info("Settings up metrics ..."))
        .doTask(
            c ->
                setContextPath(tryToGetSilent(() -> c.config.getString(CONTEXT_PATH)).orElse(null)))
        .doTask(c -> LOGGER.info("Loading applications ..."));
  }

  private static int reactivePoolSize(final Context context) {
    return min(MAX_POOL_SIZE, max(MIN_POOL_SIZE, maximumAppsPerInstance(context)));
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

  private static Duration workInterval(final Context context) {
    return configValue(context.config::getDuration, BACKGROUND_INTERVAL)
        .orElse(DEFAULT_BACKGROUND_INTERVAL);
  }

  private Consumer<String> appCounter(final String application, final String metric) {
    return appCounterConsumers.computeIfAbsent(
        application + "#" + metric,
        k ->
            meterProvider(context)
                .map(p -> p.meterBuilder(APPLICATION_FIELD).build())
                .map(
                    m ->
                        counter(
                            m,
                            metric,
                            (String app) -> applicationAttributes(app, context).build(),
                            app -> 1L,
                            appCounters))
                .orElse(a -> {}));
  }

  @SuppressWarnings("java:S106") // The logger is no longer available when shutting down.
  private void close() {
    System.out.println("SHUTDOWN");
    stop = true;
    keepAlive.stop();
    leader.stop();

    new ArrayList<>(running.values())
        .forEach(
            app -> {
              System.out.println("Stopping " + app.name());
              app.stop();
              System.out.println("Stopped " + app.name());
            });

    appCounters.forEach(c -> tryToDoSilent(c::close));
  }

  Optional<App<T, U, V, W>> createApp(final File file) {
    provider = providerSupplier.get();
    context = prepareContext(contextSupplier.get());

    return createApplications(readTopologies(file), context).findFirst();
  }

  private App<T, U, V, W> createApplication(final JsonObject specification, final Context context) {
    return new App<T, U, V, W>()
        .withSpecification(specification)
        .withContext(context)
        .withBuilder(provider::builder)
        .withStringBuilder(provider::stringBuilder)
        .withMessageLag(() -> provider.messageLag(Run::excludeCLIGenerated))
        .withOnError(restartOnError(specification, context));
  }

  private Stream<App<T, U, V, W>> createApplications(
      final Stream<Loaded> loaded, final Context context) {
    return loaded
        .map(l -> pair(l.specification, createApplicationContext(l, context)))
        .map(pair -> build(pair.first, true, pair.second))
        .filter(Validate::validateApplication)
        .map(specification -> createApplication(specification, context));
  }

  private MongoCollection<Document> getApplicationCollection() {
    return context.database.getCollection(getApplicationCollectionName());
  }

  private String getApplicationCollectionName() {
    return Common.getApplicationCollection(
        getCollectionOptions().map(o -> o.collection).orElse(null), context);
  }

  private com.mongodb.client.MongoCollection<Document> getApplicationCollectionSync() {
    return configValue(context.config::getString, MONGODB_URI)
        .map(com.mongodb.client.MongoClients::create)
        .flatMap(c -> configValue(context.config::getString, DATABASE).map(c::getDatabase))
        .map(d -> d.getCollection(getApplicationCollectionName()))
        .orElse(null);
  }

  private Stream<Loaded> getApplications() {
    return getFiles()
        .map(files -> stream(files).flatMap(Read::readTopologies))
        .orElseGet(
            () -> Common.getApplications(getApplicationCollection(), getFilter()).map(Loaded::new));
  }

  private Optional<FileOrCollection.CollectionOptions> getCollectionOptions() {
    return ofNullable(fileOrCollection).map(f -> f.collection);
  }

  private Optional<File[]> getFiles() {
    return ofNullable(fileOrCollection).map(f -> f.file).map(o -> o.files);
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

  private void restart(final JsonObject specification, final Context context) {
    final var application = application(specification);

    info(() -> "Restart " + application);
    ofNullable(running.get(application))
        .ifPresent(
            app ->
                runQueue.add(
                    () -> {
                      app.stop();
                      running.put(application, createApplication(specification, context));
                      runAsyncAfter(() -> runQueue.add(running.get(application)::start), BACKOFF);
                    }));
  }

  private Consumer<Throwable> restartOnError(
      final JsonObject specification, final Context context) {
    final State<Boolean> seen = new State<>(false);

    return t -> {
      if (FALSE.equals(seen.get())) {
        seen.set(true);
        exception(t, null, () -> getLogger(application(specification)));
        restart(specification, context);
      }
    };
  }

  public void run() {
    info(() -> "Version " + APP_VERSION);
    provider = providerSupplier.get();
    context = prepareContext(contextSupplier.get());

    if (fileOrCollection == null) {
      new Thread(this::startWork).start();
    } else {
      final Stream<App<T, U, V, W>> applications = createApplications(getApplications(), context);

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
    if (!running.containsKey(application)) {
      var apps =
          tryToGetForever(
                  () ->
                      Common.getApplicationsAsync(
                              getApplicationCollection(), eq(APPLICATION_FIELD, application))
                          .thenApply(stream -> stream.map(Loaded::new))
                          .thenApply(loaded -> createApplications(loaded, context)))
              .toCompletableFuture()
              .join();

      runQueue.add(() -> apps.forEach(this::start));
    }
  }

  private void start(final App<T, U, V, W> application) {
    tryToDo(
        () -> {
          keepAlive.start(removeSuffix(application.name(), context));
          application.start();
          running.put(application.name(), application);
          appCounter(application.name(), METRIC_STARTS).accept(application.name());
        },
        e -> {
          LOGGER.log(SEVERE, e, () -> "Cannot start application " + application.name());
          runAsyncAfter(() -> start(application.name()), BACKOFF);
        });
  }

  private void startWork() {
    final var collection = getApplicationCollectionSync();
    final var work = new Work<>(collection, provider, context);

    leader = new Leader(collection, context);
    keepAlive = new KeepAlive(collection, this::start, this::stop, context);

    doUntil(
        () -> {
          tryToDo(
              () -> {
                keepAlive.setAlive();

                if (leader.isLeader()) {
                  info(() -> context.instance + " is the leader.");
                  work.giveWork();
                }
              },
              Logging::exception);

          return stop;
        },
        workInterval(context));
  }

  private void stop(final String application) {
    runQueue.add(
        () ->
            ofNullable(running.remove(application))
                .ifPresent(
                    app -> {
                      app.stop();
                      keepAlive.stop(removeSuffix(application, context));
                      appCounter(application, METRIC_STOPS).accept(application);
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
          arity = "1..*",
          description = "JSON or YAML files containing an array of applications.")
      private File[] files;
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

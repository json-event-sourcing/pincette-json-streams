package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.eq;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.exit;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.concat;
import static net.pincette.jes.Aggregate.reducer;
import static net.pincette.jes.elastic.Logging.log;
import static net.pincette.jes.elastic.Logging.logKafka;
import static net.pincette.jes.util.Href.setContextPath;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.fromConfig;
import static net.pincette.jes.util.Mongo.withResolver;
import static net.pincette.jes.util.MongoExpressions.operators;
import static net.pincette.jes.util.Util.compose;
import static net.pincette.jes.util.Validation.validator;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.Jslt.transformerObject;
import static net.pincette.json.Jslt.tryReader;
import static net.pincette.json.JsltCustom.customFunctions;
import static net.pincette.json.JsltCustom.trace;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.getObject;
import static net.pincette.json.JsonUtil.getObjects;
import static net.pincette.json.JsonUtil.getStrings;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.toNative;
import static net.pincette.json.streams.Common.AGGREGATE;
import static net.pincette.json.streams.Common.AGGREGATE_TYPE;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.COMMANDS;
import static net.pincette.json.streams.Common.DESTINATIONS;
import static net.pincette.json.streams.Common.DESTINATION_TYPE;
import static net.pincette.json.streams.Common.DOLLAR;
import static net.pincette.json.streams.Common.DOT;
import static net.pincette.json.streams.Common.ENVIRONMENT;
import static net.pincette.json.streams.Common.EVENT_TO_COMMAND;
import static net.pincette.json.streams.Common.FILTER;
import static net.pincette.json.streams.Common.FROM_STREAM;
import static net.pincette.json.streams.Common.FROM_STREAMS;
import static net.pincette.json.streams.Common.FROM_TOPIC;
import static net.pincette.json.streams.Common.FROM_TOPICS;
import static net.pincette.json.streams.Common.JOIN;
import static net.pincette.json.streams.Common.JSLT_IMPORTS;
import static net.pincette.json.streams.Common.LEFT;
import static net.pincette.json.streams.Common.MERGE;
import static net.pincette.json.streams.Common.NAME;
import static net.pincette.json.streams.Common.ON;
import static net.pincette.json.streams.Common.PARTS;
import static net.pincette.json.streams.Common.PIPELINE;
import static net.pincette.json.streams.Common.REACTOR;
import static net.pincette.json.streams.Common.REDUCER;
import static net.pincette.json.streams.Common.RIGHT;
import static net.pincette.json.streams.Common.SLASH;
import static net.pincette.json.streams.Common.SOURCE_TYPE;
import static net.pincette.json.streams.Common.STREAM;
import static net.pincette.json.streams.Common.STREAM_TYPES;
import static net.pincette.json.streams.Common.TYPE;
import static net.pincette.json.streams.Common.VALIDATE;
import static net.pincette.json.streams.Common.VALIDATOR;
import static net.pincette.json.streams.Common.VALIDATOR_IMPORTS;
import static net.pincette.json.streams.Common.VERSION;
import static net.pincette.json.streams.Common.WINDOW;
import static net.pincette.json.streams.Common.build;
import static net.pincette.json.streams.Common.createTopologyContext;
import static net.pincette.json.streams.Common.fatal;
import static net.pincette.json.streams.Common.numberLines;
import static net.pincette.json.streams.Common.transformFieldNames;
import static net.pincette.json.streams.PipelineStages.logStage;
import static net.pincette.json.streams.PipelineStages.validateStage;
import static net.pincette.json.streams.Plugins.load;
import static net.pincette.json.streams.Validate.validateTopology;
import static net.pincette.mongo.BsonUtil.fromBson;
import static net.pincette.mongo.BsonUtil.toBsonDocument;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.Expression.replaceVariables;
import static net.pincette.mongo.JsonClient.aggregationPublisher;
import static net.pincette.mongo.JsonClient.find;
import static net.pincette.mongo.Match.predicate;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Collections.union;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGet;
import static net.pincette.util.Util.tryToGetSilent;
import static org.apache.kafka.clients.admin.AdminClientConfig.configNames;
import static org.apache.kafka.streams.kstream.Produced.valueSerde;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.typesafe.config.Config;
import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.jes.Aggregate;
import net.pincette.jes.EventToCommand;
import net.pincette.jes.GetDestinations;
import net.pincette.jes.Reactor;
import net.pincette.jes.util.JsonSerializer;
import net.pincette.jes.util.Reducer;
import net.pincette.jes.util.Streams;
import net.pincette.jes.util.Streams.Stop;
import net.pincette.jes.util.Streams.TopologyLifeCycle;
import net.pincette.jes.util.Streams.TopologyLifeCycleEmitter;
import net.pincette.json.Jslt;
import net.pincette.json.Jslt.MapResolver;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.BsonUtil;
import net.pincette.mongo.Match;
import net.pincette.mongo.Validator;
import net.pincette.mongo.Validator.Resolved;
import net.pincette.mongo.streams.Pipeline;
import net.pincette.rs.LambdaSubscriber;
import net.pincette.util.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.bson.Document;
import org.bson.conversions.Bson;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "run",
    description = "Runs topologies from a file containing a JSON array or a MongoDB collection.")
class Run implements Runnable {
  private static final String APPLICATION_ID = "application.id";
  private static final String APP_VERSION = "1.7.13";
  private static final String CONTEXT_PATH = "contextPath";
  private static final Duration DEFAULT_RESTART_BACKOFF = ofSeconds(10);
  private static final String EVENT = "$$event";
  private static final String GROUP_ID_SUFFIX = "groupIdSuffix";
  private static final String KAFKA = "kafka";
  private static final Set<String> KAFKA_ADMIN_CONFIG_NAMES =
      union(
          configNames(),
          set("ssl.endpoint.identification.algorithm", "sasl.mechanism", "sasl.jaas.config"));
  private static final String LOG = "$log";
  private static final String METRICS_INTERVAL = "metricsInterval";
  private static final String METRICS_TOPIC = "metricsTopic";
  private static final String PLUGINS = "plugins";
  private static final String PROJECT = "$project";
  private static final String RESTART_BACKOFF = "restartBackoff";
  private static final String TO_STRING = "toString";
  private static final String TO_TOPIC = "toTopic";
  private static final String TOPOLOGY_TOPIC = "topologyTopic";
  private static final String UNIQUE_EXPRESSION = "uniqueExpression";
  private final Duration restartBackoff;
  private final Map<String, TopologyEntry> running = new ConcurrentHashMap<>();
  @ArgGroup() FileOrCollection fileOrCollection;
  private Context context;

  Run(final Context context) {
    this.context = context;
    restartBackoff = getRestartBackoff(context);
  }

  private static Context addPipelineStages(final Context context) {
    return context.withStageExtensions(
        merge(
            context.stageExtensions,
            map(pair(LOG, logStage(context)), pair(VALIDATE, validateStage(context)))));
  }

  private static void addStreams(final Aggregate aggregate, final TopologyContext context) {
    final var type = aggregate.fullType();

    context.streams.put(type + "-aggregate", aggregate.aggregates());
    context.streams.put(type + "-command", aggregate.commands());
    context.streams.put(type + "-event", aggregate.events());
    context.streams.put(type + "-event-full", aggregate.eventsFull());
    context.streams.put(type + "-reply", aggregate.replies());
  }

  private static Optional<Pair<String, String>> aggregateTypeParts(final JsonObject specification) {
    final var type = specification.getString(AGGREGATE_TYPE);

    return Optional.of(type.indexOf('-'))
        .filter(index -> index != -1)
        .map(index -> pair(type.substring(0, index), type.substring(index + 1)));
  }

  private static Map<String, String> convertJsltImports(final JsonObject jsltImports) {
    return jsltImports.entrySet().stream()
        .filter(e -> isString(e.getValue()))
        .collect(toMap(Entry::getKey, e -> asString(e.getValue()).getString()));
  }

  private static StreamsBuilder createAggregate(
      final JsonObject config, final TopologyContext context) {
    aggregateTypeParts(config)
        .ifPresent(
            aggregateType -> {
              final var aggregate =
                  reducers(
                      create(
                              () ->
                                  new Aggregate()
                                      .withApp(aggregateType.first)
                                      .withType(aggregateType.second)
                                      .withMongoDatabase(context.context.database)
                                      .withMongoDatabaseArchive(context.context.databaseArchive)
                                      .withBuilder(context.builder)
                                      .withLogger(context.context.logger))
                          .updateIf(() -> environment(config, context), Aggregate::withEnvironment)
                          .updateIf(
                              () -> getValue(config, "/" + UNIQUE_EXPRESSION),
                              Aggregate::withUniqueExpression)
                          .build(),
                      config,
                      context);

              aggregate.build();
              logAggregate(aggregate, config.getString(VERSION, ""), context.context);
              addStreams(aggregate, context);
            });

    return context.builder;
  }

  private static KStream<String, JsonObject> createJoin(
      final JsonObject config, final TopologyContext context) {
    final var leftKey = function(config.getValue("/" + LEFT + "/" + ON), context.context.features);
    final var rightKey =
        function(config.getValue("/" + RIGHT + "/" + ON), context.context.features);

    return toStream(
        fromStream(config.getJsonObject(LEFT), context)
            .map((k, v) -> switchKey(v, leftKey))
            .filter((k, v) -> k != null && v != null)
            .join(
                fromStream(config.getJsonObject(RIGHT), context)
                    .map((k, v) -> switchKey(v, rightKey))
                    .filter((k, v) -> k != null && v != null),
                (v1, v2) -> createObjectBuilder().add(LEFT, v1).add(RIGHT, v2).build(),
                JoinWindows.of(Duration.ofMillis(config.getInt(WINDOW)))),
        config);
  }

  private static KStream<String, JsonObject> createMerge(
      final JsonObject config, final TopologyContext context) {
    return toStream(
        (config.containsKey(FROM_TOPICS)
                ? getStrings(config, FROM_TOPICS).map(topic -> stream(topic, context.builder))
                : getStrings(config, FROM_STREAMS).map(stream -> getStream(stream, context)))
            .reduce(KStream::merge)
            .orElse(null),
        config);
  }

  private static KStream<String, JsonObject> createPart(
      final JsonObject config, final TopologyContext context) {
    switch (config.getString(TYPE)) {
      case JOIN:
        return createJoin(config, context);
      case MERGE:
        return createMerge(config, context);
      case STREAM:
        return createStream(config, context);
      default:
        return null;
    }
  }

  private static StreamsBuilder createParts(final JsonArray parts, final TopologyContext context) {
    parts(parts, set(AGGREGATE)).forEach(part -> createAggregate(part, context));
    parts(parts, set(REACTOR)).forEach(part -> createReactor(part, context));
    parts(parts, STREAM_TYPES)
        .forEach(part -> context.configurations.put(part.getString(NAME), part));
    parts(parts, STREAM_TYPES).forEach(part -> getStream(part.getString(NAME), context));

    return context.builder;
  }

  private static StreamsBuilder createReactor(
      final JsonObject config, final TopologyContext context) {
    return create(
            () ->
                new Reactor()
                    .withSourceType(config.getString(SOURCE_TYPE))
                    .withDestinationType(config.getString(DESTINATION_TYPE))
                    .withBuilder(context.builder)
                    .withDestinations(
                        getDestinations(
                            config.getJsonArray(DESTINATIONS),
                            config.getString(DESTINATION_TYPE),
                            context.context))
                    .withEventToCommand(
                        eventToCommand(config.getString(EVENT_TO_COMMAND), context)))
        .updateIf(() -> environment(config, context), Reactor::withEnvironment)
        .updateIf(
            () -> ofNullable(config.getJsonObject(FILTER)), (r, f) -> r.withFilter(predicate(f)))
        .build()
        .build();
  }

  private static Reducer createReducer(
      final String jslt, final JsonObject validator, final TopologyContext context) {
    final var reducer = reducer(transformer(jslt, context));

    return withResolver(
        validator != null
            ? compose(validator(context.context.validator.validator(validator)), reducer)
            : reducer,
        context.context.environment,
        context.context.database);
  }

  private static KStream<String, JsonObject> createStream(
      final JsonObject config, final TopologyContext context) {
    return toStream(
        Pipeline.create(
            fromStream(config, context),
            getPipeline(config),
            new net.pincette.mongo.streams.Context()
                .withApp(context.application)
                .withDatabase(context.context.database)
                .withFeatures(context.context.features)
                .withStageExtensions(context.context.stageExtensions)
                .withKafkaAdmin(Admin.create(toAdmin(fromConfig(context.context.config, KAFKA))))
                .withProducer(context.context.producer)
                .withLogger(context.context.logger)
                .withTrace(context.context.logLevel.equals(FINEST))),
        config);
  }

  private static TopologyEntry createTopology(
      final JsonObject specification, final TopologyContext context) {
    return getLogger(specification, context.context)
        .flatMap(
            logger ->
                tryToGet(
                    () -> createTopology(specification, logger, context),
                    e -> {
                      logger.log(
                          SEVERE,
                          e,
                          () -> "Can't start " + specification.getString(APPLICATION_FIELD));
                      return null;
                    }))
        .orElse(null);
  }

  private static TopologyEntry createTopology(
      final JsonObject specification, final Logger logger, final TopologyContext context) {
    final var features =
        context.context.features.withJsltResolver(
            new MapResolver(
                ofNullable(specification.getJsonObject(JSLT_IMPORTS))
                    .map(Run::convertJsltImports)
                    .orElseGet(Collections::emptyMap)));
    final var realContext =
        context.withContext(
            context
                .context
                .withFeatures(features)
                .withValidator(
                    new Validator(
                        features,
                        (id, parent) ->
                            getObject(specification, "/" + VALIDATOR_IMPORTS + "/" + id)
                                .map(validator -> new Resolved(validator, id))))
                .doTask(Run::addPipelineStages));
    final var streamsConfig = Streams.fromConfig(realContext.context.config, KAFKA);
    final var topology = createParts(specification.getJsonArray(PARTS), realContext).build();

    streamsConfig.setProperty(APPLICATION_ID, realContext.application);
    logger.log(INFO, "Topology:\n\n {0}", topology.describe());

    return new TopologyEntry(topology, streamsConfig, null, realContext.context);
  }

  private static Optional<String> environment(
      final JsonObject config, final TopologyContext context) {
    return tryWith(() -> config.getString(ENVIRONMENT, null))
        .or(() -> context.context.environment)
        .get();
  }

  private static EventToCommand eventToCommand(final String jslt, final TopologyContext context) {
    final var transformer = transformer(jslt, context);

    return event -> completedFuture(transformer.apply(event));
  }

  private static JsonObject fromMongoDB(final JsonObject json) {
    return transformFieldNames(json, Run::unescapeFieldName);
  }

  private static KStream<String, JsonObject> fromStream(
      final JsonObject config, final TopologyContext context) {
    return ofNullable(config.getString(FROM_TOPIC, null))
        .map(topic -> stream(topic, context.builder))
        .orElseGet(() -> getStream(config.getString(FROM_STREAM), context));
  }

  private static String getApplication(final TopologyEntry entry) {
    return entry.properties.getProperty(APPLICATION_ID);
  }

  private static Optional<String> getApplication(final ChangeStreamDocument<Document> change) {
    return ofNullable(change.getDocumentKey()).map(doc -> doc.getString(ID).getValue());
  }

  private static String getCollection(final String type, final Context context) {
    return type + (context.environment != null ? ("-" + context.environment) : "");
  }

  private static GetDestinations getDestinations(
      final JsonArray pipeline, final String destinationType, final Context context) {
    final JsonArray projected =
        from(concat(pipeline.stream(), Stream.of(o(f(PROJECT, o(f(ID, v(true))))))));

    return event ->
        aggregationPublisher(
            context.database.getCollection(getCollection(destinationType, context)),
            replaceVariables(projected, map(pair(EVENT, event))).asJsonArray());
  }

  private static Optional<Logger> getLogger(final JsonObject specification, final Context context) {
    return Optional.of(specification)
        .flatMap(s -> ofNullable(s.getString(APPLICATION_FIELD, null)))
        .map(
            application ->
                getLogger(application, specification.getString(VERSION, "unknown"), context));
  }

  private static Logger getLogger(
      final String application, final String version, final Context context) {
    final var logger = Logger.getLogger(application);

    logger.setLevel(context.logLevel);

    if (context.logTopic != null) {
      log(logger, version, context.environment, context.producer, context.logTopic);
    }

    return logger;
  }

  private static JsonArray getPipeline(final JsonObject config) {
    return getValue(config, "/" + PIPELINE)
        .map(JsonValue::asJsonArray)
        .orElseGet(JsonUtil::emptyArray);
  }

  private static Duration getRestartBackoff(final Context context) {
    return tryToGetSilent(() -> context.config.getDuration(RESTART_BACKOFF))
        .orElse(DEFAULT_RESTART_BACKOFF);
  }

  private static KStream<String, JsonObject> getStream(
      final String name, final TopologyContext context) {
    return ofNullable(context.streams.get(name))
        .orElseGet(
            () ->
                ofNullable(createPart(context.configurations.get(name), context))
                    .map(
                        part ->
                            SideEffect.<KStream<String, JsonObject>>run(
                                    () -> context.streams.put(name, part))
                                .andThenGet(() -> part))
                    .orElse(null));
  }

  private static Stream<Pair<JsonObject, String>> getTopologies(
      final MongoCollection<Document> collection, final Bson filter) {
    return find(collection, filter).toCompletableFuture().join().stream()
        .map(Run::fromMongoDB)
        .map(topology -> pair(topology, null));
  }

  private static Context loadPlugins(final Context context) {
    return tryToGetSilent(() -> context.config.getString(PLUGINS))
        .map(Paths::get)
        .map(path -> load(path, context))
        .map(
            plugins ->
                context
                    .withStageExtensions(plugins.stageExtensions)
                    .withFeatures(
                        context
                            .features
                            .withExpressionExtensions(
                                merge(
                                    operators(context.database, context.environment),
                                    plugins.expressionExtensions))
                            .withMatchExtensions(plugins.matchExtensions)
                            .withCustomJsltFunctions(
                                union(
                                    customFunctions(),
                                    set(trace(context.logger)),
                                    plugins.jsltFunctions))))
        .orElse(context);
  }

  private static void logAggregate(
      final Aggregate aggregate, final String version, final Context context) {
    if (context.logTopic != null) {
      logKafka(aggregate, context.logLevel, version, context.logTopic);
    }
  }

  private static Void logChangeError(
      final Throwable thrown, final ChangeStreamDocument<Document> change, final Context context) {
    context.logger.log(
        SEVERE,
        thrown,
        () -> "Changed application " + getApplication(change).orElse(null) + " has an error.");

    return null;
  }

  private static void logging(final Context context) {
    if (context.logTopic != null) {
      log(
          context.logger,
          context.logLevel,
          APP_VERSION,
          context.environment,
          context.producer,
          context.logTopic);
    }
  }

  private static Context metrics(final Context context) {
    tryToGetSilent(() -> context.config.getString(METRICS_TOPIC))
        .ifPresent(
            topic ->
                new Metrics(
                        topic,
                        context.producer,
                        tryToGetSilent(() -> context.config.getDuration(METRICS_INTERVAL))
                            .orElseGet(() -> ofMinutes(1)))
                    .start());

    return context;
  }

  private static Stream<JsonObject> parts(final JsonArray parts, final Set<String> types) {
    return parts.stream()
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .filter(part -> types.contains(part.getString(TYPE)));
  }

  private static Context prepareContext(final Context context) {
    return context
        .doTask(c -> c.logger.info("Connecting to Kafka ..."))
        .withProducer(
            createReliableProducer(
                fromConfig(context.config, KAFKA), new StringSerializer(), new JsonSerializer()))
        .doTask(Run::logging)
        .doTask(c -> c.logger.info("Loading plugins ..."))
        .with(Run::loadPlugins)
        .doTask(c -> c.logger.info("Settings up metrics ..."))
        .with(Run::metrics)
        .doTask(c -> setContextPath(c.config.getString(CONTEXT_PATH)))
        .doTask(c -> c.logger.info("Loading applications ..."));
  }

  private static Aggregate reducers(
      final Aggregate aggregate, final JsonObject config, final TopologyContext context) {
    return getObjects(config, COMMANDS)
        .reduce(
            aggregate,
            (a, c) ->
                a.withReducer(
                    c.getString(NAME),
                    createReducer(c.getString(REDUCER), c.getJsonObject(VALIDATOR), context)),
            (a1, a2) -> a1);
  }

  private static KStream<String, JsonObject> stream(
      final String topic, final StreamsBuilder builder) {
    return builder.stream(topic);
  }

  private static KeyValue<String, JsonObject> switchKey(
      final JsonObject json, final Function<JsonObject, JsonValue> key) {
    return ofNullable(toNative(key.apply(json)))
        .map(Object::toString)
        .map(k -> new KeyValue<>(k, json))
        .orElseGet(() -> new KeyValue<>(null, null));
  }

  private static Map<String, Object> toAdmin(final Map<String, Object> config) {
    return config.entrySet().stream()
        .filter(e -> KAFKA_ADMIN_CONFIG_NAMES.contains(e.getKey()))
        .collect(toMap(Entry::getKey, Entry::getValue));
  }

  @SuppressWarnings("java:S1905") // Fixes an ambiguity.
  private static KStream<String, JsonObject> toStream(
      final KStream<String, JsonObject> stream, final JsonObject config) {
    return ofNullable(config.getString(TO_TOPIC, null))
        .map(
            topic ->
                SideEffect.<KStream<String, JsonObject>>run(
                        () -> {
                          if (config.getBoolean(TO_STRING, false)) {
                            stream
                                .mapValues((ValueMapper<JsonObject, String>) JsonUtil::string)
                                .to(topic, valueSerde(new StringSerde()));
                          } else {
                            stream.to(topic);
                          }
                        })
                    .andThenGet(() -> null))
        .orElse(stream);
  }

  private static TopologyLifeCycleEmitter topologyLifeCycleEmitter(final Context context) {
    return tryToGetSilent(() -> context.config.getString(TOPOLOGY_TOPIC))
        .map(topic -> new TopologyLifeCycleEmitter(topic, context.producer))
        .orElse(null);
  }

  private static UnaryOperator<JsonObject> transformer(
      final String jslt, final TopologyContext context) {
    final var op =
        fatal(
            () ->
                transformerObject(
                    new Jslt.Context(tryReader(jslt))
                        .withResolver(context.context.features.jsltResolver)
                        .withFunctions(context.context.features.customJsltFunctions)),
            context.context.logger,
            () -> jslt);

    return json ->
        fatal(
            () -> op.apply(json),
            context.context.logger,
            () -> "Script:\n" + numberLines(jslt) + "\n\nWith JSON:\n" + string(json, true));
  }

  private static String unescapeFieldName(final String name) {
    return name.replace(DOT, ".").replace(SLASH, "/").replace(DOLLAR, "$");
  }

  private static Properties withGroupIdSuffix(final Properties properties, final Config config) {
    return tryToGetSilent(() -> config.getString(GROUP_ID_SUFFIX))
        .map(
            suffix ->
                net.pincette.util.Util.set(
                    properties,
                    APPLICATION_ID,
                    properties.getProperty(APPLICATION_ID) + "-" + suffix))
        .orElse(properties);
  }

  private void close() {
    running.values().forEach(v -> v.stop.stop());
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

  private Stream<Pair<JsonObject, String>> getTopologies() {
    return getFile()
        .map(Common::readTopologies)
        .orElseGet(() -> getTopologies(getTopologyCollection(), getFilter()));
  }

  private MongoCollection<Document> getTopologyCollection() {
    return context.database.getCollection(
        Common.getTopologyCollection(
            getCollectionOptions().map(o -> o.collection).orElse(null), context));
  }

  private void handleChange(
      final ChangeStreamDocument<Document> change,
      final TopologyLifeCycle lifeCycle,
      final Predicate<JsonObject> filter,
      final Context context) {
    runAsync(
            () ->
                getApplication(change)
                    .ifPresent(
                        application -> {
                          switch (change.getOperationType()) {
                            case DELETE:
                              stop(application);
                              break;
                            case INSERT:
                            case REPLACE:
                            case UPDATE:
                              start(change, lifeCycle, filter);
                              break;
                            default:
                              break;
                          }
                        }))
        .exceptionally(e -> logChangeError(e, change, context));
  }

  public void run() {
    context = prepareContext(context);

    Optional.of(
            getTopologies()
                .map(
                    specification ->
                        pair(
                            specification.first,
                            createTopologyContext(
                                specification.first,
                                getFile().orElse(null),
                                specification.second,
                                context)))
                .map(pair -> pair(build(pair.first, true, pair.second), pair.second))
                .filter(pair -> validateTopology(pair.first))
                .map(pair -> createTopology(pair.first, pair.second))
                .filter(Objects::nonNull))
        .map(topologies -> start(topologies, context))
        .filter(result -> !result)
        .ifPresent(result -> exit(1));
  }

  private void restartError(final String application, final TopologyLifeCycle lifeCycle) {
    ofNullable(running.get(application))
        .ifPresent(
            entry -> {
              entry.context.logger.log(SEVERE, "Application {0} failed", getApplication(entry));
              entry.stop.stop();

              new Timer()
                  .schedule(
                      new TimerTask() {
                        public void run() {
                          restart(entry, lifeCycle);
                        }
                      },
                      restartBackoff.toMillis());
            });
  }

  private void restart(final TopologyEntry entry, final TopologyLifeCycle lifeCycle) {
    final var application = getApplication(entry);

    entry.context.logger.log(INFO, "Restarting {0}", application);
    ofNullable(running.get(application)).map(r -> r.stop).ifPresent(Stop::stop);
    running.put(application, start(entry, lifeCycle));
  }

  private boolean start(final Stream<TopologyEntry> topologies, final Context context) {
    final var filter = ofNullable(getFilter()).map(Match::predicate).orElse(json -> true);
    final var lifeCycle = topologyLifeCycleEmitter(context);

    topologies
        .map(t -> pair(getApplication(t), start(t, lifeCycle)))
        .forEach(pair -> running.put(pair.first, pair.second));

    if (getFile().isEmpty()) {
      getTopologyCollection()
          .watch()
          .subscribe(
              new LambdaSubscriber<>(change -> handleChange(change, lifeCycle, filter, context)));
    }

    getRuntime().addShutdownHook(new Thread(this::close));
    context.logger.info("Ready");

    synchronized (this) {
      tryToDoRethrow(this::wait);
    }

    return true;
  }

  private TopologyEntry start(final TopologyEntry entry, final TopologyLifeCycle lifeCycle) {
    return new TopologyEntry(
        entry.topology,
        entry.properties,
        net.pincette.jes.util.Streams.start(
            entry.topology,
            withGroupIdSuffix(entry.properties, entry.context.config),
            lifeCycle,
            (stop, app) -> restartError(app, lifeCycle),
            e ->
                entry.context.logger.log(
                    SEVERE, e, () -> "Application " + getApplication(entry) + " failed")),
        entry.context);
  }

  private void start(
      final ChangeStreamDocument<Document> change,
      final TopologyLifeCycle lifeCycle,
      final Predicate<JsonObject> filter) {
    ofNullable(change.getFullDocument())
        .map(doc -> fromBson(toBsonDocument(doc)))
        .map(Run::fromMongoDB)
        .filter(filter)
        .map(
            specification ->
                pair(specification, createTopologyContext(specification, null, null, context)))
        .map(pair -> pair(build(pair.first, true, pair.second), pair.second))
        .map(pair -> createTopology(pair.first, pair.second))
        .ifPresent(topology -> restart(topology, lifeCycle));
  }

  private void stop(final String application) {
    ofNullable(running.remove(application))
        .ifPresent(
            entry -> {
              entry.context.logger.log(INFO, "Stopping {0}", application);
              entry.stop.stop();
            });
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
          description = "A JSON file containing an array of topologies.")
      private File file;
    }

    private static class CollectionOptions {
      @Option(
          names = {"-c", "--collection"},
          description = "A MongoDB collection containing the topologies.")
      private String collection;

      @ArgGroup private TopologySelection selection;

      private static class TopologySelection {
        @Option(
            names = {"-a", "--application"},
            description =
                "An application from the MongoDB collection containing the topologies. This is "
                    + "a shorthand for the query {\"application\": \"name\"}.")
        private String application;

        @Option(
            names = {"-q", "--query"},
            description = "The MongoDB query into the collection containing the topologies.")
        private String query;
      }
    }
  }

  private static class TopologyEntry {
    private final Context context;
    private final Properties properties;
    private final Stop stop;
    private final Topology topology;

    private TopologyEntry(
        final Topology topology,
        final Properties properties,
        final Stop stop,
        final Context context) {
      this.topology = topology;
      this.properties = properties;
      this.stop = stop;
      this.context = context;
    }
  }
}

package net.pincette.json.streams;

import static com.mongodb.reactivestreams.client.MongoClients.create;
import static java.lang.String.join;
import static java.lang.System.exit;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.lines;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.parse;
import static java.util.logging.Logger.getGlobal;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.concat;
import static net.pincette.jes.Aggregate.reducer;
import static net.pincette.jes.elastic.Logging.log;
import static net.pincette.jes.util.Configuration.loadDefault;
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
import static net.pincette.json.Jslt.customFunctions;
import static net.pincette.json.Jslt.registerCustomFunctions;
import static net.pincette.json.Jslt.trace;
import static net.pincette.json.Jslt.tryTransformer;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.getNumber;
import static net.pincette.json.JsonUtil.getObject;
import static net.pincette.json.JsonUtil.getObjects;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.getStrings;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.toNative;
import static net.pincette.json.Transform.nopTransformer;
import static net.pincette.json.Transform.transform;
import static net.pincette.json.Transform.transformBuilder;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.Expression.replaceVariables;
import static net.pincette.mongo.JsonClient.aggregationPublisher;
import static net.pincette.mongo.JsonClient.find;
import static net.pincette.mongo.JsonClient.update;
import static net.pincette.mongo.Match.predicate;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Collections.union;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.getLastSegment;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetSilent;
import static net.pincette.util.Util.tryToGetWithRethrow;
import static org.apache.kafka.streams.kstream.Produced.valueSerde;

import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.typesafe.config.Config;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.jes.Aggregate;
import net.pincette.jes.EventToCommand;
import net.pincette.jes.GetDestinations;
import net.pincette.jes.Reactor;
import net.pincette.jes.util.JsonSerializer;
import net.pincette.jes.util.Reducer;
import net.pincette.jes.util.Streams;
import net.pincette.json.Jslt.MapResolver;
import net.pincette.json.JsonUtil;
import net.pincette.json.Transform.JsonEntry;
import net.pincette.json.Transform.Transformer;
import net.pincette.mongo.BsonUtil;
import net.pincette.mongo.Features;
import net.pincette.mongo.Validator;
import net.pincette.mongo.streams.Pipeline;
import net.pincette.util.Pair;
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
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
public class Application {
  private static final String AGGREGATE = "aggregate";
  private static final String AGGREGATE_TYPE = "aggregateType";
  private static final String APPLICATION_FIELD = "application";
  private static final String APPLICATION_ID = "application.id";
  private static final String COMMANDS = "commands";
  private static final String DATABASE = "mongodb.database";
  private static final String DESTINATION_TYPE = "destinationType";
  private static final String DESTINATIONS = "destinations";
  private static final String DOLLAR = "_dollar_";
  private static final String DOT = "_dot_";
  private static final String ENV = "${ENV}";
  private static final String ENVIRONMENT = "environment";
  private static final String EVENT = "$$event";
  private static final String EVENT_TO_COMMAND = "eventToCommand";
  private static final String FILTER = "filter";
  private static final String FROM_STREAM = "fromStream";
  private static final String FROM_STREAMS = "fromStreams";
  private static final String FROM_TOPIC = "fromTopic";
  private static final String FROM_TOPICS = "fromTopics";
  private static final String JOIN = "join";
  private static final String JSLT = "$jslt";
  private static final Pattern JSLT_IMPORT = compile("^.*import[ \t]+\"([a-zA-Z0-9\\./]+)\".*$");
  private static final String JSLT_IMPORTS = "jsltImports";
  private static final String KAFKA = "kafka";
  private static final String LEFT = "left";
  private static final String LOGGER = "pincette-json-streams";
  private static final String LOG_LEVEL = "logLevel";
  private static final String LOG_TOPIC = "logTopic";
  private static final String MERGE = "merge";
  private static final String MONGODB_COLLECTION = "mongodb.collection";
  private static final String MONGODB_URI = "mongodb.uri";
  private static final String NAME = "name";
  private static final String ON = "on";
  private static final String PARTS = "parts";
  private static final String PIPELINE = "pipeline";
  private static final String PROJECT = "$project";
  private static final String REACTOR = "reactor";
  private static final String REDUCER = "reducer";
  private static final String RESOURCE = "resource:";
  private static final String RIGHT = "right";
  private static final String SOURCE_TYPE = "sourceType";
  private static final String STREAM = "stream";
  private static final Set<String> STREAM_TYPES = set(JOIN, MERGE, STREAM);
  private static final String TO_STRING = "toString";
  private static final String TO_TOPIC = "toTopic";
  private static final String TYPE = "type";
  private static final String VALIDATOR = "validator";
  private static final String VERSION = "version";
  private static final String WINDOW = "window";
  private static final Transformer ESCAPE_DOTTED =
      new Transformer(
          e -> isDotted(e.path), e -> Optional.of(new JsonEntry(escapeDotted(e.path), e.value)));
  private static final Transformer ESCAPE_OPERATORS =
      new Transformer(
          e -> isOperator(e.path),
          e -> Optional.of(new JsonEntry(escapeOperator(e.path), e.value)));
  private static final Transformer UNESCAPE_DOTTED =
      new Transformer(
          e -> isEscapedDotted(e.path),
          e -> Optional.of(new JsonEntry(unescapeDotted(e.path), e.value)));
  private static final Transformer UNESCAPE_OPERATORS =
      new Transformer(
          e -> isEscapedOperator(e.path),
          e -> Optional.of(new JsonEntry(unescapeOperator(e.path), e.value)));

  private Application() {}

  private static void addStreams(final Aggregate aggregate, final TopologyContext context) {
    final String type = aggregate.fullType();

    context.streams.put(type + "-aggregate", aggregate.aggregates());
    context.streams.put(type + "-command", aggregate.commands());
    context.streams.put(type + "-event", aggregate.events());
    context.streams.put(type + "-event-full", aggregate.eventsFull());
    context.streams.put(type + "-reply", aggregate.replies());
  }

  private static Pair<String, String> aggregateTypeParts(final JsonObject specification) {
    return Optional.of(specification.getString(AGGREGATE_TYPE).split("-"))
        .filter(parts -> parts.length == 2)
        .map(parts -> pair(parts[0], parts[1]))
        .orElse(null);
  }

  private static JsonObject build(
      final JsonObject specification, final TopologyContext topologyContext) {
    final JsonObjectBuilder imports =
        ofNullable(specification.getJsonObject(JSLT_IMPORTS))
            .map(JsonUtil::createObjectBuilder)
            .orElseGet(JsonUtil::createObjectBuilder);

    return transformBuilder(
            specification,
            partsResolver(topologyContext.baseDirectory)
                .thenApply(jsltResolver(imports, topologyContext.baseDirectory))
                .thenApply(validatorResolver(topologyContext))
                .thenApply(
                    topologyContext.environment != null
                        ? replaceEnvironment(topologyContext.environment)
                        : nopTransformer()))
        .add(JSLT_IMPORTS, imports)
        .add(ID, specification.getString(APPLICATION_FIELD))
        .build();
  }

  private static Map<String, String> convertJsltImports(final JsonObject jsltImports) {
    return jsltImports.entrySet().stream()
        .filter(e -> isString(e.getValue()))
        .collect(toMap(Entry::getKey, e -> asString(e.getValue()).getString()));
  }

  private static StreamsBuilder createAggregate(
      final JsonObject config, final TopologyContext context) {
    final Pair<String, String> aggregateType = aggregateTypeParts(config);
    final Aggregate aggregate =
        reducers(
            create(
                    () ->
                        new Aggregate()
                            .withApp(aggregateType.first)
                            .withType(aggregateType.second)
                            .withMongoDatabase(context.database)
                            .withBuilder(context.builder))
                .updateIf(
                    () -> ofNullable(config.getString(ENVIRONMENT, null)),
                    Aggregate::withEnvironment)
                .build(),
            config,
            context);

    aggregate.build();
    addStreams(aggregate, context);

    return context.builder;
  }

  private static KStream<String, JsonObject> createJoin(
      final JsonObject config, final TopologyContext context) {
    final Function<JsonObject, JsonValue> leftKey =
        function(config.getValue("/" + LEFT + "/" + ON), context.features);
    final Function<JsonObject, JsonValue> rightKey =
        function(config.getValue("/" + RIGHT + "/" + ON), context.features);

    return toStream(
        fromStream(config.getJsonObject(LEFT), context)
            .map((k, v) -> switchKey(v, leftKey))
            .join(
                fromStream(config.getJsonObject(RIGHT), context)
                    .map((k, v) -> switchKey(v, rightKey)),
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
                            context))
                    .withEventToCommand(
                        eventToCommand(config.getString(EVENT_TO_COMMAND), context)))
        .updateIf(() -> ofNullable(config.getString(ENVIRONMENT, null)), Reactor::withEnvironment)
        .updateIf(
            () -> ofNullable(config.getJsonObject(FILTER)), (r, f) -> r.withFilter(predicate(f)))
        .build()
        .build();
  }

  private static Reducer createReducer(
      final String jslt, final JsonObject validator, final TopologyContext context) {
    final Reducer reducer =
        reducer(tryTransformer(jslt, null, null, context.features.jsltResolver));

    return withResolver(
        validator != null
            ? compose(validator(context.validators.validator(validator)), reducer)
            : reducer,
        context.environment,
        context.database);
  }

  private static KStream<String, JsonObject> createStream(
      final JsonObject config, final TopologyContext context) {
    return toStream(
        Pipeline.create(
            context.application,
            fromStream(config, context),
            from(resolve(config.getJsonArray(PIPELINE).stream(), context.baseDirectory)),
            context.database,
            context.logLevel.equals(FINEST),
            context.features),
        config);
  }

  @SuppressWarnings("java:S3398")
  private static Pair<Topology, Properties> createTopology(
      final JsonObject specification, final File baseDirectory, final Context context) {
    final TopologyContext topologyContext =
        createTopologyContext(specification, baseDirectory, context);
    final Properties streamsConfig = Streams.fromConfig(context.config, KAFKA);
    final JsonObject built = build(specification, topologyContext);

    topologyContext.features =
        context.features.withJsltResolver(
            new MapResolver(
                ofNullable(built.getJsonObject(JSLT_IMPORTS))
                    .map(Application::convertJsltImports)
                    .orElseGet(Collections::emptyMap)));

    final Topology topology = createParts(built.getJsonArray(PARTS), topologyContext).build();

    streamsConfig.setProperty(APPLICATION_ID, topologyContext.application);
    getLogger(specification, context).log(INFO, "Topology:\n\n {0}", topology.describe());

    return pair(topology, streamsConfig);
  }

  private static TopologyContext createTopologyContext(
      final JsonObject specification, final File baseDirectory, final Context context) {
    final TopologyContext topologyContext = new TopologyContext();

    topologyContext.application = specification.getString(APPLICATION_FIELD);
    topologyContext.baseDirectory = baseDirectory;
    topologyContext.builder = new StreamsBuilder();
    topologyContext.database = context.database;
    topologyContext.environment = context.environment;
    topologyContext.logLevel = context.logLevel;

    return topologyContext;
  }

  private static String escapeDotted(final String path) {
    return replaceField(path, s -> s.replace(".", DOT));
  }

  private static String escapeOperator(final String path) {
    return replaceField(path, s -> DOLLAR + s.substring(1));
  }

  private static EventToCommand eventToCommand(final String jslt, final TopologyContext context) {
    final UnaryOperator<JsonObject> transformer =
        tryTransformer(jslt, null, null, context.features.jsltResolver);

    return event -> completedFuture(transformer.apply(event));
  }

  private static JsonObject fromMongoDB(final JsonObject json) {
    return transform(json, UNESCAPE_DOTTED.thenApply(UNESCAPE_OPERATORS), "/");
  }

  private static KStream<String, JsonObject> fromStream(
      final JsonObject config, final TopologyContext context) {
    return ofNullable(config.getString(FROM_TOPIC, null))
        .map(topic -> stream(topic, context.builder))
        .orElseGet(() -> getStream(config.getString(FROM_STREAM), context));
  }

  private static String getCollection(final String type, final TopologyContext context) {
    return type + (context.environment != null ? ("-" + context.environment) : "");
  }

  private static String getCollection(final String collection, final Context context) {
    return collection != null
        ? collection
        : tryToGetSilent(() -> context.config.getString(MONGODB_COLLECTION)).orElse(null);
  }

  private static GetDestinations getDestinations(
      final JsonArray pipeline, final String destinationType, final TopologyContext context) {
    final JsonArray projected =
        from(concat(pipeline.stream(), Stream.of(o(f(PROJECT, o(f(ID, v(true))))))));

    return event ->
        aggregationPublisher(
            context.database.getCollection(getCollection(destinationType, context)),
            replaceVariables(projected, map(pair(EVENT, event))).asJsonArray());
  }

  private static Optional<String> getImport(final String line) {
    return Optional.of(JSLT_IMPORT.matcher(line))
        .filter(Matcher::matches)
        .map(matcher -> matcher.group(1));
  }

  private static InputStream getInputStream(final String path, final File baseDirectory) {
    return path.startsWith(RESOURCE)
        ? Application.class.getResourceAsStream(path.substring(RESOURCE.length()))
        : tryToGetRethrow(() -> new FileInputStream(new File(baseDirectory, path))).orElse(null);
  }

  private static Logger getLogger(final JsonObject specification, final Context context) {
    final String application = specification.getString(APPLICATION_FIELD);
    final Map<String, Object> kafkaConfig = fromConfig(context.config, KAFKA);
    final Logger logger = Logger.getLogger(application);

    logger.setLevel(context.logLevel);
    kafkaConfig.put(APPLICATION_ID, application);

    log(
        logger,
        specification.getString(VERSION, "unknown"),
        context.environment,
        createReliableProducer(kafkaConfig, new StringSerializer(), new JsonSerializer()),
        context.config.getString(LOG_TOPIC));

    return logger;
  }

  private static KStream<String, JsonObject> getStream(
      final String name, final TopologyContext context) {
    return context.streams.computeIfAbsent(
        name, k -> createPart(context.configurations.get(name), context));
  }

  private static String importPath(final File jslt, final File baseDirectory) {
    return removeLeadingSlash(
        jslt.getAbsoluteFile()
            .getAbsolutePath()
            .substring(baseDirectory.getAbsoluteFile().getAbsolutePath().length())
            .replace('\\', '/'));
  }

  private static boolean isDotted(final String path) {
    return isField(path, s -> s.contains("."));
  }

  private static boolean isEscapedDotted(final String path) {
    return isField(path, s -> s.contains(DOT));
  }

  private static boolean isEscapedOperator(final String path) {
    return isField(path, s -> s.startsWith(DOLLAR));
  }

  private static boolean isField(final String path, final Predicate<String> predicate) {
    return getLastSegment(path, "/").filter(predicate).isPresent();
  }

  private static boolean isJsltPath(final String path) {
    return path.endsWith(JSLT)
        || path.endsWith(COMMANDS + "." + REDUCER)
        || path.endsWith(EVENT_TO_COMMAND);
  }

  private static boolean isOperator(final String path) {
    return isField(path, s -> s.startsWith("$"));
  }

  private static boolean isPartsPath(final String path) {
    return path.endsWith(PARTS) || path.endsWith(PIPELINE);
  }

  private static boolean isValidatorPath(final String path) {
    return path.endsWith(COMMANDS + "." + VALIDATOR);
  }

  private static Transformer jsltResolver(
      final JsonObjectBuilder imports, final File baseDirectory) {
    return new Transformer(
        e -> isJsltPath(e.path) && isString(e.value),
        e ->
            Optional.of(
                new JsonEntry(
                    e.path,
                    createValue(
                        resolveJslt(asString(e.value).getString(), imports, baseDirectory)))));
  }

  private static Stream<JsonValue> loadSequence(final String path, final File baseDirectory) {
    return tryToGetWithRethrow(
            () -> createReader(getInputStream(path, baseDirectory)),
            reader ->
                resolve(
                    readParts(reader),
                    path.startsWith(RESOURCE)
                        ? baseDirectory
                        : new File(baseDirectory, path).getParentFile()))
        .orElse(null);
  }

  public static void main(final String[] args) {
    final Context context = new Context();

    context.config = loadDefault();
    context.environment = tryToGetSilent(() -> context.config.getString(ENVIRONMENT)).orElse(null);
    context.logLevel =
        parse(tryToGetSilent(() -> context.config.getString(LOG_LEVEL)).orElse("INFO"));

    final Logger logger = Logger.getLogger(LOGGER);

    logger.setLevel(context.logLevel);
    registerCustomFunctions(union(customFunctions(), set(trace(logger))));

    tryToDoWithRethrow(
        () -> create(context.config.getString(MONGODB_URI)),
        client -> {
          context.database = client.getDatabase(context.config.getString(DATABASE));
          context.features =
              new Features()
                  .withExpressionExtensions(operators(context.database, context.environment));

          Optional.of(
                  new CommandLine(new Application())
                      .addSubcommand("build", new Build(context))
                      .addSubcommand("run", new Run(context))
                      .execute(args))
              .filter(code -> code != 0)
              .ifPresent(System::exit);
        });
  }

  private static Stream<JsonObject> parts(final JsonArray parts, final Set<String> types) {
    return parts.stream()
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .filter(part -> types.contains(part.getString(TYPE)));
  }

  private static Transformer partsResolver(final File baseDirectory) {
    return new Transformer(
        e -> isPartsPath(e.path) && isArray(e.value),
        e ->
            Optional.of(
                new JsonEntry(
                    e.path, from(resolve(e.value.asJsonArray().stream(), baseDirectory)))));
  }

  private static Stream<JsonObject> readObjects(final JsonReader reader) {
    return reader.readArray().stream().filter(JsonUtil::isObject).map(JsonValue::asJsonObject);
  }

  private static Stream<JsonValue> readParts(final JsonReader reader) {
    final JsonStructure result = reader.read();

    return isArray(result) ? result.asJsonArray().stream() : Stream.of(result);
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

  private static String removeLeadingSlash(final String path) {
    return path.startsWith("/") ? path.substring(1) : path;
  }

  private static Transformer replaceEnvironment(final String environment) {
    return new Transformer(
        e -> isString(e.value) && asString(e.value).getString().contains(ENV),
        e ->
            Optional.of(
                new JsonEntry(
                    e.path, createValue(asString(e.value).getString().replace(ENV, environment)))));
  }

  private static String replaceField(final String path, final UnaryOperator<String> replace) {
    final String[] segments = path.split("/");

    segments[segments.length - 1] = replace.apply(segments[segments.length - 1]);

    return join("/", segments);
  }

  private static Stream<JsonValue> resolve(
      final Stream<JsonValue> sequence, final File baseDirectory) {
    return sequence
        .filter(element -> isObject(element) || isString(element))
        .flatMap(
            element ->
                isObject(element)
                    ? Stream.of(element)
                    : loadSequence(asString(element).getString(), baseDirectory));
  }

  private static String resolveJslt(
      final String jslt, final JsonObjectBuilder imports, final File baseDirectory) {
    return jslt.startsWith(RESOURCE)
        ? jslt
        : Optional.of(new File(baseDirectory, jslt))
            .filter(File::exists)
            .map(file -> resolveJsltImports(file, imports, baseDirectory))
            .orElse(jslt);
  }

  private static String resolveJsltImport(
      final String line,
      final String imp,
      final File jslt,
      final JsonObjectBuilder imports,
      final File baseDirectory) {
    final File imported = new File(jslt.getAbsoluteFile().getParent(), imp);
    final String path = importPath(imported, baseDirectory);

    imports.add(path, resolveJsltImports(imported, imports, baseDirectory));

    return line.replace(imp, path);
  }

  private static String resolveJsltImports(
      final File jslt, final JsonObjectBuilder imports, final File baseDirectory) {
    return tryToGetRethrow(() -> lines(jslt.toPath(), UTF_8))
        .orElseGet(Stream::empty)
        .map(
            line ->
                getImport(line)
                    .map(imp -> resolveJsltImport(line, imp, jslt, imports, baseDirectory))
                    .orElse(line))
        .collect(joining("\n"));
  }

  private static KStream<String, JsonObject> stream(
      final String topic, final StreamsBuilder builder) {
    return builder.stream(topic);
  }

  private static KeyValue<String, JsonObject> switchKey(
      final JsonObject json, final Function<JsonObject, JsonValue> key) {
    return new KeyValue<>(toNative(key.apply(json)).toString(), json);
  }

  private static JsonObject toMongoDB(final JsonObject json) {
    return transform(json, ESCAPE_DOTTED.thenApply(ESCAPE_OPERATORS), "/");
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

  private static String unescapeDotted(final String path) {
    return replaceField(path, s -> s.replace(DOT, "."));
  }

  private static String unescapeOperator(final String path) {
    return replaceField(path, s -> "$" + s.substring(DOLLAR.length()));
  }

  private static boolean validateAggregate(final JsonObject specification) {
    boolean result =
        specification.getString(AGGREGATE_TYPE, null) != null
            && specification.getJsonArray(COMMANDS) != null;

    if (!result) {
      getGlobal()
          .severe(
              "An aggregate should have the \"aggregateType\" field and the \"commands\" array.");
    }

    result &= validateCommands(specification);

    return result;
  }

  private static boolean validateCommand(final String aggregateType, final JsonObject command) {
    final boolean result =
        command.getString(NAME, null) != null
            && command.getString(REDUCER, null) != null
            && (!command.containsKey(VALIDATOR) || getObject(command, "/" + VALIDATOR).isPresent());

    if (!result) {
      getGlobal()
          .log(
              SEVERE,
              "Aggregate {0} should have a \"name\" and a \"reducer\" field and "
                  + "when the \"validator\" field is present it should be an object.",
              new Object[] {aggregateType});
    }

    return result;
  }

  private static boolean validateCommands(final JsonObject specification) {
    final String aggregateType = specification.getString(AGGREGATE_TYPE, null);

    return getObjects(specification, COMMANDS)
        .allMatch(command -> validateCommand(aggregateType, command));
  }

  private static boolean validateJoin(final JsonObject specification) {
    final String left = "/" + LEFT + "/";
    final String right = "/" + RIGHT + "/";
    final boolean result =
        specification.getString(NAME, null) != null
            && getNumber(specification, "/" + WINDOW).isPresent()
            && getString(specification, left + ON).isPresent()
            && getString(specification, right + ON).isPresent()
            && (getString(specification, left + FROM_STREAM).isPresent()
                || getString(specification, left + FROM_TOPIC).isPresent())
            && (getString(specification, right + FROM_STREAM).isPresent()
                || getString(specification, right + FROM_TOPIC).isPresent());

    if (!result) {
      getGlobal()
          .log(
              SEVERE,
              "The join {0} should have the fields \"window\", \"left.on\", \"right.on\", either "
                  + "\"left.fromStream\" or \"left.fromTopic\" and either \"right.fromStream\" or "
                  + "\"right.fromTopic\".",
              new Object[] {specification.getString(NAME, null)});
    }

    return result;
  }

  private static boolean validateMerge(final JsonObject specification) {
    final boolean result =
        specification.getString(NAME, null) != null
            && specification.getJsonArray(FROM_STREAMS) != null;

    if (!result) {
      getGlobal()
          .log(
              SEVERE,
              "The merge {0} should have the fields \"name\" and \"fromStreams\".",
              new Object[] {specification.getString(NAME, null)});
    }

    return result;
  }

  private static boolean validatePart(final JsonObject specification) {
    boolean result = specification.getString(TYPE, null) != null;

    if (!result) {
      getGlobal().severe("A part should have a \"type\" field.");
    }

    result &=
        ofNullable(specification.getString(TYPE, null))
            .filter(type -> validatePart(type, specification))
            .isPresent();

    return result;
  }

  private static boolean validatePart(final String type, final JsonObject specification) {
    switch (type) {
      case AGGREGATE:
        return validateAggregate(specification);
      case JOIN:
        return validateJoin(specification);
      case MERGE:
        return validateMerge(specification);
      case REACTOR:
        return validateReactor(specification);
      case STREAM:
        return validateStream(specification);
      default:
        return false;
    }
  }

  private static boolean validateReactor(final JsonObject specification) {
    final boolean result =
        specification.getString(SOURCE_TYPE, null) != null
            && specification.getString(DESTINATION_TYPE, null) != null
            && specification.getString(EVENT_TO_COMMAND, null) != null
            && specification.getJsonArray(DESTINATIONS) != null
            && (!specification.containsKey(FILTER)
                || getObject(specification, "/" + FILTER).isPresent());

    if (!result) {
      getGlobal()
          .log(
              SEVERE,
              "The reactor from {0} to {1} should have the fields \"sourceType\","
                  + " \"destinationType\", \"eventToCommand\", and \"destinations\" and "
                  + "optionally the object \"filter\".",
              new Object[] {
                specification.getString(SOURCE_TYPE, null),
                specification.getString(DESTINATION_TYPE, null)
              });
    }

    return result;
  }

  private static boolean validateStream(final JsonObject specification) {
    final boolean result =
        specification.getString(NAME, null) != null
            && specification.getJsonArray(PIPELINE) != null
            && (specification.getString(FROM_STREAM, null) != null
                || specification.getString(FROM_TOPIC, null) != null);

    if (!result) {
      getGlobal()
          .log(
              SEVERE,
              "The stream {0} should have the fields \"name\", \"pipeline\" and either "
                  + "\"fromStream\" or \"fromTopic\".",
              new Object[] {specification.getString(NAME, null)});
    }

    return result;
  }

  private static boolean validateTopology(final JsonObject specification) {
    boolean result =
        specification.getString(APPLICATION_FIELD, null) != null
            && specification.getJsonArray(PARTS) != null;

    if (!result) {
      getGlobal()
          .severe("A topology should have an \"application\" field and an array called \"parts\".");
    }

    result &= getObjects(specification, PARTS).allMatch(Application::validatePart);

    return result;
  }

  private static Transformer validatorResolver(final TopologyContext context) {
    return new Transformer(
        e -> isValidatorPath(e.path) && isObject(e.value),
        e ->
            Optional.of(
                new JsonEntry(
                    e.path,
                    context.validators.resolve(e.value.asJsonObject(), context.baseDirectory))));
  }

  @Command(
      name = "build",
      description = "Generates one inlined JSON per topology and uploads them to MongoDB")
  private static class Build implements Runnable {
    private final Context context;

    @Option(
        names = {"-f", "--file"},
        required = true,
        description = "A JSON file containing an array of topologies")
    File file;

    @Option(
        names = {"-c", "--collection"},
        description = "A MongoDB collection the topologies are uploaded")
    String collection;

    private Build(final Context context) {
      this.context = context;
    }

    private Optional<JsonArray> buildTopologies() {
      return tryToGetWithRethrow(
          () -> createReader(new FileInputStream(file)),
          reader ->
              readObjects(reader)
                  .filter(Application::validateTopology)
                  .map(
                      specification ->
                          build(
                              specification,
                              createTopologyContext(
                                  specification, file.getAbsoluteFile().getParentFile(), context)))
                  .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
                  .build());
    }

    @SuppressWarnings("java:S106") // Not logging.
    public void run() {
      final String col = getCollection(collection, context);

      if (col != null) {
        final MongoCollection<Document> c = context.database.getCollection(col);

        buildTopologies()
            .ifPresent(
                array ->
                    array.stream()
                        .filter(JsonUtil::isObject)
                        .map(JsonValue::asJsonObject)
                        .map(Application::toMongoDB)
                        .forEach(
                            t ->
                                update(c, t)
                                    .thenApply(result -> must(result, r -> r))
                                    .toCompletableFuture()
                                    .join()));
      } else {
        buildTopologies().ifPresent(array -> System.out.println(string(array)));
      }
    }
  }

  private static class Context {
    private Config config;
    private MongoDatabase database;
    private String environment;
    private Features features;
    private Level logLevel;
  }

  @Command(
      name = "run",
      description = "Runs topologies from a file containing a JSON array or a MongoDB collection")
  private static class Run implements Runnable {
    private final Context context;

    @ArgGroup() FileOrCollection fileOrCollection;

    private Run(final Context context) {
      this.context = context;
    }

    private static Stream<JsonObject> getTopologies(final File file) {
      return tryToGetWithRethrow(
              () -> createReader(new FileInputStream(file)),
              reader -> readObjects(reader).filter(Application::validateTopology))
          .orElseGet(Stream::empty);
    }

    private static Stream<JsonObject> getTopologies(
        final MongoCollection<Document> collection, final Bson filter) {
      return find(collection, filter).toCompletableFuture().join().stream()
          .map(Application::fromMongoDB);
    }

    private Optional<FileOrCollection.CollectionOptions> getCollectionOptions() {
      return ofNullable(fileOrCollection).map(f -> f.collection);
    }

    private Optional<File> getFile() {
      return ofNullable(fileOrCollection).map(f -> f.file).map(o -> o.file);
    }

    private Bson getFilter() {
      return getCollectionOptions()
          .map(o -> o.query)
          .flatMap(JsonUtil::from)
          .map(JsonValue::asJsonObject)
          .map(BsonUtil::fromJson)
          .orElse(null);
    }

    private Stream<JsonObject> getTopologies() {
      return getFile()
          .map(Run::getTopologies)
          .orElseGet(
              () ->
                  getTopologies(
                      context.database.getCollection(
                          getCollection(
                              getCollectionOptions().map(o -> o.collection).orElse(null), context)),
                      getFilter()));
    }

    public void run() {
      Optional.of(
              getTopologies()
                  .filter(Application::validateTopology)
                  .map(
                      specification ->
                          createTopology(
                              specification,
                              getFile()
                                  .map(file -> file.getAbsoluteFile().getParentFile())
                                  .orElse(null),
                              context)))
          .map(Streams::start)
          .filter(result -> !result)
          .ifPresent(result -> exit(1));
    }

    private static class FileOrCollection {
      @ArgGroup(exclusive = false)
      FileOptions file;

      @ArgGroup(exclusive = false)
      CollectionOptions collection;

      private static class FileOptions {
        @Option(
            names = {"-f", "--file"},
            required = true,
            description = "A JSON file containing an array of topologies")
        File file;
      }

      private static class CollectionOptions {
        @Option(
            names = {"-c", "--collection"},
            description = "A MongoDB collection containing the topologies")
        String collection;

        @Option(
            names = {"-q", "--query"},
            description = "A MongoDB query into the collection containing the topologies")
        String query;
      }
    }
  }

  private static class TopologyContext {
    private final Map<String, JsonObject> configurations = new HashMap<>();
    private final Map<String, KStream<String, JsonObject>> streams = new HashMap<>();
    private final Validator validators = new Validator();
    private String application;
    private File baseDirectory;
    private StreamsBuilder builder;
    private MongoDatabase database;
    private String environment;
    private Features features;
    private Level logLevel;
  }
}

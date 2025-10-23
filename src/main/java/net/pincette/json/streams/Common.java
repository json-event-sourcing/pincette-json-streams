package net.pincette.json.streams;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.in;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.max;
import static java.lang.String.valueOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.lines;
import static java.time.Duration.ofSeconds;
import static java.util.Arrays.fill;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Logger.getLogger;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.concat;
import static javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm;
import static net.pincette.config.Util.configValue;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.tel.OtelUtil.addOtelLogHandler;
import static net.pincette.jes.tel.OtelUtil.logRecordProcessor;
import static net.pincette.json.JsonOrYaml.read;
import static net.pincette.json.JsonUtil.asArray;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.getArray;
import static net.pincette.json.JsonUtil.getObject;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.merge;
import static net.pincette.json.JsonUtil.objectValue;
import static net.pincette.json.JsonUtil.objects;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.JsonUtil.strings;
import static net.pincette.json.JsonUtil.toNative;
import static net.pincette.json.JsonUtil.transformFieldNames;
import static net.pincette.json.Transform.transform;
import static net.pincette.json.Transform.transformBuilder;
import static net.pincette.json.streams.Application.APP_VERSION;
import static net.pincette.json.streams.Logging.LOGGER;
import static net.pincette.json.streams.Logging.LOGGER_NAME;
import static net.pincette.json.streams.Logging.exception;
import static net.pincette.json.streams.Logging.trace;
import static net.pincette.json.streams.Parameters.replaceParameters;
import static net.pincette.json.streams.Parameters.replaceParametersString;
import static net.pincette.json.streams.Parameters.resolve;
import static net.pincette.json.streams.Read.readObject;
import static net.pincette.mongo.BsonUtil.fromBson;
import static net.pincette.mongo.BsonUtil.toBsonDocument;
import static net.pincette.mongo.JsonClient.find;
import static net.pincette.mongo.JsonClient.findPublisher;
import static net.pincette.mongo.JsonClient.update;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.computeIfAbsent;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.zip;
import static net.pincette.util.Util.getLastSegment;
import static net.pincette.util.Util.isUUID;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetSilent;
import static net.pincette.util.Util.waitForCondition;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.typesafe.config.Config;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.MeterProvider;
import java.io.File;
import java.io.FileInputStream;
import java.net.http.HttpRequest;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import net.pincette.function.Fn;
import net.pincette.function.SideEffect;
import net.pincette.function.SupplierWithException;
import net.pincette.jes.tel.OtelUtil;
import net.pincette.json.JsonUtil;
import net.pincette.json.Transform.JsonEntry;
import net.pincette.json.Transform.Transformer;
import net.pincette.rs.streams.Message;
import net.pincette.util.Builder;
import net.pincette.util.Cases;
import net.pincette.util.Pair;
import net.pincette.util.Util.GeneralException;
import org.bson.Document;
import org.bson.conversions.Bson;

class Common {
  static final String AGGREGATE = "aggregate";
  static final String AGGREGATE_TYPE = "aggregateType";
  static final String ALIVE_AT = "aliveAt";
  static final String APPLICATION_FIELD = "application";
  static final Duration BACKOFF = ofSeconds(5);
  static final List<Bson> COLLECTION_CHANGES =
      list(match(in("operationType", list("insert", "replace", "update"))));
  static final String COMMAND = "command";
  static final String COMMANDS = "commands";
  static final String CONFIG_JSON_PREFIX = "config-json:";
  static final String CONFIG_PREFIX = "config:";
  static final String DATABASE = "mongodb.database";
  static final String DOLLAR = "_dollar_";
  static final String DOT = "_dot_";
  static final String ENVIRONMENT = "environment";
  static final String EVENT = "event";
  static final String EVENT_FULL = "event-full";
  static final String EVENT_TO_COMMAND = "eventToCommand";
  static final String FROM_COLLECTION = "fromCollection";
  static final String FROM_COLLECTIONS = "fromCollections";
  static final String FROM_STREAM = "fromStream";
  static final String FROM_STREAMS = "fromStreams";
  static final String FROM_TOPIC = "fromTopic";
  static final String FROM_TOPICS = "fromTopics";
  static final String INSTANCE = "instance";
  static final String JOIN = "join";
  static final String JSLT_IMPORTS = "jsltImports";
  static final String JSON_STREAMS = "json-streams";
  private static final String KEY_STORE = "keyStore";
  static final String LAG = "$lag";
  static final String LEADER = "pincette-json-streams-leader";
  static final String LEFT = "left";
  static final String LOG = "$log";
  static final String MERGE = "merge";
  static final String MONGODB_URI = "mongodb.uri";
  static final String NAME = "name";
  static final String ON = "on";
  static final String PARTS = "parts";
  private static final String PASSWORD = "password";
  static final String PIPELINE = "pipeline";
  static final String PREPROCESSOR = "preprocessor";
  static final String REDUCER = "reducer";
  static final String REPLY = "reply";
  static final String RIGHT = "right";
  static final String SCRIPT_IMPORTS = "scriptImports";
  static final String SIGN_JWT = "$signJwt";
  static final String SLASH = "_slash_";
  static final String STREAM = "stream";
  static final Set<String> STREAM_TYPES = set(JOIN, MERGE, STREAM);
  static final String S3ATTACHMENTS = "$s3Attachments";
  static final String S3CSV = "$s3Csv";
  static final String S3OUT = "$s3Out";
  static final String S3TRANSFER = "$s3Transfer";
  static final String TO_COLLECTION = "toCollection";
  static final String TO_TOPIC = "toTopic";
  static final String TYPE = "type";
  static final String VALIDATE = "$validate";
  static final String VALIDATOR = "validator";
  static final String VALIDATOR_IMPORTS = "validatorImports";
  static final String VERSION_FIELD = "version";
  static final String RESOURCE = "resource:";
  private static final Logger BUILD_LOGGER = getLogger(LOGGER_NAME + ".build");
  private static final String DEFAULT_NAMESPACE = "json-streams";
  private static final String DESCRIPTION = "description";
  private static final String ENV = "ENV";
  private static final String FILE = "file";
  private static final String INCLUDE = "include";
  private static final String JQ = "$jq";
  private static final String JSLT = "$jslt";
  private static final String MONGODB_COLLECTION = "mongodb.collection";
  private static final String NAMESPACE = "namespace";
  private static final String PARAMETERS = "parameters";
  private static final String PROFILE_FRAME_TYPE = "profile.frame.type";
  private static final String PROFILE_FRAME_VERSION = "profile.frame.version";
  private static final String REF = "ref";
  private static final String SCRIPT = "script";
  private static final Pattern SCRIPT_IMPORT = compile("^.*import[ \t]+\"([^\"]+)\".*$");

  private Common() {}

  static void addOtelLogger(final String service, final String version, final Context context) {
    OtelUtil.otelLogHandler(namespace(context.config), service, version, context.logRecordProcessor)
        .ifPresent(h -> addOtelLogHandler(context.logger.get(), h));
  }

  static String application(final JsonObject specification) {
    return specification.getString(APPLICATION_FIELD, null);
  }

  static AttributesBuilder applicationAttributes(final String application, final Context context) {
    return Attributes.builder()
        .put(APPLICATION_FIELD, application)
        .put(INSTANCE, context.instance)
        .put(PROFILE_FRAME_TYPE, JSON_STREAMS)
        .put(PROFILE_FRAME_VERSION, APP_VERSION);
  }

  static Stream<JsonValue> asStream(final JsonStructure json) {
    return isArray(json) ? json.asJsonArray().stream() : Stream.of(json.asJsonObject());
  }

  static File baseDirectory(final File file, final String path) {
    return parentDirectory(file)
        .map(parent -> path != null ? new File(parent, path).getParentFile() : parent)
        .orElse(null);
  }

  static JsonObject build(
      final JsonObject specification,
      final boolean runtime,
      final ApplicationContext applicationContext) {
    final Map<File, Pair<String, String>> scriptImports = new HashMap<>();
    final Map<File, Pair<String, JsonObject>> validatorImports = new HashMap<>();
    final Supplier<String> message = () -> instanceMessage("build", applicationContext.context);
    final var parameters =
        trace(
            message,
            parameters(specification, runtime, applicationContext),
            JsonUtil::string,
            BUILD_LOGGER);

    return trace(
        message,
        transformBuilder(
                applicationContext.baseDirectory != null
                    ? expandApplication(specification, applicationContext.baseDirectory, parameters)
                    : specification,
                createTransformer(scriptImports, validatorImports, parameters))
            .add(
                VALIDATOR_IMPORTS,
                createImports(
                    specification,
                    VALIDATOR_IMPORTS,
                    resolveScriptInValidatorImports(validatorImports.values(), scriptImports),
                    v -> v))
            .add(
                SCRIPT_IMPORTS,
                createImports(
                    specification, SCRIPT_IMPORTS, scriptImports.values(), JsonUtil::createValue))
            .add(ID, ofNullable(application(specification)).orElse("unknown"))
            .build(),
        JsonUtil::string,
        BUILD_LOGGER);
  }

  static JsonObject changedDocument(final ChangeStreamDocument<Document> change) {
    return ofNullable(change.getFullDocument())
        .map(doc -> fromBson(toBsonDocument(change.getFullDocument())))
        .orElse(null);
  }

  private static Stream<Pair<String, JsonObject>> commandAsArray(final JsonObject aggregate) {
    return ofNullable(aggregate.getJsonArray(COMMANDS)).map(Common::getCommandsArray).orElse(null);
  }

  private static Stream<Pair<String, JsonObject>> commandsAsObject(final JsonObject aggregate) {
    return getValue(aggregate, "/" + COMMANDS)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .map(Common::getCommandsObject)
        .orElse(null);
  }

  static <T> T config(final Context context, final Function<Config, T> get, final T defaultValue) {
    return ofNullable(context)
        .flatMap(c -> tryToGetSilent(() -> get.apply(c.config)))
        .orElse(defaultValue);
  }

  static <T> Optional<T> configValueApp(
      final Function<String, T> fn, final String path, final String application) {
    return configValue(fn, application + "." + path).or(() -> configValue(fn, path));
  }

  static ApplicationContext createApplicationContext(final Loaded loaded, final Context context) {
    return new ApplicationContext()
        .withApplication(application(loaded.specification))
        .withBaseDirectory(loaded.baseDirectory)
        .withContext(context);
  }

  static Context createContext(final Config config) {
    final MongoClient client =
        configValue(config::getString, MONGODB_URI).map(MongoClients::create).orElse(null);

    return new Context()
        .withConfig(config)
        .withEnvironment(configValue(config::getString, ENVIRONMENT).orElse(null))
        .withLogRecordProcessor(logRecordProcessor(config).orElse(null))
        .withClient(client)
        .withDatabase(
            client != null
                ? configValue(config::getString, DATABASE).map(client::getDatabase).orElse(null)
                : null);
  }

  private static <T> JsonObjectBuilder createImports(
      final JsonObject specification,
      final String name,
      final Collection<Pair<String, T>> imports,
      final Function<T, JsonValue> value) {
    return imports.stream()
        .reduce(
            ofNullable(specification.getJsonObject(name))
                .map(JsonUtil::createObjectBuilder)
                .orElseGet(JsonUtil::createObjectBuilder),
            (b, p) -> b.add(p.first, value.apply(p.second)),
            (b1, b2) -> b1);
  }

  static Optional<SSLContext> createSslContext(final JsonObject sslContext) {
    final String password = sslContext.getString(PASSWORD);

    return tryToGetRethrow(() -> SSLContext.getInstance("TLSv1.3"))
        .flatMap(
            context ->
                getKeyStore(sslContext.getString(KEY_STORE), password)
                    .flatMap(store -> getKeyManagerFactory(store, password))
                    .flatMap(Common::getKeyManagers)
                    .map(managers -> pair(context, managers)))
        .map(
            pair ->
                SideEffect.<SSLContext>run(
                        () -> tryToDoRethrow(() -> pair.first.init(pair.second, null, null)))
                    .andThenGet(() -> pair.first));
  }

  private static Transformer createTransformer(
      final Map<File, Pair<String, String>> jsltImports,
      final Map<File, Pair<String, JsonObject>> validatorImports,
      final JsonObject parameters) {
    return replaceParameters(parameters)
        .thenApply(validatorResolver(validatorImports, parameters))
        .thenApply(scriptResolver(jsltImports));
  }

  static boolean excludeTechnicalConsumerGroups(final String group) {
    return !isUUID(group) && !group.startsWith("sse-");
  }

  private static JsonObject expandAggregate(
      final JsonObject aggregate, final File baseDirectory, final JsonObject parameters) {
    final JsonObject expanded =
        createObjectBuilder(aggregate)
            .add(COMMANDS, expandCommands(aggregate, baseDirectory, parameters))
            .build();

    return expanded.containsKey(PREPROCESSOR)
        ? expandStream(expanded, PREPROCESSOR, baseDirectory, parameters)
        : expanded;
  }

  private static JsonObject expandApplication(
      final JsonObject application, final File baseDirectory, final JsonObject parameters) {
    return expandSequenceContainer(application, baseDirectory, parameters)
        .apply(PARTS, expandPart(application(application)));
  }

  private static JsonObject expandCommand(
      final JsonObject command, final File baseDirectory, final JsonObject parameters) {
    return create(() -> createObjectBuilder(command))
        .updateIf(
            () -> ofNullable(command.get(PREPROCESSOR)),
            (b, v) ->
                b.add(
                    PREPROCESSOR,
                    expandStream(command, PREPROCESSOR, baseDirectory, parameters)
                        .get(PREPROCESSOR)))
        .updateIf(
            () -> ofNullable(command.get(REDUCER)).filter(Common::isPipeline),
            (b, v) ->
                b.add(
                    REDUCER,
                    expandStream(command, REDUCER, baseDirectory, parameters).get(REDUCER)))
        .updateIf(
            () -> getString(command, "/" + REDUCER).filter(Common::isScript),
            (b, v) -> b.add(REDUCER, resolveFile(baseDirectory, v, parameters)))
        .updateIf(
            () -> getString(command, "/" + VALIDATOR),
            (b, v) -> b.add(VALIDATOR, resolveFile(baseDirectory, v, parameters)))
        .build()
        .build();
  }

  private static JsonObjectBuilder expandCommands(
      final JsonObject aggregate, final File baseDirectory, final JsonObject parameters) {
    return getCommands(aggregate)
        .map(pair -> pair(pair.first, expandCommand(pair.second, baseDirectory, parameters)))
        .reduce(createObjectBuilder(), (b, p) -> b.add(p.first, p.second), (b1, b2) -> b1);
  }

  private static Expander expandPart(final String application) {
    return part ->
        baseDirectory ->
            parameters -> {
              if (!part.containsKey(TYPE)) {
                throw new GeneralException(
                    "The application "
                        + application
                        + " has the part "
                        + part.getString(NAME, "unknown")
                        + ", which has no type field.");
              }

              return switch (part.getString(TYPE)) {
                case AGGREGATE -> expandAggregate(part, baseDirectory, parameters);
                case STREAM -> expandStream(part, PIPELINE, baseDirectory, parameters);
                default -> part;
              };
            };
  }

  private static JsonArray expandSequence(
      final JsonArray array,
      final File baseDirectory,
      final JsonObject parameters,
      final Expander expander) {
    return from(
        array.stream()
            .flatMap(v -> expandSequenceEntry(v, baseDirectory, parameters))
            .map(
                expansion ->
                    transform(
                        expander
                            .apply(expansion.specification.asJsonObject())
                            .apply(expansion.file)
                            .apply(expansion.parameters),
                        replaceParameters(expansion.parameters))));
  }

  private static BiFunction<String, Expander, JsonObject> expandSequenceContainer(
      final JsonObject container, final File baseDirectory, final JsonObject parameters) {
    return (field, expand) ->
        createObjectBuilder(container)
            .add(
                field,
                getSequence(container, field, baseDirectory, parameters)
                    .map(pair -> expandSequence(pair.first, pair.second, parameters, expand))
                    .orElseGet(JsonUtil::emptyArray))
            .build();
  }

  private static Stream<Expansion> expandSequenceEntry(
      final JsonValue json, final File baseDirectory, final JsonObject parameters) {
    return Cases.<JsonValue, Stream<Expansion>>withValue(json)
        .or(
            Common::isInclude,
            i ->
                getSequenceFromFile(
                    getString(i.asJsonObject(), "/" + INCLUDE + "/" + FILE).orElse(null),
                    baseDirectory,
                    getObject(i.asJsonObject(), "/" + INCLUDE + "/" + PARAMETERS)
                        .map(pars -> merge(parameters, pars))
                        .orElse(parameters)))
        .or(
            JsonUtil::isObject,
            o -> Stream.of(new Expansion(o.asJsonObject(), baseDirectory, parameters)))
        .or(
            JsonUtil::isString,
            s -> getSequenceFromFile(asString(s).getString(), baseDirectory, parameters))
        .get()
        .orElseGet(Stream::empty);
  }

  private static Expander expandStage() {
    return stage ->
        baseDirectory ->
            parameters -> transform(stage, stageFileResolver(baseDirectory, parameters));
  }

  private static JsonObject expandStream(
      final JsonObject stream,
      final String field,
      final File baseDirectory,
      final JsonObject parameters) {
    return expandSequenceContainer(stream, baseDirectory, parameters).apply(field, expandStage());
  }

  static CompletionStage<Publisher<JsonObject>> findJson(
      final MongoCollection<Document> collection, final Bson filter) {
    return tryToGetForever(() -> completedFuture(findPublisher(collection, filter)));
  }

  static JsonObject fromMongoDB(final JsonObject json) {
    return transformFieldNames(json, Common::unescapeFieldName).build();
  }

  static String getApplicationCollection(final String collection, final Context context) {
    return collection != null
        ? collection
        : tryToGetSilent(() -> context.config.getString(MONGODB_COLLECTION)).orElse(null);
  }

  static Stream<JsonObject> getApplications(
      final MongoCollection<Document> collection, final Bson filter) {
    return getApplicationsAsync(collection, filter).toCompletableFuture().join();
  }

  static CompletionStage<Stream<JsonObject>> getApplicationsAsync(
      final MongoCollection<Document> collection, final Bson filter) {
    return find(collection, filter).thenApply(list -> list.stream().map(Common::fromMongoDB));
  }

  static Stream<Pair<String, JsonObject>> getCommands(final JsonObject aggregate) {
    return tryWith(() -> commandsAsObject(aggregate))
        .or(() -> commandAsArray(aggregate))
        .get()
        .orElseGet(Stream::empty);
  }

  private static Stream<Pair<String, JsonObject>> getCommandsArray(final JsonArray commands) {
    return objects(commands)
        .map(
            command ->
                pair(
                    command.getString(NAME),
                    create(JsonUtil::createObjectBuilder)
                        .update(b -> b.add(REDUCER, command.getString(REDUCER)))
                        .updateIf(
                            () -> getValue(command, "/" + VALIDATOR), (b, v) -> b.add(VALIDATOR, v))
                        .updateIf(
                            () -> getString(command, "/" + DESCRIPTION),
                            (b, v) -> b.add(DESCRIPTION, v))
                        .updateIf(
                            () -> getArray(command, "/" + PREPROCESSOR),
                            (b, v) -> b.add(PREPROCESSOR, v))
                        .build()
                        .build()));
  }

  private static Stream<Pair<String, JsonObject>> getCommandsObject(final JsonObject commands) {
    return commands.entrySet().stream()
        .filter(e -> isObject(e.getValue()))
        .map(e -> pair(e.getKey(), e.getValue().asJsonObject()));
  }

  private static JsonValue getConfigValue(final JsonValue value, final Config config) {
    return stringValue(value)
        .map(v -> pair(v, v.indexOf(':')))
        .filter(pair -> pair.second != -1)
        .map(
            pair ->
                pair(
                    pair.first.substring(0, pair.second + 1),
                    pair.first.substring(pair.second + 1)))
        .filter(pair -> CONFIG_PREFIX.equals(pair.first) || CONFIG_JSON_PREFIX.equals(pair.first))
        .flatMap(
            pair ->
                CONFIG_PREFIX.equals(pair.first)
                    ? Optional.of(createValue(config.getValue(pair.second).unwrapped()))
                    : from(config.getString(pair.second)))
        .orElseGet(
            () -> {
              LOGGER.severe(
                  () ->
                      "The value "
                          + string(value)
                          + " could not be substituted by the configuration");
              return value;
            });
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

  static HttpRequest.Builder getRequestBuilder(
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

  private static Optional<String> getScriptImport(final String line) {
    return Optional.of(SCRIPT_IMPORT.matcher(line))
        .filter(Matcher::matches)
        .map(matcher -> matcher.group(1));
  }

  private static Optional<Pair<JsonArray, File>> getSequence(
      final JsonObject json,
      final String field,
      final File baseDirectory,
      final JsonObject parameters) {
    return getString(json, "/" + field)
        .map(s -> replaceParametersString(s, resolve(parameters)))
        .flatMap(path -> net.pincette.util.Util.resolveFile(baseDirectory, path))
        .map(file -> pair(readArray(file), file.getParentFile()))
        .or(() -> getArray(json, "/" + field).map(a -> pair(a, baseDirectory)));
  }

  private static Stream<Expansion> getSequenceFromFile(
      final String filename, final File baseDirectory, final JsonObject parameters) {
    return Stream.of(filename)
        .map(s -> replaceParametersString(s, resolve(parameters)))
        .map(s -> net.pincette.util.Util.resolveFile(baseDirectory, s).orElse(null))
        .filter(Objects::nonNull)
        .filter(File::exists)
        .flatMap(file -> getSequenceFromFile(file, parameters));
  }

  private static Stream<Expansion> getSequenceFromFile(
      final File file, final JsonObject parameters) {
    return read(file).map(Common::asStream).stream()
        .flatMap(
            s ->
                s.map(v -> new Expansion(v, file.getParentFile(), parameters))
                    .flatMap(
                        e ->
                            isInclude(e.specification) || isString(e.specification)
                                ? expandSequenceEntry(
                                    e.specification, file.getParentFile(), parameters)
                                : Stream.of(e)));
  }

  private static JsonObject injectConfiguration(final JsonObject parameters, final Config config) {
    return transform(
        parameters,
        new Transformer(
            e -> isConfigRef(e.value),
            e -> Optional.of(new JsonEntry(e.path, getConfigValue(e.value, config)))));
  }

  static String instanceMessage(final String message, final Context context) {
    return "instance: " + context.instance + ": " + message;
  }

  private static boolean isConfigRef(final JsonValue value) {
    return stringValue(value).filter(Common::isConfigRef).isPresent();
  }

  static boolean isConfigRef(final String s) {
    return s.startsWith(CONFIG_PREFIX) || s.startsWith(CONFIG_JSON_PREFIX);
  }

  private static boolean isInclude(final JsonValue json) {
    return objectValue(json)
        .filter(j -> j.size() == 1)
        .flatMap(j -> ofNullable(j.getJsonObject(INCLUDE)))
        .isPresent();
  }

  private static boolean isPipeline(final JsonValue json) {
    return stringValue(json).filter(s -> s.endsWith(".yaml") || s.endsWith(".yml")).isPresent()
        || isArray(json);
  }

  private static boolean isReducerPath(final String path) {
    return path.endsWith("." + REDUCER);
  }

  private static boolean isScript(final String s) {
    return s.endsWith(".jslt") || s.endsWith(".jq");
  }

  private static boolean isScriptPath(final String path) {
    return path.endsWith(JSLT)
        || path.endsWith(JSLT + "." + SCRIPT)
        || path.endsWith(JQ)
        || path.endsWith(JQ + "." + SCRIPT)
        || isReducerPath(path)
        || path.endsWith(EVENT_TO_COMMAND);
  }

  private static boolean isValidatorPath(final String path) {
    return path.endsWith("." + VALIDATOR) || path.endsWith(VALIDATE);
  }

  private static boolean isValidatorRef(final String path) {
    return path.equals(INCLUDE) || path.endsWith("." + REF);
  }

  static Optional<MeterProvider> meterProvider(final Context context) {
    return ofNullable(context.metrics).map(OpenTelemetry::getMeterProvider);
  }

  static String namespace(final Config config) {
    return configValue(config::getString, NAMESPACE).orElse(DEFAULT_NAMESPACE);
  }

  static String numberLines(final String s) {
    return zip(stream(s.split("\\n")).sequential(), rangeExclusive(1, MAX_VALUE))
        .map(pair -> rightAlign(valueOf(pair.second)) + " " + pair.first)
        .collect(joining("\n"));
  }

  private static JsonObject parameters(
      final JsonObject specification, final boolean runtime, final ApplicationContext context) {
    return Builder.create(
            () ->
                createObjectBuilder(
                    ofNullable(specification.getJsonObject(PARAMETERS))
                        .map(
                            parameters ->
                                runtime
                                    ? injectConfiguration(parameters, context.context.config)
                                    : parameters)
                        .orElseGet(JsonUtil::emptyObject)))
        .updateIf(() -> ofNullable(context.context.environment), (b, e) -> b.add(ENV, e))
        .build()
        .build();
  }

  private static Optional<File> parentDirectory(final File file) {
    return tryToGetRethrow(() -> file.getCanonicalFile().getParentFile());
  }

  private static JsonArray readArray(final File file) {
    return read(file).map(JsonValue::asJsonArray).orElse(null);
  }

  static String removeSuffix(final String application, final Context context) {
    return ofNullable(context.environment)
        .filter(e -> application.endsWith("-" + e))
        .map(e -> application.substring(0, application.lastIndexOf('-')))
        .orElse(application);
  }

  private static String resolveFile(
      final File baseDirectory, final String path, final JsonObject parameters) {
    return Optional.of(path)
        .filter(p -> !p.startsWith(RESOURCE))
        .flatMap(
            p ->
                net.pincette.util.Util.resolveFile(
                    baseDirectory, replaceParametersString(p, resolve(parameters))))
        .map(File::getAbsolutePath)
        .filter(p -> new File(p).exists())
        .orElse(path);
  }

  private static String resolveScript(
      final String script,
      final Map<File, Pair<String, String>> imports,
      final boolean forReducer) {
    return Optional.of(script)
        .filter(j -> !j.startsWith(RESOURCE))
        .map(File::new)
        .filter(File::exists)
        .map(
            file ->
                scriptPrefix(script, forReducer)
                    + file.getName()
                    + "\n"
                    + resolveScriptImports(file, imports, forReducer))
        .orElse(script);
  }

  private static String resolveScriptImport(
      final String line,
      final String imp,
      final File script,
      final Map<File, Pair<String, String>> imports,
      final boolean forReducer) {
    final var imported =
        net.pincette.util.Util.resolveFile(baseDirectory(script, null), imp).orElse(null);
    final var resolved = resolveScriptImports(imported, imports, forReducer);

    return line.replace(
        imp,
        imports.compute(imported, (k, v) -> v == null ? pair(randomUUID().toString(), resolved) : v)
            .first);
  }

  private static String resolveScriptImports(
      final File script, final Map<File, Pair<String, String>> imports, final boolean forReducer) {
    return tryToGetRethrow(() -> lines(script.toPath(), UTF_8))
        .orElseGet(Stream::empty)
        .map(
            line ->
                getScriptImport(line)
                    .map(imp -> resolveScriptImport(line, imp, script, imports, forReducer))
                    .orElse(line))
        .collect(joining("\n"));
  }

  private static Collection<Pair<String, JsonObject>> resolveScriptInValidatorImports(
      final Collection<Pair<String, JsonObject>> validatorImports,
      final Map<File, Pair<String, String>> scriptImports) {
    return validatorImports.stream()
        .map(pair -> pair(pair.first, transform(pair.second, scriptResolver(scriptImports))))
        .toList();
  }

  private static JsonObject resolveValidator(
      final File file,
      final Map<File, Pair<String, JsonObject>> imports,
      final JsonObject parameters) {
    return transform(
        readObject(file),
        validatorRefResolver(file, imports, parameters)
            .thenApply(stageFileResolver(file.getParentFile(), parameters)));
  }

  private static String resolveValidator(
      final String path,
      final File baseDirectory,
      final Map<File, Pair<String, JsonObject>> imports,
      final JsonObject parameters) {
    return computeIfAbsent(
            imports,
            new File(resolveFile(baseDirectory, path, parameters)),
            f -> pair(randomUUID().toString(), resolveValidator(f, imports, parameters)))
        .first;
  }

  private static JsonEntry resolveValidatorEntry(
      final JsonEntry entry,
      final File parentDirectory,
      final Map<File, Pair<String, JsonObject>> imports,
      final JsonObject parameters) {
    return new JsonEntry(
        entry.path,
        entry.path.equals(INCLUDE)
            ? resolveValidatorInclude(
                entry.value.asJsonArray(), parentDirectory, imports, parameters)
            : createValue(
                resolveValidator(
                    asString(entry.value).getString(), parentDirectory, imports, parameters)));
  }

  private static JsonValue resolveValidatorInclude(
      final JsonArray include,
      final File baseDirectory,
      final Map<File, Pair<String, JsonObject>> imports,
      final JsonObject parameters) {
    return strings(include)
        .map(path -> resolveValidator(path, baseDirectory, imports, parameters))
        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
        .build();
  }

  private static String rightAlign(final String s) {
    final var padding = new char[max(4 - s.length(), 0)];

    fill(padding, ' ');

    return new String(padding) + s;
  }

  static CompletionStage<JsonObject> saveJson(
      final MongoCollection<Document> collection, final JsonObject json) {
    return tryToGetForever(
        () ->
            update(collection, json)
                .thenApply(result -> must(result, r -> r))
                .thenApply(result -> json));
  }

  static CompletionStage<Message<String, JsonObject>> saveMessage(
      final MongoCollection<Document> collection, final Message<String, JsonObject> message) {
    return saveJson(collection, message.value).thenApply(value -> message);
  }

  private static String scriptPrefix(final String filename, final boolean forReducer) {
    final var comment = filename.endsWith(".jq") ? "# " : "// ";

    return forReducer
        ? getLastSegment(filename, ".").map(s -> s + ":" + comment).orElse(comment)
        : comment;
  }

  private static Transformer scriptResolver(final Map<File, Pair<String, String>> imports) {
    return new Transformer(
        e -> isScriptPath(e.path) && isString(e.value),
        e ->
            Optional.of(
                new JsonEntry(
                    e.path,
                    createValue(
                        resolveScript(
                            asString(e.value).getString(), imports, isReducerPath(e.path))))));
  }

  private static Transformer stageFileResolver(
      final File baseDirectory, final JsonObject parameters) {
    return new Transformer(
        e -> (isScriptPath(e.path) && isString(e.value)) || isValidatorPath(e.path),
        e ->
            stringValue(e.value)
                .map(
                    path ->
                        new JsonEntry(
                            e.path, createValue(resolveFile(baseDirectory, path, parameters)))));
  }

  static Message<String, JsonObject> toMessage(final JsonObject json) {
    return message(toNative(json.get(ID)).toString(), json);
  }

  static <T> CompletionStage<T> tryToGetForever(
      final SupplierWithException<CompletionStage<T>> fn) {
    return tryToGetForever(fn, null);
  }

  static <T> CompletionStage<T> tryToGetForever(
      final SupplierWithException<CompletionStage<T>> fn, final Supplier<String> message) {
    return tryToGetForever(fn, message, null);
  }

  static <T> CompletionStage<T> tryToGetForever(
      final SupplierWithException<CompletionStage<T>> fn,
      final Supplier<String> message,
      final Supplier<Logger> logger) {
    return net.pincette.util.Util.tryToGetForever(fn, BACKOFF, e -> exception(e, message, logger));
  }

  private static String unescapeFieldName(final String name) {
    return name.replace(DOT, ".").replace(SLASH, "/").replace(DOLLAR, "$");
  }

  private static Transformer validatorRefResolver(
      final File file,
      final Map<File, Pair<String, JsonObject>> imports,
      final JsonObject parameters) {
    return new Transformer(
        e -> isValidatorRef(e.path),
        e -> parentDirectory(file).map(p -> resolveValidatorEntry(e, p, imports, parameters)));
  }

  private static Transformer validatorResolver(
      final Map<File, Pair<String, JsonObject>> imports, final JsonObject parameters) {
    return new Transformer(
        e -> isValidatorPath(e.path) && isString(e.value),
        e ->
            Optional.of(asString(e.value).getString())
                .map(File::new)
                .map(file -> new JsonEntry(e.path, resolveValidator(file, imports, parameters))));
  }

  static boolean waitFor(final Supplier<CompletionStage<Boolean>> check) {
    return net.pincette.util.Util.waitFor(waitForCondition(check), ofSeconds(1))
        .toCompletableFuture()
        .join();
  }

  static String version(final JsonObject specification) {
    return specification.getString(VERSION_FIELD, null);
  }

  private interface Expander extends Fn<JsonObject, Fn<File, Fn<JsonObject, JsonObject>>> {}

  private record Expansion(JsonValue specification, File file, JsonObject parameters) {}
}

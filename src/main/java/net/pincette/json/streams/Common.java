package net.pincette.json.streams;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.max;
import static java.lang.String.valueOf;
import static java.lang.System.exit;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.lines;
import static java.time.Instant.now;
import static java.util.Arrays.fill;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.json.JsonOrYaml.read;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.getArray;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.objects;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.JsonUtil.strings;
import static net.pincette.json.Transform.transform;
import static net.pincette.json.Transform.transformBuilder;
import static net.pincette.json.streams.Logging.LOGGER;
import static net.pincette.json.streams.Logging.trace;
import static net.pincette.json.streams.Read.readObject;
import static net.pincette.mongo.Collection.updateOne;
import static net.pincette.mongo.JsonClient.find;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.computeIfAbsent;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.zip;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.replaceAll;
import static net.pincette.util.Util.tryToGet;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetSilent;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.typesafe.config.Config;
import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
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
import javax.json.JsonStructure;
import javax.json.JsonValue;
import net.pincette.function.Fn;
import net.pincette.function.SupplierWithException;
import net.pincette.json.JsonUtil;
import net.pincette.json.Transform;
import net.pincette.json.Transform.JsonEntry;
import net.pincette.json.Transform.Transformer;
import net.pincette.util.Builder;
import net.pincette.util.Pair;
import org.bson.BsonDateTime;
import org.bson.Document;
import org.bson.conversions.Bson;

class Common {
  static final String AGGREGATE = "aggregate";
  static final String AGGREGATE_TYPE = "aggregateType";
  static final String ALIVE_AT = "aliveAt";
  static final String APPLICATION_FIELD = "application";
  static final String COMMAND = "command";
  static final String COMMANDS = "commands";
  static final String DESTINATION_TYPE = "destinationType";
  static final String DESTINATIONS = "destinations";
  static final String DOLLAR = "_dollar_";
  static final String DOT = "_dot_";
  static final String ENVIRONMENT = "environment";
  static final String EVENT = "event";
  static final String EVENT_FULL = "event-full";
  static final String EVENT_TO_COMMAND = "eventToCommand";
  static final String FILTER = "filter";
  static final String FROM_STREAM = "fromStream";
  static final String FROM_STREAMS = "fromStreams";
  static final String FROM_TOPIC = "fromTopic";
  static final String FROM_TOPICS = "fromTopics";
  static final String INSTANCE = "instance";
  static final String JOIN = "join";
  static final String JSLT_IMPORTS = "jsltImports";
  static final String LEADER = "pincette-json-streams-leader";
  static final String LEFT = "left";
  static final String LOG = "$log";
  static final String MERGE = "merge";
  static final String NAME = "name";
  static final String ON = "on";
  static final String PARTS = "parts";
  static final String PIPELINE = "pipeline";
  static final String REACTOR = "reactor";
  static final String REDUCER = "reducer";
  static final String REPLY = "reply";
  static final String RIGHT = "right";
  static final String SLASH = "_slash_";
  static final String SOURCE_TYPE = "sourceType";
  static final String STREAM = "stream";
  static final Set<String> STREAM_TYPES = set(JOIN, MERGE, STREAM);
  static final String TO_TOPIC = "toTopic";
  static final String TYPE = "type";
  static final String VALIDATE = "$validate";
  static final String VALIDATOR = "validator";
  static final String VALIDATOR_IMPORTS = "validatorImports";
  static final String VERSION = "version";
  static final String WINDOW = "window";
  static final String RESOURCE = "resource:";
  private static final String BUILD_LOGGER = LOGGER + ".build";
  private static final String CONFIG_PREFIX = "config:";
  private static final String DESCRIPTION = "description";
  private static final String ENV = "ENV";
  private static final Pattern ENV_PATTERN = compile("\\$\\{(\\w+)}");
  private static final Pattern ENV_PATTERN_STRING = compile("\\$\\{([^:}]+:)?(\\w+)(:[^:}]+)?}");
  private static final String INCLUDE = "include";
  private static final String JSLT = "$jslt";
  private static final Pattern JSLT_IMPORT = compile("^.*import[ \t]+\"([^\"]+)\"" + ".*$");
  private static final String MONGODB_COLLECTION = "mongodb.collection";
  private static final String PARAMETERS = "parameters";
  private static final String REF = "ref";
  private static final String SCRIPT = "script";

  private Common() {}

  static <T> CompletionStage<Boolean> aliveAtUpdate(
      final Supplier<Bson> criterion,
      final Supplier<Field<T>> field,
      final MongoCollection<Document> collection,
      final String logger) {
    return updateOne(
            collection,
            criterion.get(),
            list(
                Aggregates.set(
                    new Field<>(ALIVE_AT, new BsonDateTime(now().toEpochMilli())), field.get())),
            new UpdateOptions().upsert(true))
        .thenApply(result -> trace("aliveAtUpdate", result, logger))
        .thenApply(result -> result != null && result.wasAcknowledged())
        .thenApply(result -> must(result, r -> r));
  }

  static String application(final JsonObject specification) {
    return specification.getString(APPLICATION_FIELD, null);
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
      final TopologyContext topologyContext) {
    final Map<File, Pair<String, String>> jsltImports = new HashMap<>();
    final Map<File, Pair<String, JsonObject>> validatorImports = new HashMap<>();
    final String message = instanceMessage("build", topologyContext.context);
    final var parameters =
        trace(
            message,
            parameters(specification, runtime, topologyContext),
            JsonUtil::string,
            BUILD_LOGGER);

    return trace(
        message,
        transformBuilder(
                topologyContext.baseDirectory != null
                    ? expandApplication(specification, topologyContext.baseDirectory, parameters)
                    : specification,
                createTransformer(jsltImports, validatorImports, parameters))
            .add(
                VALIDATOR_IMPORTS,
                createImports(
                    specification,
                    VALIDATOR_IMPORTS,
                    resolveJsltInValidatorImports(validatorImports.values(), jsltImports),
                    v -> v))
            .add(
                JSLT_IMPORTS,
                createImports(
                    specification, JSLT_IMPORTS, jsltImports.values(), JsonUtil::createValue))
            .add(ID, application(specification))
            .build(),
        JsonUtil::string,
        BUILD_LOGGER);
  }

  static <T> T config(final Context context, final Function<Config, T> get, final T defaultValue) {
    return ofNullable(context)
        .flatMap(c -> tryToGetSilent(() -> get.apply(c.config)))
        .orElse(defaultValue);
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

  static TopologyContext createTopologyContext(
      final Loaded loaded, final File topFile, final Context context) {
    return new TopologyContext()
        .withApplication(application(loaded.specification))
        .withBaseDirectory(topFile != null ? baseDirectory(topFile, loaded.path) : null)
        .withContext(context);
  }

  private static Transformer createTransformer(
      final Map<File, Pair<String, String>> jsltImports,
      final Map<File, Pair<String, JsonObject>> validatorImports,
      final JsonObject parameters) {
    return replaceParameters(parameters)
        .thenApply(validatorResolver(validatorImports, parameters))
        .thenApply(jsltResolver(jsltImports));
  }

  private static JsonObject expandAggregate(
      final JsonObject aggregate, final File baseDirectory, final JsonObject parameters) {
    return createObjectBuilder(aggregate)
        .add(
            COMMANDS,
            getCommands(aggregate)
                .map(
                    pair -> pair(pair.first, expandCommand(pair.second, baseDirectory, parameters)))
                .reduce(createObjectBuilder(), (b, p) -> b.add(p.first, p.second), (b1, b2) -> b1))
        .build();
  }

  private static JsonObject expandApplication(
      final JsonObject application, final File baseDirectory, final JsonObject parameters) {
    return expandSequenceContainer(application, baseDirectory, parameters)
        .apply(PARTS, expandPart());
  }

  private static JsonObject expandCommand(
      final JsonObject command, final File baseDirectory, final JsonObject parameters) {
    return create(() -> createObjectBuilder(command))
        .updateIf(
            () -> getString(command, "/" + REDUCER),
            (b, v) -> b.add(REDUCER, resolveFile(baseDirectory, v, parameters)))
        .updateIf(
            () -> getString(command, "/" + VALIDATOR),
            (b, v) -> b.add(VALIDATOR, resolveFile(baseDirectory, v, parameters)))
        .build()
        .build();
  }

  private static Expander expandPart() {
    return part ->
        baseDirectory ->
            parameters -> {
              switch (part.getString(TYPE)) {
                case AGGREGATE:
                  return expandAggregate(part, baseDirectory, parameters);
                case STREAM:
                  return expandStream(part, baseDirectory, parameters);
                default:
                  return part;
              }
            };
  }

  private static JsonArray expandSequence(
      final JsonArray array,
      final File baseDirectory,
      final JsonObject parameters,
      final Expander expander) {
    return from(
        array.stream()
            .filter(v -> isObject(v) || isString(v))
            .flatMap(
                v ->
                    isObject(v)
                        ? Stream.of(pair(v.asJsonObject(), baseDirectory))
                        : getSequenceFromFile(asString(v).getString(), baseDirectory, parameters))
            .map(
                pair ->
                    expander
                        .apply(pair.first.asJsonObject())
                        .apply(pair.second)
                        .apply(parameters)));
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

  private static Expander expandStage() {
    return stage ->
        baseDirectory ->
            parameters -> transform(stage, stageFileResolver(baseDirectory, parameters));
  }

  private static JsonObject expandStream(
      final JsonObject stream, final File baseDirectory, final JsonObject parameters) {
    return expandSequenceContainer(stream, baseDirectory, parameters)
        .apply(PIPELINE, expandStage());
  }

  static <T> T fatal(
      final SupplierWithException<T> supplier,
      final Logger logger,
      final Supplier<String> message) {
    return tryToGet(
            supplier,
            e -> {
              logger.log(Level.SEVERE, e, message);
              exit(1);
              return null;
            })
        .orElse(null);
  }

  static JsonObject fromMongoDB(final JsonObject json) {
    return transformFieldNames(json, Common::unescapeFieldName).build();
  }

  static Stream<Pair<String, JsonObject>> getCommands(final JsonObject aggregate) {
    return tryWith(
            () ->
                getValue(aggregate, "/" + COMMANDS)
                    .filter(JsonUtil::isObject)
                    .map(JsonValue::asJsonObject)
                    .map(Common::getCommandsObject)
                    .orElse(null))
        .or(
            () ->
                ofNullable(aggregate.getJsonArray(COMMANDS))
                    .map(Common::getCommandsArray)
                    .orElse(null))
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
                        .build()
                        .build()));
  }

  private static Stream<Pair<String, JsonObject>> getCommandsObject(final JsonObject commands) {
    return commands.entrySet().stream()
        .filter(e -> isObject(e.getValue()))
        .map(e -> pair(e.getKey(), e.getValue().asJsonObject()));
  }

  private static JsonValue getConfigValue(final JsonValue value, final Config config) {
    return createValue(
        config.getValue(asString(value).getString().substring(CONFIG_PREFIX.length())).unwrapped());
  }

  private static Optional<String> getImport(final String line) {
    return Optional.of(JSLT_IMPORT.matcher(line))
        .filter(Matcher::matches)
        .map(matcher -> matcher.group(1));
  }

  private static Optional<Pair<JsonArray, File>> getSequence(
      final JsonObject json,
      final String field,
      final File baseDirectory,
      final JsonObject parameters) {
    return getString(json, "/" + field)
        .map(s -> replaceParametersString(s, parameters))
        .flatMap(path -> net.pincette.util.Util.resolveFile(baseDirectory, path))
        .map(file -> pair(readArray(file), file.getParentFile()))
        .or(() -> getArray(json, "/" + field).map(a -> pair(a, baseDirectory)));
  }

  private static Stream<Pair<JsonValue, File>> getSequenceFromFile(
      final String filename, final File baseDirectory, final JsonObject parameters) {
    return Stream.of(filename)
        .map(s -> replaceParametersString(s, parameters))
        .map(s -> net.pincette.util.Util.resolveFile(baseDirectory, s).orElse(null))
        .filter(Objects::nonNull)
        .filter(File::exists)
        .flatMap(
            file ->
                read(file)
                    .map(Common::asStream)
                    .map(s -> s.map(p -> pair(p, file.getParentFile())))
                    .orElseGet(Stream::empty));
  }

  static Stream<JsonObject> getTopologies(
      final MongoCollection<Document> collection, final Bson filter) {
    return find(collection, filter).toCompletableFuture().join().stream().map(Common::fromMongoDB);
  }

  static String getTopologyCollection(final String collection, final Context context) {
    return collection != null
        ? collection
        : tryToGetSilent(() -> context.config.getString(MONGODB_COLLECTION)).orElse(null);
  }

  private static boolean hasConfigurationParameters(final JsonValue value) {
    final BooleanSupplier tryArray =
        () ->
            isArray(value)
                ? hasConfigurationParameters(value.asJsonArray())
                : stringValue(value).filter(v -> v.startsWith(CONFIG_PREFIX)).isPresent();

    return isObject(value)
        ? hasConfigurationParameters(value.asJsonObject())
        : tryArray.getAsBoolean();
  }

  private static boolean hasConfigurationParameters(final JsonObject value) {
    return hasConfigurationParameters(value.values().stream());
  }

  private static boolean hasConfigurationParameters(final JsonArray value) {
    return hasConfigurationParameters(value.stream());
  }

  private static boolean hasConfigurationParameters(final Stream<JsonValue> values) {
    return values.anyMatch(Common::hasConfigurationParameters);
  }

  private static boolean hasParameter(final JsonValue value) {
    return isString(value) && hasParameter(asString(value).getString());
  }

  private static boolean hasParameter(final String s) {
    return ENV_PATTERN_STRING.matcher(s).find();
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
    return isString(value) && asString(value).getString().startsWith(CONFIG_PREFIX);
  }

  private static boolean isJsltPath(final String path) {
    return path.endsWith(JSLT)
        || path.endsWith(JSLT + "." + SCRIPT)
        || path.endsWith("." + REDUCER)
        || path.endsWith(EVENT_TO_COMMAND);
  }

  private static boolean isValidatorPath(final String path) {
    return path.endsWith("." + VALIDATOR) || path.endsWith(VALIDATE);
  }

  private static boolean isValidatorRef(final String path) {
    return path.equals(INCLUDE) || path.endsWith("." + REF);
  }

  private static Transformer jsltResolver(final Map<File, Pair<String, String>> imports) {
    return new Transformer(
        e -> isJsltPath(e.path) && isString(e.value),
        e ->
            Optional.of(
                new JsonEntry(
                    e.path, createValue(resolveJslt(asString(e.value).getString(), imports)))));
  }

  static String numberLines(final String s) {
    return zip(stream(s.split("\\n")).sequential(), rangeExclusive(1, MAX_VALUE))
        .map(pair -> rightAlign(valueOf(pair.second), 4) + " " + pair.first)
        .collect(joining("\n"));
  }

  private static String parameterPrefix(final String s) {
    return s != null ? s.substring(0, s.length() - 1) : "";
  }

  private static String parameterSuffix(final String s) {
    return s != null ? s.substring(1) : "";
  }

  private static JsonObject parameters(
      final JsonObject specification, final boolean runtime, final TopologyContext context) {
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

  private static Transformer replaceParameters(final JsonObject parameters) {
    return Optional.of(parameters)
        .filter(pars -> !pars.isEmpty())
        .map(Common::replaceParametersTransformer)
        .orElseGet(Transform::nopTransformer);
  }

  private static Transformer replaceParametersTransformer(final JsonObject parameters) {
    return replaceParametersObject(parameters).thenApply(replaceParametersArray(parameters));
  }

  private static JsonValue replaceParameters(final String s, final JsonObject parameters) {
    return Optional.of(ENV_PATTERN.matcher(s))
        .filter(Matcher::matches)
        .map(
            matcher ->
                getValue(parameters, "/" + matcher.group(1))
                    .map(v -> hasConfigurationParameters(v) ? createValue(s) : v)
                    .orElseGet(() -> createValue("")))
        .orElseGet(() -> createValue(replaceParametersString(s, parameters)));
  }

  private static JsonArray replaceParameters(final JsonArray array, final JsonObject parameters) {
    return array.stream()
        .reduce(
            createArrayBuilder(),
            (b, v) ->
                b.add(hasParameter(v) ? replaceParameters(asString(v).getString(), parameters) : v),
            (b1, b2) -> b1)
        .build();
  }

  private static Transformer replaceParametersArray(final JsonObject parameters) {
    return new Transformer(
        e -> isArray(e.value),
        e ->
            Optional.of(
                new JsonEntry(e.path, replaceParameters(e.value.asJsonArray(), parameters))));
  }

  private static Transformer replaceParametersObject(final JsonObject parameters) {
    return new Transformer(
        e -> hasParameter(e.path) || hasParameter(e.value),
        e ->
            Optional.of(
                new JsonEntry(
                    hasParameter(e.path) ? replaceParametersString(e.path, parameters) : e.path,
                    hasParameter(e.value)
                        ? replaceParameters(asString(e.value).getString(), parameters)
                        : e.value)));
  }

  private static String replaceParametersString(final String s, final JsonObject parameters) {
    return replaceAll(
        s,
        ENV_PATTERN_STRING,
        matcher ->
            ofNullable(parameters.get(matcher.group(2)))
                .map(value -> isString(value) ? asString(value).getString() : string(value))
                .map(
                    value ->
                        parameterPrefix(matcher.group(1))
                            + value
                            + parameterSuffix(matcher.group(3)))
                .orElse(""));
  }

  private static String resolveFile(
      final File baseDirectory, final String path, final JsonObject parameters) {
    return Optional.of(path)
        .filter(p -> !p.startsWith(RESOURCE))
        .flatMap(
            p ->
                net.pincette.util.Util.resolveFile(
                    baseDirectory, replaceParametersString(p, parameters)))
        .map(File::getAbsolutePath)
        .filter(p -> new File(p).exists())
        .orElse(path);
  }

  private static String resolveJslt(
      final String jslt, final Map<File, Pair<String, String>> imports) {
    return Optional.of(jslt)
        .filter(j -> !j.startsWith(RESOURCE))
        .map(File::new)
        .filter(File::exists)
        .map(file -> "// " + file.getName() + "\n" + resolveJsltImports(file, imports))
        .orElse(jslt);
  }

  private static String resolveJsltImport(
      final String line,
      final String imp,
      final File jslt,
      final Map<File, Pair<String, String>> imports) {
    final var imported =
        net.pincette.util.Util.resolveFile(baseDirectory(jslt, null), imp).orElse(null);
    final var resolved = resolveJsltImports(imported, imports);

    return line.replace(
        imp,
        imports.compute(imported, (k, v) -> v == null ? pair(randomUUID().toString(), resolved) : v)
            .first);
  }

  private static String resolveJsltImports(
      final File jslt, final Map<File, Pair<String, String>> imports) {
    return tryToGetRethrow(() -> lines(jslt.toPath(), UTF_8))
        .orElseGet(Stream::empty)
        .map(
            line ->
                getImport(line)
                    .map(imp -> resolveJsltImport(line, imp, jslt, imports))
                    .orElse(line))
        .collect(joining("\n"));
  }

  private static Collection<Pair<String, JsonObject>> resolveJsltInValidatorImports(
      final Collection<Pair<String, JsonObject>> validatorImports,
      final Map<File, Pair<String, String>> jsltImports) {
    return validatorImports.stream()
        .map(pair -> pair(pair.first, transform(pair.second, jsltResolver(jsltImports))))
        .collect(toList());
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

  private static String rightAlign(final String s, final int size) {
    final var padding = new char[max(size - s.length(), 0)];

    fill(padding, ' ');

    return new String(padding) + s;
  }

  private static Transformer stageFileResolver(
      final File baseDirectory, final JsonObject parameters) {
    return new Transformer(
        e -> (isJsltPath(e.path) && isString(e.value)) || isValidatorPath(e.path),
        e ->
            stringValue(e.value)
                .map(
                    path ->
                        new JsonEntry(
                            e.path, createValue(resolveFile(baseDirectory, path, parameters)))));
  }

  static JsonObjectBuilder transformFieldNames(
      final JsonObject json, final UnaryOperator<String> op) {
    return json.entrySet().stream()
        .map(e -> pair(op.apply(e.getKey()), transformFieldNames(e.getValue(), op)))
        .reduce(createObjectBuilder(), (b, pair) -> b.add(pair.first, pair.second), (b1, b2) -> b1);
  }

  static JsonArrayBuilder transformFieldNames(
      final JsonArray json, final UnaryOperator<String> op) {
    return json.stream()
        .map(v -> transformFieldNames(v, op))
        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1);
  }

  static JsonValue transformFieldNames(final JsonValue json, final UnaryOperator<String> op) {
    switch (json.getValueType()) {
      case OBJECT:
        return transformFieldNames(json.asJsonObject(), op).build();
      case ARRAY:
        return transformFieldNames(json.asJsonArray(), op).build();
      default:
        return json;
    }
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

  private interface Expander extends Fn<JsonObject, Fn<File, Fn<JsonObject, JsonObject>>> {}
}

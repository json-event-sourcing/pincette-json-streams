package net.pincette.json.streams;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.max;
import static java.lang.String.valueOf;
import static java.lang.System.exit;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.lines;
import static java.util.Arrays.fill;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.joining;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.copy;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.getArray;
import static net.pincette.json.JsonUtil.getObjects;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.Transform.transform;
import static net.pincette.json.Transform.transformBuilder;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.zip;
import static net.pincette.util.Util.replaceAll;
import static net.pincette.util.Util.tryToGet;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetSilent;
import static net.pincette.util.Util.tryToGetWithRethrow;

import com.typesafe.config.Config;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
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
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import net.pincette.function.Fn;
import net.pincette.function.FunctionWithException;
import net.pincette.function.SupplierWithException;
import net.pincette.json.JsonUtil;
import net.pincette.json.Transform;
import net.pincette.json.Transform.JsonEntry;
import net.pincette.json.Transform.Transformer;
import net.pincette.mongo.Validator;
import net.pincette.util.Builder;
import net.pincette.util.Pair;

class Common {
  static final String AGGREGATE = "aggregate";
  static final String AGGREGATE_TYPE = "aggregateType";
  static final String APPLICATION_FIELD = "application";
  static final String COMMANDS = "commands";
  static final String DESTINATION_TYPE = "destinationType";
  static final String DESTINATIONS = "destinations";
  static final String DOLLAR = "_dollar_";
  static final String DOT = "_dot_";
  static final String ENVIRONMENT = "environment";
  static final String EVENT_TO_COMMAND = "eventToCommand";
  static final String FILTER = "filter";
  static final String FROM_STREAM = "fromStream";
  static final String FROM_STREAMS = "fromStreams";
  static final String FROM_TOPIC = "fromTopic";
  static final String FROM_TOPICS = "fromTopics";
  static final String JOIN = "join";
  static final String JSLT_IMPORTS = "jsltImports";
  static final String LEFT = "left";
  static final String MERGE = "merge";
  static final String NAME = "name";
  static final String ON = "on";
  static final String PARTS = "parts";
  static final String PIPELINE = "pipeline";
  static final String REACTOR = "reactor";
  static final String REDUCER = "reducer";
  static final String RIGHT = "right";
  static final String SLASH = "_slash_";
  static final String SOURCE_TYPE = "sourceType";
  static final String STREAM = "stream";
  static final Set<String> STREAM_TYPES = set(JOIN, MERGE, STREAM);
  static final String TYPE = "type";
  static final String VALIDATE = "$validate";
  static final String VALIDATOR = "validator";
  static final String VERSION = "version";
  static final String WINDOW = "window";
  private static final String CONFIG_PREFIX = "config:";
  private static final String ENV = "ENV";
  private static final Pattern ENV_PATTERN = compile("\\$\\{(\\w+)}");
  private static final String JSLT = "$jslt";
  private static final Pattern JSLT_IMPORT = compile("^.*import[ \t]+\"([^\"]+)\"" + ".*$");
  private static final String MACROS = "macros";
  private static final String MONGODB_COLLECTION = "mongodb.collection";
  private static final String PARAMETERS = "parameters";
  private static final String RESOURCE = "resource:";
  private static final String SCRIPT = "script";

  private Common() {}

  private static File baseDirectory(final File file, final String path) {
    return tryToGetRethrow(() -> file.getCanonicalFile().getParentFile())
        .map(parent -> path != null ? new File(parent, path).getParentFile() : parent)
        .orElse(null);
  }

  static JsonObject build(
      final JsonObject specification,
      final boolean runtime,
      final TopologyContext topologyContext) {
    final Map<File, Pair<String, String>> imports = new HashMap<>();
    final JsonObject parameters = parameters(specification, runtime, topologyContext);

    return transformBuilder(
            topologyContext.baseDirectory != null
                ? expandApplication(specification, topologyContext.baseDirectory, parameters)
                : specification,
            createTransformer(imports, parameters, topologyContext.validators))
        .add(JSLT_IMPORTS, createImports(specification, imports.values()))
        .add(ID, specification.getString(APPLICATION_FIELD))
        .build();
  }

  private static JsonObjectBuilder createImports(
      final JsonObject specification, final Collection<Pair<String, String>> imports) {
    return imports.stream()
        .reduce(
            ofNullable(specification.getJsonObject(JSLT_IMPORTS))
                .map(JsonUtil::createObjectBuilder)
                .orElseGet(JsonUtil::createObjectBuilder),
            (b, p) -> b.add(p.first, p.second),
            (b1, b2) -> b1);
  }

  static TopologyContext createTopologyContext(
      final JsonObject specification,
      final File topFile,
      final String topologyPath,
      final Context context) {
    final TopologyContext topologyContext = new TopologyContext(context);

    topologyContext.application = specification.getString(APPLICATION_FIELD);
    topologyContext.baseDirectory = topFile != null ? baseDirectory(topFile, topologyPath) : null;

    return topologyContext;
  }

  private static Transformer createTransformer(
      final Map<File, Pair<String, String>> imports,
      final JsonObject parameters,
      final Validator validators) {
    return replaceParameters(parameters)
        .thenApply(validatorResolver(validators)) // JSLT filenames will be expanded.
        .thenApply(jsltResolver(imports))
        .thenApply(deleteMacros());
  }

  private static Transformer deleteMacros() {
    return new Transformer(e -> e.path.endsWith(MACROS), e -> Optional.empty());
  }

  private static JsonObject expandAggregate(
      final JsonObject aggregate, final File baseDirectory, final JsonObject parameters) {
    return createObjectBuilder(aggregate)
        .add(
            COMMANDS,
            from(
                getObjects(aggregate, COMMANDS)
                    .map(JsonValue::asJsonObject)
                    .map(command -> expandCommand(command, baseDirectory, parameters))))
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

  private static JsonValue getConfigValue(final JsonValue value, final Config config) {
    return createValue(
        config.getValue(asString(value).getString().substring(CONFIG_PREFIX.length())).unwrapped());
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
            file -> read(file, Common::readSequenceParts).map(p -> pair(p, file.getParentFile())));
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
    return ENV_PATTERN.matcher(s).find();
  }

  private static JsonObject injectConfiguration(final JsonObject parameters, final Config config) {
    return transform(
        parameters,
        new Transformer(
            e -> isConfigRef(e.value),
            e -> Optional.of(new JsonEntry(e.path, getConfigValue(e.value, config)))));
  }

  private static boolean isConfigRef(final JsonValue value) {
    return isString(value) && asString(value).getString().startsWith(CONFIG_PREFIX);
  }

  private static boolean isJsltPath(final String path) {
    return path.endsWith(JSLT)
        || path.endsWith(JSLT + "." + SCRIPT)
        || path.endsWith(COMMANDS + "." + REDUCER)
        || path.endsWith(EVENT_TO_COMMAND);
  }

  private static boolean isValidatorPath(final String path) {
    return path.endsWith(COMMANDS + "." + VALIDATOR) || path.endsWith(VALIDATE);
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
                                    : removeConfiguration(parameters))
                        .orElseGet(JsonUtil::emptyObject)))
        .updateIf(() -> ofNullable(context.context.environment), (b, e) -> b.add(ENV, e))
        .build()
        .build();
  }

  private static <T> T read(final File file, final FunctionWithException<JsonReader, T> reader) {
    return tryToGetWithRethrow(() -> createReader(new FileInputStream(file)), reader).orElse(null);
  }

  private static JsonArray readArray(final File file) {
    return read(file, JsonReader::readArray);
  }

  static JsonObject readObject(final String path, final File baseDirectory) {
    return tryToGetWithRethrow(
            () -> createReader(getInputStream(path, baseDirectory)), JsonReader::readObject)
        .orElse(null);
  }

  static JsonObject readObject(final File file) {
    return read(file, JsonReader::readObject);
  }

  private static Stream<JsonValue> readSequenceParts(final JsonReader reader) {
    final JsonStructure result = reader.read();

    return isArray(result) ? result.asJsonArray().stream() : Stream.of(result);
  }

  static Stream<Pair<JsonObject, String>> readTopologies(final File file) {
    return tryToGetWithRethrow(
            () -> createReader(new FileInputStream(file)),
            reader ->
                readTopologies(reader)
                    .filter(value -> isObject(value) || isString(value))
                    .map(
                        value ->
                            isObject(value)
                                ? pair(value.asJsonObject(), (String) null)
                                : readTopology(asString(value).getString(), file)))
        .orElseGet(Stream::empty);
  }

  private static Stream<JsonValue> readTopologies(final JsonReader reader) {
    final JsonStructure s = reader.read();

    return isArray(s) ? s.asJsonArray().stream() : Stream.of(s.asJsonObject());
  }

  private static Pair<JsonObject, String> readTopology(final String path, final File file) {
    return pair(readObject(path, baseDirectory(file, null)), path);
  }

  private static JsonObject removeConfiguration(final JsonObject parameters) {
    return copy(
            parameters,
            createObjectBuilder(),
            field -> !hasConfigurationParameters(parameters.get(field)))
        .build();
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
            matcher -> getValue(parameters, "/" + matcher.group(1)).orElseGet(() -> createValue(s)))
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
        ENV_PATTERN,
        matcher ->
            ofNullable(parameters.get(matcher.group(1)))
                .map(value -> isString(value) ? asString(value).getString() : string(value))
                .orElse(s));
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
    final File imported =
        net.pincette.util.Util.resolveFile(baseDirectory(jslt, null), imp).orElse(null);
    final String resolved = resolveJsltImports(imported, imports);

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

  private static String rightAlign(final String s, final int size) {
    final char[] padding = new char[max(size - s.length(), 0)];

    fill(padding, ' ');

    return new String(padding) + s;
  }

  private static Transformer stageFileResolver(
      final File baseDirectory, final JsonObject parameters) {
    return new Transformer(
        e -> (isJsltPath(e.path) && isString(e.value)) || isValidatorPath(e.path),
        e ->
            Optional.of(e.value)
                .filter(JsonUtil::isString)
                .map(JsonUtil::asString)
                .map(JsonString::getString)
                .map(
                    path ->
                        new JsonEntry(
                            e.path, createValue(resolveFile(baseDirectory, path, parameters)))));
  }

  static JsonObject transformFieldNames(final JsonObject json, final UnaryOperator<String> op) {
    return json.entrySet().stream()
        .map(e -> pair(op.apply(e.getKey()), transformFieldNames(e.getValue(), op)))
        .reduce(createObjectBuilder(), (b, pair) -> b.add(pair.first, pair.second), (b1, b2) -> b1)
        .build();
  }

  static JsonArray transformFieldNames(final JsonArray json, final UnaryOperator<String> op) {
    return json.stream()
        .map(v -> transformFieldNames(v, op))
        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
        .build();
  }

  static JsonValue transformFieldNames(final JsonValue json, final UnaryOperator<String> op) {
    switch (json.getValueType()) {
      case OBJECT:
        return transformFieldNames(json.asJsonObject(), op);
      case ARRAY:
        return transformFieldNames(json.asJsonArray(), op);
      default:
        return json;
    }
  }

  private static Transformer validatorResolver(final Validator validators) {
    return new Transformer(
        e -> isValidatorPath(e.path) && isString(e.value),
        e ->
            Optional.of(asString(e.value).getString())
                .map(File::new)
                .map(
                    file ->
                        new JsonEntry(
                            e.path, validators.resolve(readObject(file), file.getParentFile()))));
  }

  private interface Expander extends Fn<JsonObject, Fn<File, Fn<JsonObject, JsonObject>>> {}
}

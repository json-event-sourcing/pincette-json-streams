package net.pincette.json.streams;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.lines;
import static java.util.Optional.ofNullable;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.concat;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.copy;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.Transform.transform;
import static net.pincette.json.Transform.transformBuilder;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.replaceAll;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetSilent;
import static net.pincette.util.Util.tryToGetWithRethrow;

import com.typesafe.config.Config;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
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
import net.pincette.json.JsonUtil;
import net.pincette.json.Transform;
import net.pincette.json.Transform.JsonEntry;
import net.pincette.json.Transform.Transformer;
import net.pincette.util.Builder;

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
  static final String VALIDATOR = "validator";
  static final String VERSION = "version";
  static final String WINDOW = "window";
  private static final String CONFIG_PREFIX = "config:";
  private static final String ENV = "ENV";
  private static final Pattern ENV_PATTERN = compile("\\$\\{(\\w+)}");
  private static final String JSLT = "$jslt";
  private static final Pattern JSLT_IMPORT = compile("^.*import[ \t]+\"([^\"]+)\"" + ".*$");
  private static final String MONGODB_COLLECTION = "mongodb.collection";
  private static final String PARAMETERS = "parameters";
  private static final String RESOURCE = "resource:";

  private Common() {}

  static JsonObject build(
      final JsonObject specification,
      final boolean runtime,
      final TopologyContext topologyContext) {
    final JsonObjectBuilder imports =
        ofNullable(specification.getJsonObject(JSLT_IMPORTS))
            .map(JsonUtil::createObjectBuilder)
            .orElseGet(JsonUtil::createObjectBuilder);
    final Transformer parameters = replaceParameters(specification, runtime, topologyContext);

    return transformBuilder(
            specification,
            parameters
                .thenApply(sequenceResolver(topologyContext.baseDirectory, parameters))
                .thenApply(jsltResolver(imports, topologyContext.baseDirectory))
                .thenApply(validatorResolver(topologyContext))
                .thenApply(parameters)
                .thenApply(keepConfigParameters()))
        .add(JSLT_IMPORTS, imports)
        .add(ID, specification.getString(APPLICATION_FIELD))
        .build();
  }

  private static JsonObject configParameters(
      final JsonObject parameters, final boolean runtime, final TopologyContext context) {
    return runtime
        ? injectConfiguration(parameters, context.context.config)
        : copy(parameters, createObjectBuilder(), field -> !isConfigRef(parameters.get(field)))
            .build();
  }

  static TopologyContext createTopologyContext(
      final JsonObject specification, final File baseDirectory, final Context context) {
    final TopologyContext topologyContext = new TopologyContext(context);

    topologyContext.application = specification.getString(APPLICATION_FIELD);
    topologyContext.baseDirectory = baseDirectory;

    return topologyContext;
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

  static String getTopologyCollection(final String collection, final Context context) {
    return collection != null
        ? collection
        : tryToGetSilent(() -> context.config.getString(MONGODB_COLLECTION)).orElse(null);
  }

  private static boolean hasParameter(final JsonValue value) {
    return isString(value) && hasParameter(asString(value).getString());
  }

  private static boolean hasParameter(final String s) {
    return ENV_PATTERN.matcher(s).find();
  }

  private static String importPath(final File jslt, final File baseDirectory) {
    return removeLeadingSlash(
        jslt.getAbsoluteFile()
            .getAbsolutePath()
            .substring(baseDirectory.getAbsoluteFile().getAbsolutePath().length())
            .replace('\\', '/'));
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
        || path.endsWith(COMMANDS + "." + REDUCER)
        || path.endsWith(EVENT_TO_COMMAND);
  }

  private static boolean isSequencePath(final String path) {
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

  private static Transformer keepConfigParameters() {
    return new Transformer(
        e -> e.path.equals("/" + PARAMETERS),
        e -> Optional.of(new JsonEntry(e.path, keepConfigParameters(e.value.asJsonObject()))));
  }

  private static JsonObject keepConfigParameters(final JsonObject parameters) {
    return copy(parameters, createObjectBuilder(), field -> isConfigRef(parameters.get(field)))
        .build();
  }

  private static Stream<JsonValue> loadSequence(
      final String path, final File baseDirectory, final Transformer replaceParameters) {
    return tryToGetWithRethrow(
            () -> createReader(getInputStream(path, baseDirectory)),
            reader ->
                resolve(
                    readSequenceParts(reader, replaceParameters),
                    newBaseDirectory(path, baseDirectory),
                    replaceParameters))
        .orElse(null);
  }

  private static File newBaseDirectory(final String path, final File baseDirectory) {
    return path.startsWith(RESOURCE)
        ? baseDirectory
        : new File(baseDirectory, path).getParentFile();
  }

  private static JsonObject parameters(
      final JsonObject specification, final boolean runtime, final TopologyContext context) {
    return Builder.create(
            () ->
                createObjectBuilder(
                    ofNullable(specification.getJsonObject(PARAMETERS))
                        .map(parameters -> configParameters(parameters, runtime, context))
                        .orElseGet(JsonUtil::emptyObject)))
        .updateIf(() -> ofNullable(context.context.environment), (b, e) -> b.add(ENV, e))
        .build()
        .build();
  }

  private static JsonArray readArray(final String path, final File baseDirectory) {
    return tryToGetWithRethrow(
            () -> createReader(getInputStream(path, baseDirectory)), JsonReader::readArray)
        .orElse(null);
  }

  static JsonObject readObject(final String path, final File baseDirectory) {
    return tryToGetWithRethrow(
            () -> createReader(getInputStream(path, baseDirectory)), JsonReader::readObject)
        .orElse(null);
  }

  private static Stream<JsonValue> readSequenceParts(
      final JsonReader reader, final Transformer replaceParameters) {
    final JsonStructure result = reader.read();

    return isArray(result)
        ? transform(result.asJsonArray(), replaceParameters).stream()
        : Stream.of(result);
  }

  static Stream<JsonObject> readTopologies(final File file) {
    return tryToGetWithRethrow(
            () -> createReader(new FileInputStream(file)),
            reader ->
                readTopologies(reader)
                    .filter(value -> isObject(value) || isString(value))
                    .map(
                        value ->
                            isObject(value)
                                ? value.asJsonObject()
                                : readObject(
                                    asString(value).getString(),
                                    file.getAbsoluteFile().getParentFile())))
        .orElseGet(Stream::empty);
  }

  private static Stream<JsonValue> readTopologies(final JsonReader reader) {
    final JsonStructure s = reader.read();

    return isArray(s) ? s.asJsonArray().stream() : Stream.of(s.asJsonObject());
  }

  private static String removeLeadingSlash(final String path) {
    return path.startsWith("/") ? path.substring(1) : path;
  }

  private static Transformer replaceParameters(
      final JsonObject specification, final boolean runtime, final TopologyContext context) {
    return Optional.of(parameters(specification, runtime, context))
        .filter(pars -> !pars.isEmpty())
        .map(Common::replaceParameters)
        .orElseGet(Transform::nopTransformer);
  }

  private static Transformer replaceParameters(final JsonObject parameters) {
    return replaceParametersObject(parameters).thenApply(replaceParametersArray(parameters));
  }

  private static JsonValue replaceParameters(final String s, final JsonObject parameters) {
    return Optional.of(ENV_PATTERN.matcher(s))
        .filter(Matcher::matches)
        .map(
            matcher ->
                getValue(parameters, "/" + matcher.group(1)).orElseGet(() -> createValue("")))
        .orElseGet(() -> createValue(replaceParametersString(s, parameters)));
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
                .orElse(""));
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

  private static Stream<JsonValue> resolve(
      final Stream<JsonValue> sequence,
      final File baseDirectory,
      final Transformer replaceParameters) {
    return sequence
        .filter(element -> isObject(element) || isString(element))
        .flatMap(
            element ->
                isObject(element)
                    ? Stream.of(element)
                    : loadSequence(
                        asString(element).getString(), baseDirectory, replaceParameters));
  }

  private static String resolveJslt(
      final String jslt, final JsonObjectBuilder imports, final File baseDirectory) {
    return jslt.startsWith(RESOURCE)
        ? jslt
        : Optional.of(new File(baseDirectory, jslt))
            .filter(File::exists)
            .map(
                file ->
                    "// "
                        + importPath(file, baseDirectory)
                        + "\n"
                        + resolveJsltImports(file, imports, baseDirectory))
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
    return tryToGetRethrow(
            () ->
                concat(
                    Stream.of("// " + importPath(jslt, baseDirectory)),
                    lines(jslt.toPath(), UTF_8)))
        .orElseGet(Stream::empty)
        .map(
            line ->
                getImport(line)
                    .map(imp -> resolveJsltImport(line, imp, jslt, imports, baseDirectory))
                    .orElse(line))
        .collect(joining("\n"));
  }

  private static Transformer sequenceResolver(
      final File baseDirectory, final Transformer replaceParameters) {
    return sequenceResolverFile(baseDirectory, replaceParameters)
        .thenApply(sequenceResolverArray(baseDirectory, replaceParameters));
  }

  private static Transformer sequenceResolverArray(
      final File baseDirectory, final Transformer replaceParameters) {
    return new Transformer(
        e -> isSequencePath(e.path) && isArray(e.value),
        e ->
            Optional.of(
                new JsonEntry(
                    e.path,
                    from(
                        resolve(
                            e.value.asJsonArray().stream(), baseDirectory, replaceParameters)))));
  }

  private static Transformer sequenceResolverFile(
      final File baseDirectory, final Transformer replaceParameters) {
    return new Transformer(
            e -> isSequencePath(e.path) && isString(e.value),
            e ->
                Optional.of(
                    new JsonEntry(e.path, readArray(asString(e.value).getString(), baseDirectory))))
        .thenApply(replaceParameters);
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

  private static Transformer validatorResolver(final TopologyContext context) {
    return validatorResolverString(context).thenApply(validatorResolverObject(context));
  }

  private static Transformer validatorResolverObject(final TopologyContext context) {
    return new Transformer(
        e -> isValidatorPath(e.path) && isObject(e.value),
        e ->
            Optional.of(
                new JsonEntry(
                    e.path,
                    context.validators.resolve(e.value.asJsonObject(), context.baseDirectory))));
  }

  private static Transformer validatorResolverString(final TopologyContext context) {
    return new Transformer(
        e -> isValidatorPath(e.path) && isString(e.value),
        e ->
            Optional.of(
                new JsonEntry(
                    e.path, readObject(asString(e.value).getString(), context.baseDirectory))));
  }
}
